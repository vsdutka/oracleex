// oracmdhold
package oracleex

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"gopkg.in/goracle.v1/oracle"
	"io"
	"io/ioutil"
	"os"
	//"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"time"
)

type commandForPlaying struct {
	command
	wg         sync.WaitGroup
	waiters    []int // The list of positions of the commands expecting this
	relativeBg time.Duration
	cmdPos     int
}

type commandPlayer struct {
	srcChan chan commandForPlaying
	//conn    *oracle.Connection
}
type commandSlice []commandForPlaying

func (p commandSlice) Len() int {
	return len(p)
}

func (p commandSlice) Less(i, j int) bool {
	return p[i].ClientBg.Before(p[j].ClientBg)
}

func (p commandSlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

type commandPlayers struct {
	sync.Mutex
	players map[string]commandPlayer
	sigChan chan []int //Список позиций комманд, которым нужно послать сигнал о завершении их предшественника
	cmds    commandSlice
	startDt time.Time
}

func newCommandPlayers(fileName string) commandPlayers {
	players := commandPlayers{
		players: make(map[string]commandPlayer),
		sigChan: make(chan []int, 1000),
		cmds:    make([]commandForPlaying, 0),
	}
	go func(p *commandPlayers) {
		for {
			select {
			case cmdPos := <-p.sigChan:
				{
					func() {
						p.Lock()
						defer p.Unlock()
						for _, pos := range cmdPos {
							p.cmds[pos].wg.Done()
						}
					}()
				}
			}
		}
	}(&players)
	players.commandsLoad(fileName)
	return players
}

func (cps *commandPlayers) commandsLoad(fileName string) {
	if fileName == "" {
		return
	}
	buf := func() bytes.Buffer {
		var r bytes.Buffer

		f, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		_, err = r.ReadFrom(f)
		if err != nil {
			panic(err)
		}
		return r
	}()
	//Чтение даты и времени начала сбора информации по командам
	var startDt time.Time
	dec := gob.NewDecoder(&buf)
	err := dec.Decode(&startDt)
	if err != nil {
		panic(err)
	}
	cps.startDt = startDt
	k := 1
	for {
		k++
		if k > 1000 {
			break
		}
		dec := gob.NewDecoder(&buf)
		var v commandForPlaying
		err := dec.Decode(&v)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		v.waiters = make([]int, 0)
		v.relativeBg = v.ClientBg.Sub(startDt)
		cps.cmds = append(cps.cmds, v)
	}
	// Заполнение структур waiters
	sort.Sort(cps.cmds)
	for i, _ := range cps.cmds {
		cps.cmds[i].cmdPos = i
		prevCmds := make(map[string]int)
		for j := 0; j < i; j++ {
			if cps.cmds[i].ClientBg.Sub(cps.cmds[j].ClientFn) >= 0 {
				prevCmds[cps.cmds[j].StreamID] = j
			}
		}

		for _, pos := range prevCmds {
			cps.cmds[pos].waiters = append(cps.cmds[pos].waiters, i)
			cps.cmds[i].wg.Add(1)
		}
	}
}

func (cps *commandPlayers) play(holderFileName string) {
	startDt := time.Now()
	for i, _ := range cps.cmds {
		// Ожидаем завершения всех комманд, которые должны закончиться до начала этой
		cps.cmds[i].wg.Wait()
		// Высчитывает момент, когда нужно запустить эту команду
		cmdWaitBeforeStart := startDt.Add(cps.cmds[i].relativeBg).Sub(time.Now())
		<-time.After(cmdWaitBeforeStart)
		cps.runCmd(cps.cmds[i], holderFileName)
	}

}

func (cps *commandPlayers) runCmd(cmd commandForPlaying, holderName string) {
	c := func(streamID string) chan commandForPlaying {
		cps.Lock()
		defer cps.Unlock()
		r, ok := cps.players[streamID]
		if !ok {
			r = commandPlayer{srcChan: make(chan commandForPlaying)}
			cps.players[streamID] = r
			go func() {
				defer func() {
					cps.Lock()
					defer cps.Unlock()
					delete(cps.players, streamID)
				}()
				var conn *Connection
				var err error
				for {
					select {
					case cmd := <-r.srcChan:
						{
							switch cmd.CmdType {
							case cmLogin:
								{
									conn, err = NewConnection(cmd.UserName, cmd.UserPass, cmd.ConnStr, false, holderName, streamID)
									if err != nil {
										panic(err)
									}
								}
							case cmLogout:
								{
									if conn == nil {
										panic("Conn == nil")
									}
									err = conn.Close()
									if err != nil {
										// Если выходим с ошибкой, то в вызывающей процедуре будет вызван disconnect()
										return
									}
								}
							case cmExec:
								{
									func() {
										cur := conn.NewCursor()
										defer func() { cur.Close() }()

										params := make(map[string]interface{})
										for pname, pval := range cmd.SqlNamedParams {
											var varType *oracle.VariableType
											switch pval.Type {
											case "String":
												varType = oracle.StringVarType
											case "FixedChar":
												varType = oracle.FixedCharVarType
											case "Rowid":
												varType = oracle.RowidVarType
											case "Binary":
												varType = oracle.BinaryVarType
											case "long":
												varType = oracle.LongStringVarType
											case "long_raw":
												varType = oracle.LongBinaryVarType
											case "cursor":
												varType = oracle.CursorVarType
											case "Float":
												varType = oracle.FloatVarType
											case "NativeFloat":
												varType = oracle.NativeFloatVarType
											case "Int32":
												varType = oracle.Int32VarType
											case "Int64":
												varType = oracle.Int64VarType
											case "LongInteger":
												varType = oracle.LongIntegerVarType
											case "NumberAsString":
												varType = oracle.NumberAsStringVarType
											case "Boolean":
												varType = oracle.BooleanVarType
											case "DateTime":
												varType = oracle.DateTimeVarType
											case "ClobVar":
												varType = oracle.ClobVarType
											case "NClobVar":
												varType = oracle.NClobVarType
											case "BlobVar":
												varType = oracle.BlobVarType
											case "BFileVar":
												varType = oracle.BFileVarType
											default:
												{
													fmt.Println(pname, " ", pval.Type)
												}
											}
											v, err := cur.NewVariable(pval.Count, varType, pval.Size)
											if err != nil {
												panic(err)
											}
											if pval.IsArray {

												for i, p := range pval.Value {
													if pval.Type == "Float" && reflect.TypeOf(p).Name() == "string" {
														fR, err := strconv.ParseFloat(p.(string), 64)
														if err != nil {
															panic(err)
														}
														err = v.SetValue(uint(i), fR)
														if err != nil {
															//fmt.Printf("%s, %v, %v", pname, pval.Type, vals)
															panic(err)
														}
													} else {
														err = v.SetValue(uint(i), p)
														if err != nil {
															//fmt.Printf("%s, %v, %v", pname, pval.Type, vals)
															panic(err)
														}
													}
												}
												params[pname] = v
											} else {
												var vals interface{}
												for _, p := range pval.Value {
													if pval.Type == "Float" && reflect.TypeOf(p).Name() == "string" {
														fR, err := strconv.ParseFloat(p.(string), 64)
														if err != nil {
															panic(err)
														}
														vals = fR
													} else {
														vals = p
													}
												}
												err = v.SetValue(0, vals)
												if err != nil {
													//fmt.Printf("%s, %v, %v", pname, pval.Type, vals)
													panic(err)
												}
												params[pname] = v
											}
										}
										_ = cur.Execute(cmd.Sql, nil, params)

									}()
								}
							}
							//FIXME - run
							// Сигнализируем о завершении выполнения операции
							cps.sigChan <- cmd.waiters
							if cmd.CmdType == cmLogout {
								return
							}
						}

					}
				}
			}()
		}
		return r.srcChan
	}(cmd.StreamID)
	c <- cmd
}

func Play(fileName string) {
	//ops := commandsLoad(fileName)
	p := newCommandPlayers(fileName)
	p.play(fileName + ".load")
}

func commandsDump(fileName string) {
	p := newCommandPlayers(fileName)
	var buffer bytes.Buffer
	defer buffer.Reset()
	for _, cmd := range p.cmds {
		buffer.WriteString(fmt.Sprintln("StreamID    = ", cmd.StreamID))
		buffer.WriteString(fmt.Sprintln("OperationID = ", cmd.CmdID))
		buffer.WriteString(fmt.Sprintln("Type        = ", cmdType(cmd.CmdType)))
		buffer.WriteString(fmt.Sprintln("UserName    = ", cmd.UserName))
		buffer.WriteString(fmt.Sprintln("UserPass    = ", cmd.UserPass))
		buffer.WriteString(fmt.Sprintln("ConnStr     = ", cmd.ConnStr))

		buffer.WriteString(fmt.Sprintln("ClientBg    = ", cmd.ClientBg))
		buffer.WriteString(fmt.Sprintln("ClientFn    = ", cmd.ClientFn))
		buffer.WriteString(fmt.Sprintln("ServerBg    = ", cmd.ServerBg))
		buffer.WriteString(fmt.Sprintln("ServerFn    = ", cmd.ServerFn))
		buffer.WriteString(fmt.Sprintln("ServerFnScn = ", cmd.ServerFnScn))
		buffer.WriteString(fmt.Sprintln("Success     = ", cmd.Success))
		buffer.WriteString(fmt.Sprintln("ErrorMsg    = ", cmd.ErrorMsg))
		buffer.WriteString(fmt.Sprintln("Stm         = ", cmd.Sql))
		buffer.WriteString("------------ params------------\n")
		for n, p := range cmd.SqlNamedParams {
			buffer.WriteString(fmt.Sprintf("  ------------ param ------------\n"))
			buffer.WriteString(fmt.Sprintf("  Param Name     = %s\n", n))
			buffer.WriteString(fmt.Sprintf("  Param Type     = %v\n", p.Type))
			buffer.WriteString(fmt.Sprintf("  Param IsArray  = %v\n", p.IsArray))
			buffer.WriteString(fmt.Sprintf("  Param NumElem  = %d\n", p.Count))
			buffer.WriteString(fmt.Sprintf("  Param Size     = %d\n", p.Size))

			if p.Value != nil {
				if t := reflect.TypeOf(p.Value).Kind(); t == reflect.Slice {
					for i, pv := range p.Value {
						buffer.WriteString(fmt.Sprintf("  Param Value(%d) = %v (%T)\n", i, pv, pv))
					}
				} else {
					buffer.WriteString(fmt.Sprintf("  Param Value = %v\n", p.Value))
				}
			} else {
				buffer.WriteString(fmt.Sprintf("  Param Value = nil\n"))
			}

		}
		buffer.WriteString("============  End  ============\n")
	}
	ioutil.WriteFile(fileName+".dump", []byte(buffer.String()), 0644)
}

func cmdType(v uint) string {
	switch v {
	case cmLogin:
		return "Login"
	case cmLogout:
		return "Logout"
	case cmExec:
		return "Exec"
	default:
		return fmt.Sprintf("Unknown type: %d", v)
	}
}
