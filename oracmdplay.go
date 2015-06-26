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
	"strconv"
	"sync"
)

type commandPlayer struct {
	srcChan chan command
	//conn    *oracle.Connection
}

type commandPlayers struct {
	sync.Mutex
	players      map[string]commandPlayer
	sigChan      chan uint64
	finishedCmds map[uint64]bool
}

func newCommandPlayers() commandPlayers {
	players := commandPlayers{players: make(map[string]commandPlayer), finishedCmds: make(map[uint64]bool), sigChan: make(chan uint64, 1000)}
	go func(p commandPlayers) {
		for {
			select {
			case cmdID := <-p.sigChan:
				{
					p.finishedCmds[cmdID] = true
				}
			}
		}
	}(players)
	return players
}
func (cps *commandPlayers) runOp(cmd command, holderName string) {
	c := func(streamID string) chan command {
		cps.Lock()
		defer cps.Unlock()
		r, ok := cps.players[streamID]
		if !ok {
			r = commandPlayer{srcChan: make(chan command)}
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

												var vals []interface{}
												for _, p := range pval.Value {
													if pval.Type == "Float" && reflect.TypeOf(p).Name() == "string" {
														fR, err := strconv.ParseFloat(p.(string), 64)
														if err != nil {
															panic(err)
														}
														vals = append(vals, fR)
													} else {
														vals = append(vals, p)
													}
												}
												err = v.SetValue(0, vals)
												if err != nil {
													panic(err)
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
							cps.sigChan <- cmd.CmdID
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
	ops := commandsLoad(fileName)
	p := newCommandPlayers()
	for _, op := range ops {
		p.runOp(op, fileName+".load")
	}

}

func commandsLoad(fileName string) []command {
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

	var res []command
	for {
		dec := gob.NewDecoder(&buf)
		var v command
		err := dec.Decode(&v)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		res = append(res, v)
	}
	return res
}

func commandsDump(fileName string) {
	cmds := commandsLoad(fileName)
	var buffer bytes.Buffer
	defer buffer.Reset()
	for _, cmd := range cmds {
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
