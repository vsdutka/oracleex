// oracmdhold
package oracleex

import (
	"encoding/gob"
	"fmt"
	//"github.com/xlab/closer"
	"gopkg.in/goracle.v1/oracle"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type commandHolderChannel struct {
	cmdChan  chan command
	exitChan chan bool
}
type commandHolder struct {
	channels map[string]commandHolderChannel
	wg       sync.WaitGroup
	wgSend   sync.WaitGroup
	sync.Mutex
}

var holder = commandHolder{channels: make(map[string]commandHolderChannel)}

var cmdCounter uint64

func init() {
	//closer.Bind(cleanup)
	gob.Register(time.Time{})
	gob.Register(0.5)
}

func cleanup() {
	holder.closeAll()
}

func (l *commandHolder) closeAll() {
	l.Lock()
	defer l.Unlock()
	// Дожидаемся получения и сохранения всех команд и только потом закрываем
	l.wgSend.Wait()
	for key, ch := range l.channels {
		ch.exitChan <- true
		delete(l.channels, key)
	}
	l.wg.Wait()
}

func (l *commandHolder) getHolderChannel(holderName string) chan command {
	l.Lock()
	defer l.Unlock()
	res, ok := l.channels[holderName]
	if !ok {
		res = commandHolderChannel{cmdChan: make(chan command, 10000), exitChan: make(chan bool)}
		l.channels[holderName] = res
		l.wg.Add(1)
		go func(holderName string, c commandHolderChannel) {
			defer l.wg.Done()

			dir, _ := filepath.Split(holderName)
			os.MkdirAll(dir, os.ModeDir)

			f, err := os.OpenFile(holderName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				panic(err)
			}
			defer f.Close()
			// Сохранение метки начала
			enc := gob.NewEncoder(f)
			startDt := time.Now()
			err = enc.Encode(&startDt)
			if err != nil {
				fmt.Println(err)
			}

			for {
				select {
				case cmd := <-c.cmdChan:
					{
						func() {
							enc := gob.NewEncoder(f)
							err = enc.Encode(&cmd)
							if err != nil {
								fmt.Println(err)
							}

						}()
						l.wgSend.Done()
					}
				case <-c.exitChan:
					{
						return
					}
				}
			}
		}(holderName, res)
	}
	return res.cmdChan
}

func (l *commandHolder) hold(holderName string, cmd command) {
	c := l.getHolderChannel(holderName)
	l.wgSend.Add(1)
	c <- cmd
}

func loginCmd(holderName, streamID, userName, userPass, connStr string, clientBg, clientFn time.Time, success bool, errm string) {
	loginCmdEx(holderName, streamID, atomic.AddUint64(&cmdCounter, 1), userName, userPass, connStr, clientBg, clientFn, success, errm)
}

func loginCmdEx(holderName, streamID string, cmdID uint64, userName, userPass, connStr string, clientBg, clientFn time.Time, success bool, errm string) {
	if holderName == "" {
		return
	}
	holder.hold(holderName,
		command{
			StreamID:    streamID,
			CmdID:       cmdID,
			CmdType:     cmLogin,
			UserName:    userName,
			UserPass:    userPass,
			ConnStr:     connStr,
			ClientBg:    clientBg,
			ClientFn:    clientFn,
			ServerBg:    clientBg,
			ServerFn:    clientFn,
			ServerFnScn: "",
			Success:     success,
			ErrorMsg:    errm,
		})
}

func logoutCmd(holderName, streamID string, clientBg, clientFn time.Time, success bool, errm string) {
	logoutCmdEx(holderName, streamID, atomic.AddUint64(&cmdCounter, 1), clientBg, clientFn, success, errm)
}
func logoutCmdEx(holderName, streamID string, cmdID uint64, clientBg, clientFn time.Time, success bool, errm string) {
	if holderName == "" {
		return
	}
	holder.hold(holderName,
		command{
			StreamID:    streamID,
			CmdID:       cmdID,
			CmdType:     cmLogout,
			ClientBg:    clientBg,
			ClientFn:    clientFn,
			ServerBg:    clientBg,
			ServerFn:    clientFn,
			ServerFnScn: "",
			Success:     success,
			ErrorMsg:    errm,
		})
}

func execCmd(holderName, streamID, stm string, namedParams map[string]interface{}, clientBg, clientFn, serverBg, serverFn time.Time, serverFnScn string, success bool, errm string) {
	execCmdEx(holderName, streamID, atomic.AddUint64(&cmdCounter, 1), stm, namedParams, clientBg, clientFn, serverBg, serverFn, serverFnScn, success, errm)
}

func execCmdEx(holderName, streamID string, cmdID uint64, stm string, namedParams map[string]interface{}, clientBg, clientFn, serverBg, serverFn time.Time, serverFnScn string, success bool, errm string) {
	if holderName == "" {
		return
	}

	lParams := make(map[string]sqlParam)
	for key, val := range namedParams {
		oraVar := val.(*oracle.Variable)
		oraVarType, _, _, _ := oracle.VarTypeByValue(oraVar)
		//fmt.Println(key, " ", oraVarType.Name, " size = ", oraVar.Size(), " numElements = ", oraVar.AllocatedElements(), " ", oraVar.ArrayLength())

		lParam := sqlParam{Type: oraVarType.Name, IsArray: oraVar.IsArray(),
			Count: oraVar.AllocatedElements(), Size: oraVar.Size(),
			Value: make([]interface{}, 0)}

		if !oraVar.IsArray() {
			paramValItem, err := oraVar.GetValue(0)
			if err == nil {
				ext, ok := paramValItem.(*oracle.ExternalLobVar)
				if !ok {
					lParam.Value = append(lParam.Value, paramValItem)
				}
				if ext != nil {
					size, err := ext.Size(false)
					if err != nil {
						size = 0
					}
					if size != 0 {
						buf, err := ext.ReadAll()
						if err == nil {
							lParam.Value = append(lParam.Value, buf)
						}
					}
				}
			}
		} else {
			for i := uint(0); i < oraVar.ArrayLength(); i++ {
				paramValItem, err := oraVar.GetValue(i)
				if err != nil {
					panic(err)
				}
				lParam.Value = append(lParam.Value, paramValItem)
			}
		}
		lParams[key] = lParam
	}

	holder.hold(holderName, command{
		StreamID:       streamID,
		CmdID:          cmdID,
		CmdType:        cmExec,
		Sql:            stm,
		SqlNamedParams: lParams,
		ClientBg:       clientBg,
		ClientFn:       clientFn,
		ServerBg:       serverBg,
		ServerFn:       serverFn,
		ServerFnScn:    serverFnScn,
		Success:        success,
		ErrorMsg:       errm,
	})
}

//func OracleOperationsLoad(fileName string) []Command {
//	f, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
//	if err != nil {
//		panic(err)
//	}
//	defer f.Close()
//	dec := gob.NewDecoder(f)

//	res := make([]Command, 0)
//	for {
//		var v Command
//		err := dec.Decode(&v)
//		if err != nil {
//			if err == io.EOF {
//				break
//			}
//			panic(err)
//		}
//		res = append(res, v)
//	}
//	fmt.Println("len - ", len(res))
//	return res
//}

//func OracleOperationsDump(fileName string) {
//	ops := OracleOperationsLoad(fileName)
//	var buffer bytes.Buffer
//	defer buffer.Reset()
//	for _, op := range ops {
//		fmt.Println("Dump - ", op.CmdID)
//		buffer.WriteString(fmt.Sprintln("StreamID    = ", op.StreamID))
//		buffer.WriteString(fmt.Sprintln("OperationID = ", op.CmdID))
//		buffer.WriteString(fmt.Sprintln("Type        = ", opType(op.CmdType)))
//		buffer.WriteString(fmt.Sprintln("UserName    = ", op.UserName))
//		buffer.WriteString(fmt.Sprintln("UserPass    = ", op.UserPass))
//		buffer.WriteString(fmt.Sprintln("ConnStr     = ", op.ConnStr))

//		buffer.WriteString(fmt.Sprintln("ClientBg    = ", op.ClientBg))
//		buffer.WriteString(fmt.Sprintln("ClientFn    = ", op.ClientFn))
//		buffer.WriteString(fmt.Sprintln("ServerBg    = ", op.ServerBg))
//		buffer.WriteString(fmt.Sprintln("ServerFn    = ", op.ServerFn))
//		buffer.WriteString(fmt.Sprintln("ServerFnScn = ", op.ServerFnScn))
//		buffer.WriteString(fmt.Sprintln("Success     = ", op.Success))
//		buffer.WriteString(fmt.Sprintln("ErrorMsg    = ", op.ErrorMsg))
//		buffer.WriteString(fmt.Sprintln("Stm         = ", op.Sql))
//		buffer.WriteString("------------ params------------\n")
//		for n, p := range op.SqlNamedParams {
//			buffer.WriteString(fmt.Sprintf("  ------------ param ------------\n"))
//			buffer.WriteString(fmt.Sprintf("  Param Name     = %s\n", n))
//			buffer.WriteString(fmt.Sprintf("  Param Type     = %v\n", p.Type.Name))
//			buffer.WriteString(fmt.Sprintf("  Param IsArray  = %v\n", p.IsArray))
//			buffer.WriteString(fmt.Sprintf("  Param NumElem  = %d\n", p.Count))
//			buffer.WriteString(fmt.Sprintf("  Param Size     = %d\n", p.Size))

//			if p.Value != nil {
//				if t := reflect.TypeOf(p.Value).Kind(); t == reflect.Slice {
//					for i, pv := range p.Value {
//						buffer.WriteString(fmt.Sprintf("  Param Value(%d) = %v\n", i, pv))
//					}
//				} else {
//					buffer.WriteString(fmt.Sprintf("  Param Value = %v\n", p.Value))
//				}
//			} else {
//				buffer.WriteString(fmt.Sprintf("  Param Value = nil\n"))
//			}

//		}
//		buffer.WriteString("============  End  ============\n")
//	}
//	ioutil.WriteFile(fileName+".dump", []byte(buffer.String()), 0644)
//}

//func opType(v uint) string {
//	switch v {
//	case cmLogin:
//		return "Login"
//	case cmLogout:
//		return "Logout"
//	case cmExec:
//		return "Exec"
//	default:
//		return fmt.Sprintf("Unknown type: %d", v)
//	}
//}
