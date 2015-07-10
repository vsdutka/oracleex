// oracmdplay_test
package oracleex

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"strconv"
	"testing"
	"time"
)

func dumpCsv(fileName string, p commandPlayers) {

	var buffer bytes.Buffer
	//defer buffer.Reset()
	buffer.WriteString("StreamID,CmdID,ClientBG,ClientFn,CmdType,User,Waiters\n")

	var startDt time.Time

	for i, cmd := range p.cmds {
		if i == 0 {
			startDt = cmd.ClientBg
		}
		s := ""
		for i, pos := range cmd.waiters {
			if i == 0 {
				s = strconv.Itoa(int(p.cmds[pos].CmdID))
			} else {
				s = s + "|" + strconv.Itoa(int(p.cmds[pos].CmdID))
			}
		}
		//fmt.Println(cmd.waiters)
		buffer.WriteString(fmt.Sprintf("%s,%d,%v,%v,%d,%s,%s\n", cmd.StreamID, cmd.CmdID, cmd.ClientBg.Sub(startDt).Nanoseconds()/1000, cmd.ClientFn.Sub(startDt).Nanoseconds()/1000, cmd.CmdType, cmd.UserName, s))
	}
	ioutil.WriteFile(fileName+".csv", []byte(buffer.String()), 0644)
}

func TestLoad(t *testing.T) {
	fname := "C:\\!Dev\\GOPATH\\src\\github.com\\vsdutka\\iplsgo\\log\\iPLSGo_10111\\asrolf\\load_running.gob"
	fmt.Println("Loading", time.Now())
	p := newCommandPlayers(fname)
	fmt.Println("Loaded", time.Now())
	dumpCsv(fname, p)
	fmt.Println(cmdCounter)
	p.play(fname + ".load")

	//p := newCommandPlayers(fname)
	//t.Log(len(p.cmds))
}
