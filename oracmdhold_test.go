// oracmdhold_test
package oracleex

//import (
//	"testing"
//	"time"
//)

//func TestHoldCmds(t *testing.T) {
//	const fname = "test1.gob"
//	var cmds = []command{
//		{

//			StreamID:       "s1",
//			CmdID:          1,
//			CmdType:        cmLogin,
//			UserName:       "u1",
//			UserPass:       "p1",
//			ConnStr:        "c1",
//			Sql:            "",
//			SqlNamedParams: nil,
//			ClientBg:       time.Now(),
//			ClientFn:       time.Now(),
//			ServerBg:       time.Now(),
//			ServerFn:       time.Now(),
//		},
//		{

//			StreamID:       "s1",
//			CmdID:          2,
//			CmdType:        cmExec,
//			UserName:       "",
//			UserPass:       "",
//			ConnStr:        "",
//			Sql:            "root$.startup",
//			SqlNamedParams: nil,
//			ClientBg:       time.Now(),
//			ClientFn:       time.Now(),
//			ServerBg:       time.Now(),
//			ServerFn:       time.Now(),
//		},
//		{

//			StreamID:       "s1",
//			CmdID:          3,
//			CmdType:        cmLogout,
//			UserName:       "",
//			UserPass:       "",
//			ConnStr:        "",
//			Sql:            "",
//			SqlNamedParams: nil,
//			ClientBg:       time.Now(),
//			ClientFn:       time.Now(),
//			ServerBg:       time.Now(),
//			ServerFn:       time.Now(),
//		},
//	}

//	for _, cmd := range cmds {
//		switch cmd.CmdType {
//		case cmLogin:
//			go loginCmd(fname, cmd.StreamID, cmd.UserName, cmd.UserPass, cmd.ConnStr, cmd.ClientBg, cmd.ClientFn, false, "")
//		case cmExec:
//			go execCmd(fname, cmd.StreamID, cmd.Sql, nil, cmd.ClientBg, cmd.ClientFn, cmd.ServerBg, cmd.ServerFn, cmd.ServerFnScn, false, "")
//		case cmLogout:
//			go logoutCmd(fname, cmd.StreamID, cmd.ClientBg, cmd.ClientFn, false, "")

//		}
//	}
//	<-time.After(1 * time.Second)
//	holder.closeAll()

//	//closer.Close()
//	v := commandsLoad(fname)

//	if len(v) != len(cmds) {
//		t.Fatalf("Number of commands should be \"%d\", was \"%d\"", len(cmds), len(v))
//	}
//	for i, cmd := range v {
//		if cmd.StreamID != cmds[i].StreamID || cmd.CmdID != cmds[i].CmdID || cmd.UserName != cmds[i].UserName {
//			t.Fatalf("Commands should be \n\"%v\",\n was \n\"%v\"", cmds[i], cmd)
//		}
//	}
//}
