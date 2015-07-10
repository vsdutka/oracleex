// oraex_test
package oracleex

import (
	"flag"
	"gopkg.in/goracle.v1/oracle"
	//"os"
	"strconv"
	"testing"
	"time"
)

var dsn = flag.String("dsn", "", "Oracle DSN (user/passw@sid)")

func init() {
	flag.Parse()
}

func TestFloat(t *testing.T) {
	//os.Setenv("NLS_LANG", "RUSSIAN_RUSSIA.CL8MSWIN1251")
	username, password, sid := oracle.SplitDSN(*dsn)
	conn, err := oracle.NewConnection(username, password, sid, false)
	if err != nil {
		t.Fatalf("Connect error: \"%s\"", err.Error())
	}

	defer func() {
		err := conn.Close()

		if err != nil {
			t.Errorf("Close error: \"%s\"", err.Error())
		}
	}()

	cur := conn.NewCursor()
	defer cur.Close()
	//	{
	//		if oci, client, db, err := conn.NlsSettings(cur); err != nil {
	//			t.Logf("NlsSettings: %s", err)
	//		} else {
	//			t.Logf("NLS oci=%s client=%s database=%s", oci, client, db)
	//		}
	//	}

	var v *oracle.Variable
	v, err = cur.NewVariable(0, oracle.FloatVarType, 0)
	if err != nil {
		t.Fatalf("Error with NewVariable: %v", err)

	}
	var fI float64
	fI = float64(-1) / 25
	err = v.SetValue(0, fI)
	if err != nil {
		t.Fatalf("Error with SetValue: %v", err)
	}

	var fR float64
	err = v.GetValueInto(&fR, 0)
	if err != nil {
		t.Fatalf("Error with GetValueInto: %v", err)
	}
	if fR != fI {
		t.Errorf("Value should be \n\"%v\",\n was \n\"%v\"", fI, fR)
	}

	fIntf, err1 := v.GetValue(0)
	if err1 != nil {
		t.Fatalf("Error with GetValueInto: %v", err1)
	}
	switch x := fIntf.(type) {
	case float32:
		fR = fIntf.(float64)
	case float64:
		fR = fIntf.(float64)
	case string:
		fR, err = strconv.ParseFloat(fIntf.(string), 64)
		if err != nil {
			panic(err)
		}
	default:
		panic(x)

	}
	if fR != fI {
		t.Errorf("Value should be \n\"%v\",\n was \n\"%v\"", fI, fR)
	}

}

func TestCloseNotConnected(t *testing.T) {
	conn := &Connection{holder: "TestCloseNotConnected.gob"}
	err := conn.Close()
	if err != nil {
		t.Errorf("Close error: \"%s\"", err.Error())
	}
}

func TestNewConn(t *testing.T) {
	username, password, sid := oracle.SplitDSN(*dsn)
	conn, err := NewConnection(username, password, sid, false, "TestNewConn.gob", "")
	if err != nil {
		t.Fatalf("Connect error: \"%s\"", err.Error())
	}
	err = conn.Close()
	if err != nil {
		t.Fatalf("Disconnect error: \"%s\"", err.Error())
	}
}

var execScalarTests = []struct {
	tabTyp string
	in     interface{}
	out    string
}{
	{"INTEGER(3)", []int32{1, 3, 5}, "!3!1. Typ=2 Len=2: 193,2\n2. Typ=2 Len=2: 193,4\n3. Typ=2 Len=2: 193,6\n"},
	//{"PLS_INTEGER", []int32{1, 3, 5}, "!3!1. Typ=2 Len=2: 193,2\n2. Typ=2 Len=1: 128\n3. Typ=2 Len=2: 193,4\n"},
	{"NUMBER(5,3)", []float32{1.0 / 2, -10.24}, "!2!1. Typ=2 Len=2: 192,51\n2. Typ=2 Len=10: 62,91,78,2,2,24,90,83,81,102\n"},
	{"VARCHAR2(6)", []string{"KEDV01", "KEDV02"}, "!2!1. Typ=1 Len=6: 75,69,68,86,48,49\n2. Typ=1 Len=6: 75,69,68,86,48,50\n"},
	{"VARCHAR2(40)", []string{"SELECT", "Тестовое сообщение"},
		"!2!1. Typ=1 Len=6: 83,69,76,69,67,84\n2. Typ=1 Len=18: 210,229,241,242,238,226,238,229,32,241,238,238,225,249,229,237,232,229\n"},
	{"RAW(4)", [][]byte{[]byte{0, 1, 2, 3}, []byte{5, 7, 11, 13}},
		"!2!1. Typ=23 Len=4: 0,1,2,3\n2. Typ=23 Len=4: 5,7,11,13\n"},
	{"DATE", []time.Time{time.Date(2013, 1, 2, 10, 6, 49, 0, time.Local),
		time.Date(2012, 1, 2, 10, 6, 49, 0, time.Local)},
		"!2!1. Typ=12 Len=7: 120,113,1,2,11,7,50\n2. Typ=12 Len=7: 120,112,1,2,11,7,50\n"},
}

func runExecScalarTests(t *testing.T, fname string) {
	username, password, sid := oracle.SplitDSN(*dsn)
	conn, err := NewConnection(username, password, sid, false, fname, "")
	if err != nil {
		t.Fatalf("Connect error: \"%s\"", err.Error())
	}
	defer func() {
		err = conn.Close()
		//Важно. Следующие строки должны быть обязательно строго после закрытия соединения с БД
		<-time.After(time.Second)
		holder.closeAll()
		//Важно. Предидущие строки должны быть обязательно строго после закрытия соединения с БД
		if err != nil {
			t.Fatalf("Disconnect error: \"%s\"", err.Error())
		}
	}()

	cur := conn.NewCursor()
	defer cur.Close()

	var (
		in     *oracle.Variable
		out    *oracle.Variable
		val    interface{}
		outStr string
		ok     bool
		qry    string
	)

	for i, tt := range execScalarTests {
		if out, err = cur.NewVar(""); err != nil {
			t.Errorf("cannot create out variable: %s", err)
			t.FailNow()
		}

		qry = `DECLARE
	TYPE tabTyp IS TABLE OF ` + tt.tabTyp + ` INDEX BY PLS_INTEGER;
	tab tabTyp := :in;
	v_idx PLS_INTEGER;
	v_out VARCHAR2(1000) := '!';
BEGIN
    v_out := v_out||tab.COUNT||'!';
	v_idx := tab.FIRST;
	IF FALSE and v_idx IS NULL THEN
		v_out := v_out||'EMPTY';
	END IF;
	WHILE v_idx IS NOT NULL LOOP
	    SELECT v_out||v_idx||'. '||DUMP(tab(v_idx))||CHR(10) INTO v_out FROM DUAL;
		v_idx := tab.NEXT(v_idx);
	END LOOP;
	:out := v_out;
END;`

		in, err = cur.NewVar(tt.in)
		if err != nil {
			t.Errorf("%d. error with NewVar: %v", i, err)
			continue
		}

		if err = cur.Execute(qry, nil, map[string]interface{}{"in": in, "out": out}); err != nil {
			t.Errorf("error executing `%s`: %s", qry, err)
			continue
		}
		if val, err = out.GetValue(0); err != nil {
			t.Errorf("%d. error getting value: %s", i, err)
			continue
		}
		if outStr, ok = val.(string); !ok {
			t.Logf("output is not string!?!, but %T (%v)", val, val)
		}
		//t.Logf("%d. in:%s => out:%v", i, out, outStr)
		if outStr != tt.out {
			t.Errorf("%d. exec(%q) => %q, want %q", i, tt.in, outStr, tt.out)
		}
	}

}
func TestExecScalarTests(t *testing.T) {
	fname := "ExecScalarTest.gob"
	runExecScalarTests(t, fname)
	commandsDump(fname)
	Play(fname)
	<-time.After(time.Second)
	holder.closeAll()
	commandsDump(fname + ".load")

	vI := newCommandPlayers(fname)
	vR := newCommandPlayers(fname + ".load")

	if len(vI.cmds) != len(vR.cmds) {
		t.Fatalf("Saved commands len should by %v, was %v", len(vI.cmds), len(vR.cmds))
	}
	for i, vIItem := range vI.cmds {
		vRItem := vR.cmds[i]
		if vIItem.StreamID != vRItem.StreamID {
			t.Fatalf("%d. StreamID should by \"%v\", was \"%v\"", i, vIItem.StreamID, vRItem.StreamID)
		}
		if vIItem.CmdType != vRItem.CmdType {
			t.Fatalf("%d. CmdType should by \"%v\", was \"%v\"", i, vIItem.CmdType, vRItem.CmdType)
		}
		if vIItem.Sql != vRItem.Sql {
			t.Fatalf("%d. Sql should by \"%v\", was \"%v\"", i, vIItem.Sql, vRItem.Sql)
		}
	}
}
