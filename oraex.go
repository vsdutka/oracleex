// oraex
package oracleex

import (
	"code.google.com/p/go-uuid/uuid"
	"fmt"
	"gopkg.in/errgo.v1"
	"gopkg.in/goracle.v1/oracle"
	"time"
)

type Connection struct {
	oracle.Connection
	holder   string
	streamID string
}

func NewConnection(username, password, dsn string, autocommit bool, holder, streamID string) (
	conn *Connection, err error) {
	bg := time.Now()

	conn = &Connection{holder: holder, streamID: streamID}
	if conn.streamID == "" {
		conn.streamID = uuid.New()
	}

	var connTmp *oracle.Connection

	connTmp, err = oracle.NewConnection(username, password, dsn, autocommit)
	if err != nil {
		conn = nil
		loginCmd(holder, streamID, username, password, dsn, bg, time.Now(), false, err.Error())
		return
	}
	conn.Connection = *connTmp
	loginCmd(conn.holder, conn.streamID, username, password, dsn, bg, time.Now(), true, "")
	return
}

func (conn *Connection) Close() (err error) {
	if conn.IsConnected() {
		bg := time.Now()
		err = conn.Connection.Close()
		if err != nil {
			logoutCmd(conn.holder, conn.streamID, bg, time.Now(), false, err.Error())
			return err
		}
		logoutCmd(conn.holder, conn.streamID, bg, time.Now(), true, "")
	}
	return nil
}

func (conn *Connection) Cursor() *Cursor {
	cur := &Cursor{holder: conn.holder, streamID: conn.streamID}
	cur.Cursor = *oracle.NewCursor(&conn.Connection)
	return cur
}
func (conn *Connection) NewCursor() *Cursor {
	return conn.Cursor()
}

type Cursor struct {
	oracle.Cursor
	holder   string
	streamID string
}

func (cur *Cursor) Execute(statement string, listArgs []interface{}, keywordArgs map[string]interface{}) error {

	bg := time.Now()
	stm := statement
	params := keywordArgs

	var (
		err            error
		serverBgVar    *oracle.Variable
		serverFnVar    *oracle.Variable
		serverFnScnVar *oracle.Variable
		serverBg       = time.Time{}
		serverFn       = time.Time{}
		serverFnScn    string
	)

	if cur.holder != "" {
		stm = fmt.Sprintf(stmWithStats, statement)

		serverBgVar, err = cur.NewVariable(0, oracle.StringVarType, 100)
		if err != nil {
			return errgo.Newf("error creating variable for %s: %s", "serverBg", err)
		}
		serverFnVar, err = cur.NewVariable(0, oracle.StringVarType, 100)
		if err != nil {
			return errgo.Newf("error creating variable for %s: %s", "serverFn", err)
		}
		serverFnScnVar, err = cur.NewVariable(0, oracle.StringVarType, 200)
		if err != nil {
			return errgo.Newf("error creating variable for %s: %s", "serverFnScn", err)
		}

		params["яbg"] = serverBgVar
		params["яfn"] = serverFnVar
		params["яfn_scn"] = serverFnScnVar

	}
	err = cur.Cursor.Execute(stm, nil, params)
	if cur.holder != "" {
		paramVal, err1 := serverBgVar.GetValue(0)
		if err1 == nil {
			serverBg, _ = time.Parse("2006-01-02 15:04:05.999999999 -07:00", paramVal.(string))
		}
		paramVal, err1 = serverFnVar.GetValue(0)
		if err1 == nil {
			serverFn, _ = time.Parse("2006-01-02 15:04:05.999999999 -07:00", paramVal.(string))
		}
		paramVal, err1 = serverFnScnVar.GetValue(0)
		if err1 == nil {
			serverFnScn, _ = paramVal.(string)
		}
		delete(params, "яbg")
		delete(params, "яfn")
		delete(params, "яfn_scn")
	}
	if err != nil {
		if cur.holder != "" {
			execCmd(cur.holder, cur.streamID, statement, params, bg, time.Now(), serverBg, serverFn, serverFnScn, false, err.Error())
		}
		return err
	}
	if cur.holder != "" {
		execCmd(cur.holder, cur.streamID, statement, params, bg, time.Now(), serverBg, serverFn, serverFnScn, true, "")
	}
	return nil
}

const stmWithStats = `
begin
  :яbg := to_char(systimestamp, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM');
  %s
  :яfn := to_char(systimestamp, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM');
  :яfn_scn := get_system_change_number();
end;`
