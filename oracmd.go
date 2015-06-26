// oracmdhold
package oracleex

import (
	"bytes"
	"encoding/gob"
	//"fmt"
	"time"
)

const (
	cmLogin = iota
	cmLogout
	cmExec
)

type sqlParam struct {
	Type    string        // Type of Param - string/int32/...
	IsArray bool          // Is it arry of elements of Type?
	Count   uint          // Initial Array size
	Size    uint          // Size of one element/param
	Value   []interface{} //Value
}
type command struct {
	StreamID       string
	CmdID          uint64
	CmdType        uint
	UserName       string
	UserPass       string
	ConnStr        string
	Sql            string
	SqlNamedParams map[string]sqlParam
	ClientBg       time.Time
	ClientFn       time.Time
	ServerBg       time.Time
	ServerFn       time.Time
	ServerFnScn    string
	Success        bool
	ErrorMsg       string
}

func (cmd *command) GobEncode() ([]byte, error) {
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)
	if err := encoder.Encode(cmd.StreamID); err != nil {
		return nil, err
	}
	if err := encoder.Encode(cmd.CmdID); err != nil {
		return nil, err
	}
	if err := encoder.Encode(cmd.CmdType); err != nil {
		return nil, err
	}
	if err := encoder.Encode(cmd.UserName); err != nil {
		return nil, err
	}
	if err := encoder.Encode(cmd.UserPass); err != nil {
		return nil, err
	}
	if err := encoder.Encode(cmd.ConnStr); err != nil {
		return nil, err
	}
	if err := encoder.Encode(cmd.Sql); err != nil {
		return nil, err
	}
	if err := encoder.Encode(len(cmd.SqlNamedParams)); err != nil {
		return nil, err
	}

	for nam, p := range cmd.SqlNamedParams {
		if err := encoder.Encode(nam); err != nil {
			return nil, err
		}
		if err := encoder.Encode(p.Type); err != nil {
			return nil, err
		}
		if err := encoder.Encode(p.IsArray); err != nil {
			return nil, err
		}
		if err := encoder.Encode(p.Count); err != nil {
			return nil, err
		}
		if err := encoder.Encode(p.Size); err != nil {
			return nil, err
		}
		if err := encoder.Encode(p.Value); err != nil {
			return nil, err
		}
	}

	if err := encoder.Encode(cmd.ClientBg); err != nil {
		return nil, err
	}
	if err := encoder.Encode(cmd.ClientFn); err != nil {
		return nil, err
	}
	if err := encoder.Encode(cmd.ServerBg); err != nil {
		return nil, err
	}
	if err := encoder.Encode(cmd.ServerFn); err != nil {
		return nil, err
	}
	if err := encoder.Encode(cmd.ServerFnScn); err != nil {
		return nil, err
	}
	if err := encoder.Encode(cmd.Success); err != nil {
		return nil, err
	}
	if err := encoder.Encode(cmd.ErrorMsg); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func (cmd *command) GobDecode(buf []byte) error {
	r := bytes.NewBuffer(buf)
	decoder := gob.NewDecoder(r)

	if cmd.SqlNamedParams == nil {
		cmd.SqlNamedParams = make(map[string]sqlParam)
	}
	if err := decoder.Decode(&cmd.StreamID); err != nil {
		return err
	}
	if err := decoder.Decode(&cmd.CmdID); err != nil {
		return err
	}
	if err := decoder.Decode(&cmd.CmdType); err != nil {
		return err
	}
	if err := decoder.Decode(&cmd.UserName); err != nil {
		return err
	}
	if err := decoder.Decode(&cmd.UserPass); err != nil {
		return err
	}
	if err := decoder.Decode(&cmd.ConnStr); err != nil {
		return err
	}
	if err := decoder.Decode(&cmd.Sql); err != nil {
		return err
	}

	var l int
	if err := decoder.Decode(&l); err != nil {
		return err
	}

	for i := 0; i < l; i++ {
		p := sqlParam{Value: make([]interface{}, 0)}
		var nam string
		if err := decoder.Decode(&nam); err != nil {
			return err
		}
		if err := decoder.Decode(&p.Type); err != nil {
			return err
		}
		if err := decoder.Decode(&p.IsArray); err != nil {
			return err
		}
		if err := decoder.Decode(&p.Count); err != nil {
			return err
		}
		if err := decoder.Decode(&p.Size); err != nil {
			return err
		}
		if err := decoder.Decode(&p.Value); err != nil {
			return err
		}
		cmd.SqlNamedParams[nam] = p
	}

	if err := decoder.Decode(&cmd.ClientBg); err != nil {
		return err
	}
	if err := decoder.Decode(&cmd.ClientFn); err != nil {
		return err
	}
	if err := decoder.Decode(&cmd.ServerBg); err != nil {
		return err
	}
	if err := decoder.Decode(&cmd.ServerFn); err != nil {
		return err
	}
	if err := decoder.Decode(&cmd.ServerFnScn); err != nil {
		return err
	}
	if err := decoder.Decode(&cmd.Success); err != nil {
		return err
	}
	if err := decoder.Decode(&cmd.ErrorMsg); err != nil {
		return err
	}
	return nil
}
