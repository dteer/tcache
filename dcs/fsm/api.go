package fsm

import (
	"io"
	"tcache/dcs/node"

	"github.com/hashicorp/raft"
	"github.com/vmihailenco/msgpack/v5"
)

// ===================================raft接口================================================

func (f *Fsm) Apply(l *raft.Log) any {
	data := l.Data
	var tnodeInfo node.TNodeInfo
	err := msgpack.Unmarshal(data, &tnodeInfo)
	if err != nil {
		return err
	}
	result, err := f.DB.Apply(&tnodeInfo)
	if err != nil {
		return err
	}
	return result
}

// raft接口实现
func (f *Fsm) Snapshot() (raft.FSMSnapshot, error) {
	return f.DB, nil
}

func (f *Fsm) Restore(snapshot io.ReadCloser) error {
	return nil
}

// ==================================本地实现==================================================

func (f *Fsm) ReadApply(tnodeType *node.TNodeInfo) (any, error) {
	return f.DB.Apply(tnodeType)
}
