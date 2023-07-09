package db

import (
	"sync"
	"sync/atomic"
	"tcache/art"
	"tcache/dcs/node"

	"github.com/hashicorp/raft"
)

// ============================raft接口实现======================================
func (db *TDB) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (db *TDB) Release() {

}

// =============================本地接口=========================================

func newStrsIndex() *strIndex {
	return &strIndex{
		idxTree: art.NewART(),
		mu:      new(sync.RWMutex),
	}
}

func Open() (*TDB, error) {
	db := &TDB{
		strIndex: newStrsIndex(),
	}
	return db, nil
}

func (db *TDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	atomic.StoreUint32(&db.closed, 1)
	db.strIndex = nil
	return nil
}

func (db *TDB) GetDBInfo(key string) (*DBInfo, error) {
	dbInfo, ok := supportHandler[node.DataType(key)]
	if !ok {
		return nil, ErrKeyNotSupported
	}
	return &dbInfo, nil
}

func (dbInfo DBInfo) GetTNodeInfo(args [][]byte) (*node.TNodeInfo, error) {
	return dbInfo.funcNode(args)
}

func (db *TDB) Apply(tnodeInfo *node.TNodeInfo) (any, error) {
	dbInfo, ok := supportHandler[node.DataType(tnodeInfo.DataType)]
	if !ok {
		return nil, ErrKeyNotSupported
	}
	result, err := dbInfo.funcCmd(db, tnodeInfo)
	return result, err
}
