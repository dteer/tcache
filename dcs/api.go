package dcs

import (
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"tcache/dcs/db"
	"tcache/dcs/fsm"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/vmihailenco/msgpack/v5"
)

// 初始化raft
func (dcs *DCS) NewRaft(Addr, Id, Dir string, db *db.TDB) error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(Id)
	addr, err := net.ResolveTCPAddr("tcp", Addr)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(Addr, addr, 2, 5*time.Second, os.Stderr)
	if err != nil {
		return err
	}
	snapshots, err := raft.NewFileSnapshotStore(Dir, 2, os.Stderr)
	if err != nil {
		return err
	}
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(Dir, "raft-log.db"))
	if err != nil {
		return err
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(Dir, "raft-stable.db"))
	if err != nil {
		return err
	}
	f := fsm.NewFsm(db)
	rf, err := raft.NewRaft(config, f, logStore, stableStore, snapshots, transport)
	if err != nil {
		return err
	}

	dcs.Raft = rf
	dcs.Mfsm = f
	dcs.getLeaderStatus()

	return nil
}

// 启动raft
func (dcs *DCS) Bootstrap(Cluster []string, IDs []string) error {

	servers := dcs.Raft.GetConfiguration().Configuration().Servers
	if len(servers) > 0 {
		return nil
	}
	if len(Cluster) <= 0 || len(IDs) <= 0 || len(Cluster) != len(IDs) {
		return ErrConfig
	}

	var configuration raft.Configuration
	for index, addr := range Cluster {
		id := IDs[index]
		server := raft.Server{
			ID:      raft.ServerID(id),
			Address: raft.ServerAddress(addr),
		}
		configuration.Servers = append(configuration.Servers, server)
	}
	dcs.Raft.BootstrapCluster(configuration)
	return nil
}

func (dcs *DCS) getLeaderStatus() {
	go func() {
		for leader := range dcs.Raft.LeaderCh() {
			if leader {
				atomic.StoreInt64(&dcs.Leader, 1)
			} else {
				atomic.StoreInt64(&dcs.Leader, 0)
			}
		}
	}()
}

func (dcs *DCS) Apply(args [][]byte) (any, error) {
	if dcs.Leader != 1 {
		return nil, ErrNonLeader
	}
	key := string(args[0])
	dbInfo, err := dcs.Mfsm.DB.GetDBInfo(key)
	if err != nil {
		return nil, err
	}
	tnodeInfo, err := dbInfo.GetTNodeInfo(args[1:])
	if err != nil {
		return nil, err
	}

	// need notify raft log
	if tnodeInfo.Send {
		data, err := msgpack.Marshal(tnodeInfo)
		if err != nil {
			return nil, err
		}
		future := dcs.Raft.Apply(data, 5*time.Second)
		if future.Error() != nil {
			return nil, future.Error()
		}
		return future.Response(), nil
	}
	// not need notify raft log
	result, err := dcs.Mfsm.ReadApply(tnodeInfo)
	if err != nil {
		return nil, err
	}
	return result, nil
}
