package dcs

import (
	"errors"
	"tcache/config"
	"tcache/dcs/fsm"

	"github.com/hashicorp/raft"
)

/*
利用raft算法实现分布式系统
*/

var (
	ErrNonLeader = errors.New("ERR non leaders cannot carry out tasks ")
	ErrConfig    = errors.New("ERR Config error ")
)

type DCS struct {
	Leader int64          // is Leader
	Raft   *raft.Raft     // raft pointer
	Mfsm   *fsm.Fsm       // fsm structure
	Config *config.Config // configuration information
}
