package db

import (
	"errors"
	"sync"
	"tcache/art"
	"tcache/dcs/node"
)

const (
	Response = "OK"
)

var (
	ErrKeyNotFound     = errors.New("key not found")
	ErrKeyNotSupported = errors.New("key not supported")
)

type nodeHandler func(args [][]byte) (*node.TNodeInfo, error)
type cmdHandler func(*TDB, *node.TNodeInfo) (any, error)
type DBInfo struct {
	funcNode nodeHandler //
	funcCmd  cmdHandler
}

var supportHandler = map[node.DataType]DBInfo{
	node.DeleteType:    newNodeInfo(node.DeleteNode, Delete),
	node.SetType:       newNodeInfo(node.SetNode, Set),
	node.GetType:       newNodeInfo(node.GetNode, Get),
	node.MgetType:      newNodeInfo(node.MgetNode, Mget),
	node.GetRangeType:  newNodeInfo(node.GetRangeNode, GetRange),
	node.GetDelType:    newNodeInfo(node.GetDelNode, GetDel),
	node.SetExType:     newNodeInfo(node.SetExNode, SetEx),
	node.SetnxType:     newNodeInfo(node.SetNode, SetNx), // TODO:架构存在问题，无法在判断是否入库再生成对应的Tnode
	node.MsetType:      newNodeInfo(node.MsetNode, Mset),
	node.MsetnxType:    newNodeInfo(node.MsetNxNode, Msetnx),       // TODO:架构
	node.AppendStrType: newNodeInfo(node.AppendStrNode, AppendStr), // TODO:架构
	node.DecrType:      newNodeInfo(node.DecrNode, Decr),           // TODO:架构
	node.DecrByType:    newNodeInfo(node.DecrByNode, DecrBy),       // TODO:架构
	node.IncrType:      newNodeInfo(node.IncrNode, Incr),           // TODO:架构
	node.IncrByType:    newNodeInfo(node.IncrByNode, IncrBy),       // TODO:架构
	node.StrLenType:    newNodeInfo(node.StrLenNode, StrLen),

	// list
	node.LpushType: newNodeInfo(node.LpushNode, Lpush),
}

type (
	TDB struct {
		strIndex *strIndex
		mu       sync.RWMutex
		closed   uint32
	}

	strIndex struct {
		mu      *sync.RWMutex
		idxTree *art.AdaptiveRadixTree
	}
)

func newNodeInfo(funcNode nodeHandler, funcCmd cmdHandler) DBInfo {
	return DBInfo{
		funcNode: funcNode,
		funcCmd:  funcCmd,
	}
}
