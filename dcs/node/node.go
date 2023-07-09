package node

import (
	"errors"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

/*
将字符集转成实际节点结果
*/

var (
	ErrSyntax            = errors.New("ERR syntax error ")
	ErrValueIsInvalid    = errors.New("ERR value is not an integer or out of range")
	ErrDBIndexOutOfRange = errors.New("ERR DB index is out of range")
	ErrDBDataType        = errors.New("ERR DB TYPE")
)

func newWrongNumOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}

type nodeType byte

const (
	TypeNormal nodeType = iota
	TypeDelete
)

type DataType string

const (
	SetType       DataType = "set"
	GetType       DataType = "get"
	DeleteType    DataType = "del"
	MgetType      DataType = "mget"
	GetRangeType  DataType = "getrange"
	GetDelType    DataType = "getdel"
	SetExType     DataType = "setex"
	SetnxType     DataType = "setnx"
	MsetType      DataType = "mset"
	MsetnxType    DataType = "msetnx"
	AppendStrType DataType = "append"
	DecrType      DataType = "decr"
	DecrByType    DataType = "decrby"
	IncrType      DataType = "incr"
	IncrByType    DataType = "incrbytype"
	StrLenType    DataType = "strlen"

	// list
	LpushType DataType = "lpush"
)

type TNode struct {
	Key       []byte
	Value     []byte
	Type      nodeType
	DataType  DataType
	EntrySize int
	ExpiredAt int64
	Extend    []byte // 扩展字段，其他额外字段都通过该字段存放
}

type TNodeInfo struct {
	DataType   DataType // 冗余，和TNode.DataType一致
	MoreResult bool     // 列表或单结构体
	Send       bool     // 是否需要记录到raft日志
	Data       []byte   // *TNode or []*TNode
}

func newTNodeInfo(datatype DataType, send bool, node *TNode, nodes []*TNode) (*TNodeInfo, error) {
	var moreResult bool
	var data []byte
	var err error
	if nodes != nil {
		moreResult = true
		data, err = msgpack.Marshal(nodes)
	} else {
		moreResult = false
		data, err = msgpack.Marshal(node)
	}
	if err != nil {
		return nil, err
	}

	return &TNodeInfo{
		DataType:   datatype,
		Send:       send,
		MoreResult: moreResult,
		Data:       data,
	}, nil
}

func (tnodeInfo *TNodeInfo) GetData() (any, error) {
	if !tnodeInfo.MoreResult {
		var node *TNode
		err := msgpack.Unmarshal(tnodeInfo.Data, &node)
		if err != nil {
			return nil, err
		}
		return node, nil
	} else {
		var nodes []*TNode
		err := msgpack.Unmarshal(tnodeInfo.Data, &nodes)
		if err != nil {
			return nil, err
		}
		return nodes, nil
	}
}
