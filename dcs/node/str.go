package node

import (
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/vmihailenco/msgpack/v5"
)

/*
args 命令行输入的args[1:]
*/

func SetNode(args [][]byte) (*TNodeInfo, error) {
	if len(args) < 2 {
		return nil, newWrongNumOfArgsError("set")
	}
	key, value := args[0], args[1]
	var node *TNode
	var expiredAt int64
	if len(args) > 2 {
		ex := strings.ToLower(string(args[2]))
		if ex != "ex" || len(args) != 4 {
			return nil, ErrSyntax
		}
		second, err := strconv.Atoi(string(args[3]))
		if err != nil {
			return nil, ErrSyntax
		}

		expiredAt = time.Now().Add(time.Second * time.Duration(second)).Unix()
	}
	node = &TNode{
		Key:       key,
		Value:     value,
		DataType:  SetType,
		EntrySize: int(unsafe.Sizeof(value)),
		ExpiredAt: expiredAt,
	}
	tnodeInfo, err := newTNodeInfo(SetType, true, node, nil)
	if err != nil {
		return nil, err
	}
	return tnodeInfo, nil
}

func GetNode(args [][]byte) (*TNodeInfo, error) {
	key := args[0]
	node := &TNode{
		Key:      key,
		DataType: GetType,
	}
	tnodeInfo, err := newTNodeInfo(GetType, false, node, nil)
	if err != nil {
		return nil, err
	}
	return tnodeInfo, nil
}

func DeleteNode(args [][]byte) (*TNodeInfo, error) {
	key := args[0]
	node := &TNode{
		Key:      key,
		DataType: DeleteType,
		Type:     TypeDelete,
	}
	tnodeInfo, err := newTNodeInfo(DeleteType, true, node, nil)
	if err != nil {
		return nil, err
	}
	return tnodeInfo, nil
}

func MgetNode(args [][]byte) (*TNodeInfo, error) {
	if len(args) < 2 {
		return nil, newWrongNumOfArgsError("mget")
	}
	nodes := make([]*TNode, len(args))
	for i, key := range args {
		node := &TNode{
			Key:      key,
			Value:    nil,
			DataType: MgetType,
		}
		nodes[i] = node
	}
	tnodeInfo, err := newTNodeInfo(MgetType, false, nil, nodes)
	if err != nil {
		return nil, err
	}
	return tnodeInfo, nil
}

func GetRangeNode(args [][]byte) (*TNodeInfo, error) {
	key := args[0]
	if len(args) != 3 {
		return nil, newWrongNumOfArgsError("getrange")
	}
	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return nil, ErrValueIsInvalid
	}
	end, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return nil, ErrValueIsInvalid
	}
	extend := map[string]int{
		"start": start,
		"end":   end,
	}
	extendByte, err := msgpack.Marshal(extend)
	if err != nil {
		return nil, err
	}

	node := &TNode{
		Key:      key,
		DataType: GetRangeType,
		Extend:   extendByte,
	}
	TNodeInfo, err := newTNodeInfo(GetRangeType, false, node, nil)
	if err != nil {
		return nil, err
	}
	return TNodeInfo, nil
}

func GetDelNode(args [][]byte) (*TNodeInfo, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("getdel")
	}
	node := &TNode{
		Key:      args[0],
		DataType: GetDelType,
		Type:     TypeDelete,
	}
	tnodeInfo, err := newTNodeInfo(GetDelType, true, node, nil)
	if err != nil {
		return nil, err
	}
	return tnodeInfo, nil
}

func SetNxNode(args [][]byte) (*TNodeInfo, error) {
	// if len(args) != 2 {
	// 	return nil, newWrongNumOfArgsError("setnx")
	// }
	// key, value := args[0], args[1]
	return nil, nil
}

func SetExNode(args [][]byte) (*TNodeInfo, error) {
	if len(args) != 3 {
		return nil, newWrongNumOfArgsError("setex")
	}
	key, seconds, value := args[0], args[1], args[2]
	sec, err := strconv.Atoi(string(seconds))
	if err != nil {
		return nil, ErrValueIsInvalid
	}
	expiredAt := time.Now().Add(time.Second * time.Duration(sec)).Unix()
	node := &TNode{
		Key:       key,
		Value:     value,
		DataType:  SetExType,
		ExpiredAt: expiredAt,
	}
	tnodeInfo, err := newTNodeInfo(SetExType, true, node, nil)
	return tnodeInfo, err
}

func MsetNode(args [][]byte) (*TNodeInfo, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return nil, newWrongNumOfArgsError("mset")
	}
	var tnodes = make([]*TNode, len(args)%2)
	for i := 0; i < len(args); i += 2 {
		tnode := &TNode{
			Key:   args[i],
			Value: args[i+1],
		}
		tnodes = append(tnodes, tnode)
	}
	TNodeInfo, err := newTNodeInfo(MgetType, true, nil, tnodes)
	return TNodeInfo, err
}

func MsetNxNode(args [][]byte) (*TNodeInfo, error) {
	return nil, nil
}

func AppendStrNode(args [][]byte) (*TNodeInfo, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("append")
	}
	return nil, nil

}

func DecrNode(args [][]byte) (*TNodeInfo, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("decr")
	}
	return nil, nil

}

func DecrByNode(args [][]byte) (*TNodeInfo, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("decr")
	}
	return nil, nil

}

func IncrNode(args [][]byte) (*TNodeInfo, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("decr")
	}
	return nil, nil

}

func IncrByNode(args [][]byte) (*TNodeInfo, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("decr")
	}
	return nil, nil

}

func StrLenNode(args [][]byte) (*TNodeInfo, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("strlen")
	}
	key := args[0]
	tnode := &TNode{
		Key:      key,
		DataType: StrLenType,
	}
	tnodeInfo, err := newTNodeInfo(StrLenType, false, tnode, nil)
	return tnodeInfo, err
}
