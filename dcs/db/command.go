package db

import (
	"errors"
	"fmt"
	"tcache/dcs/node"

	"github.com/vmihailenco/msgpack"
)

/*
为了兼容多查询，将所有的形参以列表的形式处理，实际单参数还是多参数同一由各自的函数决定
*/

func Get(db *TDB, nodeInfo *node.TNodeInfo) (any, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	data, err := nodeInfo.GetData()
	node := data.(*node.TNode)
	if err != nil {
		return nil, err
	}
	value, err := db.getVal(node.Key)
	return value, err
}

func Set(db *TDB, nodeInfo *node.TNodeInfo) (any, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	data, err := nodeInfo.GetData()
	node := data.(*node.TNode)
	if err != nil {
		return nil, err
	}
	db.updateIndexTree(node)
	return Response, nil
}

func Delete(db *TDB, nodeInfo *node.TNodeInfo) (any, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	data, err := nodeInfo.GetData()
	node := data.(*node.TNode)
	if err != nil {
		return nil, err
	}
	db.Delete(node.Key)
	return Response, nil
}

func Mget(db *TDB, nodeInfo *node.TNodeInfo) (any, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	data, err := nodeInfo.GetData()
	if err != nil {
		return nil, err
	}
	nodes := data.([]*node.TNode)
	values := make([][]byte, len(nodes))
	for i, n := range nodes {
		val, err := db.getVal(n.Key)
		if err != nil && !errors.Is(ErrKeyNotFound, err) {
			return nil, err
		}
		values[i] = val
	}
	return values, nil
}

func GetRange(db *TDB, nodeInfo *node.TNodeInfo) (any, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	data, err := nodeInfo.GetData()
	if err != nil {
		return nil, err
	}
	node := data.(*node.TNode)
	extendData := map[string]int{}
	fmt.Printf("%+v\n", node.Extend)
	err = msgpack.Unmarshal(node.Extend, &extendData)
	if err != nil {
		return nil, err
	}
	val, err := db.getVal(node.Key)
	if err != nil {
		return nil, err
	}
	start, end := extendData["start"], extendData["end"]
	if start < 0 {
		start = len(val) + start
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end = len(val) + end
		if end < 0 {
			end = 0
		}
	}

	if end > len(val)-1 {
		end = len(val) - 1
	}
	if start > len(val)-1 || start > end {
		return []byte{}, nil
	}
	println("start:", start, "end:", end)
	return val[start : end+1], nil
}

func GetDel(db *TDB, nodeInfo *node.TNodeInfo) (any, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	data, err := nodeInfo.GetData()
	if err != nil {
		return nil, err
	}
	node := data.(*node.TNode)
	val, err := db.getVal(node.Key)
	if err != nil && err != ErrKeyNotFound {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	db.Delete(node.Key)
	return val, nil
}

func SetEx(db *TDB, nodeInfo *node.TNodeInfo) (any, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	data, err := nodeInfo.GetData()
	if err != nil {
		return nil, err
	}
	node := data.(*node.TNode)

	db.updateIndexTree(node)
	return Response, nil
}

func SetNx(db *TDB, node *node.TNodeInfo) (any, error) {
	return nil, nil
}

func Mset(db *TDB, nodeInfo *node.TNodeInfo) (any, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	data, err := nodeInfo.GetData()
	if err != nil {
		return nil, err
	}
	nodes := data.([]*node.TNode)
	for _, n := range nodes {
		db.updateIndexTree(n)
	}
	return Response, nil
}

func Msetnx(db *TDB, nodeInfo *node.TNodeInfo) (any, error) {
	return nil, nil
}

func AppendStr(db *TDB, nodeInfo *node.TNodeInfo) (any, error) {
	return nil, nil
}

func Decr(db *TDB, nodeInfo *node.TNodeInfo) (any, error) {
	return nil, nil
}

func DecrBy(db *TDB, nodeInfo *node.TNodeInfo) (any, error) {
	return nil, nil
}

func Incr(db *TDB, nodeInfo *node.TNodeInfo) (any, error) {
	return nil, nil
}

func IncrBy(db *TDB, nodeInfo *node.TNodeInfo) (any, error) {
	return nil, nil
}

func StrLen(db *TDB, nodeInfo *node.TNodeInfo) (any, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	data, err := nodeInfo.GetData()
	if err != nil {
		return nil, err
	}
	tnode := data.(*node.TNode)
	val, err := db.getVal(tnode.Key)
	if err != nil {
		return 0, nil
	}
	return len(val), nil
}
