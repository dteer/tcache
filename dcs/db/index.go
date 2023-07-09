package db

import (
	"tcache/dcs/node"
	"time"
)

type DataType = int8

const (
	String DataType = iota
)

func (db *TDB) updateIndexTree(node *node.TNode) (oldVal any, updated bool) {
	return db.strIndex.idxTree.Put(node.Key, node)
}

func (db *TDB) getVal(key []byte) ([]byte, error) {
	rawValue := db.strIndex.idxTree.Get(key)
	if rawValue == nil {
		return nil, ErrKeyNotFound
	}
	idxNode, _ := rawValue.(*node.TNode)
	if idxNode == nil {
		return nil, ErrKeyNotFound
	}

	ts := time.Now().Unix()
	if idxNode.ExpiredAt != 0 && idxNode.ExpiredAt <= ts {
		return nil, ErrKeyNotFound
	}
	return idxNode.Value, nil
}

func (db *TDB) Delete(key []byte) error {
	db.strIndex.idxTree.Delete(key)
	return nil
}
