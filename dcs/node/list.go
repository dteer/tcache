package node

// list commands

func LpushNode(args [][]byte) (*TNodeInfo, error) {
	if len(args) < 2 {
		return nil, newWrongNumOfArgsError("lpush")
	}
	key, value := args[0], args[1:]
	node := &TNode{
		Key:   key,
		Value: value,
	}
}
