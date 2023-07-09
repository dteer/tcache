package main

import (
	"fmt"
	"runtime"
	"strings"
	"tcache/dcs/db"

	"github.com/tidwall/redcon"
)

var (
	FollowerError = "Followers cannot execute commands"
)

type Client struct {
	svr *Server
	db  *db.TDB
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))
	cli, _ := conn.Context().(*Client)
	if cli == nil {
		conn.WriteError(errClientIsNil.Error())
		return
	}

	switch command {
	case "quit":
		_ = conn.Close()
	default:
		res, err := sendCommand(cli, cmd)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteAny(res)
	}
}

func sendCommand(cli *Client, cmd redcon.Command) (any, error) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 1024)
			n := runtime.Stack(buf, false)
			fmt.Printf("Recovered: %v\n%s", r, buf[:n])
		}
	}()
	response, err := cli.svr.dcs.Apply(cmd.Args)
	if err != nil {
		return nil, err
	}
	return response, nil
}
