package main

import (
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"tcache/config"
	"tcache/dcs"
	"tcache/dcs/db"

	"github.com/tidwall/redcon"
)

var (
	errClientIsNil = errors.New("ERR client conn is nil")
)

var (
	defaultCPath = "./default.yaml"
)

// const (
// 	dbName = "tcache-%04d"
// )

type Server struct {
	dbs    map[int]*db.TDB
	ser    *redcon.Server
	signal chan os.Signal
	opts   ServerOptions
	mu     *sync.RWMutex
	dcs    *dcs.DCS
}

type ServerOptions struct {
	host      string
	port      string
	databases uint
}

func parse(defaultFpath string, serverOpts *ServerOptions) (*config.Config, error) {

	fpath := defaultFpath
	flag.StringVar(&fpath, "fpath", defaultFpath, "config path")
	flag.Parse()
	conf, err := config.ReadConfig(fpath)
	if err != nil {
		return nil, err
	}
	serverOpts.host = conf.Service.Host
	serverOpts.port = conf.Service.Port
	serverOpts.databases = conf.Service.DatabasesNum
	return conf, err
}

func main() {
	// init server options
	serverOpts := new(ServerOptions)
	conf, err := parse(defaultCPath, serverOpts)
	if err != nil {
		log.Fatalf("read configuration file: %v", err)
	}

	tdb, err := db.Open()
	if err != nil {
		log.Fatalf("open rosedb err, fail to start server. %v", err)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	dbs := make(map[int]*db.TDB)
	dbs[0] = tdb

	// init and start server
	println("raft server starting...")
	tdcs := &dcs.DCS{
		Config: conf,
	}
	err = tdcs.NewRaft(conf.SelfPerr.Host, conf.SelfPerr.ID, conf.Dir, tdb)
	if err != nil {
		log.Fatalf("new raft failed: %v\n", err)
	}
	fllowers, ids := append(conf.Followers, conf.SelfPerr.Host), append(conf.IDs, conf.SelfPerr.ID)
	err = tdcs.Bootstrap(fllowers, ids)
	if err != nil {
		log.Fatalf("start raft failed: %v\n", err)
	}
	print("raft server start")

	svr := &Server{
		dbs:    dbs,
		signal: sig,
		opts:   *serverOpts,
		mu:     new(sync.RWMutex),
		dcs:    tdcs,
	}
	addr := svr.opts.host + ":" + svr.opts.port
	redServer := redcon.NewServerNetwork("tcp", addr, execClientCommand, svr.redconAccept,
		func(conn redcon.Conn, err error) {
		},
	)
	svr.ser = redServer
	go svr.listen()
	<-svr.signal
	svr.stop()

}

func (svr *Server) listen() {
	log.Print("tcache server is running, ready to accept connections")
	if err := svr.ser.ListenAndServe(); err != nil {
		log.Fatalf("listen and serve err, fail to start. %v", err)
		return
	}
}

func (svr *Server) stop() {
	for _, db := range svr.dbs {
		if err := db.Close(); err != nil {
			log.Fatalf("close db err: %v", err)
		}
	}
	if err := svr.ser.Close(); err != nil {
		log.Fatalf("close server err: %v", err)
	}
	log.Print("tcache is ready to exit, bye bye...")
}

func (svr *Server) redconAccept(conn redcon.Conn) bool {
	cli := new(Client)
	cli.svr = svr
	svr.mu.RLock()
	cli.db = svr.dbs[0]
	svr.mu.RUnlock()
	conn.SetContext(cli)
	return true
}
