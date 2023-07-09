package fsm

import (
	"tcache/dcs/db"
)

type Fsm struct {
	DB *db.TDB
}

func NewFsm(db *db.TDB) *Fsm {
	fsm := &Fsm{
		DB: db,
	}
	return fsm
}
