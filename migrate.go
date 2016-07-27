package mig

import (
	"fmt"
)

const (
	ADD = iota
	DROP
	MODIFY
	CHANGE
)

type Column struct {
	Name              string
	Type              string
	PkFlag            bool
	UniqueFlag        bool
	AutoIncrementFlag bool
}

type Command struct {
	Table     string
	Column    Column
	AlterType int
}

type Sql struct {
	Db         string
	Operations []Command
}

func (s Sql) Check() {
	fmt.Printf("checked")
}

func (s Sql) Migrate() error {
	return nil
}
