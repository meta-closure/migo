package mig

import "github.com/lestrrat/go-jshschema"

type State struct {
	Db Db
}

type Db struct {
	Name     string
	User     string
	Password string
}

type Table struct {
	Name string
}

func StateNew(s string) (*State, error) {
	return &State{}, nil
}

func (s *State) Update(h *hschema.HyperSchema) error {
	return nil
}
