package mig

import "github.com/lestrrat/go-jshschema"

type State struct {
}

func StateNew(s string) (*State, error) {
	return &State{}, nil
}

func (s *State) Update(h *hschema.HyperSchema) error {
	return nil
}
