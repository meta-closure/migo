package mig

import (
	"io/ioutil"

	"github.com/ghodss/yaml"
	"github.com/lestrrat/go-jshschema"
	"github.com/lestrrat/go-jsschema"
	"github.com/pkg/errors"
)

var ()

type State struct {
	Db    Db      `json:"db"`
	Table []Table `json:"table"`
}

type Table struct {
	BeforeName string   `json:"before_name"`
	Name       string   `json:"name"`
	PrimaryKey []string `json:"primary_key"`
	Index      []string `json:"index"`
	Columns    []Column `json:"column"`
}

func StateNew() *State {
	return &State{}
}

func ParseState(s string) (*State, error) {
	b, err := ioutil.ReadFile(s)
	if err != nil {
		return nil, errors.Wrap(err, "YAML file open error")
	}
	st := StateNew()
	err = yaml.Unmarshal(b, st)
	if err != nil {
		return nil, errors.Wrap(err, "YAML file parse error")
	}

	return st, nil
}

func (c *Column) ParseSchema2Column(s *schema.Schema, h *hschema.HyperSchema) error {
	col, ok := s.Extras["column"].(map[string]interface{})
	if ok != true {
		return ErrTypeInvalid
	}

	for k, v := range col {
		switch k {
		case "name":
			st, ok := v.(string)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			c.Name = st
		case "before_name":
			st, ok := v.(string)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			c.BeforeName = st
		case "unique":
			b, ok := v.(bool)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			c.UniqueFlag = b

		case "auto_increment":
			b, ok := v.(bool)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			c.AutoIncrementFlag = b
		case "not_null":
			b, ok := v.(bool)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			c.NotNullFlag = b
		}
	}
	return nil
}

func (t *Table) ParseSchema2Table(s *schema.Schema, h *hschema.HyperSchema) error {

	table, ok := s.Extras["table"].(map[string]interface{})
	if ok != true {
		return ErrTypeInvalid
	}

	for k, v := range table {
		switch k {
		case "primary_key":
			a, ok := v.([]string)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			t.PrimaryKey = a
		case "index":
			a, ok := v.([]string)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			t.Index = a
		case "before_name":
			st, ok := v.(string)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			t.BeforeName = st
		case "name":
			st, ok := v.(string)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			t.Name = st
		}
	}

	for k, v := range s.Properties {
		if v.Extras["column"] == nil {
			continue
		}

		c := &Column{}
		err := c.ParseSchema2Column(v, h)
		if err != nil {
			return errors.Wrapf(err, "Parse %s column error", k)
		}
		t.Columns = append(t.Columns, *c)

	}

	return nil
}

func ParseSchema2State(h *hschema.HyperSchema) (*State, error) {
	s := StateNew()
	err := s.Db.GetDbConnect(h)
	if err != nil {
		return nil, err
	}
	for k, v := range h.Definitions {
		if v.Extras["table"] == nil {
			continue
		}
		t := &Table{}
		err = t.ParseSchema2Table(v, h)
		if err != nil {
			return nil, errors.Wrapf(err, "Parsing %s table", k)

		}
		s.Table = append(s.Table, *t)
	}

	for _, l := range h.Links {
		if l.Schema != nil {
			if l.Schema.Extras["table"] == nil {
				continue
			}

			t := &Table{}
			err = t.ParseSchema2Table(l.Schema, h)
			if err != nil {
				return nil, errors.Wrapf(err, "Parsing %s table", t.Name)

			}
			s.Table = append(s.Table, *t)

		}

		if l.TargetSchema != nil {
			if l.TargetSchema.Extras["table"] == nil {
				continue
			}
			t := &Table{}
			err = t.ParseSchema2Table(l.TargetSchema, h)
			if err != nil {
				return nil, errors.Wrapf(err, "Parsing %s table", t.Name)

			}
			s.Table = append(s.Table, *t)
		}
	}
	return s, err
}

func (s *State) ExistNewTable(m *schema.Schema) (bool, error) {

	db, ok := m.Extras["db"].(map[string]interface{})
	if ok != true {
		return false, errors.Wrap(ErrTypeInvalid, "Db")
	}

	for _, t := range s.Table {
		if db["table"] == nil {
			return false, errors.Wrapf(ErrEmpty, "Table not specified")
		}

		s, ok := db["table"].(string)
		if ok != true {
			return false, errors.Wrapf(ErrTypeInvalid, "Table not string")
		}

		if t.Name == s {
			return true, nil
		}
	}

	return false, nil
}

func (s *State) ExistColumnUpdate(m *schema.Schema) (bool, error) {
	return false, nil
}

func (s *State) Update() error {
	return nil
}
