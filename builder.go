package migo

import (
	"io/ioutil"
	"time"

	"github.com/ghodss/yaml"
	"github.com/lestrrat/go-jshschema"
	"github.com/lestrrat/go-jsschema"
	"github.com/pkg/errors"
)

type State struct {
	Db       Db
	Table    []Table
	UpdateAt time.Time
}

type Db struct {
	User   string
	Passwd string
	Addr   string
	DBName string
}

type Table struct {
	Id         string
	Name       string
	PrimaryKey []Key
	Index      []Key
	Column     []Column
}

func StateNew() *State {
	return &State{
		UpdateAt: time.Now(),
	}
}

func ParseState(s string) (*State, error) {
	b, err := ioutil.ReadFile(s)
	if err != nil {
		return nil, errors.Wrap(err, "YAML file open error")
	}

	st := StateNew()
	if len(b) == 0 {
		return st, nil
	}
	err = yaml.Unmarshal(b, st)
	if err != nil {
		return nil, errors.Wrap(err, "YAML file parse error")
	}

	return st, nil
}

func ParseSchemaYAML(h *hschema.HyperSchema, s string) error {
	y, err := ParseYAML(s)
	if err != nil {
		return err
	}
	h.Extract(y)
	return nil
}

func ParseYAML(s string) (map[string]interface{}, error) {
	y := &map[string]interface{}{}
	b, err := ioutil.ReadFile(s)
	if err != nil {
		return *y, errors.Wrap(err, "YAML file open error")
	}
	err = yaml.Unmarshal(b, y)
	if err != nil {
		return *y, errors.Wrap(err, "YAML file parse error")
	}
	return *y, nil
}

func ParseSchemaJSON(h *hschema.HyperSchema, s string) error {
	hs, err := hschema.ReadFile(s)
	if err != nil {
		return errors.Wrap(err, "JSON file parse error")
	}
	h = hs
	return nil
}

func NewDb(dbpath, env string) (*Db, error) {
	conf := &Db{}
	y, err := ParseYAML(dbpath)
	if err != nil {
		return conf, errors.New("Parse YAML")
	}
	if y[env] == nil {
		return conf, errors.New("Env not exist in db config")
	}
	conn := y[env].(map[string]interface{})
	for k, v := range conn {
		switch k {
		case "user", "passwd", "addr", "dbname":
			st, ok := v.(string)
			if ok != true {
				return conf, errors.Wrap(ErrTypeInvalid, k)
			}
			if k == "user" {
				conf.User = st
			} else if k == "passwd" {
				conf.Passwd = st
			} else if k == "addr" {
				conf.Addr = st
			} else {
				conf.DBName = st
			}
		default:
			return nil, errors.Wrap(ErrInvalidDbColumn, k)
		}
	}
	return conf, nil
}

func (c *Column) ParseSchema2Column(s *schema.Schema, h *hschema.HyperSchema) error {
	col, ok := s.Extras["column"].(map[string]interface{})
	if ok != true {
		return ErrTypeInvalid
	}

	if col["name"] == nil || col["type"] == nil {
		return errors.Wrap(ErrEmpty, "name or type")
	}

	for k, v := range col {
		switch k {
		case "name", "type":
			st, ok := v.(string)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			if st == "" {
				return errors.Wrap(ErrEmpty, k)
			}
			if k == "name" {
				c.Name = st
			} else {
				c.Type = st
			}
		case "default":
			st, ok := v.(string)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			c.Default = st
		case "unique", "auto_increment", "not_null":
			b, ok := v.(bool)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			if k == "unique" {
				c.UniqueFlag = b
			} else if k == "auto_increment" {
				c.AutoIncrementFlag = b
			} else {
				c.NotNullFlag = b
			}
		case "foreign_key":
			fk, ok := v.(map[string]interface{})
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			if fk["name"] != nil {
				nm, _ := fk["name"].(string)
				c.FK.Name = nm
			} else {
				return errors.Wrap(ErrEmpty, "foreign key's name")
			}

			if fk["target_table"] != nil {
				tt, _ := fk["target_table"].(string)
				c.FK.TargetTable = tt
			} else {
				return errors.Wrap(ErrEmpty, "foreign key's target table")
			}

			if fk["target_column"] != nil {
				tc, _ := fk["target_column"].(string)
				c.FK.TargetColumn = tc
			} else {
				return errors.Wrap(ErrEmpty, "foreign key's target column")
			}
		}
	}
	return nil
}

func (t *Table) ParseSchema2Table(s *schema.Schema, h *hschema.HyperSchema) error {

	table, ok := s.Extras["table"].(map[string]interface{})
	if ok != true {
		return ErrTypeInvalid
	}

	if table["name"] == nil {
		return errors.Wrap(ErrEmpty, "name")
	}

	for k, v := range table {
		switch k {
		case "primary_key", "index":
			if v == nil {
				return errors.Wrap(ErrEmpty, k)
			}

			l, ok := v.(map[string]interface{})
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}

			ks := []Key{}
			for name, keys := range l {
				ps := []string{}

				keylist, ok := keys.([]interface{})
				if ok != true {
					return errors.Wrap(ErrTypeInvalid, k)
				}
				for _, key := range keylist {
					p, ok := key.(string)
					if ok != true {
						return errors.Wrap(ErrTypeInvalid, k)
					}
					ps = append(ps, p)
				}
				ks = append(ks, Key{Name: name, Target: ps})
			}
			if k == "primary_key" {
				t.PrimaryKey = ks
			} else {
				t.Index = ks
			}
		case "name":
			if v == nil {
				return errors.Wrap(ErrEmpty, k)
			}
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

		c := &Column{Id: k}
		err := c.ParseSchema2Column(v, h)
		if err != nil {
			return errors.Wrapf(err, "Parse %s column error", k)
		}
		t.Column = append(t.Column, *c)

	}

	return nil
}

func ParseSchema2State(h *hschema.HyperSchema, db, env string) (*State, error) {
	s := StateNew()
	conf, err := NewDb(db, env)
	if err != nil {
		return nil, errors.Wrap(err, "Parsing Db parameter: ")
	}
	s.Db = *conf
	for k, v := range h.Definitions {
		if v.Extras["table"] == nil {
			continue
		}
		t := &Table{Id: "#/definitions/" + k}
		err = t.ParseSchema2Table(v, h)
		if err != nil {
			return nil, errors.Wrapf(err, "Parsing %s table", t.Id)

		}
		s.Table = append(s.Table, *t)
	}

	for k, v := range h.Properties {
		if v.Extras["table"] == nil {
			continue
		}
		t := &Table{Id: "#/properties/" + k}
		err = t.ParseSchema2Table(v, h)
		if err != nil {
			return nil, errors.Wrapf(err, "Parsing %s table", t.Id)

		}
		s.Table = append(s.Table, *t)
	}

	for _, l := range h.Links {
		if l.Schema != nil {
			if l.Schema.Extras["table"] == nil {
				continue
			}
			// links dont have JSON References, then use BasePath + href
			t := &Table{Id: "#/links" + l.Href + "/schema"}
			err = t.ParseSchema2Table(l.Schema, h)
			if err != nil {
				return nil, errors.Wrapf(err, "Parsing %s table", t.Id)

			}
			s.Table = append(s.Table, *t)

		}

		if l.TargetSchema != nil {
			if l.TargetSchema.Extras["table"] == nil {
				continue
			}
			t := &Table{Id: "#/links" + l.Href + "/target_schema"}
			err = t.ParseSchema2Table(l.TargetSchema, h)
			if err != nil {
				return nil, errors.Wrapf(err, "Parsing %s table", t.Name)

			}
			s.Table = append(s.Table, *t)
		}
	}
	return s, err
}
