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
	Db       Db        `json:"db"`
	Table    []Table   `json:"table"`
	UpdateAt time.Time `json:"updated_at"`
}

type Db struct {
	User   string
	Passwd string
	Addr   string
	DBName string
}

type Table struct {
	Id         string   `json:"id"`
	Name       string   `json:"table_name"`
	PrimaryKey []Key    `json:"primary_key"`
	Index      []Key    `json:"index"`
	Column     []Column `json:"column"`
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
	b, err := ioutil.ReadFile(s)
	if err != nil {
		return errors.Wrap(err, "YAML file open error")
	}
	y := &map[string]interface{}{}
	err = yaml.Unmarshal(b, y)
	if err != nil {
		return errors.Wrap(err, "YAML file parse error")
	}
	h.Extract(*y)

	return nil
}

func ParseSchemaJSON(h *hschema.HyperSchema, s string) error {
	hs, err := hschema.ReadFile(s)
	if err != nil {
		return errors.Wrap(err, "JSON file parse error")
	}
	h = hs
	return nil
}

func (db *Db) ParseSchema2Db(dbpath, env string) error {
	b, err := ioutil.ReadFile(dbpath)
	if err != nil {
		return errors.Wrap(err, "YAML file open error")
	}
	y := &map[string]interface{}{}
	err = yaml.Unmarshal(b, y)
	if err != nil {
		return errors.Wrap(err, "YAML file parse error")
	}
	if env == "" {
		env = "default"
	}
	conn := (*y)[env].(map[string]interface{})
	for k, v := range conn {
		switch k {
		case "user":
			st, ok := v.(string)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			db.User = st

		case "passwd":
			if v == nil {
				continue
			}
			st, ok := v.(string)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			db.Passwd = st

		case "addr":
			st, ok := v.(string)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			db.Addr = st

		case "dbname":
			st, ok := v.(string)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			db.DBName = st
		default:
			continue
		}
	}
	return nil
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
			if st == "" {
				return errors.Wrap(ErrEmpty, k)
			}

			c.Name = st
		case "type":
			st, ok := v.(string)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			if st == "" {
				return errors.Wrap(ErrEmpty, k)
			}

			c.Type = st
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

			if t := fk["target_table"]; t != nil {
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

	for k, v := range table {
		switch k {
		case "primary_key":
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
			t.PrimaryKey = ks

		case "index":
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
			t.Index = ks

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
	if db == "" {
		db = "./database.yml"
	}
	err := s.Db.ParseSchema2Db(db, env)
	if err != nil {
		return nil, errors.Wrap(err, "Parsing Db parameter: ")
	}
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

func (s *State) Update(path string) error {
	b, err := yaml.Marshal(s)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(path, b, 0777)
	if err != nil {
		return err
	}

	return nil
}
