package migo

import (
	"fmt"

	"github.com/pkg/errors"

	schema "github.com/lestrrat/go-jsschema"
)

type Table struct {
	Id         string  `json:"id"`
	Name       string  `json:"name"`
	PrimaryKey Keys    `json"primary_key"`
	Index      Keys    `json:"index"`
	Column     Columns `json:"column"`
}

type Tables []Table

func (t Tables) Len() int {
	return len(t)
}

func (t Tables) Less(i, j int) bool {
	return t[i].Id < t[j].Id
}

func (t Tables) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func NewTable(id string) *Table {
	return &Table{Id: id}
}

func (t Table) findKeys(i interface{}) ([]Key, error) {
	m, ok := i.(map[string]interface{})
	if !ok {
		return nil, errors.New("fail to convert type to map[string]interface{}")
	}

	keys := []Key{}
	for k, v := range m {
		var err error
		key := NewKey(k)
		key.Target, err = targetList(t, v)
		if err != nil {
			return nil, errors.Wrap(err, "getting key's target list")
		}

		keys = append(keys, key)
	}

	return keys, nil
}

func (t Table) findPrimaryKey(m map[string]interface{}) ([]Key, error) {
	if m["primary_key"] == nil {
		return nil, nil
	}
	return t.findKeys(m["primary_key"])
}

func (t Table) findIndex(m map[string]interface{}) ([]Key, error) {
	if m["index"] == nil {
		return nil, nil
	}
	return t.findKeys(m["index"])
}

func (t *Table) read(schema *schema.Schema) error {
	if hasNotTable(schema) {
		return nil
	}
	m, ok := schema.Extras["table"].(map[string]interface{})
	if !ok {
		return errors.New("convert from interface{} to map[string]interface{}")
	}
	if err := t.setName(m["name"]); err != nil {
		return errors.Wrap(err, "setting name to table")
	}

	for k, s := range schema.Properties {
		if hasNotColumn(*s) {
			continue
		}
		c := NewColumn(k)
		if err := c.read(*s); err != nil {
			return errors.Wrap(err, "reading columns")
		}
		t.Column = append(t.Column, c)
	}

	var err error
	t.PrimaryKey, err = t.findPrimaryKey(m)
	if err != nil {
		return errors.Wrap(err, "reading primary key")
	}
	t.Index, err = t.findIndex(m)
	if err != nil {
		return errors.Wrap(err, "setting index")
	}
	return nil
}

func (t *Table) setName(i interface{}) error {
	s, ok := i.(string)
	if !ok {
		return fmt.Errorf("fail to convert string type from %s", i)
	}
	t.Name = s
	return nil
}

func (t Table) findIndexWithName(s string) (Key, error) {
	for _, k := range t.Index {
		if k.Name == s {
			return k, nil
		}
	}
	return Key{}, errors.New("index not found")
}

func (t Table) hasIndex(k Key) bool {
	if _, err := t.findIndexWithName(k.Name); err != nil {
		return false
	}
	return true
}

func (t Table) findPrimaryKeyWithName(name string) (Key, error) {
	for _, k := range t.PrimaryKey {
		if k.Name == name {
			return k, nil
		}
	}
	return Key{}, errors.New("primary key not found")
}

func (t Table) hasPrimaryKey(k Key) bool {
	if _, err := t.findPrimaryKeyWithName(k.Name); err != nil {
		return false
	}
	return true
}

func (t Table) findColumnWithID(id string) (Column, error) {
	for _, c := range t.Column {
		if c.Id == id {
			return c, nil
		}
	}
	return Column{}, errors.New("column not found")
}

func (t Table) hasColumn(c Column) bool {
	if _, err := t.findColumnWithID(c.Id); err != nil {
		return false
	}
	return true
}
