package migo

import (
	"fmt"
	"io/ioutil"
	"time"

	"encoding/json"

	"github.com/ghodss/yaml"
	"github.com/lestrrat/go-jshschema"
	"github.com/lestrrat/go-jsschema"
	"github.com/pkg/errors"
)

type State struct {
	DB       DB
	Tables   []Table
	UpdateAt time.Time
}

type Table struct {
	Id         string
	Name       string
	ForeignKey []ForeignKey
	PrimaryKey []Key
	Index      []Key
	Column     []Column
}

type Key struct {
	Target []string
	Name   string
}

type ForeignKey struct {
	Name          string
	SourceTable   string
	SourceColumn  string
	TargetTable   string
	TargetColumn  string
	UpdateCascade bool
	DeleteCascade bool
}

type Column struct {
	Id            string
	BeforeName    string
	Name          string
	Type          string
	Unique        bool
	AutoIncrement bool
	AutoUpdate    bool
	NotNull       bool
	Default       string
}

func NewState() State {
	return State{
		UpdateAt: time.Now(),
	}
}

func readYAMLFormatSchema(h *hschema.HyperSchema, filePath string) error {
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return errors.Wrap(err, "YAML file open error")
	}

	y := map[string]interface{}{}
	err = yaml.Unmarshal(b, &y)
	if err != nil {
		return errors.Wrap(err, "YAML file parse error")
	}

	h.Extract(y)
	return nil
}

func readJSONFormatSchema(h *hschema.HyperSchema, filePath string) error {
	h, err := hschema.ReadFile(filePath)
	if err != nil {
		return errors.Wrap(err, "JSON file parse error")
	}
	return nil
}

func (e Environment) hasNotEnv(env string) bool {
	for k := range e.Config {
		if k == env {
			return false
		}
	}
	return true
}

func (c *Column) read(s schema.Schema) error {
	if hasNotColumn(s) {
		return nil
	}

	b, err := json.Marshal(s.Extras["column"])
	if err != nil {
		return errors.Wrap(err, "convert to json")
	}
	if err := json.Unmarshal(b, c); err != nil {
		return errors.Wrap(err, "convert to column")
	}

	return nil
}

func definitonsID(key string) string {
	return fmt.Sprintf("#/definitions/%s", key)
}
func propertiesID(key string) string {
	return fmt.Sprintf("#/properties/$s", key)
}

func hasNotTable(s *schema.Schema) bool {
	if s.Extras["table"] != nil {
		return false
	}
	return true
}

func NewTable(id string) *Table {
	return &Table{Id: id}
}

func (t *Table) setName(i interface{}) error {
	s, ok := i.(string)
	if !ok {
		return errors.New("given type is not string")
	}
	t.Name = s
	return nil
}

func hasNotColumn(s schema.Schema) bool {
	if s.Extras["column"] != nil {
		return true
	}
	return false
}

func NewColumn(id string) Column {
	return Column{Id: id}
}

func hasNotForeignKey(s schema.Schema) bool {
	if hasNotColumn(s) {
		return false
	}

	m, ok := s.Extras["column"].(map[string]interface{})
	if !ok {
		return false
	}

	if m["foreign_key"] != nil {
		return true
	}
	return false
}

func NewForeignKey(t string) ForeignKey {
	return ForeignKey{SourceTable: t}
}

func (fk *ForeignKey) read(s schema.Schema) error {
	if hasNotForeignKey(s) {
		return errors.New("column not found")
	}

	m, ok := s.Extras["column"].(map[string]interface{})
	if !ok {
		return errors.New("fail to convert type from column")
	}

	k := ForeignKey{}
	b, err := json.Marshal(m["foreign_key"])
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, &k); err != nil {
		return err
	}

	fk = &k
	return nil
}

func stringList(i interface{}) ([]string, error) {
	m, ok := i.([]interface{})
	if !ok {
		return nil, errors.New("fail to convert []interface{} type")
	}
	list := []string{}
	for _, v := range m {
		s, ok := v.(string)
		if !ok {
			return nil, errors.New("fail to convert string type")
		}
		list = append(list, s)
	}

	return list, nil
}

func NewKey(s string) Key {
	return Key{Name: s}
}

func readKeys(i interface{}) ([]Key, error) {
	m, ok := i.(map[string]interface{})
	if !ok {
		return nil, errors.New("fail to convert type to map[string]interface{}")
	}

	keys := []Key{}
	for k, v := range m {
		var err error
		key := NewKey(k)
		key.Target, err = stringList(v)
		if err != nil {
			return nil, errors.Wrap(err, "generating keys")
		}

		keys = append(keys, key)
	}

	return keys, nil
}

func (t Table) selectPrimaryKey(m map[string]interface{}) ([]Key, error) {
	if m["primary_key"] == nil {
		return nil, nil
	}
	return readKeys(m["primary_key"])
}

func (t Table) selectIndex(m map[string]interface{}) ([]Key, error) {
	if m["index"] == nil {
		return nil, nil
	}
	return readKeys(m["index"])
}

func (t *Table) read(root *hschema.HyperSchema, schema *schema.Schema) error {
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

		if hasNotForeignKey(*s) {
			continue
		}
		fk := NewForeignKey(t.Name)
		if err := fk.read(*s); err != nil {
			return errors.Wrap(err, "reading foreing key")
		}
		t.ForeignKey = append(t.ForeignKey, fk)
	}

	var err error
	t.PrimaryKey, err = t.selectPrimaryKey(m)
	if err != nil {
		return errors.Wrap(err, "reading primary key")
	}
	t.Index, err = t.selectIndex(m)
	if err != nil {
		return errors.Wrap(err, "setting index")
	}
	return nil
}

func NewStateFromSchema(schema *hschema.HyperSchema) (State, error) {
	var err error
	s := NewState()
	for k, v := range schema.Definitions {
		if hasNotTable(v) {
			continue
		}
		t := NewTable(definitonsID(k))
		if err := t.read(schema, v); err != nil {
			return s, errors.Wrap(err, "in definitions")
		}
		s.Tables = append(s.Tables, *t)
	}

	for k, v := range schema.Properties {
		if hasNotTable(v) {
			continue
		}
		t := NewTable(propertiesID(k))
		if t.read(schema, v); err != nil {
			return s, errors.Wrap(err, "in properties")
		}
		s.Tables = append(s.Tables, *t)
	}
	return s, err
}

func NewStateFromYAML(filePath string) (State, error) {
	s := NewState()

	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return s, err
	}

	if err = yaml.Unmarshal(b, s); err != nil {
		return s, err
	}

	return s, nil
}

func (s State) save(filePath string) error {
	b, err := yaml.Marshal(s)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(filePath, b, 0777)
	if err != nil {
		return err
	}
	return nil
}
