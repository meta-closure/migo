package migo

import (
	"fmt"
	"io/ioutil"
	"sort"
	"time"

	"encoding/json"

	"github.com/ghodss/yaml"
	"github.com/lestrrat/go-jshschema"
	"github.com/lestrrat/go-jsschema"
	"github.com/pkg/errors"
)

type State struct {
	DB         DB
	Tables     Tables
	ForeignKey ForeignKeys
	UpdateAt   time.Time
}

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

type Key struct {
	Target Columns
	Name   string
}

type Keys []Key

func (k Keys) Len() int {
	return len(k)
}

func (k Keys) Less(i, j int) bool {
	return k[i].Name < k[j].Name
}

func (k Keys) Swap(i, j int) {
	k[i], k[j] = k[j], k[i]
}

type ForeignKey struct {
	Name          string                 `json:"name"`
	SourceTable   Table                  `json:"source_table"`
	SourceColumn  Column                 `json:"source_column"`
	TargetTable   Table                  `json:"target_table"`
	TargetColumn  Column                 `json:"column"`
	UpdateCascade bool                   `json:"update_cascade"`
	DeleteCascade bool                   `json:"delete_cascade"`
	Raw           map[string]interface{} `json"-"`
}
type ForeignKeys []ForeignKey

func (k ForeignKeys) Len() int {
	return len(k)
}

func (k ForeignKeys) Less(i, j int) bool {
	return k[i].Name < k[j].Name
}

func (k ForeignKeys) Swap(i, j int) {
	k[i], k[j] = k[j], k[i]
}

type Column struct {
	Id            string `json:"id"`
	Name          string `json:"name"`
	Type          string `json:"type"`
	Unique        bool   `json:"unique"`
	AutoIncrement bool   `json:"auto_increment"`
	AutoUpdate    bool   `json:"auto_update"`
	NotNull       bool   `json:"not_null"`
	Default       string `json:"default"`
}

type Columns []Column

func (c Columns) Len() int {
	return len(c)
}

func (c Columns) Less(i, j int) bool {
	return c[i].Id < c[j].Id
}

func (c Columns) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func NewState() State {
	return State{
		UpdateAt: time.Now(),
	}
}

func (s State) selectForeignKeyWithSourceTableId(id string) []ForeignKey {
	fks := []ForeignKey{}
	for _, fk := range s.ForeignKey {
		if fk.SourceTable.Id == id {
			fks = append(fks, fk)
		}
	}
	return fks
}

func (s State) selectForeignKeyWithSource(tid, cid string) []ForeignKey {
	fks := []ForeignKey{}
	for _, fk := range s.ForeignKey {
		if fk.SourceColumn.Id == cid && fk.SourceTable.Id == tid {
			fks = append(fks, fk)
		}
	}
	return fks
}

func (s State) selectForeignKeyWithTarget(tid, cid string) []ForeignKey {
	included := []ForeignKey{}
	for _, fk := range s.ForeignKey {
		if fk.TargetColumn.Id == cid && fk.TargetTable.Id == tid {
			included = append(included, fk)
		}
	}
	return included
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
	if s.Extras["column"] == nil {
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

	if m["foreign_key"] == nil {
		return true
	}
	return false
}

func NewForeignKey(sourceTable Table, sourceColumn Column) ForeignKey {
	return ForeignKey{
		SourceColumn: sourceColumn,
		SourceTable:  sourceTable,
	}
}

func selectForeingKey(root *hschema.HyperSchema, s State) ([]ForeignKey, error) {
	fks := []ForeignKey{}
	for k, v := range root.Definitions {
		t, err := s.selectTableWithID(definitonsID(k))
		if err != nil {
			continue
		}
		for id, column := range v.Properties {
			if hasNotForeignKey(*column) {
				continue
			}
			c, err := t.selectColumnWithID(id)
			if err != nil {
				return nil, err
			}
			fk := NewForeignKey(t, c)
			if err := fk.read(*column); err != nil {
				return nil, err
			}
			fks = append(fks, fk)
		}
	}
	for k, v := range root.Properties {
		t, err := s.selectTableWithID(propertiesID(k))
		if err != nil {
			continue
		}
		for id, column := range v.Properties {
			if hasNotForeignKey(*column) {
				continue
			}
			c, err := t.selectColumnWithID(id)
			if err != nil {
				return nil, err
			}
			fk := NewForeignKey(t, c)
			if err := fk.read(*column); err != nil {
				return nil, err
			}
			fks = append(fks, fk)
		}
	}
	return fks, nil
}

func (fk *ForeignKey) resolve(s State) error {
	if fk.Raw["target_table"] == nil {
		return errors.New("target_table is null")
	}
	str, ok := fk.Raw["target_table"].(string)
	if !ok {
		return fmt.Errorf("%s have not string type", fk.Raw["target_table"])
	}
	t, err := s.selectTableWithID(str)
	if err != nil {
		return fmt.Errorf("JSON Path %s table is not found", fk.Raw["target_table"])
	}
	fk.TargetTable = t

	if fk.Raw["target_column"] == nil {
		return errors.New("source_column is null")
	}
	str, ok = fk.Raw["target_column"].(string)
	if !ok {
		return fmt.Errorf("%s have not string type", fk.Raw["target_column"])
	}
	c, err := t.selectColumnWithID(str)
	if err != nil {
		return fmt.Errorf("column %s in table %s is not found", fk.Raw["target_column"], t.Name)
	}
	fk.TargetColumn = c
	return nil
}

func (fk *ForeignKey) read(s schema.Schema) error {
	if hasNotForeignKey(s) {
		return errors.New("foreign not found")
	}

	m, ok := s.Extras["column"].(map[string]interface{})
	if !ok {
		return errors.New("column: fail to convert type to column")
	}

	fk.Raw, ok = m["foreign_key"].(map[string]interface{})
	if !ok {
		return errors.New("foreign key: fail to convert type")
	}

	f := ForeignKey{}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(b, &f); err != nil {
		return err
	}

	fk.DeleteCascade = f.DeleteCascade
	fk.UpdateCascade = f.UpdateCascade
	return nil
}

func keyList(t Table, i interface{}) (Columns, error) {
	m, ok := i.([]interface{})
	if !ok {
		return nil, errors.New("fail to convert []interface{} type")
	}
	targets := Columns{}
	for _, v := range m {
		s, ok := v.(string)
		if !ok {
			return nil, errors.New("fail to convert string type")
		}
		c, err := t.selectColumnWithID(s)
		if err != nil {
			return nil, fmt.Errorf("fail to searching column in table %s", t.Name)
		}
		targets = append(targets, c)
	}

	return targets, nil
}

func NewKey(s string) Key {
	return Key{Name: s}
}

func (t Table) keys(i interface{}) ([]Key, error) {
	m, ok := i.(map[string]interface{})
	if !ok {
		return nil, errors.New("fail to convert type to map[string]interface{}")
	}

	keys := []Key{}
	for k, v := range m {
		var err error
		key := NewKey(k)
		key.Target, err = keyList(t, v)
		if err != nil {
			return nil, errors.Wrap(err, "getting key's target list")
		}

		keys = append(keys, key)
	}

	return keys, nil
}

func (t Table) selectPrimaryKey(m map[string]interface{}) ([]Key, error) {
	if m["primary_key"] == nil {
		return nil, nil
	}
	return t.keys(m["primary_key"])
}

func (t Table) selectIndex(m map[string]interface{}) ([]Key, error) {
	if m["index"] == nil {
		return nil, nil
	}
	return t.keys(m["index"])
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
		if err := t.read(v); err != nil {
			return s, errors.Wrap(err, "in definitions")
		}
		s.Tables = append(s.Tables, *t)
	}

	for k, v := range schema.Properties {
		if hasNotTable(v) {
			continue
		}
		t := NewTable(propertiesID(k))
		if t.read(v); err != nil {
			return s, errors.Wrap(err, "in properties")
		}
		s.Tables = append(s.Tables, *t)
	}
	fks, err := selectForeingKey(schema, s)
	if err != nil {
		return s, errors.Wrap(err, "searching foreing key")
	}
	for _, fk := range fks {
		if err := fk.resolve(s); err != nil {
			return s, errors.Wrap(err, "fail to resolve JSON Schema id")
		}
		s.ForeignKey = append(s.ForeignKey, fk)
	}

	return s.Sort(), err
}

func (s State) Sort() State {
	sort.Sort(s.ForeignKey)
	sort.Sort(s.Tables)
	for i := range s.Tables {
		sort.Sort(s.Tables[i].Column)
		sort.Sort(s.Tables[i].PrimaryKey)
		for t := range s.Tables[i].PrimaryKey {
			sort.Sort(s.Tables[i].PrimaryKey[t].Target)
		}
		sort.Sort(s.Tables[i].Index)
		for t := range s.Tables[i].Index {
			sort.Sort(s.Tables[i].Index[t].Target)
		}
	}
	return s
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
