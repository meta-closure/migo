package migo

import (
	"io/ioutil"
	"sort"
	"time"

	"github.com/ghodss/yaml"
	"github.com/lestrrat/go-jshschema"
	"github.com/lestrrat/go-jsschema"
	"github.com/pkg/errors"
)

type State struct {
	DB         DB          `json:"db"`
	Tables     Tables      `json:"tables"`
	ForeignKey ForeignKeys `json:"foreign_key"`
	UpdatedAt  time.Time   `json:"updated_at"`
}

func NewState() State {
	return State{
		UpdatedAt: time.Now(),
	}
}

func (s State) findForeignKeyWithSourceTableId(id string) []ForeignKey {
	fks := []ForeignKey{}
	for _, fk := range s.ForeignKey {
		if fk.SourceTable.Id == id {
			fks = append(fks, fk)
		}
	}
	return fks
}

func (s State) findForeignKeyWithSource(tid, cid string) []ForeignKey {
	fks := []ForeignKey{}
	for _, fk := range s.ForeignKey {
		if fk.SourceColumn.Id == cid && fk.SourceTable.Id == tid {
			fks = append(fks, fk)
		}
	}
	return fks
}

func (s State) findForeignKeyWithTarget(tid, cid string) []ForeignKey {
	included := []ForeignKey{}
	for _, fk := range s.ForeignKey {
		if fk.TargetColumn.Id == cid && fk.TargetTable.Id == tid {
			included = append(included, fk)
		}
	}
	return included
}

func readYAMLFormatSchema(root *hschema.HyperSchema, filePath string) error {
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return errors.Wrap(err, "YAML file open error")
	}

	y := map[string]interface{}{}
	err = yaml.Unmarshal(b, &y)
	if err != nil {
		return errors.Wrap(err, "YAML file parse error")
	}

	root.Extract(y)
	return nil
}

func readJSONFormatSchema(root *hschema.HyperSchema, filePath string) error {
	root, err := hschema.ReadFile(filePath)
	if err != nil {
		return errors.Wrap(err, "JSON file parse error")
	}
	return nil
}

func hasNotTable(schema *schema.Schema) bool {
	if schema.Extras["table"] != nil {
		return false
	}
	return true
}

func hasNotColumn(schema schema.Schema) bool {
	if schema.Extras["column"] == nil {
		return true
	}
	return false
}

func hasForeignKey(schema schema.Schema) bool {
	if hasNotColumn(schema) {
		return false
	}

	m, ok := schema.Extras["column"].(map[string]interface{})
	if !ok {
		return false
	}

	if m["foreign_key"] != nil {
		return true
	}
	return false
}

func findForeingKey(root *hschema.HyperSchema, s State) ([]ForeignKey, error) {
	fks := []ForeignKey{}
	for k, v := range root.Definitions {
		t, err := s.findTableWithID(definitonsID(k))
		if err != nil {
			continue
		}
		for id, column := range v.Properties {
			c, err := t.findColumnWithID(id)
			if err != nil {
				continue
			}
			if !hasForeignKey(*column) {
				continue
			}

			fk := NewForeignKey(t, c)
			if err := fk.read(*column); err != nil {
				return nil, errors.Wrapf(err, "fail to read from id %s column", id)
			}
			fks = append(fks, fk)
		}
	}

	for k, v := range root.Properties {
		t, err := s.findTableWithID(propertiesID(k))
		if err != nil {
			continue
		}
		for id, column := range v.Properties {
			c, err := t.findColumnWithID(id)
			if err != nil {
				continue
			}
			if !hasForeignKey(*column) {
				continue
			}

			fk := NewForeignKey(t, c)
			if err := fk.read(*column); err != nil {
				return nil, errors.Wrapf(err, "fail to read from id %s column", id)
			}
			fks = append(fks, fk)
		}
	}

	return fks, nil
}

func NewStateFromSchema(root *hschema.HyperSchema) (State, error) {
	var err error
	s := NewState()
	for k, v := range root.Definitions {
		if hasNotTable(v) {
			continue
		}
		t := NewTable(definitonsID(k))
		if err := t.read(v); err != nil {
			return s, errors.Wrap(err, "in definitions")
		}
		s.Tables = append(s.Tables, *t)
	}

	for k, v := range root.Properties {
		if hasNotTable(v) {
			continue
		}
		t := NewTable(propertiesID(k))
		if t.read(v); err != nil {
			return s, errors.Wrap(err, "in properties")
		}
		s.Tables = append(s.Tables, *t)
	}

	fks, err := findForeingKey(root, s)
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

	if err = yaml.Unmarshal(b, &s); err != nil {
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

func (s State) findTableWithID(id string) (Table, error) {
	for _, t := range s.Tables {
		if t.Id == id {
			return t, nil
		}
	}
	return Table{}, errors.New("table not found")
}

func (s State) hasTable(t Table) bool {
	if _, err := s.findTableWithID(t.Id); err != nil {
		return false
	}
	return true
}

func (s State) findTablesNotIn(target State) ([]Table, error) {
	filterd := []Table{}
	for _, t := range s.Tables {
		if !target.hasTable(t) {
			filterd = append(filterd, t)
		}
	}

	return filterd, nil
}

func (s State) findTablesIn(target State) ([]Table, error) {
	filterd := []Table{}
	for _, t := range s.Tables {
		if target.hasTable(t) {
			filterd = append(filterd, t)
		}
	}
	return filterd, nil
}
