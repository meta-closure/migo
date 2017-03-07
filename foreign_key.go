package migo

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"

	schema "github.com/lestrrat/go-jsschema"
)

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

func NewForeignKey(sourceTable Table, sourceColumn Column) ForeignKey {
	return ForeignKey{
		SourceColumn: sourceColumn,
		SourceTable:  sourceTable,
	}
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
