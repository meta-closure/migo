package migo

import (
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
	Raw           map[string]interface{} `json:"-"`
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
	if !hasForeignKey(s) {
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

	if fk.Raw["name"] == nil {
		return errors.New("name is not found")
	}
	fk.Name, ok = fk.Raw["name"].(string)
	if !ok {
		return errors.New("convert name to string type")
	}

	if fk.Raw["delete_cascade"] != nil {
		fk.DeleteCascade, ok = fk.Raw["delete_cascade"].(bool)
		if !ok {
			return errors.New("convert delete_cascade to bool type")
		}
	}

	if fk.Raw["update_cascade"] != nil {
		fk.DeleteCascade, ok = fk.Raw["update_cascade"].(bool)
		if !ok {
			return errors.New("convert update_cascade to bool type")
		}
	}
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
	t, err := s.findTableWithID(str)
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
	c, err := t.findColumnWithID(str)
	if err != nil {
		return fmt.Errorf("column %s in table %s is not found", fk.Raw["target_column"], t.Name)
	}
	fk.TargetColumn = c
	return nil
}
