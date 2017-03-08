package migo

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/pkg/errors"

	schema "github.com/lestrrat/go-jsschema"
)

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

func NewColumn(id string) Column {
	return Column{Id: id}
}

func (c Column) isUpdatedFrom(target Column) (bool, error) {
	if c.Id != target.Id {
		return false, errors.New("the target column ID is wrong")
	}
	return !reflect.DeepEqual(c, target), nil
}

func (c *Column) read(schema schema.Schema) error {
	if hasNotColumn(schema) {
		return nil
	}

	b, err := json.Marshal(schema.Extras["column"])
	if err != nil {
		return errors.Wrap(err, "convert to json")
	}
	if err := json.Unmarshal(b, c); err != nil {
		return errors.Wrap(err, "convert to column")
	}

	return nil
}

func (c Column) query() string {
	s := []string{c.Name, c.Type}
	if c.AutoIncrement {
		s = append(s, "AUTO_INCREMENT")
	}
	if c.NotNull {
		s = append(s, "NOT NULL")
	}
	if c.Unique {
		s = append(s, "UNIQUE")
	}

	if c.Default != "" && !isDatetime(c.Type) {
		s = append(s, fmt.Sprintf("DEFAULT '%s'", c.Default))
	}

	if isDatetime(c.Type) {
		if c.AutoUpdate {
			s = append(s, fmt.Sprintf("ON UPDATE CURRENT_TIMESTAMP%s", digit(c.Type)))
		}
		if c.Default == "" {
			s = append(s, fmt.Sprintf("DEFAULT CURRENT_TIMESTAMP%s", digit(c.Type)))
		}
	}
	return strings.Join(s, " ")
}
