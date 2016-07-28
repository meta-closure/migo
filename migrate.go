package mig

import (
	"database/sql"
	"fmt"

	"github.com/pkg/errors"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lestrrat/go-jshschema"
)

const (
	ADD = iota
	DROP
	MODIFY
	CHANGE
	ADDPK
	DROPPK
	ADDINDEX
	DROPINDEX
	ADDFK
	DROPFK
)

type Index struct {
	Name   string
	Target []Column
}

type ForeignKey struct {
	Name   string
	Target Column
}

type Column struct {
	AlterName         string
	Name              string
	Type              string
	PkFlag            bool
	UniqueFlag        bool
	AutoIncrementFlag bool
	NotNull           bool
}

type Command struct {
	FK        ForeignKey
	Index     Index
	PK        []Column
	Table     string
	Column    Column
	AlterType int
}

type Sql struct {
	Db         string
	Url        string
	User       string
	Password   string
	Operations []Command
}

func (s Sql) Check() {

}

func (s *State) SQLBuilder(h *hschema.HyperSchema) (Sql, error) {
	return Sql{}, nil
}

func (c Command) QueryBuilder() (string, error) {
	q := fmt.Sprintf("ALTER TABLE %s", c.Table)

	switch c.AlterType {
	case ADD:
		q += fmt.Sprintf("ADD COLUMN %s", c.Column.Name)
		if c.Column.AutoIncrementFlag == true {
			q += " AUTO_INCREMENT"
		}
		if c.Column.NotNull == true {
			q += " NOT NULL"
		}
		return q, nil

	case DROP:
		q += fmt.Sprintf("DROP %s", c.Column.Name)
		return q, nil

	case MODIFY:
		q += fmt.Sprintf("MODIFY %s", c.Column.Name)
		if c.Column.AutoIncrementFlag == true {
			q += " AUTO_INCREMENT"
		}
		if c.Column.NotNull == true {
			q += " NOT NULL"
		}
		return q, nil

	case CHANGE:
		q += fmt.Sprintf("CHANGE COLUMN %s %s", c.Column.Name, c.Column.AlterName)
		if c.Column.AutoIncrementFlag == true {
			q += " AUTO_INCREMENT"
		}
		if c.Column.NotNull == true {
			q += " NOT NULL"
		}
		return q, nil

	case ADDPK:
		pk := ""
		for _, i := range c.PK {
			pk += i.Name + ","
		}
		q += fmt.Sprint("ADD PRIMARY KEY (%s)", pk)
		return q, nil

	case DROPPK:
		q += fmt.Sprint("DROP PRIMARY KEY")
		return q, nil

	case ADDINDEX:
		idx := ""
		for _, i := range c.Index.Target {
			idx += i.Name + ","
		}
		q += fmt.Sprintf("ADD INDEX  %s (%s)", c.Index.Name, idx)
		return q, nil

	case DROPINDEX:
		q += fmt.Sprint("DROP INDEX %s", c.Index.Name)
		return q, nil

	case ADDFK:
		q += fmt.Sprintf("ADD CONSTRAINT %s FOREIGM KEY %s REFERENCE %s", c.FK.Name, c.Column.Name, c.FK.Target.Name)
		return q, nil

	case DROPFK:
		q += fmt.Sprintf("DROP FOREIGN KEY %s", c.FK.Name)
		return q, nil
	default:
		return "", nil

	}
}

func (s Sql) ConnectionBuilder() string {
	return ""
}

func (s Sql) Migrate() error {
	db, err := sql.Open("mysql", "root:@/mig")
	if err != nil {
		return err
	}
	defer db.Close()

	qs := make([]string, len(s.Operations))
	for _, c := range s.Operations {
		q, err := c.QueryBuilder()
		if err != nil {
			return errors.Wrapf(err, "Table: %s, Column: %s,s Query Build Failed", c.Table, c.Column.Name)
		}
		qs = append(qs, q)
	}

	for _, q := range qs {
		_, err = db.Exec(q)
		if err != nil {
			return errors.Wrapf(err, "Query: %s", q)
		}
	}

	return nil
}
