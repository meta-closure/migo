package mig

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lestrrat/go-jshschema"
	"github.com/lestrrat/go-jsschema"
	"github.com/pkg/errors"
)

var (
	ErrEmpty       = errors.New("Required parameter is empty")
	ErrTypeInvalid = errors.New("Invalid type error")
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
	TBLCHANGE
)

type PrimaryKey struct {
	Name   string
	Target []Column
}

type Index struct {
	Name   string
	Target []Column
}

type ForeignKey struct {
	Name         string `json:"name"`
	TargetTable  string `json:"target_table"`
	TargetColumn string `json:"target_column"`
}

type Column struct {
	BeforeName        string     `json:"before_name"`
	Name              string     `json:"name"`
	Type              string     `json:"type"`
	FK                ForeignKey `json:"foreign_key"`
	UniqueFlag        bool       `json:"unique"`
	AutoIncrementFlag bool       `json:"auto_increment"`
	NotNullFlag       bool       `json:"not_null"`
}

type Operation struct {
	UniqueFlag        bool
	AutoIncrementFlag bool
	NotNullFlag       bool
	FK                ForeignKey
	Table             string
	Column            Column
	AlterType         int
	PK                PrimaryKey
	Index             Index
}

type Db struct {
	Type     string
	User     string
	Password string
	Host     string
	Port     int
}

type Sql struct {
	Db         Db
	Operations []Operation
}

func (s Sql) Check() {

}

func (db *Db) GetDbConnect(d *hschema.HyperSchema) error {
	if d.Extras["db"] == nil {
		return ErrEmpty
	}
	conn := d.Extras["db"].(map[string]interface{})
	for k, v := range conn {
		if k == "user" {
			s, ok := v.(string)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			db.User = s
		}
		if k == "password" {
			s, ok := v.(string)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			db.Password = s
		}
		if k == "host" {
			s, ok := v.(string)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			db.Host = s
		}
		if k == "port" {
			i, ok := v.(int)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			db.Port = i
		}
		if k == "type" {
			s, ok := v.(string)
			if ok != true {
				return errors.Wrap(ErrTypeInvalid, k)
			}
			if s != "mysql" {
				return errors.Wrap(ErrTypeInvalid, "Only support MySQL")
			}
			db.Type = s
		}
	}
	return nil
}

func (s *Sql) GetSql(st *State, sc *schema.Schema, hs *hschema.HyperSchema) error {

	return nil
}

func (s *State) SQLBuilder(h *hschema.HyperSchema) (*Sql, error) {
	sql := &Sql{}
	for _, j := range h.Definitions {
		if j.Extras["db"] != nil {
			sql.GetSql(s, j, h)
		}
	}

	for _, l := range h.Links {
		if l.Schema != nil && l.Schema.Extras["db"] != nil {
			err := sql.GetSql(s, l.Schema, h)
			if err != nil {
				return nil, err
			}
		}
		if l.TargetSchema != nil && l.TargetSchema.Extras["db"] != nil {
			err := sql.GetSql(s, l.TargetSchema, h)
			if err != nil {
				return nil, err
			}
		}
	}
	return sql, nil
}

func (c Operation) QueryBuilder() (string, error) {
	q := fmt.Sprintf("ALTER TABLE %s", c.Table)

	switch c.AlterType {
	case ADD:
		q += fmt.Sprintf("ADD COLUMN %s", c.Column.Name)
		if c.Column.AutoIncrementFlag == true {
			q += " AUTO_INCREMENT"
		}
		if c.Column.NotNullFlag == true {
			q += " NOT NULL"
		}
		if c.Column.UniqueFlag == true {
			q += " UNIQUE"
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
		if c.Column.NotNullFlag == true {
			q += " NOT NULL"
		}
		if c.Column.UniqueFlag == true {
			q += " UNIQUE"
		}
		return q, nil

	case CHANGE:
		q += fmt.Sprintf("CHANGE COLUMN %s %s", c.Column.BeforeName, c.Column.Name)
		if c.Column.AutoIncrementFlag == true {
			q += " AUTO_INCREMENT"
		}
		if c.Column.NotNullFlag == true {
			q += " NOT NULL"
		}
		if c.Column.UniqueFlag == true {
			q += " UNIQUE"
		}
		return q, nil

	case ADDPK:
		pk := ""
		for _, i := range c.PK.Target {
			pk += i.Name + ","
		}
		q += fmt.Sprint("ADD PRIMARY KEY %s (%s)", c.PK.Name, pk)
		return q, nil

	case DROPPK:
		q += fmt.Sprint("DROP PRIMARY KEY %s", c.PK.Name)
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
		q += fmt.Sprintf("ADD CONSTRAINT %s FOREIGM KEY %s REFERENCE %s(%s)", c.FK.Name, c.Column.Name, c.FK.TargetTable, c.FK.TargetColumn)
		return q, nil

	case DROPFK:
		q += fmt.Sprintf("DROP FOREIGN KEY %s", c.FK.Name)
		return q, nil

	default:
		return "", nil

	}
}

func (s *Sql) ConnectionBuilder() string {
	return ""
}

func (s *Sql) Migrate() error {
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
