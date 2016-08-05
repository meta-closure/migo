package mig

import (
	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

var (
	ErrEmpty       = errors.New("Required parameter is empty")
	ErrTypeInvalid = errors.New("Invalid type error")
)

const (
	TBLADD = iota
	TBLDROP
	TBLCHANGE
	ADD
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

type Key struct {
	Name   string
	Target []string
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
	FK            ForeignKey
	Table         string
	BeforeTable   string
	Column        Column
	OperationType int
	PK            Key
	Index         Key
}

type Sql struct {
	DbConf     mysql.Config
	Operations []Operation
}

func (s Sql) Check() {

}

func (s *State) ExistTable(st string) bool {
	return false
}

func (t Table) ExistColumn(st string) bool {
	return false
}

func (s *State) GetTable(st string) (Table, bool) {
	return Table{}, false
}

func (t Table) GetColumn(st string) (Column, bool) {
	return Column{}, false
}

func GetTableOperation(t Table, flag int) Operation {
	return Operation{
		Table:         t.Name,
		BeforeTable:   t.BeforeName,
		OperationType: flag,
		PK:            t.PrimaryKey,
		Index:         t.Index,
	}
}

func GetColumnOperation(t Table, c Column, flag int) Operation {
	return Operation{
		Table:         t.Name,
		BeforeTable:   t.BeforeName,
		OperationType: flag,
		Column:        c,
	}
}

func (o *State) SQLBuilder(n *State) (*Sql, error) {

	// Setting database connection configure
	sql := &Sql{
		DbConf: mysql.Config{
			User:   n.Db.User,
			Addr:   n.Db.Addr,
			Net:    "tcp",
			Passwd: n.Db.Passwd,
		},
	}
	// if given db connection data are odd, not lock old state
	if o.Db.Addr != o.Db.Addr {
		o = StateNew()
	}

	var op Operation

	// add and change table check
	for _, tab := range n.Table {
		if tab.BeforeName != "" {
			ok := o.ExistTable(tab.BeforeName)
			if ok != true {
				return nil, errors.New("In changing table, before table not exist : " + tab.BeforeName)
			}
			op = GetTableOperation(tab, TBLCHANGE)
		} else {
			ok := o.ExistTable(tab.Name)
			if ok == true {
				continue
			}
			op = GetTableOperation(tab, TBLADD)
		}
		sql.Operations = append(sql.Operations, op)
	}

	// delete table and delete column check
	for _, old := range o.Table {

		var b bool
		var tab Table
		for _, tab = range n.Table {
			b = false
			if old.Name == tab.Name || old.Name == tab.BeforeName {
				b = true
				break
			}
		}

		// drop table
		if b != true {
			op = GetTableOperation(old, TBLDROP)
			sql.Operations = append(sql.Operations, op)
			continue
		}

		for _, oldcol := range old.Columns {
			b = false
			for _, col := range tab.Columns {
				if oldcol.Name == col.Name || oldcol.Name == col.BeforeName {
					b = true
					break
				}
			}

			// drop column
			if b != true {
				op = GetColumnOperation(tab, oldcol, DROP)
				sql.Operations = append(sql.Operations, op)
			}
		}
	}

	// add and change column check
	for _, tab := range n.Table {
		for _, col := range tab.Columns {
			var old Table
			var ok bool
			if tab.BeforeName != "" {
				old, ok = o.GetTable(tab.BeforeName)
				if ok != true {
					continue
				}
			} else {
				old, ok = o.GetTable(tab.Name)
				if ok != true {
					continue
				}
			}

			var oldcol Column
			if col.BeforeName != "" {
				oldcol, ok = old.GetColumn(col.BeforeName)
				if ok != true {
					return nil, errors.New("Declared before column not exist in old state :" + col.BeforeName)
				}
				// append operation to change column data
				op = GetColumnOperation(tab, col, CHANGE)
				sql.Operations = append(sql.Operations, op)

			} else {
				oldcol, ok = old.GetColumn(col.Name)
				// append operation to add column data
				if ok != true {
					op = GetColumnOperation(tab, col, ADD)
					sql.Operations = append(sql.Operations, op)
					continue
				}
			}
			// append operation to change column data
			if oldcol != col {
				op = GetColumnOperation(tab, col, MODIFY)
				sql.Operations = append(sql.Operations, op)
				continue
			}
		}
	}

	// add index and primary key and foreign key check
	for _, tab := range n.Table {
		var ok bool
		var old Table
		if tab.BeforeName != "" {
			old, ok = o.GetTable(tab.BeforeName)
			if ok != true {
				return nil, errors.New("In changing table, before table not exist : " + tab.BeforeName)
			}
		} else {
			// key adding is after creating table and column. then existance is not matter
			old, _ = o.GetTable(tab.Name)
		}

		// add index
		if old.Index.Target == nil && tab.Index.Target != nil {
			op = GetTableOperation(tab, ADDINDEX)
			sql.Operations = append(sql.Operations, op)
		}

		// add primary key
		if old.PrimaryKey.Target == nil && tab.PrimaryKey.Target != nil {
			op = GetTableOperation(tab, ADDPK)
			sql.Operations = append(sql.Operations, op)
		}

		// drop index
		if old.Index.Target != nil && tab.Index.Target == nil {
			op = GetTableOperation(tab, DROPINDEX)
			sql.Operations = append(sql.Operations, op)
		}

		// drop primary key
		if old.PrimaryKey.Target != nil && tab.PrimaryKey.Target == nil {
			op = GetTableOperation(tab, DROPPK)
			sql.Operations = append(sql.Operations, op)
		}

		var oldcol Column
		for _, col := range tab.Columns {
			if col.BeforeName != "" {
				oldcol, ok = old.GetColumn(col.BeforeName)
				if ok != true {
					return nil, errors.New("Declared before column not exist in old state :" + col.BeforeName)
				}
			} else {
				oldcol, _ = old.GetColumn(col.Name)
			}

			// add FK
			if oldcol.FK.TargetColumn == "" && col.FK.TargetColumn != "" {
				op = GetTableOperation(tab, ADDFK)
				sql.Operations = append(sql.Operations, op)
			}

			// drop FK
			if oldcol.FK.TargetColumn != "" && col.FK.TargetColumn == "" {
				op = GetTableOperation(tab, DROPFK)
				sql.Operations = append(sql.Operations, op)
			}
		}
	}

	return sql, nil
}

func (c Operation) QueryBuilder() (string, error) {
	q := fmt.Sprintf("ALTER TABLE %s", c.Table)

	switch c.OperationType {
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
			pk += i + ","
		}
		q += fmt.Sprint("ADD PRIMARY KEY %s (%s)", c.PK.Name, pk)
		return q, nil

	case DROPPK:
		q += fmt.Sprint("DROP PRIMARY KEY %s", c.PK.Name)
		return q, nil

	case ADDINDEX:
		idx := ""
		for _, i := range c.Index.Target {
			idx += i + ","
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

func (s *Sql) Migrate() error {
	db, err := sql.Open("mysql", s.DbConf.FormatDSN())
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
