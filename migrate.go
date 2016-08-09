package mig

import (
	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

var (
	ErrEmpty        = errors.New("Required parameter is empty")
	ErrTypeInvalid  = errors.New("Invalid type error")
	ErrInvalidTable = errors.New("Invalid type name")
)

const (
	ADDTBL = iota
	CHANGETBL
	DROPTBL
	ADDCLM
	DROPCLM
	MODIFYCLM
	MODIFYAICLM
	CHANGECLM
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

func (op Operation) Strings() string {
	s := "OPERATION ---->>>>  "
	switch op.OperationType {
	case ADDTBL:
		return s + fmt.Sprintf("ADD TABLE: [%s]\n", op.Table)
	case CHANGETBL:
		return s + fmt.Sprintf("CHANGE TABLE: [%s] -> [%s]\n", op.BeforeTable, op.Table)
	case DROPTBL:
		return s + fmt.Sprintf("DROP TABLE: [%s]\n", op.Table)
	case ADDCLM:
		return s + fmt.Sprintf("ADD COLUMN TO [%s]: [%s]\n", op.Table, op.Column.Name)
	case DROPCLM:
		return s + fmt.Sprintf("DROP COLUMN TO [%s]: [%s]\n", op.Table, op.Column.Name)
	case MODIFYCLM:
		return s + fmt.Sprintf("MODIFY COLUMN TO [%s]: [%s]\n", op.Table, op.Column.Name)
	case MODIFYAICLM:
		return s + fmt.Sprintf("MODIFY COLUMN TO [%s]: [%s]\n", op.Table, op.Column.Name)
	case CHANGECLM:
		return s + fmt.Sprintf("CHANGE COLUMN TO [%s]: [%s] -> [%s]\n", op.Table, op.Column.Name)
	case ADDPK:
		return s + fmt.Sprintf("ADD PRIMARY KEY TO [%s]: [%s]\n", op.Table, op.PK.Name)
	case DROPPK:
		return s + fmt.Sprintf("DROP PRIMARY KEY TO [%s]: [%s]\n", op.Table, op.PK.Name)
	case ADDINDEX:
		return s + fmt.Sprintf("ADD INDEX TO [%s]: [%s]\n", op.Table, op.Index.Name)
	case DROPINDEX:
		return s + fmt.Sprintf("DROP INDEX KEY TO [%s]: [%s]\n", op.Table, op.Index.Name)
	case ADDFK:
		return s + fmt.Sprintf("ADD FOREIGN KEY TO [%s]: [%s] -> [%s] IN [%s]\n", op.Table, op.Column.Name, op.FK.TargetColumn, op.FK.TargetTable)
	case DROPFK:
		return s + fmt.Sprintf("DROP FOREIGN KEY TO [%s]: [%s] -> [%s] IN [%s]\n", op.Table, op.Column.Name, op.FK.TargetColumn, op.FK.TargetTable)
	default:
		return s + fmt.Sprintln("CANT RECOGNIZE OPERATION")
	}
}

func (s Sql) Check() {
	fmt.Println("\n---------- DATABASE MIGRRATION IS .......\n\n")

	fmt.Printf("DATABASE CONFIGURE: %s \n\n", s.DbConf.FormatDSN())
	for _, op := range s.Operations {
		if op.Column.Name == "padding" {
			continue
		}

		if op.OperationType == MODIFYAICLM {
			continue
		}

		fmt.Println(op.Strings())
	}
}

func (s *State) ExistTable(st string) bool {
	for _, v := range s.Table {
		if v.Name == st {
			return true
		}
	}
	return false
}

func (t Table) ExistColumn(st string) bool {
	for _, v := range t.Column {
		if v.Name == st {
			return true
		}
	}
	return false
}

func (s *State) GetTable(st string) (Table, bool) {
	for _, v := range s.Table {
		if v.Name == st {
			return v, true
		}
	}
	return Table{}, false
}

func (t Table) GetColumn(st string) (Column, bool) {
	for _, v := range t.Column {
		if v.Name == st {
			return v, true
		}
	}
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

func GetDropPaddingOperation(s string) Operation {
	return Operation{
		Table:         s,
		OperationType: DROPCLM,
		Column: Column{
			Name: "padding",
		},
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
			DBName: n.Db.DBName,
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
			op = GetTableOperation(tab, CHANGETBL)
		} else {
			ok := o.ExistTable(tab.Name)
			if ok == true {
				continue
			}
			op = GetTableOperation(tab, ADDTBL)
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
			op = GetTableOperation(old, DROPTBL)
			sql.Operations = append(sql.Operations, op)
			continue
		}

		for _, oldcol := range old.Column {
			b = false
			for _, col := range tab.Column {
				if oldcol.Name == col.Name || oldcol.Name == col.BeforeName {
					b = true
					break
				}
			}

			// drop column
			if b != true {
				op = GetColumnOperation(tab, oldcol, DROPCLM)
				sql.Operations = append(sql.Operations, op)
			}
		}
	}

	// add and change column check
	for _, tab := range n.Table {
		for _, col := range tab.Column {
			var old Table
			var ok bool

			if tab.BeforeName != "" {
				old, ok = o.GetTable(tab.BeforeName)
				if ok != true {
					return nil, ErrInvalidTable
				}
			} else {
				// add and change column after creating new table
				old, _ = o.GetTable(tab.Name)
			}

			var oldcol Column
			if col.BeforeName != "" {
				oldcol, ok = old.GetColumn(col.BeforeName)
				if ok != true {
					return nil, errors.New("Declared before column not exist in old state :" + col.BeforeName)
				}
				// append operation to change column data
				op = GetColumnOperation(tab, col, CHANGECLM)
				sql.Operations = append(sql.Operations, op)

			} else {
				oldcol, ok = old.GetColumn(col.Name)
				// append operation to add column data
				if ok != true {
					op = GetColumnOperation(tab, col, ADDCLM)
					sql.Operations = append(sql.Operations, op)
					continue
				}
			}
			// append operation to change column data
			if oldcol != col {
				op = GetColumnOperation(tab, col, MODIFYCLM)
				sql.Operations = append(sql.Operations, op)
				continue
			}
		}
	}

	// drop padding column in creating new table
	for _, opr := range sql.Operations {
		if opr.OperationType == ADDTBL {
			op = GetDropPaddingOperation(opr.Table)
			sql.Operations = append(sql.Operations, op)
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

			// check column have auto_increment flag
			for _, idx := range op.Index.Target {
				col, _ := tab.GetColumn(idx)
				op = GetColumnOperation(tab, col, MODIFYAICLM)
				sql.Operations = append(sql.Operations, op)
			}
		}

		// add primary key
		if old.PrimaryKey.Target == nil && tab.PrimaryKey.Target != nil {
			op = GetTableOperation(tab, ADDPK)
			sql.Operations = append(sql.Operations, op)

			// check column have auto_increment flag
			for _, pk := range op.PK.Target {
				col, _ := tab.GetColumn(pk)
				op = GetColumnOperation(tab, col, MODIFYAICLM)
				sql.Operations = append(sql.Operations, op)
			}
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
		for _, col := range tab.Column {
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
	q := fmt.Sprintf("ALTER TABLE %s ", c.Table)

	switch c.OperationType {
	case ADDTBL:
		q = fmt.Sprintf("CREATE TABLE %s (padding int)", c.Table)
		return q, nil
	case CHANGETBL:
		q = fmt.Sprintf("ALTER TABLE %s RENAME %s", c.BeforeTable, c.Table)
		return q, nil
	case DROPTBL:
		q = fmt.Sprintf("DROP TABLE %S", c.Table)
		return q, nil
	case ADDCLM:
		q += fmt.Sprintf("ADD COLUMN %s %s", c.Column.Name, c.Column.Type)
		if c.Column.NotNullFlag == true {
			q += " NOT NULL"
		}
		if c.Column.UniqueFlag == true {
			q += " UNIQUE"
		}
		return q, nil

	case DROPCLM:
		q += fmt.Sprintf("DROP %s", c.Column.Name)
		return q, nil

	case MODIFYCLM:
		q += fmt.Sprintf("MODIFY %s  %s", c.Column.Name, c.Column.Type)
		if c.Column.NotNullFlag == true {
			q += " NOT NULL"
		}
		if c.Column.UniqueFlag == true {
			q += " UNIQUE"
		}
		return q, nil

	case MODIFYAICLM:
		q += fmt.Sprintf("MODIFY %s  %s", c.Column.Name, c.Column.Type)
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

	case CHANGECLM:
		q += fmt.Sprintf("CHANGE COLUMN %s %s", c.Column.BeforeName, c.Column.Name)

		if c.Column.NotNullFlag == true {
			q += " NOT NULL"
		}
		if c.Column.UniqueFlag == true {
			q += " UNIQUE"
		}
		return q, nil

	case ADDPK:
		pk := ""
		if len(c.PK.Target) == 1 {
			pk = c.PK.Target[0]
		} else {
			for _, i := range c.PK.Target {
				pk += i + ","
			}
			pk = pk[:len(pk)-1]
		}
		q += fmt.Sprintf("ADD PRIMARY KEY %s (%s)", c.PK.Name, pk)
		return q, nil

	case DROPPK:
		q += fmt.Sprintf("DROP PRIMARY KEY %s", c.PK.Name)
		return q, nil

	case ADDINDEX:
		idx := ""
		if len(c.Index.Target) == 1 {
			idx = c.Index.Target[0]
		} else {
			for _, i := range c.Index.Target {
				idx += i + ","
			}
		}
		q += fmt.Sprintf("ADD CONSTRAINT %s  INDEX  %s (%s)", c.Index.Name, idx)
		return q, nil

	case DROPINDEX:
		q += fmt.Sprintf("DROP INDEX %s", c.Index.Name)
		return q, nil

	case ADDFK:
		q += fmt.Sprintf("ADD CONSTRAINT %s CONSTRAINT %s FOREIGM KEY %s REFERENCE %s(%s)", c.FK.Name, c.Column.Name, c.FK.TargetTable, c.FK.TargetColumn)
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

	var qs []string
	for _, c := range s.Operations {
		q, err := c.QueryBuilder()
		if err != nil {
			return errors.Wrapf(err, "Table: %s, Column: %s,s Query Build Failed", c.Table, c.Column.Name)
		}
		qs = append(qs, q)
	}

	for _, q := range qs {
		fmt.Println(q)
		_, err = db.Exec(q)
		if err != nil {
			return errors.Wrapf(err, "Query: %s", q)
		}
	}

	return nil
}
