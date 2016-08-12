package migo

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
	Target []string
}

type ForeignKey struct {
	Name         string `json:"name"`
	TargetTable  string `json:"target_table"`
	TargetColumn string `json:"target_column"`
}

type Column struct {
	Id                string     `json:"id"`
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
	s := "OPERATION >>>>>>>>>    "
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
		return s + fmt.Sprintf("CHANGE COLUMN TO [%s]: [%s] -> [%s]\n", op.Table, op.Column.BeforeName, op.Column.Name)
	case ADDPK:
		return s + fmt.Sprintf("ADD PRIMARY KEY TO [%s]: [%s]\n", op.Table, op.PK.Target)
	case DROPPK:
		return s + fmt.Sprintf("DROP PRIMARY KEY TO [%s]: [%s]\n", op.Table, op.PK.Target)
	case ADDINDEX:
		return s + fmt.Sprintf("ADD INDEX TO [%s]: [%s]\n", op.Table, op.Index.Target)
	case DROPINDEX:
		return s + fmt.Sprintf("DROP INDEX KEY TO [%s]: [%s]\n", op.Table, op.Index.Target)
	case ADDFK:
		fmt.Printf("%+v", op)
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

func (s *State) GetTable(st string) (Table, bool) {
	for _, v := range s.Table {
		if v.Id == st {
			return v, true
		}
	}
	return Table{}, false
}

func (t Table) GetColumn(st string) (Column, bool) {
	for _, v := range t.Column {
		if v.Id == st {
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
	op := Operation{
		Table:         t.Name,
		BeforeTable:   t.BeforeName,
		OperationType: flag,
		Column:        c,
	}
	op.FK = c.FK
	return op
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

func SameColumn(o, n Column) bool {

	if o.Type != n.Type {
		return false
	}
	if o.NotNullFlag != n.NotNullFlag {
		return false
	}
	if o.UniqueFlag != n.UniqueFlag {
		return false
	}
	if o.AutoIncrementFlag != n.AutoIncrementFlag {
		return false
	}
	return true
}

func SQLBuilder(o, n *State) (*Sql, error) {
	return nil, nil
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
		oldtab, ok := o.GetTable(tab.Id)

		if ok != true {
			op = GetTableOperation(tab, ADDTBL)
			sql.Operations = append(sql.Operations, op)
			continue
		}

		if oldtab.Name != tab.Name {
			tab.BeforeName = oldtab.Name
			op = GetTableOperation(tab, CHANGETBL)
			sql.Operations = append(sql.Operations, op)
		}
	}

	// delete table and delete column check
	for _, oldtab := range o.Table {

		// drop table
		tab, ok := n.GetTable(oldtab.Id)
		if ok != true {
			op = GetTableOperation(oldtab, DROPTBL)
			sql.Operations = append(sql.Operations, op)
			continue
		}

		for _, oldcol := range oldtab.Column {
			_, ok := tab.GetColumn(oldcol.Id)

			// drop column
			if ok != true {
				op = GetColumnOperation(tab, oldcol, DROPCLM)
				sql.Operations = append(sql.Operations, op)
			}
		}

	}

	// add and change column check
	for _, tab := range n.Table {
		oldtab, _ := o.GetTable(tab.Id)

		for _, col := range tab.Column {

			oldcol, ok := oldtab.GetColumn(col.Id)
			if ok != true {
				// append operation to add column
				op = GetColumnOperation(tab, col, ADDCLM)
				sql.Operations = append(sql.Operations, op)
				continue
			} else {
				// append operation to change column data
				if oldcol.Name != col.Name {
					col.BeforeName = oldcol.Name
					op = GetColumnOperation(tab, col, CHANGECLM)
					sql.Operations = append(sql.Operations, op)
				}
			}

			// append operation to change column data
			// auto increment need key setting, then skip AI config after key set
			if SameColumn(oldcol, col) != true {
				if oldcol.AutoIncrementFlag == true {
					op = GetColumnOperation(tab, col, MODIFYAICLM)
					sql.Operations = append(sql.Operations, op)
				} else if oldcol.AutoIncrementFlag != col.AutoIncrementFlag {
					continue
				} else {
					op = GetColumnOperation(tab, col, MODIFYCLM)
					sql.Operations = append(sql.Operations, op)
				}
			}
		}
	}

	// drop padding column in creating new table
	for _, ops := range sql.Operations {
		if ops.OperationType == ADDTBL {
			op = GetDropPaddingOperation(ops.Table)
			sql.Operations = append(sql.Operations, op)
		}
	}

	// add index and primary key and foreign key check
	for _, tab := range n.Table {
		oldtab, _ := o.GetTable(tab.Id)

		// add index
		if oldtab.Index.Target == nil && tab.Index.Target != nil {
			op = GetTableOperation(tab, ADDINDEX)
			sql.Operations = append(sql.Operations, op)

			// check column have auto_increment flag
			for _, idx := range op.Index.Target {
				col, _ := tab.GetColumn(idx)
				if col.AutoIncrementFlag == true {
					op = GetColumnOperation(tab, col, MODIFYAICLM)
					sql.Operations = append(sql.Operations, op)
				}
			}
		}

		// add primary key
		if oldtab.PrimaryKey.Target == nil && tab.PrimaryKey.Target != nil {
			op = GetTableOperation(tab, ADDPK)
			sql.Operations = append(sql.Operations, op)

			// check column have auto_increment flag
			for _, pk := range op.PK.Target {
				col, _ := tab.GetColumn(pk)
				if col.AutoIncrementFlag == true {
					op = GetColumnOperation(tab, col, MODIFYAICLM)
					sql.Operations = append(sql.Operations, op)
				}
			}
		}

		// drop index
		if oldtab.Index.Target != nil && tab.Index.Target == nil {
			op = GetTableOperation(tab, DROPINDEX)
			sql.Operations = append(sql.Operations, op)
		}

		// drop primary key
		if oldtab.PrimaryKey.Target != nil && tab.PrimaryKey.Target == nil {
			op = GetTableOperation(tab, DROPPK)
			sql.Operations = append(sql.Operations, op)
		}
	}

	for _, tab := range n.Table {
		oldtab, _ := o.GetTable(tab.Id)
		for _, col := range tab.Column {
			oldcol, _ := oldtab.GetColumn(col.Id)

			// add FK
			if oldcol.FK.TargetColumn == "" && col.FK.TargetColumn != "" {
				op = GetColumnOperation(tab, col, ADDFK)
				sql.Operations = append(sql.Operations, op)
			}

			// drop FK
			if oldcol.FK.TargetColumn != "" && col.FK.TargetColumn == "" {
				op = GetColumnOperation(tab, oldcol, DROPFK)
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
		q += fmt.Sprintf("CHANGE COLUMN %s %s %s", c.Column.BeforeName, c.Column.Name, c.Column.Type)

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
		if len(c.PK.Target) == 1 {
			pk = c.PK.Target[0]
		} else {
			for _, i := range c.PK.Target {
				pk += i + ","
			}
			pk = pk[:len(pk)-1]
		}
		q += fmt.Sprintf("ADD PRIMARY KEY (%s)", pk)
		return q, nil

	case DROPPK:
		q += "DROP PRIMARY KEY"
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
		q += fmt.Sprintf("ADD INDEX (%s)", idx)
		return q, nil

	case DROPINDEX:
		q += "DROP INDEX"
		return q, nil

	case ADDFK:
		q += fmt.Sprintf("ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)", c.FK.Name, c.Column.Name, c.FK.TargetTable, c.FK.TargetColumn)
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
		_, err = db.Exec(q)
		if err != nil {
			return errors.Wrapf(err, "Query: %s", q)
		}
	}

	fmt.Println(">>>>>>>> MIGRATION SUCCEED")
	return nil
}
