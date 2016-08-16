package migo

import (
	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

var (
	ErrEmpty             = errors.New("Required parameter is empty")
	ErrTypeInvalid       = errors.New("Invalid type error")
	ErrInvalidTable      = errors.New("Invalid type name")
	ErrOperationType     = errors.New("Operation type invalid")
	ErrNotExistReference = errors.New("Reference is empty")
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
	Table         Table
	OldTable      Table
	Column        Column
	OldColumn     Column
	OperationType int
}

type Query struct {
	Operation Operation
	Command   string
}

type Sql struct {
	DbConf     mysql.Config
	Operations []Operation
}

func (op Operation) Strings() string {
	s := "OPERATION >>>>>>>>>    "
	switch op.OperationType {
	case ADDTBL:
		return s + fmt.Sprintf("ADD TABLE: [%s]\n", op.Table.Name)
	case CHANGETBL:
		return s + fmt.Sprintf("CHANGE TABLE: [%s] -> [%s]\n", op.OldTable.Name, op.Table.Name)
	case DROPTBL:
		return s + fmt.Sprintf("DROP TABLE: [%s]\n", op.Table.Name)
	case ADDCLM:
		return s + fmt.Sprintf("ADD COLUMN TO [%s]: [%s]\n", op.Table.Name, op.Column.Name)
	case DROPCLM:
		return s + fmt.Sprintf("DROP COLUMN TO [%s]: [%s]\n", op.Table.Name, op.Column.Name)
	case MODIFYCLM:
		return s + fmt.Sprintf("MODIFY COLUMN TO [%s]: [%s]\n", op.Table.Name, op.Column.Name)
	case MODIFYAICLM:
		return s + fmt.Sprintf("MODIFY COLUMN TO CHANGE AUTO INCREMENT [%s]: [%s]\n", op.Table.Name, op.Column.Name)
	case CHANGECLM:
		return s + fmt.Sprintf("CHANGE COLUMN TO [%s]: [%s] -> [%s]\n", op.Table.Name, op.Column.BeforeName, op.Column.Name)
	case ADDPK:
		return s + fmt.Sprintf("ADD PRIMARY KEY TO [%s]: [%s]\n", op.Table.Name, op.Table.PrimaryKey.Target)
	case DROPPK:
		return s + fmt.Sprintf("DROP PRIMARY KEY TO [%s]: [%s]\n", op.Table.Name, op.OldTable.PrimaryKey.Target)
	case ADDINDEX:
		return s + fmt.Sprintf("ADD INDEX TO [%s]: [%s]\n", op.Table.Name, op.Table.Index.Target)
	case DROPINDEX:
		return s + fmt.Sprintf("DROP INDEX KEY TO [%s]: [%s]\n", op.Table.Name, op.Table.Index.Target)
	case ADDFK:
		return s + fmt.Sprintf("ADD FOREIGN KEY TO [%s]: [%s] -> [%s] IN [%s]\n", op.Table.Name, op.Column.Name, op.Column.FK.TargetColumn, op.Column.FK.TargetTable)
	case DROPFK:
		return s + fmt.Sprintf("DROP FOREIGN KEY TO [%s]: [%s] -> [%s] IN [%s]\n", op.Table.Name, op.Column.Name, op.Column.FK.TargetColumn, op.Column.FK.TargetTable)
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

func ConvertKeyId2Name(t Table) (Table, error) {
	var pk []string
	for i, key := range t.PrimaryKey.Target {
		col, ok := t.GetColumn(key)
		if ok != true {
			return t, errors.Wrap(ErrNotExistReference, t.PrimaryKey.Target[i])
		}
		pk = append(pk, col.Name)
	}

	var idx []string
	for i, key := range t.Index.Target {
		col, ok := t.GetColumn(key)
		if ok != true {
			return t, errors.Wrap(ErrNotExistReference, t.PrimaryKey.Target[i])
		}
		idx = append(idx, col.Name)
	}

	t.PrimaryKey.Target = pk
	t.Index.Target = idx
	return t, nil
}

func (s *State) ConvertFKId2Name(col Column) (Column, error) {
	stab, ok := s.GetTable(col.FK.TargetTable)
	if ok != true {
		return col, errors.Wrapf(ErrNotExistReference, "%s Table not found", col.FK.TargetTable)
	}

	scol, ok := stab.GetColumn(col.FK.TargetColumn)
	if ok != true {
		return col, errors.Wrapf(ErrNotExistReference, "%s Column not found", col.FK.TargetColumn)
	}

	col.FK.Name = col.Name
	col.FK.TargetTable = stab.Name
	col.FK.TargetColumn = scol.Name
	return col, nil
}

func GetTableOperation(oldtab, tab Table, flag int) Operation {
	return Operation{
		Table:         tab,
		OldTable:      oldtab,
		OperationType: flag,
	}
}

func GetColumnOperation(oldtab, tab Table, oldcol, col Column, flag int) Operation {
	op := Operation{
		Table:         tab,
		OldTable:      oldtab,
		OperationType: flag,
		Column:        col,
		OldColumn:     oldcol,
	}
	return op
}

func GetDropPaddingOperation(tbl Table) Operation {
	return Operation{
		Table:         tbl,
		OperationType: DROPCLM,
		Column: Column{
			Name: "padding",
			Type: "int",
		},
		OldColumn: Column{
			Name: "padding",
			Type: "int",
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

	return true
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
			op = GetTableOperation(oldtab, tab, ADDTBL)
			sql.Operations = append(sql.Operations, op)
			continue
		}

		if oldtab.Name != tab.Name {
			op = GetTableOperation(oldtab, tab, CHANGETBL)
			sql.Operations = append(sql.Operations, op)
		}
	}

	// delete table and delete column check
	for _, oldtab := range o.Table {

		// drop table
		tab, ok := n.GetTable(oldtab.Id)
		if ok != true {
			op = GetTableOperation(oldtab, tab, DROPTBL)
			sql.Operations = append(sql.Operations, op)
			continue
		}

		for _, oldcol := range oldtab.Column {
			_, ok := tab.GetColumn(oldcol.Id)

			// drop column
			if ok != true {
				op = GetColumnOperation(oldtab, tab, oldcol, oldcol, DROPCLM)
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
				op = GetColumnOperation(oldtab, tab, oldcol, col, ADDCLM)
				sql.Operations = append(sql.Operations, op)
				continue
			} else {
				// append operation to change column data
				if oldcol.Name != col.Name {
					op = GetColumnOperation(oldtab, tab, oldcol, col, CHANGECLM)
					sql.Operations = append(sql.Operations, op)
				}
			}

			// append operation to change column data
			// auto increment need key setting, then skip AI config after key set
			if SameColumn(oldcol, col) != true {
				if oldcol.AutoIncrementFlag == true {
					op = GetColumnOperation(oldtab, tab, oldcol, col, MODIFYAICLM)
					sql.Operations = append(sql.Operations, op)
				} else if oldcol.AutoIncrementFlag != col.AutoIncrementFlag {
					continue
				} else {
					op = GetColumnOperation(oldtab, tab, oldcol, col, MODIFYCLM)
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

		// change table id to table name
		ctab, err := ConvertKeyId2Name(tab)
		if err != nil {
			return nil, errors.Wrapf(err, "Converting new state [%s] table key id into name", tab.Name)
		}

		coldtab, err := ConvertKeyId2Name(oldtab)
		if err != nil {
			return nil, errors.Wrapf(err, "Converting old state [%s] table key id into name", oldtab.Name)
		}

		// add index
		if oldtab.Index.Target == nil && tab.Index.Target != nil {
			op = GetTableOperation(coldtab, ctab, ADDINDEX)
			sql.Operations = append(sql.Operations, op)

			// check column have auto_increment flag
			for _, idx := range tab.Index.Target {
				col, _ := tab.GetColumn(idx)
				if col.AutoIncrementFlag == true {
					op = GetColumnOperation(coldtab, ctab, Column{}, col, MODIFYAICLM)
					sql.Operations = append(sql.Operations, op)
				}
			}
		}

		// add primary key
		if oldtab.PrimaryKey.Target == nil && tab.PrimaryKey.Target != nil {
			op = GetTableOperation(coldtab, ctab, ADDPK)
			sql.Operations = append(sql.Operations, op)

			// check column have auto_increment flag
			for _, pk := range tab.PrimaryKey.Target {
				col, _ := tab.GetColumn(pk)
				if col.AutoIncrementFlag == true {
					op = GetColumnOperation(coldtab, ctab, Column{}, col, MODIFYAICLM)
					sql.Operations = append(sql.Operations, op)
				}
			}
		}

		// drop index
		if oldtab.Index.Target != nil && tab.Index.Target == nil {
			op = GetTableOperation(coldtab, ctab, DROPINDEX)
			sql.Operations = append(sql.Operations, op)
		}

		// drop primary key
		if oldtab.PrimaryKey.Target != nil && tab.PrimaryKey.Target == nil {
			op = GetTableOperation(coldtab, ctab, DROPPK)
			sql.Operations = append(sql.Operations, op)
		}
	}

	for _, tab := range n.Table {
		oldtab, _ := o.GetTable(tab.Id)
		for _, col := range tab.Column {
			oldcol, _ := oldtab.GetColumn(col.Id)

			// add FK
			if oldcol.FK.TargetColumn == "" && col.FK.TargetColumn != "" {
				op = GetColumnOperation(oldtab, tab, oldcol, col, ADDFK)
				col, err := n.ConvertFKId2Name(op.Column)
				if err != nil {
					return nil, errors.Wrapf(err, "Converting new state [%s] column key id into name", col.Name)
				}
				op.Column = col
				sql.Operations = append(sql.Operations, op)
			}

			// drop FK
			if oldcol.FK.TargetColumn != "" && col.FK.TargetColumn == "" {
				op = GetColumnOperation(oldtab, tab, oldcol, col, DROPFK)
				oldcol, err := o.ConvertFKId2Name(op.OldColumn)
				if err != nil {
					return nil, errors.Wrapf(err, "Converting old state [%s] column key id into name", oldcol.Name)
				}
				op.OldColumn = oldcol
				sql.Operations = append(sql.Operations, op)
			}
		}
	}

	return sql, nil
}

func (c Operation) QueryBuilder() (string, error) {
	q := fmt.Sprintf("ALTER TABLE %s ", c.Table.Name)

	switch c.OperationType {
	case ADDTBL:
		q = fmt.Sprintf("CREATE TABLE %s (padding int)", c.Table.Name)
		return q, nil
	case CHANGETBL:
		q = fmt.Sprintf("ALTER TABLE %s RENAME %s", c.OldTable.Name, c.Table.Name)
		return q, nil
	case DROPTBL:
		q = fmt.Sprintf("DROP TABLE %S", c.Table.Name)
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
		if c.Column.AutoIncrementFlag == true && c.OldColumn.AutoIncrementFlag == true {
			q += " AUTO_INCREMENT"
		}
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

		if c.Column.AutoIncrementFlag == true && c.OldColumn.AutoIncrementFlag == true {
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
		if len(c.Table.PrimaryKey.Target) == 1 {
			pk = c.Table.PrimaryKey.Target[0]
		} else {
			for _, i := range c.Table.PrimaryKey.Target {
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
		if len(c.Table.Index.Target) == 1 {
			idx = c.Table.Index.Target[0]
		} else {
			for _, i := range c.Table.Index.Target {
				idx += i + ","
			}
		}
		q += fmt.Sprintf("ADD INDEX (%s)", idx)
		return q, nil

	case DROPINDEX:
		q += "DROP INDEX"
		return q, nil

	case ADDFK:
		q += fmt.Sprintf("ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)", c.Column.FK.Name, c.Column.Name, c.Column.FK.TargetTable, c.Column.FK.TargetColumn)
		return q, nil

	case DROPFK:
		q += fmt.Sprintf("DROP FOREIGN KEY %s", c.Column.FK.Name)
		return q, nil

	default:
		return "", ErrOperationType

	}
}

func (c Operation) RecoveryQueryBuilder() (string, error) {
	q := fmt.Sprintf("ALTER TABLE %s ", c.Table.Name)

	switch c.OperationType {
	case ADDTBL:
		q = fmt.Sprintf("DROP TABLE %s", c.Table.Name)
		return q, nil

	case CHANGETBL:
		q = fmt.Sprintf("ALTER TABLE %s RENAME %s", c.Table.Name, c.OldTable.Name)
		return q, nil

	case DROPTBL:
		q = fmt.Sprintf("CREATE TABLE %S (padding int)", c.Table.Name)
		return q, nil

	case DROPCLM:
		q += fmt.Sprintf("ADD COLUMN %s %s", c.OldColumn.Name, c.OldColumn.Type)
		if c.OldColumn.AutoIncrementFlag == true {
			q += " AUTO_INCREMENT"
		}
		if c.OldColumn.NotNullFlag == true {
			q += " NOT NULL"
		}
		if c.OldColumn.UniqueFlag == true {
			q += " UNIQUE"
		}
		return q, nil

	case ADDCLM:
		q += fmt.Sprintf("DROP %s", c.Column.Name)
		return q, nil

	case MODIFYCLM:
		q += fmt.Sprintf("MODIFY %s  %s", c.OldColumn.Name, c.OldColumn.Type)
		if c.OldColumn.AutoIncrementFlag == true {
			q += " AUTO_INCREMENT"
		}
		if c.OldColumn.NotNullFlag == true {
			q += " NOT NULL"
		}
		if c.OldColumn.UniqueFlag == true {
			q += " UNIQUE"
		}
		return q, nil

	case MODIFYAICLM:
		q += fmt.Sprintf("MODIFY %s  %s", c.Column.Name, c.Column.Type)
		if c.Column.NotNullFlag == true {
			q += " NOT NULL"
		}
		if c.Column.UniqueFlag == true {
			q += " UNIQUE"
		}
		return q, nil

	case CHANGECLM:
		q += fmt.Sprintf("CHANGE COLUMN %s %s %s", c.Column.Name, c.OldColumn.Name, c.OldColumn.Type)

		if c.OldColumn.AutoIncrementFlag == true {
			q += " AUTO_INCREMENT"
		}
		if c.OldColumn.NotNullFlag == true {
			q += " NOT NULL"
		}
		if c.OldColumn.UniqueFlag == true {
			q += " UNIQUE"
		}
		return q, nil

	case DROPPK:
		pk := ""
		if len(c.OldTable.PrimaryKey.Target) == 1 {
			pk = c.OldTable.PrimaryKey.Target[0]
		} else {
			for _, i := range c.OldTable.PrimaryKey.Target {
				pk += i + ","
			}
			pk = pk[:len(pk)-1]
		}
		q += fmt.Sprintf("ADD PRIMARY KEY (%s)", pk)
		return q, nil

	case ADDPK:
		q += "DROP PRIMARY KEY"
		return q, nil

	case DROPINDEX:
		idx := ""
		if len(c.OldTable.Index.Target) == 1 {
			idx = c.OldTable.Index.Target[0]
		} else {
			for _, i := range c.OldTable.Index.Target {
				idx += i + ","
			}
		}
		q += fmt.Sprintf("ADD INDEX (%s)", idx)
		return q, nil

	case ADDINDEX:
		q += "DROP INDEX"
		return q, nil

	case DROPFK:
		q += fmt.Sprintf("ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)", c.OldColumn.FK.Name, c.OldColumn.Name, c.OldColumn.FK.TargetTable, c.OldColumn.FK.TargetColumn)
		return q, nil

	case ADDFK:
		q += fmt.Sprintf("DROP FOREIGN KEY %s", c.OldColumn.FK.Name)
		return q, nil

	default:
		return "", errors.New("Operation type invalid")

	}
}

func (s *Sql) Recovery(i int) error {

	db, err := sql.Open("mysql", s.DbConf.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()

	var qs []string
	for _, c := range s.Operations[0:i] {
		q, err := c.RecoveryQueryBuilder()
		if err != nil {
			fmt.Println(">>>>>>>> RECOVERY FAILED")
			return errors.Wrapf(err, "Table: %s, Column: %s,s Query Build Failed", c.Table.Name, c.Column.Name)
		}
		if q == "" {
			continue
		}
		qs = append(qs, q)
	}

	for idx := 1; idx < i+1; idx++ {
		_, err = db.Exec(qs[i-idx])
		if err != nil {
			fmt.Println(">>>>>>>> RECOVERY FAILED")
			return errors.Wrapf(err, "Query: %s", qs[i-idx])
		}
	}

	fmt.Println(">>>>>>>> REVOCERY SUCCEED")
	return nil
}

func (s *Sql) Migrate() (int, error) {

	db, err := sql.Open("mysql", s.DbConf.FormatDSN())
	if err != nil {
		return 0, err
	}
	defer db.Close()

	var qs []string
	for _, c := range s.Operations {
		q, err := c.QueryBuilder()
		if err != nil {
			fmt.Println(">>>>>>>> MIGRATION FAILED")
			return 0, errors.Wrapf(err, "Table: %s, Column: %s,s Query Build Failed", c.Table.Name, c.Column.Name)
		}
		qs = append(qs, q)
	}

	for i, q := range qs {
		_, err = db.Exec(q)
		if err != nil {
			fmt.Println(">>>>>>>> MIGRATION FAILED")
			return i, errors.Wrapf(err, "Query: %s", q)
		}
	}

	fmt.Println(">>>>>>>> MIGRATION SUCCEED")
	return 0, nil
}
