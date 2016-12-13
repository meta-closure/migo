package migo

import (
	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

var (
	ErrInvalidDbColumn   = errors.New("Invalid Database column")
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
	Name   string
}

type ForeignKey struct {
	Name          string
	TargetTable   string
	TargetColumn  string
	UpdateCascade bool
	DeleteCascade bool
}

type Column struct {
	Id                string
	BeforeName        string
	Name              string
	Type              string
	FK                ForeignKey
	UniqueFlag        bool
	AutoIncrementFlag bool
	AutoUpdateFlag    bool
	NotNullFlag       bool
	Default           string
}

type Operation struct {
	Table         Table
	OldTable      Table
	Column        Column
	OldColumn     Column
	Key           Key
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
		return s + fmt.Sprintf("ADD TABLE: [%s]", op.Table.Name)
	case CHANGETBL:
		return s + fmt.Sprintf("CHANGE TABLE: [%s] -> [%s]", op.OldTable.Name, op.Table.Name)
	case DROPTBL:
		return s + fmt.Sprintf("DROP TABLE: [%s]", op.Table.Name)
	case ADDCLM:
		return s + fmt.Sprintf("ADD COLUMN TO [%s]: [%s]", op.Table.Name, op.Column.Name)
	case DROPCLM:
		return s + fmt.Sprintf("DROP COLUMN TO [%s]: [%s]", op.Table.Name, op.Column.Name)
	case MODIFYCLM:
		return s + fmt.Sprintf("MODIFY COLUMN TO [%s]: [%s]", op.Table.Name, op.Column.Name)
	case MODIFYAICLM:
		return s + fmt.Sprintf("MODIFY COLUMN TO CHANGE AUTO INCREMENT [%s]: [%s]", op.Table.Name, op.Column.Name)
	case CHANGECLM:
		return s + fmt.Sprintf("CHANGE COLUMN TO [%s]: [%s] -> [%s]", op.Table.Name, op.OldColumn.Name, op.Column.Name)
	case ADDPK:
		return s + fmt.Sprintf("ADD PRIMARY KEY TO [%s]; [%s] -> %s", op.Table.Name, op.Key.Name, op.Key.Target)
	case DROPPK:
		return s + fmt.Sprintf("DROP PRIMARY KEY TO [%s]: [%s] -> %s", op.Table.Name, op.Key.Name, op.Key.Target)
	case ADDINDEX:
		return s + fmt.Sprintf("ADD INDEX TO [%s]: %s", op.Key.Name, op.Key.Target)
	case DROPINDEX:
		return s + fmt.Sprintf("DROP INDEX KEY TO [%s]: %s", op.Key.Name, op.Key.Target)
	case ADDFK:
		return s + fmt.Sprintf("ADD FOREIGN KEY TO [%s]: [%s] -> [%s] IN [%s]", op.Table.Name, op.Column.Name, op.Column.FK.TargetColumn, op.Column.FK.TargetTable)
	case DROPFK:
		return s + fmt.Sprintf("DROP FOREIGN KEY TO [%s]: [%s] -> [%s] IN [%s]", op.Table.Name, op.Column.Name, op.Column.FK.TargetColumn, op.Column.FK.TargetTable)
	default:
		return s + fmt.Sprintln("CANT RECOGNIZE OPERATION")
	}
}

func (s Sql) Check() {
	fmt.Println("\n---------- DATABASE MIGRATION IS .......\n")

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

func ConvertKeyId2Name(t Table, k Key) (Key, error) {
	l := []string{}
	for i, key := range k.Target {
		col, ok := t.GetColumn(key)
		if ok != true {
			return Key{}, errors.Wrap(ErrNotExistReference, k.Target[i])
		}
		l = append(l, col.Name)
	}

	return Key{Name: k.Name, Target: l}, nil
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

	col.FK.TargetTable = stab.Name
	col.FK.TargetColumn = scol.Name
	return col, nil
}

func GetKeyOperation(oldtab, tab Table, key Key, flag int) Operation {
	return Operation{
		Table:         tab,
		Key:           key,
		OperationType: flag,
	}
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

func SameKey(m, n []string) bool {
	var b bool
	for _, s := range m {
		b = false
		for _, t := range n {
			if s == t {
				b = true
			}
		}
		if b == false {
			return false
		}
	}
	for _, t := range n {
		b = false
		for _, s := range m {
			if s == t {
				b = true
			}
		}
		if b == false {
			return false
		}
	}
	return true
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
	if o.Default != n.Default {
		return false
	}
	if o.AutoUpdateFlag != n.AutoUpdateFlag {
		return false
	}

	return true
}

func NewSql(db Db) *Sql {
	return &Sql{
		DbConf: mysql.Config{
			User:   db.User,
			Addr:   db.Addr,
			Net:    "tcp",
			Passwd: db.Passwd,
			DBName: db.DBName,
		},
	}
}

func (o *State) SQLBuilder(n *State) (*Sql, error) {

	// Setting database connection configure
	sql := NewSql(n.Db)
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
			fmt.Println(tab.Id, tab.Name)
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
	var aiop []Operation
	for _, tab := range n.Table {
		oldtab, _ := o.GetTable(tab.Id)
		for _, col := range tab.Column {
			oldcol, ok := oldtab.GetColumn(col.Id)
			if !ok {
				// append operation to add column
				op = GetColumnOperation(oldtab, tab, oldcol, col, ADDCLM)
				sql.Operations = append(sql.Operations, op)
			} else {
				// append operation to change column data
				if oldcol.Name != col.Name {
					op = GetColumnOperation(oldtab, tab, oldcol, col, CHANGECLM)
					sql.Operations = append(sql.Operations, op)
				}
			}

			// append operation to change column data
			// auto increment need key setting, then skip AI config after key set
			if !SameColumn(oldcol, col) {
				op = GetColumnOperation(oldtab, tab, oldcol, col, MODIFYCLM)
				sql.Operations = append(sql.Operations, op)
			}
			if !oldcol.AutoIncrementFlag && col.AutoIncrementFlag {
				op = GetColumnOperation(oldtab, tab, oldcol, col, MODIFYAICLM)
				aiop = append(aiop, op)
			}
			if oldcol.AutoIncrementFlag && !col.AutoIncrementFlag {
				op = GetColumnOperation(oldtab, tab, oldcol, col, MODIFYAICLM)
				sql.Operations = append(sql.Operations, op)
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

		for _, key := range tab.Index {
			b := false
			for _, oldkey := range oldtab.Index {
				if oldkey.Name == key.Name {
					b = true
					// change key
					// if key selection change, then drop old key and create new key
					if SameKey(oldkey.Target, key.Target) != true {
						// drop index key and auto_increment
						conv, err := ConvertKeyId2Name(oldtab, oldkey)
						if err != nil {
							return nil, errors.Wrapf(err, "Converting new state [%s] table key id into name", tab.Name)
						}
						op = GetKeyOperation(oldtab, tab, conv, DROPINDEX)
						sql.Operations = append(sql.Operations, op)

						// add index key and auto_increment
						conv, err = ConvertKeyId2Name(tab, key)
						if err != nil {
							return nil, errors.Wrapf(err, "Converting new state [%s] table key id into name", tab.Name)
						}
						op = GetKeyOperation(oldtab, tab, conv, ADDINDEX)
						sql.Operations = append(sql.Operations, op)
					}
				}
			}

			// add index
			if b != true {
				conv, err := ConvertKeyId2Name(tab, key)
				if err != nil {
					return nil, errors.Wrapf(err, "Converting new state [%s] table key id into name", tab.Name)
				}
				op = GetKeyOperation(oldtab, tab, conv, ADDINDEX)
				sql.Operations = append(sql.Operations, op)
			}
		}

		for _, oldkey := range oldtab.Index {
			b := false
			for _, key := range tab.Index {
				if oldkey.Name == key.Name {
					b = true
				}
			}
			// drop index
			if b != true {
				conv, err := ConvertKeyId2Name(oldtab, oldkey)
				if err != nil {
					return nil, errors.Wrapf(err, "Converting new state [%s] table key id into name", tab.Name)
				}
				op = GetKeyOperation(oldtab, tab, conv, DROPINDEX)
				sql.Operations = append(sql.Operations, op)
			}
		}

		for _, key := range tab.PrimaryKey {
			b := false
			for _, oldkey := range oldtab.PrimaryKey {
				if oldkey.Name == key.Name {
					b = true
					// if key selection change, then drop old key and create new key
					if SameKey(oldkey.Target, key.Target) != true {
						// drop PrimaryKey key
						conv, err := ConvertKeyId2Name(oldtab, oldkey)
						if err != nil {
							return nil, errors.Wrapf(err, "Converting new state [%s] table key id into name", tab.Name)
						}
						op = GetKeyOperation(oldtab, tab, conv, DROPPK)
						sql.Operations = append(sql.Operations, op)

						// add PrimaryKey key
						conv, err = ConvertKeyId2Name(tab, key)
						if err != nil {
							return nil, errors.Wrapf(err, "Converting new state [%s] table key id into name", tab.Name)
						}
						op = GetKeyOperation(oldtab, tab, conv, ADDPK)
						sql.Operations = append(sql.Operations, op)
					}
				}
			}

			// add PrimaryKey
			if b != true {
				conv, err := ConvertKeyId2Name(tab, key)
				if err != nil {
					return nil, errors.Wrapf(err, "Converting new state [%s] table key id into name", tab.Name)
				}
				op = GetKeyOperation(oldtab, tab, conv, ADDPK)
				sql.Operations = append(sql.Operations, op)
			}
		}

		for _, oldkey := range oldtab.PrimaryKey {
			b := false
			for _, key := range tab.PrimaryKey {
				if oldkey.Name == key.Name {
					b = true
				}
			}
			// drop PrimaryKey and auto_increment
			if b != true {
				conv, err := ConvertKeyId2Name(oldtab, oldkey)
				if err != nil {
					return nil, errors.Wrapf(err, "Converting new state [%s] table key id into name", tab.Name)
				}
				op = GetKeyOperation(oldtab, tab, conv, DROPPK)
				sql.Operations = append(sql.Operations, op)
			}
		}
	}

	sql.Operations = append(sql.Operations, aiop...)
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

// GetColumnOption is getting column option,
// aiflag is flag to add auto increment option, becouse
// auto increment flag need key setting
func (c Operation) GetColumnOption(aiflag bool) string {
	q := ""
	if aiflag {
		q += " AUTO_INCREMENT"
	}
	if c.Column.NotNullFlag {
		q += " NOT NULL"
	}
	if c.Column.UniqueFlag {
		q += " UNIQUE"
	}
	if c.Column.Default != "" {
		q += fmt.Sprintf(" DEFAULT '%s'", c.Column.Default)
	}

	if len(c.Column.Type) > 8 && c.Column.Type[:8] == "datetime" {
		if c.Column.AutoUpdateFlag {
			q += " ON UPDATE CURRENT_TIMESTAMP" + c.Column.Type[8:]
		}
		if c.Column.Default == "" {
			q += " DEFAULT CURRENT_TIMESTAMP" + c.Column.Type[8:]
		}
	}
	return q
}

func (c Operation) GetColumnRecoverOption(aiflag bool) string {
	q := ""
	if !aiflag {
		q += " AUTO_INCREMENT"
	}
	if c.OldColumn.NotNullFlag {
		q += " NOT NULL"
	}
	if c.OldColumn.UniqueFlag {
		q += " UNIQUE"
	}
	if c.OldColumn.AutoUpdateFlag {
		q += " ON UPDATE CURRENT_TIMESTAMP"
	}
	if c.OldColumn.Default != "" {
		q += fmt.Sprintf(" DEFAULT '%s'", c.Column.Default)
	}
	if c.OldColumn.Default == "" && c.Column.Type == "datetime" {
		q += " DEFAULT CURRENT_TIMESTAMP"
	}
	return q
}

func ConvertList2SQL(l []string) string {
	sq := ""
	for j, i := range l {
		if j == 0 {
			sq += i
		} else {
			sq += ", " + i
		}
	}
	return sq
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
		q = fmt.Sprintf("DROP TABLE %s", c.Table.Name)
		return q, nil
	case ADDCLM:
		q += fmt.Sprintf("ADD COLUMN %s %s", c.Column.Name, c.Column.Type)
		q += c.GetColumnOption(false)
		return q, nil

	case DROPCLM:
		q += fmt.Sprintf("DROP %s", c.Column.Name)
		return q, nil

	case MODIFYCLM:
		q += fmt.Sprintf("MODIFY %s  %s", c.Column.Name, c.Column.Type)
		q += c.GetColumnOption(false)
		return q, nil

	case MODIFYAICLM:
		q += fmt.Sprintf("MODIFY %s  %s", c.Column.Name, c.Column.Type)
		q += c.GetColumnOption(true)
		return q, nil

	case CHANGECLM:
		q += fmt.Sprintf("CHANGE COLUMN %s %s %s", c.OldColumn.Name, c.Column.Name, c.Column.Type)
		q += c.GetColumnOption(false)
		return q, nil

	case ADDPK:
		q += fmt.Sprintf("ADD PRIMARY KEY %s (%s)", c.Key.Name, ConvertList2SQL(c.Key.Target))
		return q, nil

	case DROPPK:
		q += fmt.Sprintf("DROP PRIMARY KEY")
		return q, nil

	case ADDINDEX:
		q += fmt.Sprintf("ADD INDEX %s (%s)", c.Key.Name, ConvertList2SQL(c.Key.Target))
		return q, nil

	case DROPINDEX:
		q += fmt.Sprintf("DROP INDEX %s", c.Key.Name)
		return q, nil

	case ADDFK:
		q += fmt.Sprintf("ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)", c.Column.FK.Name, c.Column.Name, c.Column.FK.TargetTable, c.Column.FK.TargetColumn)
		if c.Column.FK.UpdateCascade {
			q += " ON UPDATE CASCADE"
		}
		if c.Column.FK.DeleteCascade {
			q += " ON DELETE CASCADE"
		}
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
		q = fmt.Sprintf("CREATE TABLE %s (padding int)", c.Table.Name)
		return q, nil

	case DROPCLM:
		q += fmt.Sprintf("ADD COLUMN %s %s", c.OldColumn.Name, c.OldColumn.Type)
		q += c.GetColumnRecoverOption(true)
		return q, nil

	case ADDCLM:
		q += fmt.Sprintf("DROP %s", c.Column.Name)
		return q, nil

	case MODIFYCLM:
		q += fmt.Sprintf("MODIFY %s  %s", c.OldColumn.Name, c.OldColumn.Type)
		q += c.GetColumnRecoverOption(true)
		return q, nil

	case MODIFYAICLM:
		q += fmt.Sprintf("MODIFY %s  %s", c.Column.Name, c.Column.Type)
		q += c.GetColumnRecoverOption(false)
		return q, nil

	case CHANGECLM:
		q += fmt.Sprintf("CHANGE COLUMN %s %s %s", c.Column.Name, c.OldColumn.Name, c.OldColumn.Type)
		q += c.GetColumnRecoverOption(true)
		return q, nil

	case DROPPK:
		q += fmt.Sprintf("ADD PRIMARY KEY %s (%s)", c.Key.Name, ConvertList2SQL(c.Key.Target))
		return q, nil

	case ADDPK:
		q += fmt.Sprintf("DROP PRIMARY KEY")
		return q, nil

	case DROPINDEX:
		q += fmt.Sprintf("ADD INDEX %s (%s)", c.Key.Name, ConvertList2SQL(c.Key.Target))
		return q, nil

	case ADDINDEX:
		q += fmt.Sprintf("DROP INDEX %s", c.Key.Name)
		return q, nil

	case DROPFK:
		q += fmt.Sprintf("ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)", c.OldColumn.FK.Name, c.OldColumn.Name, c.OldColumn.FK.TargetTable, c.OldColumn.FK.TargetColumn)
		if c.OldColumn.FK.UpdateCascade {
			q += " ON UPDATE CASCADE"
		}
		if c.OldColumn.FK.DeleteCascade {
			q += " ON DELETE CASCADE"
		}
		return q, nil

	case ADDFK:
		q += fmt.Sprintf("DROP FOREIGN KEY %s", c.Column.FK.Name)
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
			return errors.Wrapf(err, "Table: %s, Column: %s, Query Build Failed", c.Table.Name, c.Column.Name)
		}
		if q == "" {
			continue
		}
		qs = append(qs, q)
	}

	for idx := 1; idx < i+1; idx++ {
		_, err = db.Exec(qs[i-idx])
		if err != nil {
			return errors.Wrapf(err, "Query: %s", qs[i-idx])
		}
	}

	fmt.Println(">>>>>>>> RECOVERY SUCCEED")
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
			return 0, errors.Wrapf(err, "Table: %s, Column: %s, Query Build Failed", c.Table.Name, c.Column.Name)
		}
		qs = append(qs, q)
	}

	for i, q := range qs {
		_, err = db.Exec(q)
		if err != nil {
			fmt.Println(q, err)
			fmt.Println(">>>>>>>> MIGRATION FAILED")
			return i, errors.Wrapf(err, "Query: %s", q)
		}
	}

	fmt.Println(">>>>>>>> MIGRATION SUCCEED")
	return 0, nil
}
