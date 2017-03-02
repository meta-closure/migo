package migo

import (
	"reflect"

	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

/*
func (ops Operations) Check() {
	fmt.Println("\n---------- DATABASE MIGRATION IS .......\n")

	fmt.Printf("DATABASE CONFIGURE: %s \n\n", ops.Target.FormatDSN())
	for _, op := range ops.Operation {
		if op.Column.Name == "padding" {
			continue
		}
		fmt.Println(op.Strings())
	}
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

func (s *State) ConvertForeignKeyId2Name(col Column) (Column, error) {
	stab, ok := s.GetTable(col.ForeignKey.TargetTable)
	if ok != true {
		return col, errors.Wrapf(ErrNotExistReference, "%s Table not found", col.ForeignKey.TargetTable)
	}

	scol, ok := stab.GetColumn(col.ForeignKey.TargetColumn)
	if ok != true {
		return col, errors.Wrapf(ErrNotExistReference, "%s Column not found", col.ForeignKey.TargetColumn)
	}

	col.ForeignKey.TargetTable = stab.Name
	col.ForeignKey.TargetColumn = scol.Name
	return col, nil
}

func SameColumn(o, n Column) bool {

	if o.Type != n.Type {
		return false
	}
	if o.NotNull != n.NotNull {
		return false
	}
	if o.Unique != n.Unique {
		return false
	}
	if o.Default != n.Default {
		return false
	}
	if o.AutoUpdate != n.AutoUpdate {
		return false
	}

	return true
}
*/

func NewMySQLConfig(db DB) mysql.Config {
	return mysql.Config{
		User:   db.User,
		Addr:   db.Addr,
		Net:    "tcp",
		Passwd: db.Passwd,
		DBName: db.DBName,
	}
}

func (s State) selectTableWithID(id string) (Table, error) {
	for _, t := range s.Tables {
		if t.Id == id {
			return t, nil
		}
	}
	return Table{}, errors.New("table not found")
}

func (s State) hasTable(t Table) bool {
	if _, err := s.selectTableWithID(t.Id); err != nil {
		return false
	}
	return true
}

func (s State) selectTablesNotIn(target State) ([]Table, error) {
	filterd := []Table{}
	for _, t := range s.Tables {
		if !target.hasTable(t) {
			filterd = append(filterd, t)
		}
	}

	return filterd, nil
}

func (s State) selectTablesIn(target State) ([]Table, error) {
	filterd := []Table{}
	for _, t := range s.Tables {
		if target.hasTable(t) {
			filterd = append(filterd, t)
		}
	}
	return filterd, nil
}

func (t Table) selectIndexWithName(s string) (Key, error) {
	for _, k := range t.Index {
		if k.Name == s {
			return k, nil
		}
	}
	return Key{}, errors.New("index not found")
}

func (t Table) hasIndex(k Key) bool {
	if _, err := t.selectIndexWithName(k.Name); err != nil {
		return false
	}
	return true
}

func (t Table) selectPrimaryKeyWithName(s string) (Key, error) {
	for _, k := range t.PrimaryKey {
		if k.Name == s {
			return k, nil
		}
	}
	return Key{}, errors.New("primary key not found")
}

func (t Table) hasPrimaryKey(k Key) bool {
	if _, err := t.selectPrimaryKeyWithName(k.Name); err != nil {
		return false
	}
	return true
}

func (t Table) selectForeignKeyWithColumn(c Column) []ForeignKey {
	included := []ForeignKey{}
	for _, fk := range t.ForeignKey {
		if fk.SourceColumn == c.Id {
			included = append(included, fk)
		}
	}
	return included
}
func (t Table) selectColumnWithID(id string) (Column, error) {
	for _, c := range t.Column {
		if c.Id == id {
			return c, nil
		}
	}
	return Column{}, errors.New("column not found")
}

func (t Table) hasColumn(c Column) bool {
	if _, err := t.selectColumnWithID(c.Id); err != nil {
		return false
	}
	return true
}

func (k Key) isUpdatedFrom(target Key) (bool, error) {
	if k.Name != target.Name {
		return false, errors.New("the target key name is wrong")
	}
	return !reflect.DeepEqual(k, target), nil
}

func (c Column) isUpdatedFrom(target Column) (bool, error) {
	if c.Id != target.Id {
		return false, errors.New("the target column ID is wrong")
	}
	return !reflect.DeepEqual(c, target), nil
}

func (ops *Operations) UpdateTable(current, new Table) error {
	if current.Name != new.Name {
		ops.Operation = append(ops.Operation, NewRenameTable(current, new))
	}

	for _, k := range current.Index {
		if !new.hasIndex(k) {
			ops.Operation = append(ops.Operation, NewDropIndex(new, k))
		}
	}

	for _, k := range current.PrimaryKey {
		if !new.hasPrimaryKey(k) {
			ops.Operation = append(ops.Operation, NewDropPrimaryKey(new, k))
		}
	}

	for _, c := range current.Column {
		if !new.hasColumn(c) {
			keys := current.selectForeignKeyWithColumn(c)
			for _, fk := range keys {
				ops.Operation = append(ops.Operation, NewDropForeignKey(fk))
			}
			ops.Operation = append(ops.Operation, NewDropColumn(new, c))
		}
	}

	for _, c := range new.Column {
		if !current.hasColumn(c) {
			ops.Operation = append(ops.Operation, NewAddColumn(new, c))
		}
		keys := new.selectForeignKeyWithColumn(c)
		for _, fk := range keys {
			ops.Operation = append(ops.Operation, NewAddForeignKey(fk))
		}
	}

	for _, k := range new.Index {
		old, err := current.selectIndexWithName(k.Name)
		if err != nil {
			ops.Operation = append(ops.Operation, NewAddIndex(new, k))
			continue
		}
		isUpdated, err := k.isUpdatedFrom(old)
		if err != nil {
			return err
		}
		if isUpdated {
			ops.Operation = append(ops.Operation, NewDropIndex(new, old))
			ops.Operation = append(ops.Operation, NewAddIndex(new, k))
		}
	}

	for _, k := range new.PrimaryKey {
		old, err := current.selectPrimaryKeyWithName(k.Name)
		if err != nil {
			ops.Operation = append(ops.Operation, NewAddPrimaryKey(new, k))
			continue
		}
		isUpdated, err := k.isUpdatedFrom(old)
		if err != nil {
			return err
		}
		if isUpdated {
			ops.Operation = append(ops.Operation, NewDropPrimaryKey(new, old))
			ops.Operation = append(ops.Operation, NewAddPrimaryKey(new, k))
		}
	}

	for _, c := range new.Column {
		old, err := current.selectColumnWithID(c.Id)
		if err != nil {
			continue
		}
		ops.Operation = append(ops.Operation, NewUpdateColumn(new, old, c))
	}

	return nil
}

func (ops *Operations) DropTables(ts []Table) error {
	for _, t := range ts {
		ops.Operation = append(ops.Operation, NewDropTable(t))
	}
	return nil
}

func (ops *Operations) CreateTables(ts []Table) error {
	for _, t := range ts {
		ops.Operation = append(ops.Operation, NewCreateTable(t))
	}
	for _, t := range ts {
		for _, fk := range t.ForeignKey {
			ops.Operation = append(ops.Operation, NewAddForeignKey(fk))
		}
	}
	return nil
}

func NewOperations(current, update State) (Operations, error) {
	ops := Operations{}
	ts, err := current.selectTablesNotIn(update)
	if err != nil {
		return ops, err
	}
	if err := ops.DropTables(ts); err != nil {
		return ops, err
	}

	ts, err = update.selectTablesNotIn(current)
	if err != nil {
		return ops, err
	}
	if err := ops.CreateTables(ts); err != nil {
		return ops, err
	}

	ts, err = update.selectTablesIn(current)
	for _, t := range ts {
		s, err := current.selectTableWithID(t.Id)
		if err != nil {
			continue
		}
		if err := ops.UpdateTable(s, t); err != nil {
			return ops, err
		}
	}
	return ops, nil
}
