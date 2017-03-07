package migo

import (
	"fmt"
	"reflect"

	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

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

func (ops *Operations) UpdateTable(currentTable, newTable Table) error {
	if len(newTable.Column) == 0 {
		return errors.New("table's column should not be empty")
	}

	pk := []Key{}
	for _, k := range newTable.PrimaryKey {
		_, err := Table{PrimaryKey: pk}.selectPrimaryKeyWithName(k.Name)
		if err == nil {
			return fmt.Errorf("primary key %s is not unique", k.Name)
		}
		pk = append(pk, k)
	}

	idx := []Key{}
	for _, k := range newTable.Index {
		_, err := Table{Index: idx}.selectIndexWithName(k.Name)
		if err == nil {
			return fmt.Errorf("index %s is not unique", k.Name)
		}
		idx = append(idx, k)
	}

	if currentTable.Name != newTable.Name {
		ops.Operation = append(ops.Operation, NewRenameTable(currentTable, newTable))
	}

	for _, k := range currentTable.Index {
		if !newTable.hasIndex(k) {
			ops.Operation = append(ops.Operation, NewDropIndex(newTable, k))
		}
	}

	for _, k := range currentTable.PrimaryKey {
		if !newTable.hasPrimaryKey(k) {
			ops.Operation = append(ops.Operation, NewDropPrimaryKey(newTable, k))
		}
	}

	for _, c := range currentTable.Column {
		if !newTable.hasColumn(c) {
			ops.Operation = append(ops.Operation, NewDropColumn(newTable, c))
		}
	}

	for _, c := range newTable.Column {
		if !currentTable.hasColumn(c) {
			ops.Operation = append(ops.Operation, NewAddColumn(newTable, c))
		}
	}

	for _, k := range newTable.Index {
		if len(k.Target) == 0 {
			return errors.New("index's target is should not be empty")
		}

		old, err := currentTable.selectIndexWithName(k.Name)
		if err != nil {
			ops.Operation = append(ops.Operation, NewAddIndex(newTable, k))
			continue
		}
		isUpdated, err := k.isUpdatedFrom(old)
		if err != nil {
			return err
		}
		if isUpdated {
			ops.Operation = append(ops.Operation, NewDropIndex(newTable, old))
			ops.Operation = append(ops.Operation, NewAddIndex(newTable, k))
		}
	}

	for _, k := range newTable.PrimaryKey {
		if len(k.Target) == 0 {
			return errors.New("primary key's target is should not be empty")
		}

		old, err := currentTable.selectPrimaryKeyWithName(k.Name)
		if err != nil {
			ops.Operation = append(ops.Operation, NewAddPrimaryKey(newTable, k))
			continue
		}
		isUpdated, err := k.isUpdatedFrom(old)
		if err != nil {
			return err
		}
		if isUpdated {
			ops.Operation = append(ops.Operation, NewDropPrimaryKey(newTable, old))
			ops.Operation = append(ops.Operation, NewAddPrimaryKey(newTable, k))
		}
	}

	for _, c := range newTable.Column {
		old, err := currentTable.selectColumnWithID(c.Id)
		if err != nil {
			continue
		}
		ops.Operation = append(ops.Operation, NewUpdateColumn(newTable, old, c))
	}

	return nil
}

func (ops *Operations) DropTables(ts []Table) error {
	for _, t := range ts {
		ops.Operation = append(ops.Operation, NewDropTable(t))
	}
	return nil
}

func (ops *Operations) CreateTables(s State, ts []Table) error {
	for _, t := range ts {
		ops.Operation = append(ops.Operation, NewCreateTable(t))
	}
	return nil
}

func NewOperations(currentState, newState State) (Operations, error) {
	ops := Operations{}
	for _, fk := range currentState.ForeignKey {
		ops.Operation = append(ops.Operation, NewDropForeignKey(fk))
	}

	ts, err := currentState.selectTablesNotIn(newState)
	if err != nil {
		return ops, err
	}
	if err := ops.DropTables(ts); err != nil {
		return ops, err
	}

	ts, err = newState.selectTablesNotIn(currentState)
	if err != nil {
		return ops, err
	}
	if err := ops.CreateTables(newState, ts); err != nil {
		return ops, err
	}

	ts, err = newState.selectTablesIn(currentState)
	for _, t := range ts {
		s, err := currentState.selectTableWithID(t.Id)
		if err != nil {
			continue
		}
		if err := ops.UpdateTable(s, t); err != nil {
			return ops, err
		}
	}

	for _, fk := range newState.ForeignKey {
		ops.Operation = append(ops.Operation, NewAddForeignKey(fk))
	}

	return ops, nil
}
