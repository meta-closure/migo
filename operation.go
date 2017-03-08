package migo

import (
	"fmt"
	"regexp"
	"strings"
)

type Operations struct {
	execCount int
	Operation []Operation
}

type Operation interface {
	RollBack() string
	Query() string
	String() string
}

type CreateTable struct {
	Table Table
}

func NewCreateTable(t Table) Operation {
	return CreateTable{Table: t}
}

func (op CreateTable) Query() string {
	cols := []string{}
	for _, c := range op.Table.Column {
		cols = append(cols, c.query())
	}
	for _, k := range op.Table.PrimaryKey {
		cols = append(cols, k.queryAsPrimaryKey())
	}
	for _, k := range op.Table.Index {
		cols = append(cols, k.queryAsIndex())
	}
	return fmt.Sprintf("CREATE TABLE %s (%s)ENGINE=innoDB", op.Table.Name, strings.Join(cols, ","))
}

func (op CreateTable) String() string {
	return fmt.Sprintf("ADD TABLE: [%s]", op.Table.Name)
}

func (op CreateTable) RollBack() string {
	return NewDropTable(op.Table).Query()
}

func isDatetime(s string) bool {
	return len(s) > 8 && s[:8] == "datetime"
}

func digit(s string) string {
	return regexp.MustCompile(`\(.+\)`).FindString(s)
}

type DropTable struct {
	Table Table
}

func NewDropTable(t Table) Operation {
	return DropTable{Table: t}
}

func (op DropTable) Query() string {
	return fmt.Sprintf("DROP TABLE %s", op.Table.Name)
}

func (op DropTable) RollBack() string {
	return NewCreateTable(op.Table).Query()
}

func (op DropTable) String() string {
	return fmt.Sprintf("DROP TABLE: [%s]", op.Table.Name)
}

type AddForeignKey struct {
	ForeignKey ForeignKey
}

func NewAddForeignKey(key ForeignKey) Operation {
	return AddForeignKey{ForeignKey: key}
}

func (op AddForeignKey) String() string {
	return fmt.Sprintf("ADD FOREIGN KEY FROM [%s] IN [%s] => [%s] IN [%s]",
		op.ForeignKey.SourceColumn.Name,
		op.ForeignKey.SourceTable.Name,
		op.ForeignKey.TargetColumn.Name,
		op.ForeignKey.TargetTable.Name)
}

func (op AddForeignKey) Query() string {
	s := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
		op.ForeignKey.SourceTable.Name,
		op.ForeignKey.Name,
		op.ForeignKey.SourceColumn.Name,
		op.ForeignKey.TargetTable.Name,
		op.ForeignKey.TargetColumn.Name,
	)

	fk := []string{s}
	if op.ForeignKey.UpdateCascade {
		fk = append(fk, "ON UPDATE CASCADE")
	}

	if op.ForeignKey.DeleteCascade {
		fk = append(fk, "ON DELETE CASCADE")
	}

	return strings.Join(fk, " ")
}

func (op AddForeignKey) RollBack() string {
	return NewDropForeignKey(op.ForeignKey).Query()
}

type DropForeignKey struct {
	ForeignKey ForeignKey
}

func NewDropForeignKey(key ForeignKey) Operation {
	return DropForeignKey{
		ForeignKey: key,
	}
}
func (op DropForeignKey) String() string {
	return fmt.Sprintf("DROP FOREIGN KEY FROM [%s]: [%s] => [%s]: [%s]",
		op.ForeignKey.SourceTable.Name,
		op.ForeignKey.SourceColumn.Name,
		op.ForeignKey.TargetColumn.Name,
		op.ForeignKey.TargetColumn.Name)
}

func (op DropForeignKey) Query() string {
	return fmt.Sprintf("ALTER TABLE %s DROP FOREIGN KEY %s",
		op.ForeignKey.SourceTable.Name,
		op.ForeignKey.Name,
	)
}

func (op DropForeignKey) RollBack() string {
	return NewAddForeignKey(op.ForeignKey).Query()
}

type RenameTable struct {
	CurrentTable Table
	NewTable     Table
}

func NewRenameTable(old, new Table) Operation {
	return RenameTable{
		CurrentTable: old,
		NewTable:     new,
	}
}

func (op RenameTable) String() string {
	return fmt.Sprintf("RENAME TABLE: [%s] => [%s]", op.CurrentTable.Name, op.NewTable.Name)
}
func (op RenameTable) Query() string {
	return fmt.Sprintf("ALTER TABLE %s RENAME %s", op.CurrentTable.Name, op.NewTable.Name)
}
func (op RenameTable) RollBack() string {
	return NewRenameTable(op.NewTable, op.CurrentTable).Query()
}

type DropColumn struct {
	Column Column
	Table  Table
}

func NewDropColumn(t Table, c Column) Operation {
	return DropColumn{
		Table:  t,
		Column: c,
	}
}

func (op DropColumn) String() string {
	return fmt.Sprintf("DROP COLUMN [%s] IN [%s]", op.Column.Name, op.Table.Name)
}
func (op DropColumn) Query() string {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", op.Table.Name, op.Column.Name)
}
func (op DropColumn) RollBack() string {
	return NewAddColumn(op.Table, op.Column).Query()
}

type AddColumn struct {
	Column Column
	Table  Table
}

func NewAddColumn(t Table, c Column) Operation {
	return AddColumn{
		Column: c,
		Table:  t,
	}
}

func (op AddColumn) String() string {
	return fmt.Sprintf("ADD COLUMN [%s] IN [%s]", op.Column.Name, op.Table.Name)
}
func (op AddColumn) Query() string {
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", op.Table.Name, op.Column.query())
}
func (op AddColumn) RollBack() string {
	return NewDropColumn(op.Table, op.Column).Query()
}

type UpdateColumn struct {
	Table         Table
	CurrentColumn Column
	NewColumn     Column
}

func NewUpdateColumn(t Table, old, new Column) Operation {
	return UpdateColumn{
		Table:         t,
		CurrentColumn: old,
		NewColumn:     new,
	}
}
func (op UpdateColumn) String() string {
	return fmt.Sprintf("CHANGE COLUMN [%s] IN [%s]", op.CurrentColumn.Name, op.Table.Name)
}

func (op UpdateColumn) Query() string {
	return fmt.Sprintf("ALTER TABLE %s CHANGE COLUMN %s %s",
		op.Table.Name,
		op.CurrentColumn.Name,
		op.NewColumn.query())
}

func (op UpdateColumn) RollBack() string {
	return NewUpdateColumn(op.Table, op.NewColumn, op.CurrentColumn).Query()
}

type DropIndex struct {
	Table Table
	Index Key
}

func NewDropIndex(t Table, k Key) DropIndex {
	return DropIndex{
		Table: t,
		Index: k,
	}
}
func (op DropIndex) String() string {
	return fmt.Sprintf("DROP INDEX %s IN %s", op.Index.Name, op.Table.Name)
}
func (op DropIndex) Query() string {
	return fmt.Sprintf("ALTER TABLE %s DROP INDEX %s", op.Table.Name, op.Index.Name)
}
func (op DropIndex) RollBack() string {
	return NewAddIndex(op.Table, op.Index).Query()
}

type DropPrimaryKey struct {
	Table      Table
	PrimaryKey Key
}

func NewDropPrimaryKey(t Table, k Key) DropPrimaryKey {
	return DropPrimaryKey{
		Table:      t,
		PrimaryKey: k,
	}
}
func (op DropPrimaryKey) String() string {
	return fmt.Sprintf("DROP PRIMARY KEY %s IN %s", op.PrimaryKey.Name, op.Table.Name)
}
func (op DropPrimaryKey) Query() string {
	return fmt.Sprintf("ALTER TABLE %s DROP PRIMARY KEY", op.Table.Name)
}
func (op DropPrimaryKey) RollBack() string {
	return NewAddPrimaryKey(op.Table, op.PrimaryKey).Query()
}

type AddPrimaryKey struct {
	Table      Table
	PrimaryKey Key
}

func NewAddPrimaryKey(t Table, k Key) AddPrimaryKey {
	return AddPrimaryKey{
		Table:      t,
		PrimaryKey: k,
	}
}
func (op AddPrimaryKey) String() string {
	return fmt.Sprintf("ADD PRIMARY KEY %s IN %s", op.PrimaryKey.Name, op.Table.Name)
}

func (op AddPrimaryKey) Query() string {
	return fmt.Sprintf("ALTER TABLE %s ADD %s", op.Table.Name, op.PrimaryKey.queryAsPrimaryKey())
}

func (op AddPrimaryKey) RollBack() string {
	return NewDropPrimaryKey(op.Table, op.PrimaryKey).Query()
}

type AddIndex struct {
	Table Table
	Index Key
}

func NewAddIndex(t Table, k Key) AddIndex {
	return AddIndex{
		Table: t,
		Index: k,
	}
}
func (op AddIndex) String() string {
	return fmt.Sprintf("ADD INDEX %s IN %s", op.Index.Name, op.Table.Name)
}
func (op AddIndex) Query() string {
	return fmt.Sprintf("ALTER TABLE %s ADD %s", op.Table.Name, op.Index.queryAsIndex())
}
func (op AddIndex) RollBack() string {
	return NewDropIndex(op.Table, op.Index).Query()
}
