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
	Exec() string
	String() string
}

type CreateTable struct {
	Table Table
}

func NewCreateTable(t Table) Operation {
	return CreateTable{Table: t}
}

func (op CreateTable) Exec() string {
	cols := []string{}
	for _, c := range op.Table.Column {
		cols = append(cols, c.definitionString())
	}
	return fmt.Sprintf("CREATE TABLE %s (%s)ENGINE=innoDB", op.Table.Name, strings.Join(cols, ","))
}
func (op CreateTable) String() string {
	return fmt.Sprintf("ADD TABLE: [%s]", op.Table.Name)
}

func (op CreateTable) RollBack() string {
	return NewDropTable(op.Table).Exec()
}

func (c Column) definitionString() string {
	s := []string{c.Name, c.Type}
	if c.AutoIncrement {
		s = append(s, "AUTO_INCREMENT")
	}
	if c.NotNull {
		s = append(s, "NOT NULL")
	}
	if c.Unique {
		s = append(s, "UNIQUE")
	}
	if c.Default != "" && !isDatetime(c.Type) {
		s = append(s, "DEFAULT '%s'", c.Default)
	}
	if isDatetime(c.Type) {
		if c.AutoUpdate {
			s = append(s, fmt.Sprintf("ON UPDATE CURRENT_TIMESTAMP%s", digit(c.Type)))
		}
		if c.Default == "" {
			s = append(s, fmt.Sprintf("DEFAULT CURRENT_TIMESTAMP%s", digit(c.Type)))
		}
	}
	return strings.Join(s, " ")
}

func isDatetime(s string) bool {
	return len(s) > 8 && s[:8] == "datetime"
}

func digit(s string) string {
	return regexp.MustCompile(`\(.+\))`).FindString(s)
}

type DropTable struct {
	Table Table
}

func NewDropTable(t Table) Operation {
	return DropTable{Table: t}
}

func (op DropTable) Exec() string {
	return fmt.Sprintf("DROP TABLE %s", op.Table.Name)
}

func (op DropTable) RollBack() string {
	return NewCreateTable(op.Table).Exec()
}

func (op DropTable) String() string {
	return fmt.Sprintf("DROP TABLE: [%s]", op.Table.Name)
}

type AddForeignKey struct {
	ForeignKey ForeignKey
}

func NewAddForeignKey(key ForeignKey) Operation {
	return AddForeignKey{
		ForeignKey: key,
	}
}

func (op AddForeignKey) String() string {
	return fmt.Sprintf("ADD FOREIGN KEY FROM [%s] IN [%s] => [%s] IN [%s]",
		op.ForeignKey.SourceColumn,
		op.ForeignKey.SourceTable,
		op.ForeignKey.TargetColumn,
		op.ForeignKey.TargetTable)
}

func (op AddForeignKey) Exec() string {
	s := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
		op.ForeignKey.SourceTable,
		op.ForeignKey.Name,
		op.ForeignKey.SourceColumn,
		op.ForeignKey.TargetTable,
		op.ForeignKey.TargetColumn,
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
	return NewDropForeignKey(op.ForeignKey).Exec()
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
		op.ForeignKey.SourceTable, op.ForeignKey.SourceColumn,
		op.ForeignKey.TargetColumn, op.ForeignKey.TargetColumn)
}

func (op DropForeignKey) Exec() string {
	return fmt.Sprintf("ALTER TABLE %s DROP FOREIGN KEY %s",
		op.ForeignKey.SourceTable,
		op.ForeignKey.Name,
	)
}

func (op DropForeignKey) RollBack() string {
	return NewAddForeignKey(op.ForeignKey).Exec()
}

type RenameTable struct {
	CurrentTable Table
	UpdatedTable Table
}

func NewRenameTable(old, new Table) Operation {
	return RenameTable{
		CurrentTable: old,
		UpdatedTable: new,
	}
}

func (op RenameTable) String() string {
	return fmt.Sprintf("RENAME TABLE: [%s] => [%s]", op.CurrentTable.Name, op.UpdatedTable.Name)
}
func (op RenameTable) Exec() string {
	return fmt.Sprintf("ALTER TABLE %s RENAME %s", op.CurrentTable.Name, op.UpdatedTable.Name)
}
func (op RenameTable) RollBack() string {
	return NewRenameTable(op.UpdatedTable, op.CurrentTable).Exec()
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
func (op DropColumn) Exec() string {
	return fmt.Sprintf("ALTER TABLE %s DROP %s", op.Table.Name, op.Column.Name)
}
func (op DropColumn) RollBack() string {
	return NewAddColumn(op.Table, op.Column).Exec()
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
func (op AddColumn) Exec() string {
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", op.Table.Name, op.Column.definitionString)
}
func (op AddColumn) RollBack() string {
	return NewDropColumn(op.Table, op.Column).Exec()
}

type UpdateColumn struct {
	Table         Table
	CurrentColumn Column
	UpdatedColumn Column
}

func NewUpdateColumn(t Table, old, new Column) Operation {
	return UpdateColumn{
		Table:         t,
		CurrentColumn: old,
		UpdatedColumn: new,
	}
}
func (op UpdateColumn) String() string {
	return fmt.Sprintf("CHANGE COLUMN [%s] IN [%s]", op.CurrentColumn.Name, op.Table.Name)
}

func (op UpdateColumn) Exec() string {
	return fmt.Sprintf("ALTER TABLE %s CHANGE COLUMN %s %s",
		op.Table.Name,
		op.CurrentColumn.Name,
		op.UpdatedColumn.definitionString())
}

func (op UpdateColumn) RollBack() string {
	return NewUpdateColumn(op.Table, op.UpdatedColumn, op.CurrentColumn).Exec()
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
func (op DropIndex) Exec() string {
	return fmt.Sprintf("ALTER TABLE %s DROP INDEX %s", op.Table.Name, op.Index.Name)
}
func (op DropIndex) RollBack() string {
	return NewAddIndex(op.Table, op.Index).Exec()
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
func (op DropPrimaryKey) Exec() string {
	return fmt.Sprintf("ALTER TABLE %s DROP PRIMARY KEY", op.Table.Name)
}
func (op DropPrimaryKey) RollBack() string {
	return NewAddPrimaryKey(op.Table, op.PrimaryKey).Exec()
}

type AddPrimaryKey struct {
	Table      Table
	PrimaryKey Key
}

func NewAddPrimaryKey(t Table, k Key) AddPrimaryKey {
	return AddPrimaryKey{PrimaryKey: k}
}
func (op AddPrimaryKey) String() string {
	return fmt.Sprintf("ADD PRIMARY KEY %s IN %s", op.PrimaryKey.Name, op.Table.Name)
}
func (op AddPrimaryKey) Exec() string {
	return fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY %s (%s)", op.PrimaryKey.Name, strings.Join(op.PrimaryKey.Target, ","))
}
func (op AddPrimaryKey) RollBack() string {
	return NewDropPrimaryKey(op.Table, op.PrimaryKey).Exec()
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
func (op AddIndex) Exec() string {
	return fmt.Sprintf("ALTER TABLE %s ADD INDEX %s (%s)", op.Table.Name, op.Index.Name, strings.Join(op.Index.Target, ","))
}
func (op AddIndex) RollBack() string {
	return NewDropIndex(op.Table, op.Index).Exec()
}
