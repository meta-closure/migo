package migo

import "testing"

// test case state that have normal table setting
func TableCase() *State {
	st := StateNew()

	st.Table = []Table{
		Table{
			Id:   "test_table_id",
			Name: "test_table_name",
		},
	}

	return st
}

// test case state that have normal column setting
func ColumnCase() *State {
	st := TableCase()

	st.Table[0].Column = []Column{
		Column{
			Id:   "test_column_id",
			Name: "test_column",
			Type: "int",
		},
	}

	return st
}

// test case state that have primary key
func PKCase() *State {
	st := ColumnCase()

	st.Table[0].PrimaryKey = []Key{
		Key{Target: []string{"test_column_id"}, Name: "test_pk"},
	}

	return st
}

// test case state that have index
func IndexCase() *State {
	st := ColumnCase()

	st.Table[0].Index = []Key{
		Key{Target: []string{"test_column_id"}, Name: "test_index"},
	}

	return st
}

// test case state that have foreign key
func FKCase() *State {
	st := &State{}
	st.Table = []Table{{
		Id:   "test_fk_id1",
		Name: "test_fk_name1",
	}, {
		Id:   "test_fk_id2",
		Name: "test_fk_name2",
	}}

	st.Table[0].Column = []Column{{
		Id:   "test_fk_column1",
		Name: "test_fk_column_name1",
	}, {
		Id:   "test_fk_column2",
		Name: "test_fk_column_name2",
	}}

	st.Table[1].Column = []Column{{
		Id:   "test_fk_column1",
		Name: "test_fk_column_name1",
	}, {
		Id:   "test_fk_column2",
		Name: "test_fk_column_name2",
		FK: ForeignKey{
			Name:         "test2test",
			TargetColumn: "test_fk_column1",
			TargetTable:  "test_fk_id1",
		},
	}}

	return st
}

func TestSQLBuildAddTable(t *testing.T) {
	o := StateNew()
	n := TableCase()

	sql, err := o.SQLBuilder(n)
	if err != nil {
		t.Error("`Build error: %s", err)
	}

	if len(sql.Operations) != 2 {
		t.Fatal("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != ADDTBL {
		t.Error("Should Operation type ADDTBL")
	}

	if sql.Operations[1].Column.Name != "padding" {
		t.Error("Should drop padding column")
	}
}

func TestSQLBuildDropTable(t *testing.T) {
	o := TableCase()
	n := StateNew()

	sql, err := o.SQLBuilder(n)
	if err != nil {
		t.Error("`Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Fatal("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != DROPTBL {
		t.Error("Should Operation type DROPTBL")
	}

}

func TestSQLBuildRenameTable(t *testing.T) {
	o := TableCase()
	n := TableCase()

	n.Table[0].Name = "new_name"

	sql, err := o.SQLBuilder(n)
	if err != nil {
		t.Error("`Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Fatal("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != CHANGETBL {
		t.Error("Should Operation type CHANGETBL")
	}

}

func TestSQLBuildEmptyState(t *testing.T) {
	s := StateNew()
	sql, err := s.SQLBuilder(s)
	if err != nil {
		t.Errorf("Build error: %s", err)
	}
	if len(sql.Operations) != 0 {
		t.Error("Empty state should build empty operation")
	}
}

func TestSQLBuildAddColumn(t *testing.T) {
	o := TableCase()
	n := ColumnCase()

	sql, err := o.SQLBuilder(n)
	if err != nil {
		t.Error("`Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Fatal("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != ADDCLM {
		t.Errorf("Should Operation type ADDCLM: %+v", sql.Operations)
	}

}

func TestSQLBuildDropColumn(t *testing.T) {
	o := ColumnCase()
	n := TableCase()

	sql, err := o.SQLBuilder(n)
	if err != nil {
		t.Error("`Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Fatal("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != DROPCLM {
		t.Errorf("Should Operation type DROPCLM: %s", sql.Operations)
	}

}

func TestSQLBuildSameColumn(t *testing.T) {

	o := ColumnCase()
	n := ColumnCase()

	sql, err := o.SQLBuilder(n)
	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 0 {
		t.Fatalf("Should build 0 Operation: %+v", sql.Operations)
	}

}

func TestSQLBuildOddColumn(t *testing.T) {
	o := ColumnCase()
	n := ColumnCase()

	n.Table[0].Column[0].UniqueFlag = true

	sql, err := o.SQLBuilder(n)

	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Fatal("Should build 0 peration")
	}

	if sql.Operations[0].OperationType != MODIFYCLM {
		t.Error("Should OperationType MODIFYCLM")
	}
}

func TestSQLBuildRenameColumn(t *testing.T) {
	o := ColumnCase()
	n := ColumnCase()

	n.Table[0].Column[0].Name = "test_new_name"

	sql, err := o.SQLBuilder(n)

	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Fatal("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != CHANGECLM {
		t.Error("Should OperationType CHANGECLM")
	}

}

func TestSQLBuildAddPK(t *testing.T) {
	o := ColumnCase()
	n := PKCase()

	sql, err := o.SQLBuilder(n)

	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Fatal("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != ADDPK {
		t.Error("Should OperationType ADDPK")
	}
}

func TestSQLBuildDropPK(t *testing.T) {
	o := PKCase()
	n := ColumnCase()

	sql, err := o.SQLBuilder(n)

	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Fatal("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != DROPPK {
		t.Error("Should OperationType DROPPK")
	}
}

func TestSQLBuildAddIndex(t *testing.T) {
	o := ColumnCase()
	n := IndexCase()

	sql, err := o.SQLBuilder(n)

	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Fatal("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != ADDINDEX {
		t.Error("Should OperationType ADDINDEX")
	}
}

func TestSQLBuildDropIndex(t *testing.T) {
	o := IndexCase()
	n := ColumnCase()

	sql, err := o.SQLBuilder(n)

	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Fatal("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != DROPINDEX {
		t.Error("Should OperationType DROPINDEX")
	}
}

func TestSQLAddPK(t *testing.T) {
	o := ColumnCase()
	n := PKCase()

	n.Table[0].Column[0].AutoIncrementFlag = true

	sql, err := o.SQLBuilder(n)

	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 2 {
		t.Fatal("Should build 2 operation: %+v", sql.Operations)
	}

	if sql.Operations[0].OperationType != ADDPK {
		t.Errorf("Should first operation type is ADDPK: %d", sql.Operations[0].OperationType)
	}

	if sql.Operations[1].OperationType != MODIFYAICLM {
		t.Errorf("Should first operation type is MODEFYAICLM: %d", sql.Operations[1].OperationType)
	}
}

func TestSQLDropPK(t *testing.T) {
	o := PKCase()
	n := ColumnCase()

	o.Table[0].Column[0].AutoIncrementFlag = true

	sql, err := o.SQLBuilder(n)

	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 2 {
		t.Fatalf("Should build 2 operation: %+v", sql.Operations)
	}

	if sql.Operations[0].OperationType != MODIFYAICLM {
		t.Errorf("Should first operation type is MODEFYAICLM: %d", sql.Operations[0].OperationType)
	}

	if sql.Operations[1].OperationType != DROPPK {
		t.Errorf("Should first operation type is DROPPK: %d", sql.Operations[1].OperationType)
	}
}

func TestSQLBuildAddAutoIncrement(t *testing.T) {

	o := PKCase()
	n := PKCase()

	n.Table[0].Column[0].AutoIncrementFlag = true

	sql, err := o.SQLBuilder(n)

	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Fatal("Should build 1 operation: %+v", sql.Operations)
	}

	if sql.Operations[0].OperationType != MODIFYAICLM {
		t.Error("Should OperationType MODIFYAICLM: %+v", sql.Operations)
	}
}

func TestSQLBuildDropAutoIncrement(t *testing.T) {

	o := PKCase()
	n := PKCase()

	o.Table[0].Column[0].AutoIncrementFlag = true

	sql, err := o.SQLBuilder(n)

	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Fatal("Should build 1 operation: %+v", sql.Operations)
	}

	if sql.Operations[0].OperationType != MODIFYAICLM {
		t.Error("Should OperationType MODIFYAICLM: %+v", sql.Operations)
	}
}

func TestSQLBuildAddFK(t *testing.T) {
	o := FKCase()
	n := FKCase()

	o.Table[1].Column[1].FK = ForeignKey{}

	sql, err := o.SQLBuilder(n)

	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Fatal("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != ADDFK {
		t.Error("Should OperationType ADDFK")
	}
}

func TestSQLBuildDropFK(t *testing.T) {
	o := FKCase()
	n := FKCase()

	n.Table[1].Column[1].FK = ForeignKey{}

	sql, err := o.SQLBuilder(n)

	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Fatal("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != DROPFK {
		t.Error("Should OperationType DROPFK")
	}
}
