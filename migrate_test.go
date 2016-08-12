package migo

import (
	"testing"
)

func TestSQLBuildAddTable(t *testing.T) {
	o := StateNew()
	n := StateNew()

	n.Table = []Table{Table{
		Name: "test_table",
	}}

	sql, err := o.SQLBuilder(n)
	if err != nil {
		t.Error("`Build error: %s", err)
	}

	if len(sql.Operations) != 2 {
		t.Error("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != ADDTBL {
		t.Error("Should Operation type ADDTBL")
	}

	if sql.Operations[1].Column.Name != "padding" {
		t.Error("Should drop padding column")
	}
}

func TestSQLBuildDropTable(t *testing.T) {
	o := StateNew()
	n := StateNew()

	o.Table = []Table{Table{
		Name: "test_table",
	}}

	sql, err := o.SQLBuilder(n)
	if err != nil {
		t.Error("`Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Error("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != DROPTBL {
		t.Error("Should Operation type DROPTBL")
	}

}

func TestSQLBuildRenameTable(t *testing.T) {
	o := StateNew()
	n := StateNew()

	o.Table = []Table{Table{
		Name: "test_table",
	}}

	n.Table = []Table{Table{
		BeforeName: "test_table",
		Name:       "test_table_new",
	}}

	sql, err := o.SQLBuilder(n)
	if err != nil {
		t.Error("`Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Error("Should build 1 operation")
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
	o := StateNew()
	n := StateNew()

	o.Table = []Table{Table{
		Name: "test_table",
	}}

	n.Table = []Table{Table{
		Name: "test_table",
		Column: []Column{Column{
			Name: "test_column",
			Type: "int",
		}},
	}}

	sql, err := o.SQLBuilder(n)
	if err != nil {
		t.Error("`Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Error("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != ADDCLM {
		t.Error("Should Operation type ADDCLM")
	}

}

func TestSQLBuildDropColumn(t *testing.T) {
	o := StateNew()
	n := StateNew()

	n.Table = []Table{Table{
		Name: "test_table",
	}}

	o.Table = []Table{Table{
		Name: "test_table",
		Column: []Column{Column{
			Name: "test_column",
			Type: "int",
		}},
	}}

	sql, err := o.SQLBuilder(n)
	if err != nil {
		t.Error("`Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Error("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != DROPCLM {
		t.Error("Should Operation type DROPCLM")
	}

}

func TestSQLBuildSameColumn(t *testing.T) {

	o := StateNew()
	n := StateNew()
	n.Table = []Table{Table{
		Name: "test_table",
		Column: []Column{Column{
			Name: "test_column",
			Type: "int",
		}},
	}}

	o.Table = n.Table
	sql, err := o.SQLBuilder(n)
	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 0 {
		t.Error("Should build 0 peration")
	}

}

func TestSQLBuildOddColumn(t *testing.T) {
	o := StateNew()
	n := StateNew()
	n.Table = []Table{Table{
		Name: "test_table",
		Column: []Column{Column{
			Name:        "test_column",
			Type:        "int",
			NotNullFlag: true,
		}},
	}}

	o.Table = []Table{Table{
		Name: "test_table",
		Column: []Column{Column{
			Name: "test_column",
			Type: "int",
		}},
	}}

	sql, err := o.SQLBuilder(n)

	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Error("Should build 0 peration")
	}

	if sql.Operations[0].OperationType != MODIFYCLM {
		t.Error("Should OperationType MODIFYCLM")
	}

}

func TestSQLBuildRenameColumn(t *testing.T) {
	o := StateNew()
	n := StateNew()
	o.Table = []Table{Table{
		Name: "test_table",
		Column: []Column{Column{
			Name: "test_column",
			Type: "int",
		}},
	}}

	n.Table = []Table{Table{
		Name: "test_table",
		Column: []Column{Column{
			BeforeName: "test_column",
			Name:       "test_column_new",
			Type:       "int",
		}},
	}}

	sql, err := o.SQLBuilder(n)

	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Error("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != CHANGECLM {
		t.Error("Should OperationType CHANGECLM")
	}

}

func TestSQLBuildAddPK(t *testing.T) {
	o := StateNew()
	n := StateNew()

	o.Table = []Table{Table{
		Name: "test_table",
		Column: []Column{Column{
			Name: "test_column",
			Type: "int",
		}},
	}}

	n.Table = []Table{Table{
		Name: "test_table",
		PrimaryKey: Key{
			Target: []string{"test_column"},
		},
		Column: []Column{Column{
			Name: "test_column",
			Type: "int",
		}},
	}}

	sql, err := o.SQLBuilder(n)

	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Error("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != ADDPK {
		t.Error("Should OperationType ADDPK")
	}
}

func TestSQLBuildDropPK(t *testing.T) {
	o := StateNew()
	n := StateNew()

	o.Table = []Table{Table{
		Name: "test_table",
		PrimaryKey: Key{
			Target: []string{"test_column"},
		},
		Column: []Column{Column{
			Name: "test_column",
			Type: "int",
		}},
	}}

	n.Table = []Table{Table{
		Name: "test_table",
		Column: []Column{Column{
			Name: "test_column",
			Type: "int",
		}},
	}}

	sql, err := o.SQLBuilder(n)

	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Error("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != DROPPK {
		t.Error("Should OperationType DROPPK")
	}
}

func TestSQLBuildAddIndex(t *testing.T) {
	o := StateNew()
	n := StateNew()

	o.Table = []Table{Table{
		Name: "test_table",
		Column: []Column{Column{
			Name: "test_column",
			Type: "int",
		}},
	}}

	n.Table = []Table{Table{
		Name: "test_table",
		Index: Key{
			Target: []string{"test_column"},
		},
		Column: []Column{Column{
			Name: "test_column",
			Type: "int",
		}},
	}}

	sql, err := o.SQLBuilder(n)

	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Error("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != ADDINDEX {
		t.Error("Should OperationType ADDINDEX")
	}
}

func TestSQLBuildDropIndex(t *testing.T) {
	o := StateNew()
	n := StateNew()

	o.Table = []Table{Table{
		Name: "test_table",
		Index: Key{
			Target: []string{"test_column"},
		},
		Column: []Column{Column{
			Name: "test_column",
			Type: "int",
		}},
	}}

	n.Table = []Table{Table{
		Name: "test_table",
		Column: []Column{Column{
			Name: "test_column",
			Type: "int",
		}},
	}}

	sql, err := o.SQLBuilder(n)

	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Error("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != DROPINDEX {
		t.Error("Should OperationType DROPINDEX")
	}
}

func TestSQLBuildAddAutoIncrement(t *testing.T) {

	o := StateNew()
	n := StateNew()

	o.Table = []Table{Table{
		Name: "test_table",
		Column: []Column{Column{
			Name: "test_column",
			Type: "int",
		}},
	}}

	n.Table = []Table{Table{
		Name: "test_table",
		PrimaryKey: Key{
			Target: []string{"test_column"},
		},
		Column: []Column{Column{
			AutoIncrementFlag: true,
			Name:              "test_column",
			Type:              "int",
		}},
	}}

	sql, err := o.SQLBuilder(n)

	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 2 {
		t.Error("Should build 2 operation")
	}

	if sql.Operations[0].OperationType != ADDPK {
		t.Error("Should First Operation's OperationType ADDPK")
	}

	if sql.Operations[1].OperationType != MODIFYAICLM {
		t.Error("Should Second Operation's OperationType MODIFYAICLM")
	}
}

func TestSQLBuildAddFK(t *testing.T) {
	o := StateNew()
	n := StateNew()

	o.Table = []Table{Table{
		Name: "test_table",
		PrimaryKey: Key{
			Target: []string{"test_column"},
		},
		Column: []Column{Column{
			Name: "test_column",
			Type: "int",
		}},
	}}

	n.Table = []Table{Table{
		Name: "test_table",
		PrimaryKey: Key{
			Target: []string{"test_column"},
		},
		Column: []Column{Column{
			Name: "test_column",
			Type: "int",
			FK: ForeignKey{
				Name:         "test2test",
				TargetColumn: "test_column_parent",
				TargetTable:  "test_table_parent",
			},
		}},
	}}

	sql, err := o.SQLBuilder(n)

	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Error("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != ADDFK {
		t.Error("Should OperationType ADDFK")
	}
}

func TestSQLBuildDropFK(t *testing.T) {
	o := StateNew()
	n := StateNew()

	n.Table = []Table{Table{
		Name: "test_table",
		PrimaryKey: Key{
			Target: []string{"test_column"},
		},
		Column: []Column{Column{
			Name: "test_column",
			Type: "int",
		}},
	}}

	o.Table = []Table{Table{
		Name: "test_table",
		PrimaryKey: Key{
			Target: []string{"test_column"},
		},
		Column: []Column{Column{
			Name: "test_column",
			Type: "int",
			FK: ForeignKey{
				Name:         "test2test",
				TargetColumn: "test_column_parent",
				TargetTable:  "test_table_parent",
			},
		}},
	}}

	sql, err := o.SQLBuilder(n)

	if err != nil {
		t.Error("Build error: %s", err)
	}

	if len(sql.Operations) != 1 {
		t.Error("Should build 1 operation")
	}

	if sql.Operations[0].OperationType != DROPFK {
		t.Error("Should OperationType DROPFK")
	}
}
