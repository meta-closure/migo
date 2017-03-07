package migo_test

import (
	"reflect"
	"testing"

	"github.com/meta-closure/migo"
)

func TestNewOperations(t *testing.T) {
	type Input struct {
		CurrentState migo.State
		NewState     migo.State
	}

	type Case struct {
		input           Input
		expectedQueries []string
		isSuccess       bool
		spec            string
	}

	cases := []Case{
		{
			spec: "create table with foreign key",
			input: Input{
				NewState: migo.State{
					ForeignKey: []migo.ForeignKey{
						{
							SourceTable: migo.Table{
								Id:   "#/definitions/source_table",
								Name: "table1",
								Column: []migo.Column{
									{
										Id:   "source_column",
										Name: "column1",
										Type: "type1",
									},
								},
							},
							SourceColumn: migo.Column{
								Id:   "source_column",
								Name: "column1",
								Type: "type1",
							},
							TargetTable: migo.Table{
								Id:   "#/definitions/target_table",
								Name: "table2",
								Column: []migo.Column{
									{
										Id:   "target_column",
										Name: "column2",
										Type: "type2",
									},
								},
							},
							TargetColumn: migo.Column{
								Id:   "target_column",
								Name: "column2",
								Type: "type2",
							},
						},
					},
					Tables: []migo.Table{
						{
							Id:   "#/definitions/source_table",
							Name: "table1",
							Column: []migo.Column{
								{
									Id:   "source_column",
									Name: "column1",
									Type: "type1",
								},
							},
						},
						{
							Id:   "#/definitions/target_table",
							Name: "table2",
							Column: []migo.Column{
								{
									Id:   "target_column",
									Name: "column2",
									Type: "type2",
								},
							},
						},
					},
				},
			},
			expectedQueries: []string{
				"CREATE TABLE table1 (column1 type1)ENGINE=innoDB",
				"CREATE TABLE table2 (column2 type2)ENGINE=innoDB",
				"ALTER TABLE table1 ADD CONSTRAINT  FOREIGN KEY (column1) REFERENCES table2 (column2)",
			},
			isSuccess: true,
		},
		{
			spec: "drop table with foreign key",
			input: Input{
				CurrentState: migo.State{
					ForeignKey: []migo.ForeignKey{
						{
							SourceTable: migo.Table{
								Id:   "#/definitions/source_table",
								Name: "table1",
								Column: []migo.Column{
									{
										Id:   "source_column",
										Name: "column1",
										Type: "type1",
									},
								},
							},
							SourceColumn: migo.Column{
								Id:   "source_column",
								Name: "column1",
								Type: "type1",
							},
							TargetTable: migo.Table{
								Id:   "#/definitions/target_table",
								Name: "table2",
								Column: []migo.Column{
									{
										Id:   "target_column",
										Name: "column2",
										Type: "type2",
									},
								},
							},
							TargetColumn: migo.Column{
								Id:   "target_column",
								Name: "column2",
								Type: "type2",
							},
						},
					},
					Tables: []migo.Table{
						{
							Id:   "#/definitions/source_table",
							Name: "table1",
							Column: []migo.Column{
								{
									Id:   "source_column",
									Name: "column1",
									Type: "type1",
								},
							},
						},
						{
							Id:   "#/definitions/target_table",
							Name: "table2",
							Column: []migo.Column{
								{
									Id:   "target_column",
									Name: "column2",
									Type: "type2",
								},
							},
						},
					},
				},
			},
			expectedQueries: []string{
				"ALTER TABLE table1 DROP FOREIGN KEY ",
				"DROP TABLE table1",
				"DROP TABLE table2",
			},
			isSuccess: true,
		},
	}

	for _, c := range cases {
		op, err := migo.NewOperations(c.input.CurrentState, c.input.NewState)
		if !c.isSuccess && err == nil {
			t.Errorf("in %s, error is expected but null", c.spec)
			continue
		}
		if c.isSuccess && err != nil {
			t.Errorf("in %s, catche the unexpected error %s", c.spec, err)
			continue
		}

		if err != nil {
			continue
		}

		if len(op.Operation) != len(c.expectedQueries) {
			t.Errorf("in %s, expected query length is %d, but actual %d", c.spec, len(c.expectedQueries), len(op.Operation))
			continue
		}
		for i := range op.Operation {
			if c.expectedQueries[i] != op.Operation[i].Query() {
				t.Errorf("in %s, expected query is %s, but actual %s", c.spec, c.expectedQueries[i], op.Operation[i].Query())
			}
		}
	}
}

func TestCreateTables(t *testing.T) {
	type Input struct {
		state  migo.State
		tables []migo.Table
	}
	type Case struct {
		input           Input
		expectedQueries []string
		isSuccess       bool
		spec            string
	}

	cases := []Case{
		{
			spec: "create table",
			input: Input{
				tables: []migo.Table{
					{
						Id:   "table1_id",
						Name: "table1",
					},
					{
						Id:   "table2_id",
						Name: "table2",
					},
				},
			},
			expectedQueries: []string{
				"CREATE TABLE table1 ()ENGINE=innoDB",
				"CREATE TABLE table2 ()ENGINE=innoDB",
			},
			isSuccess: true,
		},
		{
			spec: "create indiced table",
			input: Input{
				tables: []migo.Table{
					{
						Id:   "table1_id",
						Name: "table1",
						Column: []migo.Column{
							{
								Id:   "indiced_column",
								Name: "column1",
								Type: "type1",
							},
						},
						Index: []migo.Key{
							{
								Name: "test_index",
								Target: []migo.Column{
									{
										Id:   "indiced_column",
										Name: "column1",
										Type: "type1",
									},
								},
							},
						},
					},
				},
			},
			expectedQueries: []string{
				"CREATE TABLE table1 (column1 type1)ENGINE=innoDB",
			},
			isSuccess: true,
		},
	}
	for _, c := range cases {
		op := migo.Operations{}
		err := op.CreateTables(c.input.state, c.input.tables)

		if !c.isSuccess && err == nil {
			t.Errorf("in %s, error is expected but null", c.spec)
			continue
		}
		if c.isSuccess && err != nil {
			t.Errorf("in %s, catche the unexpected error %s", c.spec, err)
			continue
		}

		if len(op.Operation) != len(c.expectedQueries) {
			t.Errorf("in %s, expected query length is %d, but actual %d", c.spec, len(c.expectedQueries), len(op.Operation))
			continue
		}
		for i := range op.Operation {
			if c.expectedQueries[i] != op.Operation[i].Query() {
				t.Errorf("in %s, expected query is %s, but actual %s", c.spec, c.expectedQueries[i], op.Operation[i].Query())
			}
		}
	}
}

func TestUpdateTable(t *testing.T) {
	type Input struct {
		CurrentTable migo.Table
		NewTable     migo.Table
	}
	type Case struct {
		input           Input
		expectedQueries []string
		isSuccess       bool
		spec            string
	}

	cases := []Case{
		{
			input: Input{
				CurrentTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "before",
					Column: []migo.Column{
						{
							Id:   "column",
							Name: "column",
						},
					},
				},
				NewTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "after",
					Column: []migo.Column{
						{
							Id:   "column",
							Name: "column",
						},
					},
				},
			},
			expectedQueries: []string{
				"ALTER TABLE before RENAME after",
				"ALTER TABLE after CHANGE COLUMN column column ",
			},
			isSuccess: true,
			spec:      "rename table",
		},
		{
			input: Input{
				CurrentTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Id:   "column",
							Name: "before_column",
						},
					},
				},
				NewTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Id:            "column",
							Name:          "after_column",
							Type:          "type",
							AutoIncrement: true,
							Unique:        true,
							NotNull:       true,
							Default:       "default",
						},
					},
				},
			},
			expectedQueries: []string{
				"ALTER TABLE table CHANGE COLUMN before_column after_column type AUTO_INCREMENT NOT NULL UNIQUE DEFAULT 'default'",
			},
			isSuccess: true,
			spec:      "update column field",
		},
		{
			input: Input{
				CurrentTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Id:   "column1",
							Name: "column",
						},
						{
							Id:   "column2",
							Name: "before_column",
						},
					},
				},
				NewTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Id:   "column1",
							Name: "column",
						},
					},
				},
			},
			expectedQueries: []string{
				"ALTER TABLE table DROP COLUMN before_column",
				"ALTER TABLE table CHANGE COLUMN column column ",
			},
			isSuccess: true,
			spec:      "delete column",
		},
		{
			input: Input{
				CurrentTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
				},
				NewTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Id:   "column",
							Name: "after_column",
						},
					},
				},
			},
			expectedQueries: []string{
				"ALTER TABLE table ADD COLUMN after_column ",
			},
			isSuccess: true,
			spec:      "add column",
		},
		{
			input: Input{
				CurrentTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Id:   "column1",
							Name: "primary_key_column1",
						},
						{
							Id:   "column2",
							Name: "primary_key_column2",
						},
					},
				},
				NewTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Id:   "column1",
							Name: "primary_key_column1",
						},
						{
							Id:   "column2",
							Name: "primary_key_column2",
						},
					},
					PrimaryKey: []migo.Key{
						{
							Name: "key",
							Target: []migo.Column{
								{
									Id:   "column1",
									Name: "primary_key_column1",
								},
								{
									Id:   "column2",
									Name: "primary_key_column2",
								},
							},
						},
					},
				},
			},
			expectedQueries: []string{
				"ALTER TABLE table ADD PRIMARY KEY key (primary_key_column1,primary_key_column2)",
				"ALTER TABLE table CHANGE COLUMN primary_key_column1 primary_key_column1 ",
				"ALTER TABLE table CHANGE COLUMN primary_key_column2 primary_key_column2 ",
			},
			isSuccess: true,
			spec:      "add primary key",
		},
		{
			input: Input{
				CurrentTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Id:   "column1",
							Name: "primary_key_column1",
						},
						{
							Id:   "column2",
							Name: "primary_key_column2",
						},
					},
					PrimaryKey: []migo.Key{
						{
							Name: "key",
							Target: []migo.Column{
								{
									Id:   "column1",
									Name: "primary_key_column1",
								},
								{
									Id:   "column2",
									Name: "primary_key_column2",
								},
							},
						},
					},
				},
				NewTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Id:   "column1",
							Name: "primary_key_column1",
						},
						{
							Id:   "column2",
							Name: "primary_key_column2",
						},
					},
				},
			},
			expectedQueries: []string{
				"ALTER TABLE table DROP PRIMARY KEY",
				"ALTER TABLE table CHANGE COLUMN primary_key_column1 primary_key_column1 ",
				"ALTER TABLE table CHANGE COLUMN primary_key_column2 primary_key_column2 ",
			},
			isSuccess: true,
			spec:      "drop primary key",
		},
		{
			input: Input{
				CurrentTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Id:   "column1",
							Name: "index_column1",
						},
						{
							Id:   "column2",
							Name: "index_column2",
						},
					},
				},
				NewTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Id:   "column1",
							Name: "index_column1",
						},
						{
							Id:   "column2",
							Name: "index_column2",
						},
					},
					Index: []migo.Key{
						{
							Name: "key",
							Target: []migo.Column{
								{
									Id:   "column1",
									Name: "index_column1",
								},
								{
									Id:   "column2",
									Name: "index_column2",
								},
							},
						},
					},
				},
			},
			expectedQueries: []string{
				"ALTER TABLE table ADD INDEX key (index_column1,index_column2)",
				"ALTER TABLE table CHANGE COLUMN index_column1 index_column1 ",
				"ALTER TABLE table CHANGE COLUMN index_column2 index_column2 ",
			},
			isSuccess: true,
			spec:      "add index",
		},
		{
			input: Input{
				CurrentTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Index: []migo.Key{
						{
							Name: "key",
							Target: []migo.Column{
								{
									Id:   "column1",
									Name: "index_column1",
								},
								{
									Id:   "column2",
									Name: "index_column2",
								},
							},
						},
					},
					Column: []migo.Column{
						{
							Id:   "column1",
							Name: "index_column1",
						},
						{
							Id:   "column2",
							Name: "index_column2",
						},
					},
				},
				NewTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Id:   "column1",
							Name: "index_column1",
						},
						{
							Id:   "column2",
							Name: "index_column2",
						},
					},
				},
			},
			expectedQueries: []string{
				"ALTER TABLE table DROP INDEX key",
				"ALTER TABLE table CHANGE COLUMN index_column1 index_column1 ",
				"ALTER TABLE table CHANGE COLUMN index_column2 index_column2 ",
			},
			isSuccess: true,
			spec:      "drop index",
		},
		{
			input: Input{
				CurrentTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
				},
				NewTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Name: "column",
							Id:   "pk_column",
							Type: "int",
						},
					},
					PrimaryKey: []migo.Key{
						{
							Name: "column_primary_key",
							Target: []migo.Column{
								{
									Name: "column",
									Id:   "pk_column",
									Type: "int",
								},
							},
						},
					},
				},
			},
			isSuccess: true,
			spec:      "add primary keyed column",
			expectedQueries: []string{
				"ALTER TABLE table ADD COLUMN column int",
				"ALTER TABLE table ADD PRIMARY KEY column_primary_key (column)",
			},
		},
		{
			input: Input{
				CurrentTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Name: "column1",
							Id:   "pk_column",
							Type: "int",
						},
						{
							Name: "column2",
							Id:   "not_pk_column",
							Type: "int",
						},
					},
					PrimaryKey: []migo.Key{
						{
							Name: "column_primary_key",
							Target: []migo.Column{
								{
									Name: "column1",
									Id:   "pk_column",
									Type: "int",
								},
							},
						},
					},
				},
				NewTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Name: "column2",
							Id:   "not_pk_column",
							Type: "int",
						},
					},
				},
			},
			isSuccess: true,
			spec:      "drop primary keyed column",
			expectedQueries: []string{
				"ALTER TABLE table DROP PRIMARY KEY",
				"ALTER TABLE table DROP COLUMN column1",
				"ALTER TABLE table CHANGE COLUMN column2 column2 int",
			},
		},
		{
			input: Input{
				CurrentTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
				},
				NewTable: migo.Table{
					Column: []migo.Column{
						{
							Name: "column",
							Id:   "indiced_column",
							Type: "int",
						},
					},
					Index: []migo.Key{
						{
							Name: "column_index",
							Target: []migo.Column{
								{
									Name: "column",
									Id:   "indiced_column",
									Type: "int",
								},
							},
						},
					},
					Id:   "#/definitions/table",
					Name: "table",
				},
			},
			isSuccess: true,
			spec:      "add indiced column",
			expectedQueries: []string{
				"ALTER TABLE table ADD COLUMN column int",
				"ALTER TABLE table ADD INDEX column_index (column)",
			},
		},
		{
			input: Input{
				CurrentTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Name: "column1",
							Id:   "indiced_column",
							Type: "int",
						},
						{
							Name: "column2",
							Id:   "not_indiced_column",
							Type: "int",
						},
					},
					Index: []migo.Key{
						{
							Name: "column_index",
							Target: []migo.Column{
								{
									Name: "column1",
									Id:   "indiced_column",
									Type: "int",
								},
							},
						},
					},
				},
				NewTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Name: "column2",
							Id:   "not_indiced_column",
							Type: "int",
						},
					},
				},
			},
			isSuccess: true,
			spec:      "drop indiced column",
			expectedQueries: []string{
				"ALTER TABLE table DROP INDEX column_index",
				"ALTER TABLE table DROP COLUMN column1",
				"ALTER TABLE table CHANGE COLUMN column2 column2 int",
			},
		},
		{
			input: Input{
				CurrentTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Name: "column1",
							Id:   "indiced_column1",
							Type: "int",
						},
						{
							Name: "column2",
							Id:   "indiced_column2",
							Type: "int",
						},
					},
					Index: []migo.Key{
						{
							Name: "column_index",
							Target: []migo.Column{
								{
									Name: "column1",
									Id:   "indiced_column1",
									Type: "int",
								},
								{
									Name: "column2",
									Id:   "indiced_column2",
									Type: "int",
								},
							},
						},
					},
				},
				NewTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Name: "column1",
							Id:   "indiced_column1",
							Type: "int",
						},
					},
					Index: []migo.Key{
						{
							Name: "column_index",
							Target: []migo.Column{
								{
									Name: "column1",
									Id:   "indiced_column1",
									Type: "int",
								},
							},
						},
					},
				},
			},
			isSuccess: true,
			spec:      "drop indiced member column",
			expectedQueries: []string{
				"ALTER TABLE table DROP COLUMN column2",
				"ALTER TABLE table DROP INDEX column_index",
				"ALTER TABLE table ADD INDEX column_index (column1)",
				"ALTER TABLE table CHANGE COLUMN column1 column1 int",
			},
		},
		{
			input: Input{
				CurrentTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Name: "column1",
							Id:   "primary_keyed_column1",
							Type: "int",
						},
						{
							Name: "column2",
							Id:   "primary_keyed_column2",
							Type: "int",
						},
					},
					PrimaryKey: []migo.Key{
						{
							Name: "column_primary_key",
							Target: []migo.Column{
								{
									Name: "column1",
									Id:   "primary_keyed_column1",
									Type: "int",
								},
								{
									Name: "column2",
									Id:   "primary_keyed_column2",
									Type: "int",
								},
							},
						},
					},
				},
				NewTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Name: "column1",
							Id:   "primary_keyed_column1",
							Type: "int",
						},
					},
					PrimaryKey: []migo.Key{
						{
							Name: "column_primary_key",
							Target: []migo.Column{
								{
									Name: "column1",
									Id:   "primary_keyed_column1",
									Type: "int",
								},
							},
						},
					},
				},
			},
			isSuccess: true,
			spec:      "drop primary keyed member column",
			expectedQueries: []string{
				"ALTER TABLE table DROP COLUMN column2",
				"ALTER TABLE table DROP PRIMARY KEY",
				"ALTER TABLE table ADD PRIMARY KEY column_primary_key (column1)",
				"ALTER TABLE table CHANGE COLUMN column1 column1 int",
			},
		},
		{
			input: Input{
				CurrentTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Id:   "column1",
							Name: "column",
						},
					},
				},
				NewTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Id:   "column1",
							Name: "column",
						},
					},
					Index: []migo.Key{
						{
							Name: "column_index",
						},
					},
				},
			},
			isSuccess: false,
			spec:      "index is exist, but member is empty",
		},
		{
			input: Input{
				CurrentTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Id:   "column1",
							Name: "column",
						},
					},
				},
				NewTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
				},
			},
			isSuccess: false,
			spec:      "table's column is empty",
		},
		{
			input: Input{
				CurrentTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Id:   "column1",
							Name: "column",
						},
					},
				},
				NewTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Id:   "column1",
							Name: "column",
						},
					},
					PrimaryKey: []migo.Key{
						{
							Name: "column_primary_key",
						},
					},
				},
			},
			isSuccess: false,
			spec:      "primary key is exist, but member is empty",
		},
		{
			input: Input{
				CurrentTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Id:   "column1",
							Name: "column",
						},
					},
				},
				NewTable: migo.Table{
					Id:   "#/definitions/table",
					Name: "table",
					Column: []migo.Column{
						{
							Id:   "column1",
							Name: "column",
						},
					},
					Index: []migo.Key{
						{
							Name: "column_index",
							Target: []migo.Column{
								{
									Id:   "column1",
									Name: "column",
								},
							},
						},
						{
							Name: "column_index",
							Target: []migo.Column{
								{
									Id:   "column1",
									Name: "column",
								},
							},
						},
					},
				},
			},
			isSuccess: false,
			spec:      "index name is not unique",
		},
	}
	for _, c := range cases {
		op := migo.Operations{}
		err := op.UpdateTable(c.input.CurrentTable, c.input.NewTable)
		if !c.isSuccess && err == nil {
			t.Errorf("in %s, error is expected but null", c.spec)
			continue
		}
		if c.isSuccess && err != nil {
			t.Errorf("in %s, catche the unexpected error %s", c.spec, err)
			continue
		}

		if len(op.Operation) != len(c.expectedQueries) {
			t.Errorf("in %s, expected query length is %d, but actual %d", c.spec, len(c.expectedQueries), len(op.Operation))
			continue
		}
		for i := range op.Operation {
			if c.expectedQueries[i] != op.Operation[i].Query() {
				t.Errorf("in %s, expected query is %s, but actual %s.", c.spec, c.expectedQueries[i], op.Operation[i].Query())
			}
		}
	}
}

func TestDropTables(t *testing.T) {
	type Case struct {
		inputTables        []migo.Table
		expectedOperations migo.Operations
		isSuccess          bool
		spec               string
	}

	cases := []Case{}
	for _, c := range cases {
		op := migo.Operations{}
		err := op.DropTables(c.inputTables)

		if !c.isSuccess && err == nil {
			t.Errorf("in %s, error is expected but null", c.spec)
			continue
		}
		if c.isSuccess && err != nil {
			t.Errorf("in %s, catche the unexpected error %s", c.spec, err)
		}

		if !reflect.DeepEqual(c.expectedOperations, op) {
			t.Errorf("in %s, expected state is %#v, but actual %#v,", c.spec, c.expectedOperations, op)
		}
	}
}
