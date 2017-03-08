package migo_test

import (
	"reflect"
	"testing"

	"github.com/meta-closure/migo"
)

func TestNewStateFromSchema(t *testing.T) {

	type Case struct {
		input         migo.MigrateOption
		expectedState migo.State
		isSuccess     bool
		spec          string
	}

	cases := []Case{
		{
			input: migo.MigrateOption{
				SchemaFile: "./test/parse_test_fail_by_index.yml",
				FormatType: "yaml",
			},
			spec:      "incorrect index setting",
			isSuccess: false,
		},
		{
			input: migo.MigrateOption{
				SchemaFile: "./test/parse_test_fail_by_pk.yml",
				FormatType: "yaml",
			},
			spec:      "incorrect primary key setting",
			isSuccess: false,
		},
		{
			input: migo.MigrateOption{
				SchemaFile: "./test/parse_test_fail_by_fk.yml",
				FormatType: "yaml",
			},
			spec:      "incorrect foreign key setting",
			isSuccess: false,
		},
		{
			input: migo.MigrateOption{
				SchemaFile: "./test/parse_test_column.yml",
				FormatType: "yaml",
			},
			expectedState: migo.State{
				Tables: []migo.Table{
					{
						Id:   "#/definitions/test",
						Name: "test",
						Column: []migo.Column{
							{
								Id:            "column",
								Name:          "column",
								Type:          "type",
								Unique:        true,
								Default:       "default_test",
								AutoIncrement: true,
								AutoUpdate:    true,
								NotNull:       true,
							},
						},
					},
				},
			},
			spec:      "correct column",
			isSuccess: true,
		},
		{
			input: migo.MigrateOption{
				SchemaFile: "./test/parse_test_fk.yml",
				FormatType: "yaml",
			},
			expectedState: migo.State{
				Tables: []migo.Table{
					{
						Id:   "#/definitions/source_table",
						Name: "test2",
						Column: []migo.Column{
							{
								Name:    "source_column",
								Id:      "source_column",
								Type:    "source_type",
								Unique:  true,
								Default: "default_test",
							},
						},
					},
					{
						Id:   "#/definitions/target_table",
						Name: "test1",
						Column: []migo.Column{
							{
								Id:      "target_column",
								Name:    "target_column",
								Type:    "target_type",
								Unique:  true,
								Default: "default_test",
							},
						},
						PrimaryKey: []migo.Key{
							{
								Target: []migo.Column{
									{
										Id:      "target_column",
										Name:    "target_column",
										Type:    "target_type",
										Unique:  true,
										Default: "default_test",
									},
								},
								Name: "test_pk",
							},
						},
					},
				},
				ForeignKey: []migo.ForeignKey{
					{
						Name:          "fk_test",
						DeleteCascade: true,
						SourceColumn: migo.Column{
							Name:    "source_column",
							Id:      "source_column",
							Type:    "source_type",
							Unique:  true,
							Default: "default_test",
						},
						SourceTable: migo.Table{
							Id:   "#/definitions/source_table",
							Name: "test2",
							Column: []migo.Column{
								{
									Name:    "source_column",
									Id:      "source_column",
									Type:    "source_type",
									Unique:  true,
									Default: "default_test",
								},
							},
						},
						TargetColumn: migo.Column{
							Id:      "target_column",
							Name:    "target_column",
							Type:    "target_type",
							Unique:  true,
							Default: "default_test",
						},
						TargetTable: migo.Table{
							Id:   "#/definitions/target_table",
							Name: "test1",
							Column: []migo.Column{
								{
									Id:      "target_column",
									Name:    "target_column",
									Type:    "target_type",
									Unique:  true,
									Default: "default_test",
								},
							},
							PrimaryKey: []migo.Key{
								{
									Target: []migo.Column{
										{
											Id:      "target_column",
											Name:    "target_column",
											Type:    "target_type",
											Unique:  true,
											Default: "default_test",
										},
									},
									Name: "test_pk",
								},
							},
						},
					},
				},
			},
			spec:      "correct foreign key setting",
			isSuccess: true,
		},
		{
			input: migo.MigrateOption{
				SchemaFile: "./test/parse_test_pk.yml",
				FormatType: "yaml",
			},
			expectedState: migo.State{
				Tables: []migo.Table{
					{
						Id:   "#/definitions/test_table",
						Name: "test",
						Column: []migo.Column{
							{
								Id:      "test_primary_key_column",
								Name:    "test_column",
								Type:    "test_type",
								Unique:  true,
								Default: "default_test",
							},
						},
						PrimaryKey: []migo.Key{
							{
								Name: "test_pk",
								Target: []migo.Column{
									{
										Id:      "test_primary_key_column",
										Name:    "test_column",
										Type:    "test_type",
										Unique:  true,
										Default: "default_test",
									},
								},
							},
						},
					},
				},
			},
			spec:      "correct primary key setting",
			isSuccess: true,
		},
		{
			input: migo.MigrateOption{
				SchemaFile: "./test/parse_test_index.yml",
				FormatType: "yaml",
			},
			expectedState: migo.State{
				Tables: []migo.Table{
					{
						Id:   "#/definitions/test_table",
						Name: "test",
						Column: []migo.Column{
							{
								Id:      "test_index_column",
								Name:    "test_column",
								Type:    "test_type",
								Unique:  true,
								Default: "default_test",
							},
						},
						Index: []migo.Key{
							{
								Name: "test_index",
								Target: []migo.Column{
									{
										Id:      "test_index_column",
										Name:    "test_column",
										Type:    "test_type",
										Unique:  true,
										Default: "default_test",
									},
								},
							},
						},
					},
				},
			},
			spec:      "correct index setting",
			isSuccess: true,
		},
	}

	for _, c := range cases {
		h, err := migo.ReadSchema(c.input)
		if err != nil {
			t.Errorf("in %s, fail to read %s path file because %s", c.spec, c.input.SchemaFile, err)
			continue
		}

		s, err := migo.NewStateFromSchema(h)
		if !c.isSuccess && err == nil {
			t.Errorf("in %s, error is expected but null", c.spec)
			continue
		}
		if c.isSuccess && err != nil {
			t.Errorf("in %s, catche the unexpected error %s", c.spec, err)
		}

		if err != nil {
			continue
		}

		c.expectedState.UpdatedAt = s.UpdatedAt
		for i := range c.expectedState.ForeignKey {
			s.ForeignKey[i].Raw = c.expectedState.ForeignKey[i].Raw
		}
		if !reflect.DeepEqual(c.expectedState, s) {
			t.Errorf("in %s, expected state is %+v\n, but actual %+v\n", c.spec, c.expectedState, s)
		}
	}
}
