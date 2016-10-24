package migo

import (
	"io/ioutil"
	"testing"

	"github.com/ghodss/yaml"
	"github.com/lestrrat/go-jshschema"
	"github.com/pkg/errors"
)

type NewDbTestCase struct {
	DbPath       string
	Env          string
	ExpectDbName string
}

func ParseSchema(h *hschema.HyperSchema, pt string) error {
	b, err := ioutil.ReadFile(pt)
	if err != nil {
		return errors.Wrap(err, "YAML file open error")
	}
	y := &map[string]interface{}{}
	err = yaml.Unmarshal(b, y)
	if err != nil {
		return errors.Wrap(err, "YAML file parse error")
	}
	h.Extract(*y)
	return nil
}

func TestNewDb(t *testing.T) {
	tests := []NewDbTestCase{{
		DbPath:       "./test/database.yml",
		Env:          "default",
		ExpectDbName: "default",
	}, {
		DbPath:       "./test/database.yml",
		Env:          "other",
		ExpectDbName: "env",
	}}
	for _, test := range tests {
		db, err := NewDb(test.DbPath, test.Env)
		if err != nil {
			t.Error(err)
		}
		if db.DBName != test.ExpectDbName {
			t.Errorf("Parse db config error: config: %+v, result: %+v", test, db)
		}
	}
}

func TestScm2State(t *testing.T) {
	hs := hschema.New()

	err := ParseSchema(hs, "./test/parse_test_schema.yml")
	if err != nil {
		t.Errorf("Test YAML Parse error: %s", err)
	}

	s, err := ParseSchema2State(hs, "./test/database.yml", "default")
	if err != nil {
		t.Fatalf("Should pass parse Schema: %s", err)
	}

	tbl, ok := s.GetTable("#/definitions/test_table")

	if ok != true {
		t.Error("Should exist test table")
	}

	if len(tbl.Index[0].Target) != 2 {
		t.Error("Should index size is 2")
	}

	if len(tbl.PrimaryKey[0].Target) != 2 {
		t.Error("Should primary key size is 2")
	}

	if len(tbl.Column) != 1 {
		t.Error("Should column size is 1")
	}

	col, ok := tbl.GetColumn("test_column")

	if ok != true {
		t.Error("Should exist test_column column")
	}

	if col.Type != "test_type" {
		t.Error("Should column type is test_type")
	}

	if col.NotNullFlag != true {
		t.Error("Should not null flag is true")
	}

	if col.UniqueFlag != true {
		t.Error("Should unique flag is true")
	}

	if col.Default != "default_test" {
		t.Error("Should default is default_test")
	}

	if col.AutoIncrementFlag != true {
		t.Error("Should auto_increment flag is true")
	}

	fk := col.FK

	if fk.Name != "fk_test" {
		t.Error("Should fk name is fk_test")
	}

	if fk.TargetColumn != "fk_column" {
		t.Error("Should fk target_column is fk_column")
	}

	if fk.TargetTable != "#/definitions/fk_table" {
		t.Error("Should fk target_table is #/definitions/fk_table")
	}
}
