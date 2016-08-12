package migo

import (
	"io/ioutil"
	"testing"

	"github.com/ghodss/yaml"
	"github.com/lestrrat/go-jshschema"
	"github.com/pkg/errors"
)

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

func TestScm2State(t *testing.T) {
	hs := hschema.New()

	err := ParseSchema(hs, "./test/parse_test_schema.yml")
	if err != nil {
		t.Errorf("Test YAML Parse error: %s", err)
	}

	s, err := ParseSchema2State(hs)
	if err != nil {
		t.Errorf("Should pass parse Schema: %s", err)
	}

	if s.Db.User != "test_user" {
		t.Error("Should exist test_user user")
	}

	if s.Db.DBName != "test_db" {
		t.Error("Should exist test_db dbname")
	}

	if s.Db.Passwd != "test_pass" {
		t.Error("Should exist test_pass password")
	}

	if s.Db.Addr != "test_addr" {
		t.Error("Should exist test_addr address")
	}

	tbl, ok := s.GetTable("test_table")

	if ok != true {
		t.Error("Should exist test table")
	}

	if len(tbl.Index.Target) != 2 {
		t.Error("Should index size is 2")
	}

	if len(tbl.PrimaryKey.Target) != 2 {
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

	if fk.TargetTable != "fk_table" {
		t.Error("Should fk target_table is fk_table")
	}
}
