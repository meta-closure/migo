package migo_test

import (
	"os"
	"testing"

	"github.com/go-xorm/xorm"

	"github.com/meta-closure/migo"
)

func TestSetup(t *testing.T) {
	if err := migo.Setup(migo.InitOption{
		ConfigFilePath: databaseFilePath,
		Environment:    "test",
	}); err != nil {
		t.Fatalf("fail to setup: %s", err)
	}

	engine, err := xorm.NewEngine("mysql", DSN)
	if err != nil {
		t.Fatalf("fail to connetion to %s with error %s", DSN, err)
	}
	defer engine.Close()

	result, err := engine.Query("SHOW DATABASES")
	if err != nil {
		t.Fatalf("fail to get database list with error %s", err)
	}

	isContain := false
	for _, b := range result {
		if "test" == string(b["Database"]) {
			isContain = true
		}
	}
	if !isContain {
		t.Fatalf("fail to create database")
	}

	if _, err := os.Stat("./database_state.yml"); err != nil {
		t.Fatalf("fail to create database status file")
	}
	if err := os.Remove("./database_state.yml"); err != nil {
		t.Fatalf("fail to delete database state file")
	}
}
