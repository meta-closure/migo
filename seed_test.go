package migo_test

import (
	"database/sql"
	"testing"

	"github.com/meta-closure/migo"
)

const (
	parentSeedFilePath = "test/foreign_key_test_parent_seed.yml"
	childSeedFilePath  = "test/foreign_key_test_child_seed.yml"
	databaseFilePath   = "test/seed_test_database.yml"
)

func TestSeedWithForeingKey(t *testing.T) {
	db, err := sql.Open("mysql", DSN)
	if err != nil {
		t.Fatalf("fail to connetion to %s with error %s", DSN, err)
	}
	defer db.Close()

	// Create database
	qs := []string{
		"CREATE DATABASE IF NOT EXISTS test",
		"USE test",
		"CREATE TABLE parent(id int NOT NULL PRIMARY KEY, name varchar(20))",
		"CREATE TABLE child(parent_id int, FOREIGN KEY(parent_id) REFERENCES parent(id), name varchar(20))",
	}

	for _, q := range qs {
		if _, err = db.Exec(q); err != nil {
			t.Fatalf("Failed to exec query with error %s", err)
		}
	}
	defer func() {
		q := "DROP DATABASE test"
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("Failed to drop database with error %s", err)
		}
	}()

	if err := migo.Seed(migo.SeedOption{
		RecordFilePath: childSeedFilePath,
		ConfigFilePath: databaseFilePath,
		Environment:    "test",
	}); err != nil {
		t.Fatalf("fail to seed child table records: %s", err)
	}

	if err := migo.Seed(migo.SeedOption{
		RecordFilePath: parentSeedFilePath,
		ConfigFilePath: databaseFilePath,
		Environment:    "test",
	}); err != nil {
		t.Fatalf("fail to seed parent table records: %s", err)
	}

	// Assert
	p := parent{}
	r := db.QueryRow("SELECT id, name FROM parent")
	if err != nil {
		t.Fatal("Failed to select parent:", err)
	}
	r.Scan(&p.ID, &p.Name)
	if p.ID != 1 || p.Name != "parent1" {
		t.Errorf("Expected arguments are (1, \"parent1\") but actual (%d, %q)", p.ID, p.Name)
	}

	c := child{}
	r = db.QueryRow("SELECT parent_id, name FROM child")
	if err != nil {
		t.Fatal("Failed to select child:", err)
	}
	r.Scan(&c.ParentID, &c.Name)
	if c.ParentID != 1 || c.Name != "child1" {
		t.Errorf("Expected arguments are (1, \"child1\") but actual (%d, %q)", c.ParentID, c.Name)
	}
}
