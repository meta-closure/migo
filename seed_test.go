package migo

import (
	"database/sql"
	"os/exec"
	"testing"

	"github.com/lestrrat/go-test-mysqld"
)

type parent struct {
	ID   int
	Name string
}

type child struct {
	ParentID int
	Name     string
}

var (
	port = 13306
	DSN  = "root@tcp(127.0.0.1:13306)/"

	parentSeedFilePath = "test/parent_seed.yml"
	childSeedFilePath  = "test/child_seed.yml"
	databaseFilePath   = "test/seed_test_database.yml"
)

func TestSeedWithForeingKey(t *testing.T) {
	// Starts mysql
	conf := mysqltest.NewConfig()
	conf.SkipNetworking = false
	conf.Port = port
	mysqld, err := mysqltest.NewMysqld(conf)
	if err != nil {
		t.Fatal("Failed to start mysqld:", err)
	}
	defer mysqld.Stop()

	// Connects mysql
	db, err := sql.Open("mysql", DSN)
	if err != nil {
		t.Fatal("Failed to connect database, ", DSN, ":", err)
	}
	defer db.Close()

	// Create database
	qs := []string{}
	qs = append(qs, "CREATE DATABASE IF NOT EXISTS test")
	qs = append(qs, "USE test")
	qs = append(qs, "CREATE TABLE parent(id int NOT NULL PRIMARY KEY, name varchar(20))")
	qs = append(qs, "CREATE TABLE child(parent_id int, FOREIGN KEY(parent_id) REFERENCES parent(id), name varchar(20))")
	for _, q := range qs {
		if _, err = db.Exec(q); err != nil {
			t.Fatal("Failed to exec query:", err)
		}
	}
	defer func() {
		q := "DROP DATABASE test"
		if _, err := db.Exec(q); err != nil {
			t.Fatal("Failed to drop database:", err)
		}
	}()

	// Seeding child
	err = exec.Command("go", "run", "cmd/migo/main.go", "-S", childSeedFilePath, "-d", databaseFilePath, "-e", "test", "seed").Run()
	if err != nil {
		t.Fatal("Failed to seed parent:", err)
	}
	// Seeding parent
	err = exec.Command("go", "run", "cmd/migo/main.go", "-S", parentSeedFilePath, "-d", databaseFilePath, "-e", "test", "seed").Run()
	if err != nil {
		t.Fatal("Failed to seed child:", err)
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
