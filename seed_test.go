package migo

import (
	"database/sql"
	"fmt"
	"log"
	"os/exec"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/lestrrat/go-test-mysqld"
)

func TestSeedWithForeingKey(t *testing.T) {
	conf := mysqltest.NewConfig()
	conf.SkipNetworking = false
	conf.Port = 13306

	// start new instance of mysql
	mysqld, err := mysqltest.NewMysqld(conf)
	if err != nil {
		t.Fatal("Failed to start mysqld:", err)
	}
	defer mysqld.Stop()

	// connect to instance
	sqlconf := mysql.Config{
		User:   "root",
		Addr:   "127.0.0.1:13306",
		Net:    "tcp",
		Passwd: "",
		DBName: "",
	}
	db, err := sql.Open("mysql", sqlconf.FormatDSN())
	if err != nil {
		t.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	// create new database
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS test")
	if _, err := db.Exec(query); err != nil {
		t.Fatal("Failed to create database if not exist:", err)
	}
	defer func() {
		query = fmt.Sprint("DROP DATABASE test")
		if _, err := db.Exec(query); err != nil {
			t.Fatal("Failed to drop test database:", err)
		}
	}()
	query = fmt.Sprint("USE test")
	if _, err := db.Exec(query); err != nil {
		t.Fatal("Failed to use test database:", err)
	}

	// create tables
	query = fmt.Sprint("CREATE TABLE test.parent(id int NOT NULL PRIMARY KEY, name varchar(20))")
	if _, err := db.Exec(query); err != nil {
		t.Fatal("Failed to create parent table:", err)
	}
	query = fmt.Sprint("CREATE TABLE test.child(parent_id int, FOREIGN KEY(parent_id) REFERENCES parent(id), name varchar(20))")
	if _, err := db.Exec(query); err != nil {
		t.Fatal("Failed to create child table:", err)
	}

	// seeding child
	err = exec.Command("go", "run", "cmd/migo/main.go", "-S", "test/child_seed.yml", "-d", "test/seed_test_database.yml", "-e", "test", "seed").Run()
	if err != nil {
		t.Error(err.Error())
	}
	// seeding parent
	err = exec.Command("go", "run", "cmd/migo/main.go", "-S", "test/parent_seed.yml", "-d", "test/seed_test_database.yml", "-e", "test", "seed").Run()
	if err != nil {
		t.Error(err.Error())
	}

	// assert
	var id int
	var name string

	parent := db.QueryRow("SELECT id, name FROM parent LIMIT 1")
	if err != nil {
		log.Fatal(err)
	}
	parent.Scan(&id, &name)
	parentID, parentName := id, name
	if parentID != 0 || parentName != "parent0" {
		t.Error("Expected parentID is 0 and parentName is parent0 but not.")
	}

	child := db.QueryRow("SELECT parent_id, name FROM child LIMIT 1")
	if err != nil {
		log.Fatal(err)
	}
	child.Scan(&id, &name)
	childID, childName := id, name
	if childID != 0 || childName != "child0" {
		t.Error("Expected childID is 0 and childName is child0 but not.")
	}
}
