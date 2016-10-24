package migo

import (
	"database/sql"
	"fmt"
	"io/ioutil"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

func StateInit() error {
	n := StateNew()
	err := n.Update("./database_state.yml")
	if err != nil {
		return errors.Wrap(err, "State file init")
	}
	return nil
}

func DbInit(dbpath, env string) error {
	dbconf, err := NewDb(dbpath, env)
	if err != nil {
		return errors.Wrap(err, "Parse db config")
	}

	dbname := dbconf.DBName
	dbconf.DBName = ""

	sqlconf := NewSql(*dbconf)
	db, err := sql.Open("mysql", sqlconf.DbConf.FormatDSN())
	if err != nil {
		return errors.Wrap(err, "Create mysql connection")
	}
	defer db.Close()

	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s CHARACTER SET utf8mb4", dbname)
	if _, err := db.Exec(query); err != nil {
		return errors.Wrap(err, "Create database if not exist")
	}

	return nil
}

func (s *State) Update(path string) error {
	b, err := yaml.Marshal(s)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(path, b, 0777)
	if err != nil {
		return err
	}
	return nil
}
