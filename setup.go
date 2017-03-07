package migo

import (
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
)

const (
	defaultStateFilePath = "./database_state.yml"
)

func Setup(op InitOption) error {
	var err error
	s := NewState()
	db, err := NewDB(op.ConfigFilePath, op.Environment)
	if err != nil {
		return errors.Wrap(err, "parsing db config")
	}
	s.DB = *db
	if err := s.DB.setup(); err != nil {
		return errors.Wrapf(err, "creating database in %s", s.DB.FormatDSN())
	}

	if err := s.save(defaultStateFilePath); err != nil {
		return errors.Wrap(err, "creating initial state file")
	}
	return nil
}

func (db DB) setup() error {
	mysql, err := sql.Open("mysql", db.FormatDBUnspecifiedDSN())
	if err != nil {
		return errors.Wrap(err, "create mysql connection")
	}
	defer mysql.Close()

	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s CHARACTER SET utf8mb4", db.DBName)
	if _, err := mysql.Exec(query); err != nil {
		return errors.Wrap(err, "create database if not exist")
	}

	return nil
}
