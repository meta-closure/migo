package migo

import (
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
	"github.com/urfave/cli"
)

const (
	defaultStateFilePath = "./database_state.yml"
)

func Setup(c *cli.Context) error {
	op, err := NewInitOption(c)
	if err != nil {
		return errors.Wrap(err, "parsing option")
	}

	if err := NewState().save(defaultStateFilePath); err != nil {
		return errors.Wrap(err, "creating initial state file")
	}

	db, err := NewDB(op.ConfigFilePath, op.Environment)
	if err != nil {
		return errors.Wrap(err, "parsing db config")
	}
	return db.setup()
}

func (db DB) setup() error {
	m := NewMySQLConfig(db)
	mysql, err := sql.Open("mysql", m.FormatDSN())
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
