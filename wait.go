package migo

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

const (
	timeoutForWait = time.Minute
)

type Logger struct{}

func (l Logger) Print(v ...interface{}) {
	// do nothing
}

func Wait(op WaitOption) error {
	db, err := NewDB(op.ConfigFilePath, op.Environment)
	if err != nil {
		return errors.Wrap(err, "Parse db config")
	}
	return db.wait()
}

func (db DB) wait() error {
	db.DBName = ""

	if err := mysql.SetLogger(Logger{}); err != nil {
		return errors.Wrap(err, "Can't set logger")
	}

	m := NewMySQLConfig(db)
	mysql, err := sql.Open("mysql", m.FormatDSN())
	if err != nil {
		return errors.Wrap(err, "Create mysql connection")
	}
	defer mysql.Close()

	fmt.Printf("Waiting for accepting query ")
	startedAt := time.Now()
	for {
		fmt.Printf(".")
		query := fmt.Sprintf("show databases")
		if time.Now().Sub(startedAt) > timeoutForWait {
			fmt.Printf("\n")
			return errors.New("Timeout for waiting")
		}
		if _, err := mysql.Exec(query); err == nil {
			fmt.Printf("\nAccepted\n")
			break
		}

		time.Sleep(time.Second)
	}

	return nil
}
