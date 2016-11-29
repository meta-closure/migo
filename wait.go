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

func DbWait(dbpath, env string) error {
	dbconf, err := NewDb(dbpath, env)
	if err != nil {
		return errors.Wrap(err, "Parse db config")
	}

	dbconf.DBName = ""

	if err := mysql.SetLogger(Logger{}); err != nil {
		return errors.Wrap(err, "Can't set logger")
	}

	sqlconf := NewSql(*dbconf)
	var db *sql.DB
	db, err = sql.Open("mysql", sqlconf.DbConf.FormatDSN())
	if err != nil {
		return errors.Wrap(err, "Create mysql connection")
	}

	fmt.Printf("Waiting for accepting query ")
	startedAt := time.Now()
	for {
		fmt.Printf(".")
		query := fmt.Sprintf("show databases")
		if time.Now().Sub(startedAt) > timeoutForWait {
			fmt.Printf("\n")
			return errors.New("Timeout for waiting")
		}
		if _, err := db.Exec(query); err == nil {
			fmt.Printf("\nAccepted\n")
			break
		}

		time.Sleep(time.Second)
	}
	defer db.Close()

	return nil
}
