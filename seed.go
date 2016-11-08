package migo

import (
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
)

const (
	offFKCheck = "SET FOREIGN_KEY_CHECKS = 0"
	onFKCheck  = "SET FOREIGN_KEY_CHECKS = 1"
)

func GetDSN(dbpath, env string) (string, error) {
	dbconf, err := NewDb(dbpath, env)
	if err != nil {
		return "", errors.Wrap(err, "Parse db configue")
	}
	sqlconf := NewSql(*dbconf)
	return sqlconf.DbConf.FormatDSN(), nil
}

func GetInsertQuery(table string, data interface{}) (string, error) {
	q := fmt.Sprintf("INSERT INTO %s ", table)

	l, ok := data.(map[string]interface{})
	if !ok {
		return q, errors.Wrapf(ErrInvalidTable, "%+v", data)
	}
	var key, val string
	for k, v := range l {
		key += fmt.Sprintf("%s, ", k)
		switch v := v.(type) {
		default:
			val += fmt.Sprintf("'%v', ", v)
		case string:
			val += fmt.Sprintf("'%s', ", v)
		}
	}
	q += fmt.Sprintf("(%s) VALUES (%s)", key[0:len(key)-2], val[0:len(val)-2])
	return q, nil
}

func GetInsertQueries(y map[string]interface{}) ([]string, error) {
	qs := []string{offFKCheck}
	for table, data := range y {
		datalist, ok := data.([]interface{})
		if !ok {
			return nil, errors.Wrapf(ErrInvalidTable, "%+v", data)
		}
		for _, d := range datalist {
			q, err := GetInsertQuery(table, d)
			if err != nil {
				return nil, errors.Wrap(err, "Query build failed")
			}
			qs = append(qs, q)
		}
	}
	qs = append(qs, onFKCheck)
	return qs, nil
}

func GetDeleteQueries(y map[string]interface{}) ([]string, error) {

	qs := []string{offFKCheck}
	for table, _ := range y {
		qs = append(qs, fmt.Sprintf("TRUNCATE TABLE %s", table))
	}
	qs = append(qs, onFKCheck)
	return qs, nil
}

func Exec(qs []string, dsn string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return errors.Wrap(err, "Create db connection")
	}
	defer db.Close()

	for _, q := range qs {
		_, err := db.Exec(q)
		if err != nil {
			return errors.Wrapf(err, "Query execute error: %s", q)
		}
	}
	return nil
}

func InsertRecords(y map[string]interface{}, dsn string) error {
	qs, err := GetInsertQueries(y)
	if err != nil {
		return err
	}
	if err := Exec(qs, dsn); err != nil {
		return errors.Wrap(err, "insert records")
	}
	return nil
}

func DropRecords(y map[string]interface{}, dsn string) error {
	qs, err := GetDeleteQueries(y)
	if err != nil {
		return err
	}
	if err := Exec(qs, dsn); err != nil {
		return errors.Wrap(err, "truncate records")
	}
	return nil
}

func Seed(path, dbpath, env string) error {
	dsn, err := GetDSN(dbpath, env)
	if err != nil {
		return err
	}

	y, err := ParseYAML(path)
	if err != nil {
		return err
	}

	if err := InsertRecords(y, dsn); err != nil {
		return err
	}

	return nil

}
