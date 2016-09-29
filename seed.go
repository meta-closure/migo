package migo

import (
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
)

func Seed(path, dbpath, env string) error {
	dbconf, err := ParseSchema2Db(dbpath, env)
	if err != nil {
		return errors.Wrap(err, "Parse db configue")
	}
	sqlconf := NewSql(*dbconf)
	db, err := sql.Open("mysql", sqlconf.DbConf.FormatDSN())
	if err != nil {
		return errors.Wrap(err, "Create db connection")
	}
	defer db.Close()

	y, err := ParseYAML(path)
	if err != nil {
		return errors.Wrap(err, "Parse YAML")
	}

	qs := []string{}
	for table, data := range y {
		datalist, ok := data.([]interface{})
		if !ok {
			return errors.Wrapf(ErrInvalidTable, "%+v", data)
		}
		for _, d := range datalist {
			q, err := InsertQueryBuilder(table, d)
			if err != nil {
				return errors.Wrap(err, "Query build failed")
			}
			qs = append(qs, q)
		}
	}

	for _, q := range qs {
		_, err := db.Exec(q)
		if err != nil {
			return errors.Wrapf(err, "Query execute error: %s", q)
		}
	}
	return nil
}

func InsertQueryBuilder(table string, data interface{}) (string, error) {
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
