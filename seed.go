package migo

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"sort"
	"strings"

	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
)

const (
	setForeignKeyOff = "SET FOREIGN_KEY_CHECKS=0"
	setForeignKeyOn  = "SET FOREIGN_KEY_CHECKS=1"
)

type Records struct {
	Table string
	Items []map[string]interface{}
}

func NewRecords(table string, items interface{}) (Records, error) {
	r := Records{Table: table}
	qs, ok := items.([]interface{})
	if !ok {
		return Records{}, fmt.Errorf("fail to convert []interface{} type from %s", items)
	}

	m := make([]map[string]interface{}, len(qs))
	for i, q := range qs {
		item, ok := q.(map[string]interface{})
		if !ok {
			return Records{}, fmt.Errorf("fail to convert map[string]interface{} type from %s", q)
		}
		m[i] = item
	}
	r.Items = m
	return r, nil
}

func Seed(op SeedOption) error {

	b, err := ioutil.ReadFile(op.RecordFile)
	if err != nil {
		return errors.Wrap(err, "opening seed file")
	}

	m := map[string]interface{}{}
	err = yaml.Unmarshal(b, &m)
	if err != nil {
		return errors.Wrap(err, "reading seed file")
	}

	rs := []Records{}
	for k, v := range m {
		r, err := NewRecords(k, v)
		if err != nil {
			return errors.Wrapf(err, "parsing %s table record", k)
		}
		rs = append(rs, r)
	}

	db, err := NewDB(op.ConfigFile, op.Environment)
	if err != nil {
		return err
	}

	return db.seed(rs)
}

func (db DB) seed(requests []Records) error {
	mysql, err := sql.Open("mysql", db.FormatDSN())
	if err != nil {
		return errors.Wrap(err, "create mysql connection")
	}
	defer mysql.Close()

	if _, err := mysql.Exec(setForeignKeyOff); err != nil {
		return errors.Wrap(err, "drop foreign key check")
	}

	defer func() {
		if _, err := mysql.Exec(setForeignKeyOn); err != nil {
			fmt.Println(err, "add foreign key check")
		}
	}()

	for _, r := range requests {
		if _, err := mysql.Exec(r.Query()); err != nil {
			return errors.Wrapf(err, "fail to insert request: `%s`", r.Query())
		}
	}

	return nil
}
func (r Records) keys() []string {
	m := map[string]bool{}
	for _, record := range r.Items {
		for k := range record {
			if !m[k] {
				m[k] = true
			}
		}
	}

	keys := make([]string, len(m))
	count := 0
	for k := range m {
		keys[count] = k
		count++
	}
	sort.Strings(keys)
	return keys
}

func fillWhenEmpty(list []string, target map[string]interface{}, fill string) []string {
	m := make([]string, len(list))
	for i, key := range list {
		if target[key] == nil {
			m[i] = fill
			continue
		}
		m[i] = fmt.Sprintf("'%v'", target[key])
	}

	return m
}

func JoinWithComma(list []string, prefix, suffix string) string {
	return prefix + strings.Join(list, ",") + suffix
}

func request(list []string, target map[string]interface{}) string {
	return JoinWithComma(fillWhenEmpty(list, target, "NULL"), "(", ")")
}

func (r Records) Query() string {
	if r.Table == "" || len(r.Items) == 0 {
		return ""
	}

	m := make([]string, len(r.Items))
	keys := r.keys()
	for i, item := range r.Items {
		m[i] = request(keys, item)
	}

	return fmt.Sprintf("INSERT INTO `%s` %s VALUES %s",
		r.Table, JoinWithComma(keys, "(", ")"), strings.Join(m, ","))
}
