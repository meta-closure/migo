package migo

import (
	"fmt"
	"io/ioutil"

	"encoding/json"

	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
)

type DB struct {
	User   string `json:"user"`
	Passwd string `json:"passwd"`
	Addr   string `json:"addr"`
	DBName string `json:"dbname"`
}

type DatabaseConfigure struct {
	Config map[string]*DB
}

func (c DatabaseConfigure) hasEnv(env string) bool {
	for k := range c.Config {
		if k == env {
			return true
		}
	}
	return false
}

func NewDB(filePath, env string) (*DB, error) {
	c, err := NewDatabaseConfigure(filePath)
	if err != nil {
		return nil, errors.Wrap(err, "reading database config")
	}
	if !c.hasEnv(env) {
		return nil, fmt.Errorf("%s is not in database config", env)
	}
	return c.Config[env], nil
}

func NewDatabaseConfigure(filePath string) (DatabaseConfigure, error) {
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return DatabaseConfigure{}, errors.Wrap(err, "YAML file open error")
	}

	m := map[string]interface{}{}
	if err := yaml.Unmarshal(b, &m); err != nil {
		return DatabaseConfigure{}, errors.Wrap(err, "YAML file parse error")
	}

	c := map[string]*DB{}
	for k, v := range m {
		b, err := json.Marshal(v)
		if err != nil {
			return DatabaseConfigure{}, err
		}
		db := DB{}
		if err := json.Unmarshal(b, &db); err != nil {
			return DatabaseConfigure{}, err
		}
		c[k] = &db
	}

	return DatabaseConfigure{Config: c}, nil
}

func (db DB) FormatDSN() string {
	m := NewMySQLConfig(db)
	return m.FormatDSN()
}

func (db DB) FormatDBUnspecifiedDSN() string {
	m := NewMySQLConfig(db)
	m.DBName = ""
	return m.FormatDSN()
}
