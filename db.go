package migo

import (
	"fmt"
	"io/ioutil"

	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
)

type DB struct {
	User   string
	Passwd string
	Addr   string
	DBName string
}

type Environment struct {
	Config map[string]DB
}

func NewDB(filePath, env string) (DB, error) {
	e, err := NewEnvironment(filePath)
	if err != nil {
		return DB{}, errors.Wrap(err, "reading database config")
	}

	if e.hasNotEnv(env) {
		return DB{}, fmt.Errorf("%s is not in database config", env)
	}
	return e.Config[env], nil
}

func NewEnvironment(filePath string) (Environment, error) {
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return Environment{}, errors.Wrap(err, "YAML file open error")
	}

	e := Environment{}
	if err := yaml.Unmarshal(b, &e); err != nil {
		return Environment{}, errors.Wrap(err, "YAML file parse error")
	}
	return e, nil
}
