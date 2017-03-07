package migo_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/lestrrat/go-test-mysqld"
)

type parent struct {
	ID   int
	Name string
}

type child struct {
	ParentID int
	Name     string
}

var (
	port = 13306
	DSN  = "root@tcp(127.0.0.1:13306)/"
)

func NewMySQLTest(port int) *mysqltest.MysqldConfig {
	conf := mysqltest.NewConfig()
	conf.SkipNetworking = false
	conf.Port = port
	return conf
}

func TestMain(m *testing.M) {
	config := NewMySQLTest(port)
	mysqld, err := mysqltest.NewMysqld(config)
	if err != nil {
		fmt.Printf("Failed to start mysqld with error %s", err)
		os.Exit(1)
	}
	defer mysqld.Stop()
	os.Exit(m.Run())
}
