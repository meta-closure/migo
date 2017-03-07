package migo_test

import (
	"testing"

	"github.com/meta-closure/migo"

	"reflect"
)

func TestNewDB(t *testing.T) {
	type Input struct {
		filePath    string
		environment string
	}
	type Cases struct {
		input      Input
		spec       string
		expectedDB migo.DB
		isSuccess  bool
	}

	cases := []Cases{
		{
			input: Input{
				filePath:    "./test/database_test.yml",
				environment: "default",
			},
			expectedDB: migo.DB{
				User:   "default",
				Passwd: "test",
				Addr:   "127.0.0.1:3306",
				DBName: "default",
			},
			isSuccess: true,
			spec:      "read default environment database configure",
		},
		{
			input: Input{
				filePath:    "./test/database_test.yml",
				environment: "invalid",
			},
			isSuccess: false,
			spec:      "incorrect environment",
		},
	}

	for _, c := range cases {
		db, err := migo.NewDB(c.input.filePath, c.input.environment)
		if !c.isSuccess && err == nil {
			t.Errorf("in %s, error is expected but null", c.spec)
			continue
		}
		if c.isSuccess && err != nil {
			t.Errorf("in %s, catche the unexpected error %s", c.spec, err)
			continue
		}

		if err != nil {
			continue
		}

		if !reflect.DeepEqual(db, &c.expectedDB) {
			t.Errorf("in %s, expected DB is %+v, but actual %+v", c.spec, c.expectedDB, db)
		}
	}
}
