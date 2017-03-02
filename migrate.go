package migo

import (
	"database/sql"
	"fmt"

	hschema "github.com/lestrrat/go-jshschema"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
)

func (op MigrateOption) isYAMLFormat() bool {
	return op.FormatType == "yaml"
}

func (op MigrateOption) isJSONFormat() bool {
	return op.FormatType == "json"
}

func ReadSchema(op MigrateOption) (*hschema.HyperSchema, error) {
	h := hschema.New()
	if op.isYAMLFormat() {
		if err := readYAMLFormatSchema(h, op.StateFilePath); err != nil {
			return h, err
		}
		return h, nil
	}

	if op.isJSONFormat() {
		if err := readJSONFormatSchema(h, op.StateFilePath); err != nil {
			return h, err
		}
		return h, nil
	}

	return nil, NewMigrateOptionInvalidError()
}

func Plan(c *cli.Context) error {
	op, err := NewMigrateOption(c)
	if err != nil {
		return err
	}

	db, err := NewDB(op.ConfigFilePath, op.Environment)
	if err != nil {
		return err
	}

	old, err := NewStateFromYAML(op.StateFilePath)
	if err != nil {
		return errors.Wrap(err, "State YAML file parse error")
	}

	h, err := ReadSchema(op)
	if err != nil {
		return errors.Wrap(err, "parsing hyper-schema from yaml")
	}

	new, err := NewStateFromSchema(h)
	if err != nil {
		return errors.Wrap(err, "parsing state from hyper-schema")
	}
	new.DB = db

	ops, err := NewOperations(old, new)
	if err != nil {
		return errors.Wrap(err, "creating requests")
	}
	_ = ops
	return nil
}

func Run(c *cli.Context) error {
	op, err := NewMigrateOption(c)
	if err != nil {
		return err
	}

	db, err := NewDB(op.ConfigFilePath, op.Environment)
	if err != nil {
		return err
	}

	old, err := NewStateFromYAML(op.StateFilePath)
	if err != nil {
		return errors.Wrap(err, "State YAML file parse error")
	}

	h, err := ReadSchema(op)
	if err != nil {
		return errors.Wrap(err, "parsing hyper-schema from yaml")
	}

	new, err := NewStateFromSchema(h)
	if err != nil {
		return errors.Wrap(err, "parsing state from hyper-schema")
	}
	new.DB = db

	ops, err := NewOperations(old, new)
	if err != nil {
		return errors.Wrap(err, "creating requests")
	}
	if err := db.migrate(ops); err != nil {
		return err
	}

	if err = new.save(op.StateFilePath); err != nil {
		return errors.Wrap(err, "saving state file")
	}
	return nil
}

func (db DB) migrate(ops Operations) error {
	if err := db.exec(ops); err != nil {
		if rerr := db.rollback(ops); err != nil {
			return errors.Wrapf(rerr, "migrate error with `%s` and recovery failed", err)
		}
		return errors.Wrap(err, "migration failed")
	}
	return nil
}

func (db DB) rollback(ops Operations) error {
	m := NewMySQLConfig(db)
	mysql, err := sql.Open("mysql", m.FormatDSN())
	if err != nil {
		return err
	}
	defer mysql.Close()

	for i := 1; i < i+1; i++ {
		if _, err = mysql.Exec(ops.Operation[ops.execCount-i].Exec()); err != nil {
			fmt.Printf("FAILED: %s", ops.Operation[ops.execCount-i].Exec())
			fmt.Printf("ERROR:  %s", err)
			fmt.Println(">>>>>>>> RECOVERY FAILED")
			return errors.Wrapf(err, "Query: %s", ops.Operation[ops.execCount-i].Exec())
		}
	}

	fmt.Println(">>>>>>>> RECOVERY SUCCEED")
	return nil
}

func (db DB) exec(ops Operations) error {
	m := NewMySQLConfig(db)
	mysql, err := sql.Open("mysql", m.FormatDSN())
	if err != nil {
		return err
	}
	defer mysql.Close()

	for i, op := range ops.Operation {
		if _, err := mysql.Exec(op.Exec()); err != nil {
			fmt.Printf("FAILED: %s", op.Exec())
			fmt.Printf("ERROR:  %s", err)
			fmt.Println(">>>>>>>> MIGRATION FAILED")
			ops.execCount = i
			return errors.Wrapf(err, "Query: %s", op.Exec())
		}
	}

	fmt.Println(">>>>>>>> MIGRATION SUCCEED")
	return nil
}
