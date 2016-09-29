package main

import (
	"log"
	"os"

	"github.com/lestrrat/go-jshschema"
	"github.com/meta-closure/migo"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
)

func SetupCmd() *cli.App {
	app := cli.NewApp()

	app.Name = "migo"
	app.Usage = "database migrater from a JSON Schema file"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "environment, e",
			Value: "default",
			Usage: "Set environment to migrate",
		},
		cli.StringFlag{
			Name:  "database, d",
			Value: "./database.yml",
			Usage: "Load configuration from YAML format file. default database.yml",
		},
		cli.StringFlag{
			Name:  "json, j",
			Usage: "Load configuration from `Schema` JSON format file.",
		},
		cli.StringFlag{
			Name:  "yaml, y",
			Usage: "Load configuration from `Schema` YAML format file.",
		},
		cli.StringFlag{
			Name:  "state, s",
			Usage: "Load internal state from `State` YAML format file.",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:   "run",
			Usage:  "run migrate task",
			Action: Run,
		},
		{
			Name:   "plan",
			Usage:  "get migration plan from Schema file",
			Action: Plan,
		},
		{
			Name:   "init",
			Usage:  "create initial state file",
			Action: StateInit,
		}, {
			Name:   "seed",
			Usage:  "insert seed record",
			Action: Seed,
		},
	}

	return app
}

func Runner(c *cli.Context, mode string) error {
	h := hschema.New()
	if j := c.GlobalString("json"); j != "" {
		err := migo.ParseSchemaJSON(h, j)
		if err != nil {
			return errors.Wrap(err, "Parse Schema error")
		}
	} else if y := c.GlobalString("yaml"); y != "" {
		err := migo.ParseSchemaYAML(h, y)
		if err != nil {
			return errors.Wrap(err, "Parse Schema error")
		}
	} else {
		return errors.New("Source Schema is not specified")
	}

	s := c.GlobalString("state")
	if s == "" {
		return errors.New("State file is not speficied")
	}

	o, err := migo.ParseState(s)
	if err != nil {
		return errors.Wrap(err, "State YAML file parse error")
	}

	db, env := c.GlobalString("database"), c.GlobalString("environment")
	n, err := migo.ParseSchema2State(h, db, env)
	if err != nil {
		return errors.Wrap(err, "Parse HyperSchema to State failed")
	}

	sql, err := o.SQLBuilder(n)
	if err != nil {
		return errors.Wrap(err, "SQL Build error")
	}

	sql.Check()

	if mode == "plan" {
		return nil
	}

	// crash i th Query operation, then return err and i th number
	i, err := sql.Migrate()
	if err != nil {
		merr := errors.Wrap(err, "Database migration error")
		err := sql.Recovery(i)
		if err != nil {
			return errors.Wrap(err, "Recovery error")
		} else {
			return merr
		}
	}

	err = n.Update(s)
	if err != nil {
		return errors.Wrap(err, "Failed to save State file")
	}

	return nil
}

func Run(c *cli.Context) error {
	err := Runner(c, "run")
	if err != nil {
		return errors.Wrap(err, "RUN")
	}
	return nil
}

func Plan(c *cli.Context) error {
	err := Runner(c, "plan")
	if err != nil {
		return errors.Wrap(err, "PLAN")
	}
	return nil
}

func Seed(c *cli.Context) error {
	err := migo.Seed()
	if err != nil {
		return errors.Wrap(err, "SEED")
	}
	return nil
}

func StateInit(c *cli.Context) error {
	n := migo.StateNew()
	err := n.Update("./database_state.yml")
	if err != nil {
		return errors.Wrap(err, "StateInit")
	}
	return nil
}

func _cmd() int {
	cmd := SetupCmd()
	err := cmd.Run(os.Args)
	if err != nil {
		log.Println(err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(_cmd())
}
