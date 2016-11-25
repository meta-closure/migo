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
	app.Usage = "Migrate database with a JSON Schema file"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "seed, S",
			Value: "seed.yml",
			Usage: "`Seeding` YAML file path",
		},
		cli.StringFlag{
			Name:  "environment, e",
			Value: "default",
			Usage: "Set `environment` to migrate",
		},
		cli.StringFlag{
			Name:  "database, d",
			Value: "./database.yml",
			Usage: "Load `database` configuration from YAML formatted file. In default database.yml",
		},
		cli.StringFlag{
			Name:  "json, j",
			Usage: "Load configuration from `Schema` JSON formatted file.",
		},
		cli.StringFlag{
			Name:  "yaml, y",
			Usage: "Load configuration from `Schema` YAML formatted file.",
		},
		cli.StringFlag{
			Name:  "state, s",
			Usage: "Load internal state from `State` YAML formatted file.",
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
			Usage:  "create initial state file and create database if not exist",
			Action: Init,
		}, {
			Name:   "seed",
			Usage:  "insert seed record",
			Action: Seed,
		},
	}

	return app
}

func runner(c *cli.Context, mode string) error {
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
		return errors.New("State file is not specified")
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

	// if crash i th Query operation, then return err and i th number
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

func seed(c *cli.Context) error {
	s, db, env := c.GlobalString("seed"), c.GlobalString("database"), c.GlobalString("environment")
	if err := migo.Seed(s, db, env); err != nil {
		return errors.Wrap(err, "Failed to seeding")
	}
	return nil
}

func initMigo(c *cli.Context) error {
	if err := migo.StateInit(); err != nil {
		return errors.Wrap(err, "State init")
	}

	db, env := c.GlobalString("database"), c.GlobalString("environment")
	if err := migo.DbInit(db, env); err != nil {
		return errors.Wrap(err, "Database create")
	}
	return nil
}

func Run(c *cli.Context) error {
	err := runner(c, "run")
	if err != nil {
		return errors.Wrap(err, "RUN")
	}
	return nil
}

func Plan(c *cli.Context) error {
	err := runner(c, "plan")
	if err != nil {
		return errors.Wrap(err, "PLAN")
	}
	return nil
}

func Seed(c *cli.Context) error {
	err := seed(c)
	if err != nil {
		return errors.Wrap(err, "SEED")
	}
	return nil
}

func Init(c *cli.Context) error {
	err := initMigo(c)
	if err != nil {
		return errors.Wrap(err, "INIT")
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
