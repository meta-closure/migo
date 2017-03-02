package main

import (
	"log"
	"os"

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
			Name:   "wait",
			Usage:  "wait for connecting to database",
			Action: Wait,
		},
		{
			Name:   "init",
			Usage:  "create initial state file and create database if not exist",
			Action: Init,
		},
		{
			Name:   "seed",
			Usage:  "insert seed record",
			Action: Seed,
		},
	}

	return app
}

func Seed(c *cli.Context) error {
	if err := migo.Seed(c); err != nil {
		return errors.Wrap(err, "SEED")
	}
	return nil
}

func Run(c *cli.Context) error {
	if err := migo.Run(c); err != nil {
		return errors.Wrap(err, "RUN")
	}
	return nil
}

func Plan(c *cli.Context) error {
	if err := migo.Plan(c); err != nil {
		return errors.Wrap(err, "PLAN")
	}
	return nil
}

func Wait(c *cli.Context) error {
	if err := migo.Wait(c); err != nil {
		return errors.Wrap(err, "WAIT")
	}
	return nil
}

func Init(c *cli.Context) error {
	if err := migo.Setup(c); err != nil {
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
