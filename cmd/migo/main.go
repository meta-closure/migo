package main

import (
	"io/ioutil"
	"log"
	"migo"
	"os"

	"github.com/ghodss/yaml"
	"github.com/lestrrat/go-jshschema"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
)

func SetupCmd() *cli.App {
	app := cli.NewApp()

	app.Name = "mig"
	app.Usage = "database migrater from a JSON Schema file"

	app.Flags = []cli.Flag{
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
		},
	}

	return app
}

func Parse(c *cli.Context, mode string) error {

	h := hschema.New()
	if j := c.GlobalString("json"); j != "" {
		err := ParseSchemaJSON(h, j)
		if err != nil {
			return errors.Wrap(err, "Parse Schema error")
		}
	} else if y := c.GlobalString("yaml"); y != "" {
		err := ParseSchemaYAML(h, y)
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

	o, err := mig.ParseState(s)
	if err != nil {
		return errors.Wrap(err, "State YAML file parse error")
	}

	n, err := mig.ParseSchema2State(h)
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

	err = sql.Migrate()
	if err != nil {
		return errors.Wrap(err, "Database migration error")
	}

	err = n.Update(s)
	if err != nil {
		return errors.Wrap(err, "Failed to save State file")
	}

	return nil
}

func Run(c *cli.Context) error {
	err := Parse(c, "run")
	if err != nil {
		return errors.Wrap(err, "RUN")
	}
	return nil
}

func Plan(c *cli.Context) error {
	err := Parse(c, "plan")
	if err != nil {
		return errors.Wrap(err, "PLAN")
	}
	return nil
}

func StateInit(c *cli.Context) error {
	n := mig.StateNew()
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

func ParseSchemaYAML(h *hschema.HyperSchema, s string) error {
	b, err := ioutil.ReadFile(s)
	if err != nil {
		return errors.Wrap(err, "YAML file open error")
	}
	y := &map[string]interface{}{}
	err = yaml.Unmarshal(b, y)
	if err != nil {
		return errors.Wrap(err, "YAML file parse error")
	}
	h.Extract(*y)

	return nil
}

func ParseSchemaJSON(h *hschema.HyperSchema, s string) error {
	hs, err := hschema.ReadFile(s)
	if err != nil {
		return errors.Wrap(err, "JSON file parse error")
	}
	h = hs
	return nil
}

func main() {
	os.Exit(_cmd())
}
