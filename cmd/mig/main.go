package main

import (
	"io/ioutil"
	"mig"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/ghodss/yaml"
	"github.com/jessevdk/go-flags"
	"github.com/lestrrat/go-jshschema"
	"github.com/pkg/errors"
)

type Option struct {
	JSON      string `short:"j" long:"json" description:"Source Schema JSON File"`
	YANL      string `short:"y" long:"yaml" description:"Source Schema YANL File"`
	StateFile string `short:"i" long:"state file" description:"Internal generated data JSON file"`
}

func cmd() (*Option, error) {
	opts := &Option{}
	_, err := flags.Parse(opts)
	if err != nil {
		return nil, err
	}
	return opts, nil
}

func ParseSchema(h *hschema.HyperSchema, opt *Option) error {
	if opt.JSON != "" {
		hs, err := hschema.ReadFile(opt.JSON)
		if err != nil {
			return errors.Wrap(err, "JSON file parse error")
		}
		h = hs
		return nil
	} else if opt.YANL != "" {
		b, err := ioutil.ReadFile(opt.YANL)
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
	} else {
		return errors.New("Source Schema file is empty")
	}
}

func _main() int {
	opt, err := cmd()
	if err != nil {
		log.Printf("Command parse error: %v", err)
		return 1
	}

	h := hschema.New()
	err = ParseSchema(h, opt)
	if err != nil {
		log.Printf("HyperSchema parse error: %v", err)
		return 1
	}

	n, err := mig.ParseSchema2State(h)
	if err != nil {
		log.Printf("Parse HyperSchema to State failed: %v", err)
		return 1
	}

	o, err := mig.ParseState(opt.StateFile)
	if err != nil {
		log.Printf("State YAML file parse error: %v", err)
		return 1
	}

	sql, err := o.SQLBuilder(n)
	if err != nil {
		log.Printf("SQL Build error: %v", err)
		return 1
	}

	sql.Check()

	err = sql.Migrate()
	if err != nil {
		log.Printf("Database migration error: %v", err)
		return 1
	}

	err = n.Update(opt.StateFile)
	if err != nil {
		log.Printf("Failed to save State file: %v", err)
		return 1
	}

	return 0
}

func main() {
	os.Exit(_main())
}
