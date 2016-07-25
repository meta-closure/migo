package main

import (
	"io/ioutil"
	"os"

	"encoding/json"

	log "github.com/Sirupsen/logrus"
	"github.com/ghodss/yaml"
	"github.com/jessevdk/go-flags"
	"github.com/lestrrat/go-jshschema"
	"github.com/pkg/errors"
)

type Option struct {
	JSON         string `short:"j" long:"json" description:"Source Schema JSON File"`
	YANL         string `short:"y" long:"yaml" description:"Source Schema YANL File"`
	InternalFile string `short:"i" long:"internal" description:"Internal generated data JSON file" required:"true"`
}

func cmd() (*Option, error) {
	opts := &Option{}
	_, err := flags.Parse(opts)
	if err != nil {
		return nil, errors.Wrap(err, "Internal JSON file not specified:")
	}
	return opts, nil
}

func SchemaParse(h *hschema.HyperSchema, opt *Option) error {
	if opt.JSON != "" {
		hs, err := hschema.ReadFile(opt.JSON)
		if err != nil {
			return errors.Wrap(err, "JSON file parse error:")
		}
		h = hs
		return nil
	} else if opt.YANL != "" {
		b, err := ioutil.ReadFile(opt.YANL)
		if err != nil {
			return errors.Wrap(err, "YAML file open error:")
		}
		var y map[string]interface{}
		err = yaml.Unmarshal(b, y)
		if err != nil {
			return errors.Wrap(err, "YAML file parse error")
		}
		h.Extract(y)
		return nil
	} else {
		return errors.New("Source Schema file is empty")
	}
}

func InternalParse(s string) (map[string]interface{}, error) {
	var i map[string]interface{}
	b, err := ioutil.ReadFile(s)
	if err != nil {
		return i, errors.Wrap(err, "Internal JSON file read error:")
	}

	err = json.Unmarshal(b, i)
	if err != nil {
		return i, errors.Wrap(err, "Internal JSON file parse error:")
	}
	return i, nil
}

func _main() int {
	opt, err := cmd()
	if err != nil {
		log.Printf("Command parse error: %v", err)
		return -1
	}

	h := hschema.New()
	err = SchemaParse(h, opt)
	if err != nil {
		log.Printf("HyperSchema parse error: %v", err)
		return -1
	}

	old, err := InternalParse(opt.InternalFile)
	if err != nil {
		log.Printf("Internal JSON file parse error: %v", err)
		return -1
	}

	new, err := mig.InternalBuilder(h)
	if err != nil {
		log.Printf("HyperSchema convert Internal representation error: %v", err)
		return -1
	}

	sql, err := mig.SQLBuilder(new, old)
	if err != nil {
		log.Printf("SQL Build error: %v", err)
		return -1
	}

	mig.Check(sql)
	err := mig.Migrate(sql)
	if err != nil {
		log.Printf("Database migration error: %v", err)
		return -1
	}

	return 1
}

func main() {
	os.Exit(_main())
}
