package migo

import "github.com/urfave/cli"

type WaitOption struct {
	ConfigFile  string
	Environment string
}

func (op *WaitOption) setConfigFile(config string) error {
	if config == "" {
		return NewOptionEmptyError("database")
	}
	op.ConfigFile = config
	return nil
}

func (op *WaitOption) setEnvironment(env string) error {
	if env == "" {
		return NewOptionEmptyError("environment")
	}
	op.Environment = env
	return nil
}

func NewWaitOption(c *cli.Context) (WaitOption, error) {
	op := WaitOption{}
	db, env := c.GlobalString("database"), c.GlobalString("environment")
	if err := op.setConfigFile(db); err != nil {
		return op, err
	}
	if err := op.setEnvironment(env); err != nil {
		return op, err
	}
	return op, nil
}

type SeedOption struct {
	RecordFile  string
	ConfigFile  string
	Environment string
}

func (op *SeedOption) setRecordFile(record string) error {
	if record == "" {
		return NewOptionEmptyError("seed")
	}
	op.RecordFile = record
	return nil
}

func (op *SeedOption) setConfigFile(config string) error {
	if config == "" {
		return NewOptionEmptyError("database")
	}
	op.ConfigFile = config
	return nil
}

func (op *SeedOption) setEnvironment(env string) error {
	if env == "" {
		return NewOptionEmptyError("environment")
	}
	op.Environment = env
	return nil
}

func NewSeedOption(c *cli.Context) (SeedOption, error) {
	op := SeedOption{}
	db, env, seed := c.GlobalString("database"), c.GlobalString("environment"), c.GlobalString("seed")
	if err := op.setConfigFile(db); err != nil {
		return op, err
	}
	if err := op.setEnvironment(env); err != nil {
		return op, err
	}
	if err := op.setRecordFile(seed); err != nil {
		return op, err
	}
	return op, nil
}

type InitOption struct {
	ConfigFile  string
	Environment string
}

func (op *InitOption) setConfigFile(config string) error {
	if config == "" {
		return NewOptionEmptyError("database")
	}
	op.ConfigFile = config
	return nil
}

func (op *InitOption) setEnvironment(env string) error {
	if env == "" {
		return NewOptionEmptyError("environment")
	}
	op.Environment = env
	return nil
}

func NewInitOption(c *cli.Context) (InitOption, error) {
	op := InitOption{}
	db, env := c.GlobalString("database"), c.GlobalString("environment")
	if err := op.setConfigFile(db); err != nil {
		return op, err
	}
	if err := op.setEnvironment(env); err != nil {
		return op, err
	}
	return op, nil
}

type MigrateOption struct {
	FormatType  string
	ConfigFile  string
	StateFile   string
	SchemaFile  string
	Environment string
}

func (op *MigrateOption) SetJSONFormatSchema(schema string) {
	op.SchemaFile = schema
	op.FormatType = "json"
}

func (op *MigrateOption) SetYAMLFormatSchema(schema string) {
	op.SchemaFile = schema
	op.FormatType = "yaml"
}

func (op *MigrateOption) SetStateFile(state string) error {
	if state == "" {
		return NewOptionEmptyError("state")
	}
	op.StateFile = state
	return nil
}

func (op *MigrateOption) SetConfigFile(config string) error {
	if config == "" {
		return NewOptionEmptyError("database")
	}
	op.ConfigFile = config
	return nil
}

func (op *MigrateOption) SetEnvironment(env string) error {
	if env == "" {
		return NewOptionEmptyError("environment")
	}
	op.Environment = env
	return nil
}

func NewMigrateOption(c *cli.Context) (MigrateOption, error) {
	op := MigrateOption{}
	j, y := c.GlobalString("json"), c.GlobalString("yaml")
	if j == "" && y == "" {
		return op, NewOptionEmptyError("schema")
	}

	if j != "" && y != "" {
		return op, NewMigrateOptionInvalidError()
	}

	if j != "" {
		op.SetJSONFormatSchema(j)
	}
	if y != "" {
		op.SetYAMLFormatSchema(y)
	}

	state, db, env := c.GlobalString("state"), c.GlobalString("database"), c.GlobalString("environment")
	if err := op.SetStateFile(state); err != nil {
		return op, err
	}
	if err := op.SetConfigFile(db); err != nil {
		return op, err
	}
	if err := op.SetEnvironment(env); err != nil {
		return op, err
	}

	return op, nil
}
