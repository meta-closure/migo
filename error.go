package migo

import "fmt"

type ConnectionError struct {
	err error
}

func NewConnectionError(err error) ConnectionError {
	return ConnectionError{err: err}
}

func (err ConnectionError) Error() string {
	return fmt.Sprintf("connection error: %s", err)
}

type OptionEmptyError struct {
	Option string
}

func NewOptionEmptyError(op string) error {
	return OptionEmptyError{Option: op}
}

func (err OptionEmptyError) Error() string {
	return fmt.Sprintf("Option %s is empty", err.Option)
}

type MigrateOptionInvalidError struct {
}

func NewMigrateOptionInvalidError() error {
	return MigrateOptionInvalidError{}
}

func (err MigrateOptionInvalidError) Error() string {
	return fmt.Sprintf("Option is invalid")
}
