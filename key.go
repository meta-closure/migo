package migo

import (
	"fmt"
	"reflect"

	"github.com/pkg/errors"
)

type Key struct {
	Target Columns `json:"target"`
	Name   string  `json:"name"`
}

type Keys []Key

func (k Keys) Len() int {
	return len(k)
}

func (k Keys) Less(i, j int) bool {
	return k[i].Name < k[j].Name
}

func (k Keys) Swap(i, j int) {
	k[i], k[j] = k[j], k[i]
}

func NewKey(s string) Key {
	return Key{Name: s}
}

func keyList(t Table, i interface{}) (Columns, error) {
	m, ok := i.([]interface{})
	if !ok {
		return nil, errors.New("fail to convert []interface{} type")
	}
	targets := Columns{}
	for _, v := range m {
		s, ok := v.(string)
		if !ok {
			return nil, errors.New("fail to convert string type")
		}
		c, err := t.selectColumnWithID(s)
		if err != nil {
			return nil, fmt.Errorf("fail to searching column in table %s", t.Name)
		}
		targets = append(targets, c)
	}

	return targets, nil
}

func definitonsID(key string) string {
	return fmt.Sprintf("#/definitions/%s", key)
}
func propertiesID(key string) string {
	return fmt.Sprintf("#/properties/$s", key)
}

func (k Key) isUpdatedFrom(target Key) (bool, error) {
	if k.Name != target.Name {
		return false, errors.New("the target key name is wrong")
	}
	return !reflect.DeepEqual(k, target), nil
}
