package main

import (
	"errors"
	"reflect"
	"unsafe"

	"github.com/loadimpact/k6/lib"
)

func GetState(ctx interface{}) (*lib.State, error) {
	// FIXME: This function is a hack to get lib.State from the context

	contextValues := reflect.ValueOf(ctx).Elem()
	contextKeys := reflect.TypeOf(ctx).Elem()

	if contextKeys.Kind() == reflect.Struct {
		for i := 0; i < contextValues.NumField(); i++ {
			if i != 2 {
				// Hack to get past a value in context
				continue
			}
			reflectValue := contextValues.Field(i)
			reflectValue = reflect.NewAt(reflectValue.Type(), unsafe.Pointer(reflectValue.UnsafeAddr())).Elem()
			reflectField := contextKeys.Field(i)

			if reflectField.Name != "Context" {
				return reflectValue.Interface().(*lib.State), nil
			}
		}
	}

	return nil, errors.New("State is nil")
}
