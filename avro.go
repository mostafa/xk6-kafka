package main

import (
	"log"

	"github.com/linkedin/goavro/v2"
)

func ToAvro(value string, schema string) []byte {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		log.Fatal(err)
	}

	native, _, err := codec.NativeFromTextual([]byte(value))
	if err != nil {
		log.Fatal(err)
	}

	binary, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		log.Fatal(err)
	}

	return binary
}

func FromAvro(message []byte, schema string) interface{} {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		log.Fatal(err)
	}

	native, _, err := codec.NativeFromBinary(message)
	if err != nil {
		log.Fatal(err)
	}

	return native
}
