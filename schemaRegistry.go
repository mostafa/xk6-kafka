package kafka

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/linkedin/goavro/v2"
	"io/ioutil"
	"net/http"
	"strings"
)

func verifyProperties(properties map[string]string) error {
	for _, prefix := range []string{"key", "value"} {
		if properties[prefix + ".serializer"] == "io.confluent.kafka.serializers.KafkaAvroSerializer" {
			if properties["schema.registry.url"] == "" {
				return errors.New("you have to provide a value for \"schema.registry.url\" to use a serializer " +
					"of type \"io.confluent.kafka.serializers.KafkaAvroSerializer\"")
			}
		}
	}
	return nil;
}

func i32tob(val uint32) []byte {
	r := make([]byte, 4)
	for i := uint32(0); i < 4; i++ {
		r[3 - i] = byte((val >> (8 * i)) & 0xff)
	}
	return r
}

// Account for proprietary 5-byte prefix before the Avro payload:
// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
func removeMagicByteAdnSchemaIdPrefix(properties map[string]string, messageData []byte, keyOrValue string) []byte {
	if properties[keyOrValue + ".deserializer"] == "io.confluent.kafka.serializers.KafkaAvroDeserializer" {
		return messageData[5:]
	}
	return messageData
}

// Add proprietary 5-byte prefix before the Avro payload:
// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
func addMagicByteAdnSchemaIdPrefix(properties map[string]string, avroData []byte, topic string, keyOrValue string, schema string) ([]byte, error) {
	var schemaId, err = getSchemaId(properties, topic, keyOrValue, schema)
	if err != nil {
		ReportError(err, "Retrieval of schema id failed.")
		return nil, err
	}
	if schemaId != 0 {
		return append(append([]byte{ 0 }, i32tob(schemaId)...), avroData...), nil
	}
	return avroData, nil
}

var schemaIdCache = make(map[string]uint32)

func getSchemaId(properties map[string]string, subject string, identifier string, schema string) (uint32, error) {
	if schemaIdCache[schema] > 0 {
		return schemaIdCache[schema], nil
	}
	if properties[identifier + ".serializer"] == "io.confluent.kafka.serializers.KafkaAvroSerializer" {
		url := properties["schema.registry.url"] + "/subjects/" + subject + "-" + identifier + "/versions"
		codec, _ := goavro.NewCodec(schema);

		body := "{\"schema\":\"" + strings.Replace(codec.CanonicalSchema(), "\"", "\\\"", -1) + "\"}"
		resp, err := http.Post(url, "application/vnd.schemaregistry.v1+json", bytes.NewReader([]byte(body)))
		if err != nil {
			return 0, err
		}
		if resp.StatusCode >= 400 {
			return 0, errors.New(fmt.Sprintf("Retrieval of schema ids failed. Details: url= %v, body=%v, response=%v", url, body, resp))
		}
		defer resp.Body.Close()
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return 0, err
		}

		var result map[string]int32
		err = json.Unmarshal(bodyBytes, &result)
		if err != nil {
			return 0, err;
		}
		schemaId := uint32(result["id"])
		schemaIdCache[schema] = schemaId
		return schemaId, nil
	}
	return 0, nil
}
