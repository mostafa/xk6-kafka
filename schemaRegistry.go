package kafka

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"fmt"
	"strings"
	"github.com/linkedin/goavro/v2"
	"errors"
)

func VerifyProperties(properties map[string]string) error {
	for _, prefix := range []string{"key", "value"} {
		if (properties[prefix + ".serializer"] == "io.confluent.kafka.serializers.KafkaAvroSerializer") {
			if (properties["schema.registry.url"] == "") {
				return errors.New("You have to provide a value for \"schema.registry.url\" to use a serializer of type \"io.confluent.kafka.serializers.KafkaAvroSerializer\"")
			}
		}
	}
	return nil;
}

func addMagicByteAdnSchemaIdPrefix(properties map[string]string, avroData []byte, topic string, keyOrValue string, schema string) ([]byte, error) {
	var schemaId, err = getSchemaId(properties, topic, keyOrValue, schema);
	if err != nil {
		ReportError(err, "Retrieval of schema id failed.")
		return nil, err;
	}
	if (schemaId != -1) {
		return append([]byte{ 0, 0, 0, 0, byte(schemaId) }, avroData...), nil;
	}
	return avroData, nil;
}

func getSchemaId(properties map[string]string, subject string, identifier string, schema string) (int64, error) {
	if (properties[identifier + ".serializer"] == "io.confluent.kafka.serializers.KafkaAvroSerializer") {
		url := properties["schema.registry.url"] + "/subjects/" + subject + "-" + identifier + "/versions";
		codec, _ := goavro.NewCodec(schema);
		body := "{\"schema\":\"" + strings.Replace(codec.CanonicalSchema(), "\"", "\\\"", -1) + "\"}";
		resp, err := http.Post(url, "application/vnd.schemaregistry.v1+json", bytes.NewReader([]byte(body)))
		if err != nil {
			return -1, err;
		}
		if resp.StatusCode >= 400 {
			return -1, errors.New(fmt.Sprintf("Retrieval of schema ids failed. Details: url= %v, body=%v, response=%v", url, body, resp));
		}
		defer resp.Body.Close()
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return -1, err;
		}

		var result map[string]int64;
		err = json.Unmarshal(bodyBytes, &result);
		if err != nil {
			return -1, err;
		}
		return int64(result["id"]), nil;
	}
	return -1, nil;
}
