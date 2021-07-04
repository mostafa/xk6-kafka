package kafka

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/linkedin/goavro/v2"
)

func i32tob(val uint32) []byte {
	r := make([]byte, 4)
	for i := uint32(0); i < 4; i++ {
		r[3-i] = byte((val >> (8 * i)) & 0xff)
	}
	return r
}

// Account for proprietary 5-byte prefix before the Avro payload:
// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
func removeMagicByteAndSchemaIdPrefix(configuration Configuration, messageData []byte, keyOrValue string) []byte {
	if useKafkaAvroDeserializer(configuration, keyOrValue) {
		return messageData[5:]
	}
	return messageData
}

// Add proprietary 5-byte prefix before the Avro payload:
// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
func addMagicByteAndSchemaIdPrefix(configuration Configuration, avroData []byte, topic string, keyOrValue string, schema string) ([]byte, error) {
	var schemaId, err = getSchemaId(configuration, topic, keyOrValue, schema)
	if err != nil {
		ReportError(err, "Retrieval of schema id failed.")
		return nil, err
	}
	if schemaId != 0 {
		return append(append([]byte{0}, i32tob(schemaId)...), avroData...), nil
	}
	return avroData, nil
}

var schemaIdCache = make(map[string]uint32)

func getSchemaId(configuration Configuration, topic string, keyOrValue string, schema string) (uint32, error) {
	if schemaIdCache[schema] > 0 {
		return schemaIdCache[schema], nil
	}
	if useKafkaAvroSerializer(configuration, keyOrValue) {
		url := configuration.SchemaRegistry.Url + "/subjects/" + topic + "-" + keyOrValue + "/versions"
		codec, _ := goavro.NewCodec(schema)

		body := "{\"schema\":\"" + strings.Replace(codec.CanonicalSchema(), "\"", "\\\"", -1) + "\"}"

		client := &http.Client{}
		req, err := http.NewRequest("POST", url, bytes.NewReader([]byte(body)))
		if err != nil {
			return 0, err
		}
		req.Header.Add("Content-Type", "application/vnd.schemaregistry.v1+json")
		if useBasicAuthWithCredentialSourceUserInfo(configuration) {
			username := strings.Split(configuration.SchemaRegistry.BasicAuth.UserInfo, ":")[0]
			password := strings.Split(configuration.SchemaRegistry.BasicAuth.UserInfo, ":")[1]
			req.SetBasicAuth(username, password)
		}
		resp, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		if resp.StatusCode >= 400 {
			return 0, errors.New(fmt.Sprintf("Retrieval of schema ids failed. Details: Url= %v, body=%v, response=%v", url, body, resp))
		}
		defer resp.Body.Close()
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return 0, err
		}

		var result map[string]int32
		err = json.Unmarshal(bodyBytes, &result)
		if err != nil {
			return 0, err
		}
		schemaId := uint32(result["id"])
		schemaIdCache[schema] = schemaId
		return schemaId, nil
	}
	return 0, nil
}
