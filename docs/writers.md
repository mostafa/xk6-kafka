# Manage writers (producers) with xk6-kafka

## What is the Writer?

The writer is a Kafka producer that allows you to send messages to Kafka topics in a load test using k6. It is configured based on the environment and application settings.
Once you've created the writer, you can use it to produce messages to Kafka topics.
Writers are unique for each topic, if you want to produce messages to multiple topics, you need to create a writer for each topic.

## Writers Configuration
### Create single writer
```javascript
const producer = new Writer({
    brokers, // The broker addresses as array, e.g., ["localhost:9092"]
    topic: topicName, // The topic name to produce messages to
    autoCreateTopic: false, // Should the topic be created automatically if it doesn't exist?
    compression: CODEC_SNAPPY, // Optional: Compression codec to use for messages
    tls: tlsConfig // Config object for TLS settings, if needed
});


producer.produce({
    messages: messagesToProduce
});
```

Tip: here you can find more options for your writer:

```golang
type WriterConfig struct {
	AutoCreateTopic bool          `json:"autoCreateTopic"`
	ConnectLogger   bool          `json:"connectLogger"`
	MaxAttempts     int           `json:"maxAttempts"`
	BatchSize       int           `json:"batchSize"`
	BatchBytes      int           `json:"batchBytes"`
	RequiredAcks    int           `json:"requiredAcks"`
	Topic           string        `json:"topic"`
	Balancer        string        `json:"balancer"`
	Compression     string        `json:"compression"`
	Brokers         []string      `json:"brokers"`
	BatchTimeout    time.Duration `json:"batchTimeout"`
	ReadTimeout     time.Duration `json:"readTimeout"`
	WriteTimeout    time.Duration `json:"writeTimeout"`
	SASL            SASLConfig    `json:"sasl"`
	TLS             TLSConfig     `json:"tls"`
}
```

All the parameters are named following the camelCase notation style.


### Manage multiple writers for various topics
A lot of times, you will need to produce messages to multiple topics. In this case, you can create a writer for each topic and manage them in an object or a map.
```javascript
function createKafkaWriters(brokers, topicNamesList, tlsConfig)  {
    const writers = new Map();
    Object.values(topicNamesList).forEach(topicName => {
        writers.set(topicName, new Writer({
            brokers,
            topic: topicName,
            autoCreateTopic: false,
            compression: CODEC_SNAPPY,
            tls: tlsConfig,
        }));
    });
    return writers;
}
```
You can then use the `writers` map to produce messages to the topics:
```javascript
const producers = createKafkaWriters(brokers, topicNamesList, tlsConfig);
producers.get(topicName).produce({
    messages: messagesToProduce
});
```

## Message Production

You can see above that the `produce` method is used to send messages to the Kafka topic. 
The `messages` parameter is an array of messages you want to send. 
Each message can be a simple string or an object, depending on your Kafka topic configuration.

You can here find an example of how to create a messages:

### Create a JSON message

```javascript
let messages = [
    {
        // The data type of the key is JSON
        key: schemaRegistry.serialize({
            data: {
                correlationId: "test-id-abc-" + index,
            },
            schemaType: SCHEMA_TYPE_JSON,
        }),
        // The data type of the value is JSON
        value: schemaRegistry.serialize({
            data: {
                name: "xk6-kafka",
                version: "0.9.0",
                author: "Mostafa Moradian",
                description:
                    "k6 extension to load test Apache Kafka with support for Avro messages",
                index: index,
            },
            schemaType: SCHEMA_TYPE_JSON,
        }),
        headers: {
            mykey: "myvalue",
        },
        offset: index,
        partition: 0,
        time: new Date(), // Will be converted to timestamp automatically
    }
]
```

### Create an AVRO message
For this usage, you'll need to have the Avro schema registered in your Schema Registry.
Please refer to the [Schema Registry documentation](./schema-registry.md) for more details on how to register schemas.
```javascript
let messages = [
    {
        key: schemaRegistry.serialize({
            data: {
                correlationId: "test-id-abc-" + index,
            },
            schema: myAvroKeySchema, // The Javascript object containing Avro schema for the key
            schemaType: SCHEMA_TYPE_AVRO,
        }),
        value: schemaRegistry.serialize({
            data: {
                name: "xk6-kafka",
                version: "0.9.0",
                author: "Mostafa Moradian",
                description:
                    "k6 extension to load test Apache Kafka with support for Avro messages",
                index: index,
            },
            schema: myAvroValueSchema, // The Javascript object containing Avro schema for the key
            schemaType: SCHEMA_TYPE_AVRO,
        })// Will be converted to timestamp automatically
    }
]
```
