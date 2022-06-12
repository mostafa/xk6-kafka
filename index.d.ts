/**
 * @packageDocumentation
 * xk6-kafka is a k6 extension to load test Apache Kafka
 */

/**
 * Create a new Writer object for writing messages to Kafka.
 *
 * @constructor
 * @param   {[String]}          brokers     An array of brokers, e.g. ["host:port", ...].
 * @param   {String}            topic       The topic to write to.
 * @param   {Object}            saslConfig  The SASL configuration.
 * @param   {Object}            tlsConfig   The TLS configuration.
 * @param   {String}            compression The Compression algorithm.
 * @returns {[Object, Object]}   An array of two objects: A Writer object and an error object.
 */
export declare function writer(brokers: [String], topic: String, saslConfig: object, tlsConfig: object, compression: String): [Object, Object];

/**
 * Write a sequence of messages to Kafka.
 *
 * @function
 * @param   {Object}    writer      The writer object created with the writer constructor.
 * @param   {[Object]}  messages    An array of message objects containing an optional key and a value. Topic, offset and time and headers are also available and optional. Headers are objects.
 * @param   {String}    keySchema   An optional Avro/JSONSchema schema for the key.
 * @param   {String}    valueSchema An optional Avro/JSONSchema schema for the value.
 * @param   {boolean}   autoCreateTopic     Automatically creates the topic on the first produced message. Defaults to false.
 * @returns {Object}    A error object.
 */
export declare function produce(writer: Object, messages: [Object], keySchema: String, valueSchema: String, autoCreateTopic: boolean): Object;

/**
 * Write a sequence of messages to Kafka with a specific serializer/deserializer.
 *
 * @function
 * @param   {Object}    writer              The writer object created with the writer constructor.
 * @param   {[Object]}  messages            An array of message objects containing an optional key and a value. Topic, offset and time and headers are also available and optional. Headers are objects.
 * @param   {String}    configurationJson   Serializer, deserializer and schemaRegistry configuration.
 * @param   {String}    keySchema           An optional Avro/JSONSchema schema for the key.
 * @param   {String}    valueSchema         An optional Avro/JSONSchema schema for the value.
 * @param   {boolean}   autoCreateTopic     Automatically creates the topic on the first produced message. Defaults to false.
 * @returns {Object}    A error object.
 */
export declare function produceWithConfiguration(writer: Object, messages: [Object], configurationJson: String, keySchema: String, valueSchema: String, autoCreateTopic: boolean): Object;

/**
 * Create a new Reader object for reading messages from Kafka.
 *
 * @constructor
 * @param   {[String]}          brokers     An array of brokers, e.g. ["host:port", ...].
 * @param   {String}            topic       The topic to read from.
 * @param   {Number}            partition   The partition.
 * @param   {Number}            groupID     The group ID.
 * @param   {Number}            offset      The offset to begin reading from.
 * @param   {Object}            saslConfig  The SASL configuration.
 * @param   {Object}            tlsConfig   The TLS configuration.
 * @returns {[Object, error]}   An array of two objects: A Reader object and an error object.
 */
export declare function reader(brokers: [String], topic: String, partition: Number, groupID: String, offset: Number, saslConfig: object, tlsConfig: object): [Object, Object];

/**
 * Read a sequence of messages from Kafka.
 *
 * @function
 * @param   {Object}            reader      The reader object created with the reader constructor.
 * @param   {Number}            limit       How many messages should be read in one go, which blocks. Defaults to 1.
 * @param   {String}            keySchema   An optional Avro/JSONSchema schema for the key.
 * @param   {String}            valueSchema An optional Avro/JSONSchema schema for the value.
 * @returns {[[Object], error]} An array of two objects: an array of objects and an error object. Each message object can contain a value and an optional set of key, topic, partition, offset, time, highWaterMark and headers. Headers are objects.
 */
export declare function consume(reader: Object, limit: Number, keySchema: String, valueSchema: String): [[Object], Object];

/**
 * Read a sequence of messages from Kafka.
 *
 * @function
 * @param   {Object}            reader              The reader object created with the reader constructor.
 * @param   {Number}            limit               How many messages should be read in one go, which blocks. Defaults to 1.
 * @param   {String}            configurationJson   Serializer, deserializer and schemaRegistry configuration.
 * @param   {String}            keySchema           An optional Avro/JSONSchema schema for the key.
 * @param   {String}            valueSchema         An optional Avro/JSONSchema schema for the value.
 * @returns {[[Object], Object]} An array of two objects: an array of objects and an error object. Each message object can contain a value and an optional set of key, topic, partition, offset, time, highWaterMark and headers. Headers are objects.
 */
export declare function consumeWithConfiguration(reader: object, limit: Number, configurationJson: String, keySchema: String, valueSchema: String): [[Object], Object];

/**
 * Create a topic in Kafka. It does nothing if the topic exists.
 *
 * @function
 * @param   {String}    address             The broker address.
 * @param   {String}    topic               The topic name.
 * @param   {Number}    partitions          The Number of partitions.
 * @param   {Number}    replicationFactor   The replication factor in a clustered setup.
 * @param   {String}    compression         The compression algorithm.
 * @param   {Object}    saslConfig          The SASL configuration.
 * @param   {Object}    tlsConfig           The TLS configuration.
 * @returns {Object}    A error object.
 */
export declare function createTopic(address: String, topic: String, partitions: Number, replicationFactor: Number, compression: String, saslConfig: object, tlsConfig: object): Object;

/**
 * Delete a topic from Kafka. It raises an error if the topic doesn't exist.
 *
 * @function
 * @param   {String}    address             The broker address.
 * @param   {String}    topic               The topic name.
 * @param   {Object}    saslConfig          The SASL configuration.
 * @param   {Object}    tlsConfig           The TLS configuration.
 * @returns {Object}    A error object.
 */
export declare function deleteTopic(address: String, topic: String, saslConfig: Object, tlsConfig: object): Object;

/**
 * List all topics in Kafka.
 *
 * @function
 * @param   {String}            address The broker address.
 * @param   {Object}            saslConfig          The SASL configuration.
 * @param   {Object}            tlsConfig           The TLS configuration.
 * @returns {[String], Object}  A nested list of strings containing a list of topics and the error object (if any).
 */
export declare function listTopics(address: String, saslConfig: Object, tlsConfig: Object): [[String], Object];