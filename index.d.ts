/**
 * @packageDocumentation
 * xk6-kafka is a k6 extension to load test Apache Kafka
 */

/**
 * @module k6/x/kafka
 * @description
 * The xk6-kafka project is a k6 extension that enables k6 users to load test Apache Kafka using a producer and possibly a consumer for debugging.
 * This documentation refers to the development version of the xk6-kafka project, which means the latest changes on `main` branch and might not be released yet, as explained in [the release process](https://github.com/mostafa/xk6-kafka#the-release-process).
 * @see {@link https://github.com/mostafa/xk6-kafka}
 */

/** Compression codecs for compressing messages when producing to a topic or reading from it. */
export enum COMPRESSION_CODECS {
    CODEC_GZIP = "gzip",
    CODEC_SNAPPY = "snappy",
    CODEC_LZ4 = "lz4",
    CODEC_ZSTD = "zstd",
}

/* SASL mechanisms for authenticating to Kafka. */
export enum SASL_MECHANISMS {
    NONE = "none",
    SASL_PLAIN = "sasl_plain",
    SASL_SCRAM_SHA256 = "sasl_scram_sha256",
    SASL_SCRAM_SHA512 = "sasl_scram_sha512",
    SASL_SSL = "sasl_ssl",
}

/* TLS versions for creating a secure communication channel with Kafka. */
export enum TLS_VERSIONS {
    TLS_1_0 = "tlsv1.0",
    TLS_1_1 = "tlsv1.1",
    TLS_1_2 = "tlsv1.2",
    TLS_1_3 = "tlsv1.3",
}

/* Message serializers for producing messages to a topic. */
export enum SERIALIZERS {
    STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer",
    BYTE_ARRAY_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer",
    JSON_SCHEMA_SERIALIZER = "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer",
    AVRO_SERIALIZER = "io.confluent.kafka.serializers.KafkaAvroSerializer",
    PROTOBUF_SERIALIZER = "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer",
}

/* Message deserializers for consuming messages from a topic. */
export enum DESERIALIZERS {
    STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer",
    BYTE_ARRAY_DESERIALIZER = "org.apache.kafka.common.serialization.ByteArrayDeserializer",
    JSON_SCHEMA_DESERIALIZER = "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer",
    AVRO_DESERIALIZER = "io.confluent.kafka.serializers.KafkaAvroDeserializer",
    PROTOBUF_DESERIALIZER = "io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer",
}

/* Isolation levels controls the visibility of transactional records. */
export enum ISOLATION_LEVEL {
    ISOLATION_LEVEL_READ_UNCOMMITTED = "isolation_level_read_uncommitted",
    ISOLATION_LEVEL_READ_COMMITTED = "isolation_level_read_committed",
}

/* Subject name strategy for storing a schema in Schema Registry. */
export enum SUBJECT_NAME_STRATEGY {
    TOPIC_NAME_STRATEGY = "TopicNameStrategy",
    RECORD_NAME_STRATEGY = "RecordNameStrategy",
    TOPIC_RECORD_NAME_STRATEGY = "TopicRecordNameStrategy",
}

/* Balancers for distributing messages to partitions. */
export enum BALANCERS {
    BALANCER_ROUND_ROBIN = "balancer_roundrobin",
    BALANCER_LEAST_BYTES = "balancer_leastbytes",
    BALANCER_HASH = "balancer_hash",
    BALANCER_CRC32 = "balancer_crc32",
    BALANCER_MURMUR2 = "balancer_murmur2",
}

/* Consumer group balancing strategies for consuming messages. */
export enum GROUP_BALANCERS {
    GROUP_BALANCER_RANGE = "group_balancer_range",
    GROUP_BALANCER_ROUND_ROBIN = "group_balancer_round_robin",
    GROUP_BALANCER_RACK_AFFINITY = "group_balancer_rack_affinity",
}

/* SASL configurations for authenticating to Kafka. */
export interface SASLConfig {
    username: string;
    password: string;
    algorithm: SASL_MECHANISMS;
}

/* TLS configurations for creating a secure communication channel with Kafka. */
export interface TLSConfig {
    enableTLS: boolean;
    insecureSkipVerify: boolean;
    minVersion: TLS_VERSIONS;
    clientCertPem: string;
    clientKeyPem: string;
    serverCertPem: string;
}

/* Writer configurations for producing messages to a topic. */
export interface WriterConfig {
    brokers: string[];
    topic: string;
    autoCreateTopic: boolean;
    balancer: BALANCERS;
    maxAttempts: number;
    batchSize: number;
    batchBytes: number;
    batchTimeout: number;
    readTimeout: number;
    writeTimeout: number;
    compression: COMPRESSION_CODECS;
    sasl: SASLConfig;
    tls: TLSConfig;
    connectLogger: boolean;
}

/*
 * Message format for producing messages to a topic.
 * @note: The message format will be adopted by the reader at some point.
 */
export interface Message {
    topic: string;
    partition: number;
    offset: number;
    highwaterMark: number;
    key: string;
    value: any;
    headers: Map<string, any>;
    time: Date;
}

/* Consumer configurations for consuming deserialized messages from a topic. */
export interface ConsumerConfiguration {
    keyDeserializer: DESERIALIZERS;
    valueDeserializer: DESERIALIZERS;
    SubjectNameStrategy: SUBJECT_NAME_STRATEGY;
    useMagicPrefix: boolean;
}

/* Producer configurations for producing serialized messages to a topic. */
export interface ProducerConfiguration {
    keyDeserializer: SERIALIZERS;
    valueDeserializer: SERIALIZERS;
    SubjectNameStrategy: SUBJECT_NAME_STRATEGY;
}

/* Basic authentication for connecting to Schema Registry. */
export interface BasicAuth {
    username: string;
    password: string;
}

/* Schema Registry configurations for creating a possible secure communication channel with Schema Registry for storing and retrieving schemas. */
export interface SchemaRegistryConfiguration {
    url: string;
    basicAuth: BasicAuth;
    useLatest: boolean;
    tlsConfig: TLSConfig;
}

/* Configuration for Produce and Consume methods. */
export interface Configuration {
    consumer: ConsumerConfiguration;
    producer: ProducerConfiguration;
    schemaRegistry: SchemaRegistryConfiguration;
}

/* Configuration for producing messages to a topic. */
export interface ProduceConfig {
    messages: Message[];
    config: Configuration;
    keySchema: string;
    valueSchema: string;
}

/* Configuration for creating a Reader instance. */
export interface ReaderConfig {
    brokers: string[];
    groupID: string;
    groupTopics: string[];
    topic: string;
    partition: number;
    queueCapacity: number;
    minBytes: number;
    maxBytes: number;
    maxWait: number;
    readLagInterval: number;
    groupBalancers: GROUP_BALANCERS[];
    heartbeatInterval: number;
    commitInterval: number;
    partitionWatchInterval: number;
    watchPartitionChanges: boolean;
    sessionTimeout: number;
    rebalanceTimeout: number;
    joinGroupBackoff: number;
    retentionTime: number;
    startOffset: number;
    readBackoffMin: number;
    readBackoffMax: number;
    connectLogger: boolean;
    maxAttempts: number;
    isolationLevel: ISOLATION_LEVEL;
    offset: number;
    sasl: SASLConfig;
    tls: TLSConfig;
}

/* Configuration for Consume method. */
export interface ConsumeConfig {
    limit: number;
    config: Configuration;
    keySchema: string;
    valueSchema: string;
}

/* Configuration for creating a Connector instance for working with topics. */
export interface ConnectionConfig {
    address: string;
    sasl: SASLConfig;
    tls: TLSConfig;
}

/* ReplicaAssignment among kafka brokers for this topic partitions. */
export interface ReplicaAssignment {
    partition: number;
    replicas: number[];
}

/* ConfigEntry holds topic level configuration for topic to be set. */
export interface ConfigEntry {
    configName: string;
    configValue: string;
}

/* TopicConfig for creating a new topic. */
export interface TopicConfig {
    topic: string;
    numPartitions: number;
    replicationFactor: number;
    replicaAssignments: ReplicaAssignment[];
    configEntries: ConfigEntry[];
}

/**
 * @class
 * @classdesc Writer can write messages to Kafka.
 * @example
 *
 * ```javascript
 * // In init context
 * const writer = new Writer({
 *   brokers: ["localhost:9092"],
 *   topic: "my-topic",
 *   autoCreateTopic: true,
 * });
 *
 * // In VU code (default function)
 * writer.produce({
 *   messages: [
 *     {
 *       key: "key",
 *       value: "value",
 *     }
 *   ]
 * });
 *
 * // In teardown function
 * writer.close();
 * ```
 */
export class Writer {
    /**
     * @constructor
     * Create a new Writer.
     * @param {WriterConfig} writerConfig - Writer configuration.
     * @returns {Writer} - Writer instance.
     */
    constructor(writerConfig: WriterConfig);
    /**
     * @method
     * Write messages to Kafka.
     * @param {ProduceConfig} produceConfig - Produce configuration.
     * @returns {void} - Nothing.
     */
    produce(produceConfig: ProduceConfig): void;
    /**
     * @destructor
     * @description Close the writer.
     * @returns {void} - Nothing.
     */
    close(): void;
}

/**
 * @class
 * @classdesc Reader can read messages from Kafka.
 * @example
 *
 * ```javascript
 * // In init context
 * const reader = new Reader({
 *   brokers: ["localhost:9092"],
 *   topic: "my-topic",
 * });
 *
 * // In VU code (default function)
 * const messages = reader.consume({limit: 10});
 *
 * // In teardown function
 * reader.close();
 * ```
 */
export class Reader {
    /**
     * @constructor
     * Create a new Reader.
     * @param {ReaderConfig} readerConfig - Reader configuration.
     * @returns {Reader} - Reader instance.
     */
    constructor(readerConfig: ReaderConfig);
    /**
     * @method
     * Read messages from Kafka.
     * @param {ConsumeConfig} consumeConfig - Consume configuration.
     * @returns {Message[]} - Messages.
     */
    consume(consumeConfig: ConsumeConfig): Message[];
    /**
     * @destructor
     * @description Close the reader.
     * @returns {void} - Nothing.
     */
    close(): void;
}

/**
 * @class
 * @classdesc Connection can connect to Kafka for working with topics.
 * @example
 *
 * ```javascript
 * // In init context
 * const connection = new Connection({
 *   address: "localhost:9092",
 * });
 *
 * // In VU code (default function)
 * const topics = connection.listTopics();
 *
 * // In teardown function
 * connection.close();
 * ```
 */
export class Connection {
    /**
     * @constructor
     * Create a new Connection.
     * @param {ConnectionConfig} connectionConfig - Connection configuration.
     * @returns {Connection} - Connection instance.
     */
    constructor(connectionConfig: ConnectionConfig);
    /**
     * @method
     * Create a new topic.
     * @param {TopicConfig} topicConfig - Topic configuration.
     * @returns {void} - Nothing.
     */
    createTopic(topicConfig: TopicConfig): void;
    /**
     * @method
     * Delete a topic.
     * @param {string} topic - Topic name.
     * @returns {void} - Nothing.
     */
    deleteTopic(topic: string): void;
    /**
     * @method
     * List topics.
     * @returns {string[]} - Topics.
     */
    listTopics(): string[];
    /**
     * @destructor
     * @description Close the connection.
     * @returns {void} - Nothing.
     */
    close(): void;
}
