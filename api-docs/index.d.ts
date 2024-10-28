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
  SASL_AWS_IAM = "sasl_aws_iam",
}

/* TLS versions for creating a secure communication channel with Kafka. */
export enum TLS_VERSIONS {
  TLS_1_0 = "tlsv1.0",
  TLS_1_1 = "tlsv1.1",
  TLS_1_2 = "tlsv1.2",
  TLS_1_3 = "tlsv1.3",
}

/* Element types for publishing schemas to Schema Registry. */
export enum ELEMENT_TYPES {
  KEY = "KEY",
  VALUE = "VALUE",
}

/* Isolation levels controls the visibility of transactional records. */
export enum ISOLATION_LEVEL {
  ISOLATION_LEVEL_READ_UNCOMMITTED = "isolation_level_read_uncommitted",
  ISOLATION_LEVEL_READ_COMMITTED = "isolation_level_read_committed",
}

/* Start offsets for consuming messages from a consumer group. */
export enum START_OFFSETS {
  START_OFFSETS_LAST_OFFSET = "start_offset_last_offset",
  START_OFFSETS_FIRST_OFFSET = "start_offset_first_offset", // default
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

/* Schema types used in identifying schema and data type in serdes. */
export enum SCHEMA_TYPES {
  SCHEMA_TYPE_STRING = "STRING",
  SCHEMA_TYPE_BYTES = "BYTES",
  SCHEMA_TYPE_AVRO = "AVRO",
  SCHEMA_TYPE_JSON = "JSON",
  SCHEMA_TYPE_PROTOBUF = "PROTOBUF",
}

/* Time units for use in timeouts. */
export enum TIME {
  NANOSECOND = 1,
  MICROSECOND = 1000,
  MILLISECOND = 1000000,
  SECOND = 1000000000,
  MINUTE = 60000000000,
  HOUR = 3600000000000,
}

/* SASL configurations for authenticating to Kafka. */
export interface SASLConfig {
  username: string;
  password: string;
  algorithm: SASL_MECHANISMS;
}

/* TLS configurations for creating a secure communication channel with Kafka. */
export interface TLSConfig {
  enableTls: boolean;
  insecureSkipTlsVerify: boolean;
  minVersion: TLS_VERSIONS;
  clientCertPem: string;
  clientKeyPem: string;
  serverCaPem: string;
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
  requiredAcks: number;
  writeTimeout: number;
  compression: COMPRESSION_CODECS;
  sasl: SASLConfig;
  tls: TLSConfig;
  connectLogger: boolean;
}

/**
 * Message format for producing messages to a topic.
 * @note: The message format will be adopted by the reader at some point.
 */
export interface Message {
  topic: string;
  partition: number;
  offset: number;
  highwaterMark: number;
  key: Uint8Array;
  value: Uint8Array;
  headers: Map<string, any>;
  time: Date;
}

/* Basic authentication for connecting to Schema Registry. */
export interface BasicAuth {
  username: string;
  password: string;
}

/* Schema Registry configurations for creating a possible secure communication channel with Schema Registry for storing and retrieving schemas. */
export interface SchemaRegistryConfig {
  url: string;
  basicAuth: BasicAuth;
  tls: TLSConfig;
}

/* Configuration for producing messages to a topic. */
export interface ProduceConfig {
  messages: Message[];
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
  readBatchTimeout: number;
  maxWait: string;
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
  startOffset: START_OFFSETS;
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
  nanoPrecision: boolean;
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

/* Reference uses the import statement of Protobuf
and the $ref field of JSON Schema. */
export interface Reference {
  name: string;
  subject: string;
  version: number;
}

/* Schema for Schema Registry client to help with serdes. */
export interface Schema {
  enableCaching: boolean;
  id: number;
  schema: string;
  schemaType: SCHEMA_TYPES;
  version: number;
  references: Reference[];
  subject: string;
}

export interface SubjectNameConfig {
  schema: String;
  topic: String;
  element: ELEMENT_TYPES;
  subjectNameStrategy: SUBJECT_NAME_STRATEGY;
}

export interface Container {
  data: any;
  schema: Schema;
  schemaType: SCHEMA_TYPES;
}

export interface JKSConfig {
  path: string;
  password: string;
  clientCertAlias: string;
  clientKeyAlias: string;
  clientKeyPassword: string;
  serverCaAlias: string;
}

export interface JKS {
  clientCertsPem: string[];
  clientKeyPem: string;
  serverCaPem: string;
}

/**
 * @class
 * @classdesc Writer writes messages to Kafka.
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
 * @classdesc Reader reads messages from Kafka.
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
 * const messages = reader.consume({limit: 10, nanoPrecision: false});
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
 * @classdesc Connection connects to Kafka for working with topics.
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

/**
 * @class
 * @classdesc Schema Registry is a client for Schema Registry and handles serdes.
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
 * const schemaRegistry = new SchemaRegistry({
 *   url: "localhost:8081",
 * });
 *
 * const keySchema = schemaRegistry.createSchema({
 *   version: 0,
 *   element: KEY,
 *   subject: "...",
 *   schema: "...",
 *   schemaType: "AVRO"
 * });
 *
 * const valueSchema = schemaRegistry.createSchema({
 *   version: 0,
 *   element: VALUE,
 *   subject: "...",
 *   schema: "...",
 *   schemaType: "AVRO"
 * });
 *
 * // In VU code (default function)
 * writer.produce({
 *   messages: [
 *     {
 *       key: schemaRegistry.serialize({
 *         data: "key",
 *         schema: keySchema,
 *         schemaType: SCHEMA_TYPE_AVRO
 *       }),
 *       value: schemaRegistry.serialize({
 *         data: "value",
 *         schema: valueSchema,
 *         schemaType: SCHEMA_TYPE_AVRO
 *       }),
 *     }
 *   ]
 * });
 * ```
 */
export class SchemaRegistry {
  /**
   * @constructor
   * Create a new SchemaRegistry client.
   * @param {SchemaRegistryConfig} schemaRegistryConfig - Schema Registry configuration.
   * @returns {SchemaRegistry} - SchemaRegistry instance.
   */
  constructor(schemaRegistryConfig: SchemaRegistryConfig);
  /**
   * @method
   * Get latest schema from Schema Registry by subject.
   * Alternatively a specific schema version can be fetched by either specifing schema.version of schema.schema
   * @param {Schema} schema - Schema configuration.
   * @returns {Schema} - Schema.
   */
  getSchema(schema: Schema): Schema;
  /**
   * @method
   * Create or update a schema on Schema Registry.
   * @param {Schema} schema - Schema configuration.
   * @returns {Schema} - Schema.
   */
  createSchema(schema: Schema): Schema;
  /**
   * @method
   * Returns the subject name for the given SubjectNameConfig.
   * @param {SubjectNameConfig} subjectNameConfig - Subject name configuration.
   * @returns {string} - Subject name.
   */
  getSubjectName(subjectNameConfig: SubjectNameConfig): string;
  /**
   * @method
   * Serializes the given data and schema into a byte array.
   * @param {Container} container - Container including data, schema and schemaType.
   * @returns {Uint8Array} - Serialized data as byte array.
   */
  serialize(container: Container): Uint8Array;
  /**
   * @method
   * Deserializes the given data and schema into its original form.
   * @param {Container} container - Container including data, schema and schemaType.
   * @returns {any} - Deserialized data as string, byte array or JSON object.
   */
  deserialize(container: Container): any;
}

/**
 * @function
 * @description Load a JKS keystore from a file.
 * @param {JKSConfig} jksConfig - JKS configuration.
 * @returns {JKS} - JKS client and server certificates and private key.
 * @example
 * ```javascript
 * const jks = LoadJKS({
 *  path: "/path/to/keystore.jks",
 *  password: "password",
 *  clientCertAlias: "localhost",
 *  clientKeyAlias: "localhost",
 *  clientKeyPassword: "password",
 *  serverCaAlias: "ca",
 * });
 * ```
 */
export function LoadJKS(jksConfig: JKSConfig): JKS;
