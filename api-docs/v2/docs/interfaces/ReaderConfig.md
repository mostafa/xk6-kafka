[**xk6-kafka**](../README.md)

---

# Interface: ReaderConfig

Defined in: [index.d.ts:199](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L199)

## Properties

### brokers

> **brokers**: `string`[]

Defined in: [index.d.ts:200](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L200)

---

### commitInterval

> **commitInterval**: `string` \| `number`

Defined in: [index.d.ts:220](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L220)

---

### connectLogger

> **connectLogger**: `boolean`

Defined in: [index.d.ts:230](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L230)

---

### fetchMessageMaxBytes?

> `optional` **fetchMessageMaxBytes?**: `number`

Defined in: [index.d.ts:210](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L210)

---

### groupBalancers

> **groupBalancers**: [`GROUP_BALANCERS`](../enumerations/GROUP_BALANCERS.md)[]

Defined in: [index.d.ts:218](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L218)

---

### groupId

> **groupId**: `string`

Defined in: [index.d.ts:201](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L201)

---

### ~~groupID?~~

> `optional` **groupID?**: `string`

Defined in: [index.d.ts:203](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L203)

#### Deprecated

Use `groupId` instead.

---

### groupTopics

> **groupTopics**: `string`[]

Defined in: [index.d.ts:204](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L204)

---

### heartbeatInterval

> **heartbeatInterval**: `string` \| `number`

Defined in: [index.d.ts:219](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L219)

---

### isolationLevel

> **isolationLevel**: [`ISOLATION_LEVEL`](../enumerations/ISOLATION_LEVEL.md)

Defined in: [index.d.ts:232](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L232)

---

### joinGroupBackoff

> **joinGroupBackoff**: `string` \| `number`

Defined in: [index.d.ts:225](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L225)

---

### maxAttempts

> **maxAttempts**: `number`

Defined in: [index.d.ts:231](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L231)

---

### maxBytes

> **maxBytes**: `number`

Defined in: [index.d.ts:213](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L213)

---

### maxPartitionFetchBytes?

> `optional` **maxPartitionFetchBytes?**: `number`

Defined in: [index.d.ts:211](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L211)

---

### maxPollInterval?

> `optional` **maxPollInterval?**: `string`

Defined in: [index.d.ts:216](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L216)

---

### maxWait

> **maxWait**: `string`

Defined in: [index.d.ts:215](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L215)

---

### metadataMaxAge?

> `optional` **metadataMaxAge?**: `string`

Defined in: [index.d.ts:235](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L235)

---

### minBytes

> **minBytes**: `number`

Defined in: [index.d.ts:212](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L212)

---

### offset

> **offset**: `number`

Defined in: [index.d.ts:233](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L233)

---

### partition

> **partition**: `number`

Defined in: [index.d.ts:206](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L206)

---

### partitionWatchInterval

> **partitionWatchInterval**: `string` \| `number`

Defined in: [index.d.ts:221](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L221)

---

### queueCapacity

> **queueCapacity**: `number`

Defined in: [index.d.ts:207](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L207)

---

### queuedMaxMessagesKbytes?

> `optional` **queuedMaxMessagesKbytes?**: `number`

Defined in: [index.d.ts:209](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L209)

---

### queuedMinMessages?

> `optional` **queuedMinMessages?**: `number`

Defined in: [index.d.ts:208](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L208)

---

### readBackoffMax

> **readBackoffMax**: `string` \| `number`

Defined in: [index.d.ts:229](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L229)

---

### readBackoffMin

> **readBackoffMin**: `string` \| `number`

Defined in: [index.d.ts:228](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L228)

---

### readBatchTimeout

> **readBatchTimeout**: `string` \| `number`

Defined in: [index.d.ts:214](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L214)

---

### readLagInterval

> **readLagInterval**: `string` \| `number`

Defined in: [index.d.ts:217](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L217)

---

### rebalanceTimeout

> **rebalanceTimeout**: `string` \| `number`

Defined in: [index.d.ts:224](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L224)

---

### retentionTime

> **retentionTime**: `string` \| `number`

Defined in: [index.d.ts:226](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L226)

---

### sasl

> **sasl**: [`SASLConfig`](SASLConfig.md)

Defined in: [index.d.ts:236](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L236)

---

### sessionTimeout

> **sessionTimeout**: `string` \| `number`

Defined in: [index.d.ts:223](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L223)

---

### socketKeepAlive?

> `optional` **socketKeepAlive?**: `boolean`

Defined in: [index.d.ts:234](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L234)

---

### startOffset

> **startOffset**: [`START_OFFSETS`](../enumerations/START_OFFSETS.md)

Defined in: [index.d.ts:227](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L227)

---

### tls

> **tls**: [`TLSConfig`](TLSConfig.md)

Defined in: [index.d.ts:237](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L237)

---

### topic

> **topic**: `string`

Defined in: [index.d.ts:205](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L205)

---

### watchPartitionChanges

> **watchPartitionChanges**: `boolean`

Defined in: [index.d.ts:222](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L222)
