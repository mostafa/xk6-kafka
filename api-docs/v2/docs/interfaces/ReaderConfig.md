[**xk6-kafka**](../README.md)

---

# Interface: ReaderConfig

Defined in: [index.d.ts:180](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L180)

## Properties

### brokers

> **brokers**: `string`[]

Defined in: [index.d.ts:181](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L181)

---

### commitInterval

> **commitInterval**: `number`

Defined in: [index.d.ts:196](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L196)

---

### connectLogger

> **connectLogger**: `boolean`

Defined in: [index.d.ts:206](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L206)

---

### groupBalancers

> **groupBalancers**: [`GROUP_BALANCERS`](../enumerations/GROUP_BALANCERS.md)[]

Defined in: [index.d.ts:194](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L194)

---

### groupId

> **groupId**: `string`

Defined in: [index.d.ts:182](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L182)

---

### ~~groupID?~~

> `optional` **groupID?**: `string`

Defined in: [index.d.ts:184](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L184)

#### Deprecated

Use `groupId` instead.

---

### groupTopics

> **groupTopics**: `string`[]

Defined in: [index.d.ts:185](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L185)

---

### heartbeatInterval

> **heartbeatInterval**: `number`

Defined in: [index.d.ts:195](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L195)

---

### isolationLevel

> **isolationLevel**: [`ISOLATION_LEVEL`](../enumerations/ISOLATION_LEVEL.md)

Defined in: [index.d.ts:208](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L208)

---

### joinGroupBackoff

> **joinGroupBackoff**: `number`

Defined in: [index.d.ts:201](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L201)

---

### maxAttempts

> **maxAttempts**: `number`

Defined in: [index.d.ts:207](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L207)

---

### maxBytes

> **maxBytes**: `number`

Defined in: [index.d.ts:190](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L190)

---

### maxWait

> **maxWait**: `string`

Defined in: [index.d.ts:192](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L192)

---

### minBytes

> **minBytes**: `number`

Defined in: [index.d.ts:189](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L189)

---

### offset

> **offset**: `number`

Defined in: [index.d.ts:209](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L209)

---

### partition

> **partition**: `number`

Defined in: [index.d.ts:187](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L187)

---

### partitionWatchInterval

> **partitionWatchInterval**: `number`

Defined in: [index.d.ts:197](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L197)

---

### queueCapacity

> **queueCapacity**: `number`

Defined in: [index.d.ts:188](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L188)

---

### readBackoffMax

> **readBackoffMax**: `number`

Defined in: [index.d.ts:205](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L205)

---

### readBackoffMin

> **readBackoffMin**: `number`

Defined in: [index.d.ts:204](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L204)

---

### readBatchTimeout

> **readBatchTimeout**: `number`

Defined in: [index.d.ts:191](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L191)

---

### readLagInterval

> **readLagInterval**: `number`

Defined in: [index.d.ts:193](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L193)

---

### rebalanceTimeout

> **rebalanceTimeout**: `number`

Defined in: [index.d.ts:200](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L200)

---

### retentionTime

> **retentionTime**: `number`

Defined in: [index.d.ts:202](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L202)

---

### sasl

> **sasl**: [`SASLConfig`](SASLConfig.md)

Defined in: [index.d.ts:210](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L210)

---

### sessionTimeout

> **sessionTimeout**: `number`

Defined in: [index.d.ts:199](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L199)

---

### startOffset

> **startOffset**: [`START_OFFSETS`](../enumerations/START_OFFSETS.md)

Defined in: [index.d.ts:203](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L203)

---

### tls

> **tls**: [`TLSConfig`](TLSConfig.md)

Defined in: [index.d.ts:211](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L211)

---

### topic

> **topic**: `string`

Defined in: [index.d.ts:186](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L186)

---

### watchPartitionChanges

> **watchPartitionChanges**: `boolean`

Defined in: [index.d.ts:198](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L198)
