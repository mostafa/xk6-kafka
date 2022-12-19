# Interface: ReaderConfig

## Table of contents

### Properties

- [brokers](ReaderConfig.md#brokers)
- [commitInterval](ReaderConfig.md#commitinterval)
- [connectLogger](ReaderConfig.md#connectlogger)
- [groupBalancers](ReaderConfig.md#groupbalancers)
- [groupID](ReaderConfig.md#groupid)
- [groupTopics](ReaderConfig.md#grouptopics)
- [heartbeatInterval](ReaderConfig.md#heartbeatinterval)
- [isolationLevel](ReaderConfig.md#isolationlevel)
- [joinGroupBackoff](ReaderConfig.md#joingroupbackoff)
- [maxAttempts](ReaderConfig.md#maxattempts)
- [maxBytes](ReaderConfig.md#maxbytes)
- [maxWait](ReaderConfig.md#maxwait)
- [minBytes](ReaderConfig.md#minbytes)
- [offset](ReaderConfig.md#offset)
- [partition](ReaderConfig.md#partition)
- [partitionWatchInterval](ReaderConfig.md#partitionwatchinterval)
- [queueCapacity](ReaderConfig.md#queuecapacity)
- [readBackoffMax](ReaderConfig.md#readbackoffmax)
- [readBackoffMin](ReaderConfig.md#readbackoffmin)
- [readLagInterval](ReaderConfig.md#readlaginterval)
- [rebalanceTimeout](ReaderConfig.md#rebalancetimeout)
- [retentionTime](ReaderConfig.md#retentiontime)
- [sasl](ReaderConfig.md#sasl)
- [sessionTimeout](ReaderConfig.md#sessiontimeout)
- [startOffset](ReaderConfig.md#startoffset)
- [tls](ReaderConfig.md#tls)
- [topic](ReaderConfig.md#topic)
- [watchPartitionChanges](ReaderConfig.md#watchpartitionchanges)

## Properties

### brokers

• **brokers**: `string`[]

#### Defined in

[index.d.ts:154](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L154)

---

### commitInterval

• **commitInterval**: `number`

#### Defined in

[index.d.ts:166](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L166)

---

### connectLogger

• **connectLogger**: `boolean`

#### Defined in

[index.d.ts:176](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L176)

---

### groupBalancers

• **groupBalancers**: [`GROUP_BALANCERS`](../enums/GROUP_BALANCERS.md)[]

#### Defined in

[index.d.ts:164](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L164)

---

### groupID

• **groupID**: `string`

#### Defined in

[index.d.ts:155](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L155)

---

### groupTopics

• **groupTopics**: `string`[]

#### Defined in

[index.d.ts:156](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L156)

---

### heartbeatInterval

• **heartbeatInterval**: `number`

#### Defined in

[index.d.ts:165](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L165)

---

### isolationLevel

• **isolationLevel**: [`ISOLATION_LEVEL`](../enums/ISOLATION_LEVEL.md)

#### Defined in

[index.d.ts:178](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L178)

---

### joinGroupBackoff

• **joinGroupBackoff**: `number`

#### Defined in

[index.d.ts:171](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L171)

---

### maxAttempts

• **maxAttempts**: `number`

#### Defined in

[index.d.ts:177](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L177)

---

### maxBytes

• **maxBytes**: `number`

#### Defined in

[index.d.ts:161](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L161)

---

### maxWait

• **maxWait**: `number`

#### Defined in

[index.d.ts:162](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L162)

---

### minBytes

• **minBytes**: `number`

#### Defined in

[index.d.ts:160](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L160)

---

### offset

• **offset**: `number`

#### Defined in

[index.d.ts:179](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L179)

---

### partition

• **partition**: `number`

#### Defined in

[index.d.ts:158](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L158)

---

### partitionWatchInterval

• **partitionWatchInterval**: `number`

#### Defined in

[index.d.ts:167](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L167)

---

### queueCapacity

• **queueCapacity**: `number`

#### Defined in

[index.d.ts:159](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L159)

---

### readBackoffMax

• **readBackoffMax**: `number`

#### Defined in

[index.d.ts:175](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L175)

---

### readBackoffMin

• **readBackoffMin**: `number`

#### Defined in

[index.d.ts:174](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L174)

---

### readLagInterval

• **readLagInterval**: `number`

#### Defined in

[index.d.ts:163](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L163)

---

### rebalanceTimeout

• **rebalanceTimeout**: `number`

#### Defined in

[index.d.ts:170](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L170)

---

### retentionTime

• **retentionTime**: `number`

#### Defined in

[index.d.ts:172](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L172)

---

### sasl

• **sasl**: [`SASLConfig`](SASLConfig.md)

#### Defined in

[index.d.ts:180](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L180)

---

### sessionTimeout

• **sessionTimeout**: `number`

#### Defined in

[index.d.ts:169](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L169)

---

### startOffset

• **startOffset**: `number`

#### Defined in

[index.d.ts:173](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L173)

---

### tls

• **tls**: [`TLSConfig`](TLSConfig.md)

#### Defined in

[index.d.ts:181](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L181)

---

### topic

• **topic**: `string`

#### Defined in

[index.d.ts:157](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L157)

---

### watchPartitionChanges

• **watchPartitionChanges**: `boolean`

#### Defined in

[index.d.ts:168](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L168)
