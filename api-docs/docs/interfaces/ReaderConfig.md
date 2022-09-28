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

[index.d.ts:153](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L153)

___

### commitInterval

• **commitInterval**: `number`

#### Defined in

[index.d.ts:165](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L165)

___

### connectLogger

• **connectLogger**: `boolean`

#### Defined in

[index.d.ts:175](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L175)

___

### groupBalancers

• **groupBalancers**: [`GROUP_BALANCERS`](../enums/GROUP_BALANCERS.md)[]

#### Defined in

[index.d.ts:163](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L163)

___

### groupID

• **groupID**: `string`

#### Defined in

[index.d.ts:154](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L154)

___

### groupTopics

• **groupTopics**: `string`[]

#### Defined in

[index.d.ts:155](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L155)

___

### heartbeatInterval

• **heartbeatInterval**: `number`

#### Defined in

[index.d.ts:164](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L164)

___

### isolationLevel

• **isolationLevel**: [`ISOLATION_LEVEL`](../enums/ISOLATION_LEVEL.md)

#### Defined in

[index.d.ts:177](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L177)

___

### joinGroupBackoff

• **joinGroupBackoff**: `number`

#### Defined in

[index.d.ts:170](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L170)

___

### maxAttempts

• **maxAttempts**: `number`

#### Defined in

[index.d.ts:176](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L176)

___

### maxBytes

• **maxBytes**: `number`

#### Defined in

[index.d.ts:160](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L160)

___

### maxWait

• **maxWait**: `number`

#### Defined in

[index.d.ts:161](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L161)

___

### minBytes

• **minBytes**: `number`

#### Defined in

[index.d.ts:159](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L159)

___

### offset

• **offset**: `number`

#### Defined in

[index.d.ts:178](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L178)

___

### partition

• **partition**: `number`

#### Defined in

[index.d.ts:157](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L157)

___

### partitionWatchInterval

• **partitionWatchInterval**: `number`

#### Defined in

[index.d.ts:166](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L166)

___

### queueCapacity

• **queueCapacity**: `number`

#### Defined in

[index.d.ts:158](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L158)

___

### readBackoffMax

• **readBackoffMax**: `number`

#### Defined in

[index.d.ts:174](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L174)

___

### readBackoffMin

• **readBackoffMin**: `number`

#### Defined in

[index.d.ts:173](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L173)

___

### readLagInterval

• **readLagInterval**: `number`

#### Defined in

[index.d.ts:162](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L162)

___

### rebalanceTimeout

• **rebalanceTimeout**: `number`

#### Defined in

[index.d.ts:169](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L169)

___

### retentionTime

• **retentionTime**: `number`

#### Defined in

[index.d.ts:171](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L171)

___

### sasl

• **sasl**: [`SASLConfig`](SASLConfig.md)

#### Defined in

[index.d.ts:179](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L179)

___

### sessionTimeout

• **sessionTimeout**: `number`

#### Defined in

[index.d.ts:168](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L168)

___

### startOffset

• **startOffset**: `number`

#### Defined in

[index.d.ts:172](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L172)

___

### tls

• **tls**: [`TLSConfig`](TLSConfig.md)

#### Defined in

[index.d.ts:180](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L180)

___

### topic

• **topic**: `string`

#### Defined in

[index.d.ts:156](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L156)

___

### watchPartitionChanges

• **watchPartitionChanges**: `boolean`

#### Defined in

[index.d.ts:167](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L167)
