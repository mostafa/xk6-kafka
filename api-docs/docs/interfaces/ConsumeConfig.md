# Interface: ConsumeConfig

Configuration for Consume method.

## Table of contents

### Properties

- [expectTimeout](ConsumeConfig.md#expecttimeout)
- [limit](ConsumeConfig.md#limit)
- [nanoPrecision](ConsumeConfig.md#nanoprecision)

## Properties

### expectTimeout

• **expectTimeout**: `boolean`

If true, return whatever messages have been collected when maxWait is
passed.

#### Defined in

[index.d.ts:212](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L212)

---

### limit

• **limit**: `number`

collect this many messages before returning.

#### Defined in

[index.d.ts:205](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L205)

---

### nanoPrecision

• **nanoPrecision**: `boolean`

If true, returned message RFC3339 timestamps carry nanosecond precision.

#### Defined in

[index.d.ts:207](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L207)
