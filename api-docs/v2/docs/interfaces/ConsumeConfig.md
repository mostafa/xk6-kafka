[**xk6-kafka**](../README.md)

---

# Interface: ConsumeConfig

Defined in: [index.d.ts:241](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L241)

Configuration for Consume method.

## Properties

### expectTimeout

> **expectTimeout**: `boolean`

Defined in: [index.d.ts:252](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L252)

If true, return whatever messages have been collected when maxWait is
passed.

---

### limit?

> `optional` **limit?**: `number`

Defined in: [index.d.ts:243](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L243)

collect this many messages before returning.

---

### maxMessages?

> `optional` **maxMessages?**: `number`

Defined in: [index.d.ts:245](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L245)

preferred v2 alias for limit.

---

### nanoPrecision

> **nanoPrecision**: `boolean`

Defined in: [index.d.ts:247](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L247)

If true, returned message RFC3339 timestamps carry nanosecond precision.
