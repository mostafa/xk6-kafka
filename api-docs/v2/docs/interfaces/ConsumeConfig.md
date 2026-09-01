[**xk6-kafka**](../README.md)

---

# Interface: ConsumeConfig

Defined in: [index.d.ts:227](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L227)

Configuration for Consume method.

## Properties

### expectTimeout

> **expectTimeout**: `boolean`

Defined in: [index.d.ts:238](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L238)

If true, return whatever messages have been collected when maxWait is
passed.

---

### limit?

> `optional` **limit?**: `number`

Defined in: [index.d.ts:229](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L229)

collect this many messages before returning.

---

### maxMessages?

> `optional` **maxMessages?**: `number`

Defined in: [index.d.ts:231](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L231)

preferred v2 alias for limit.

---

### nanoPrecision

> **nanoPrecision**: `boolean`

Defined in: [index.d.ts:233](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L233)

If true, returned message RFC3339 timestamps carry nanosecond precision.
