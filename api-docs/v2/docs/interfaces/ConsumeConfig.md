[**xk6-kafka**](../README.md)

---

# Interface: ConsumeConfig

Defined in: [index.d.ts:215](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L215)

Configuration for Consume method.

## Properties

### expectTimeout

> **expectTimeout**: `boolean`

Defined in: [index.d.ts:226](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L226)

If true, return whatever messages have been collected when maxWait is
passed.

---

### limit?

> `optional` **limit?**: `number`

Defined in: [index.d.ts:217](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L217)

collect this many messages before returning.

---

### maxMessages?

> `optional` **maxMessages?**: `number`

Defined in: [index.d.ts:219](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L219)

preferred v2 alias for limit.

---

### nanoPrecision

> **nanoPrecision**: `boolean`

Defined in: [index.d.ts:221](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L221)

If true, returned message RFC3339 timestamps carry nanosecond precision.
