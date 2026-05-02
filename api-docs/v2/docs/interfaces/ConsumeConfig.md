[**xk6-kafka**](../README.md)

---

# Interface: ConsumeConfig

Defined in: [index.d.ts:214](https://github.com/tnewman/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L214)

Configuration for Consume method.

## Properties

### expectTimeout

> **expectTimeout**: `boolean`

Defined in: [index.d.ts:225](https://github.com/tnewman/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L225)

If true, return whatever messages have been collected when maxWait is
passed.

---

### limit?

> `optional` **limit?**: `number`

Defined in: [index.d.ts:216](https://github.com/tnewman/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L216)

collect this many messages before returning.

---

### maxMessages?

> `optional` **maxMessages?**: `number`

Defined in: [index.d.ts:218](https://github.com/tnewman/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L218)

preferred v2 alias for limit.

---

### nanoPrecision

> **nanoPrecision**: `boolean`

Defined in: [index.d.ts:220](https://github.com/tnewman/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L220)

If true, returned message RFC3339 timestamps carry nanosecond precision.
