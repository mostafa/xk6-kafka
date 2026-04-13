[**xk6-kafka**](../README.md)

---

# Interface: ConsumeConfig

Defined in: index.d.ts:213

Configuration for Consume method.

## Properties

### expectTimeout

> **expectTimeout**: `boolean`

Defined in: index.d.ts:224

If true, return whatever messages have been collected when maxWait is
passed.

---

### limit?

> `optional` **limit?**: `number`

Defined in: index.d.ts:215

collect this many messages before returning.

---

### maxMessages?

> `optional` **maxMessages?**: `number`

Defined in: index.d.ts:217

preferred v2 alias for limit.

---

### nanoPrecision

> **nanoPrecision**: `boolean`

Defined in: index.d.ts:219

If true, returned message RFC3339 timestamps carry nanosecond precision.
