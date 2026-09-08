[**xk6-kafka**](../README.md)

---

# Interface: ConsumerGroupOffset

Defined in: [index.d.ts:305](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L305)

One topic-partition offset captured for a consumer group.

## Properties

### offset

> **offset**: `number`

Defined in: [index.d.ts:309](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L309)

Loses integer precision above 2^53; only a concern for extremely large offsets.

---

### partition

> **partition**: `number`

Defined in: [index.d.ts:307](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L307)

---

### topic

> **topic**: `string`

Defined in: [index.d.ts:306](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L306)
