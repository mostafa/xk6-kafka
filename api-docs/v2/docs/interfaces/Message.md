[**xk6-kafka**](../README.md)

---

# Interface: Message

Defined in: index.d.ts:149

Message format for producing messages to a topic.
@note: The message format will be adopted by the reader at some point.

## Properties

### headers

> **headers**: `Map`\<`string`, `any`\>

Defined in: index.d.ts:156

---

### highwaterMark

> **highwaterMark**: `number`

Defined in: index.d.ts:153

---

### key

> **key**: `Uint8Array`

Defined in: index.d.ts:154

---

### offset

> **offset**: `number`

Defined in: index.d.ts:152

---

### partition

> **partition**: `number`

Defined in: index.d.ts:151

---

### time

> **time**: `Date`

Defined in: index.d.ts:157

---

### topic

> **topic**: `string`

Defined in: index.d.ts:150

---

### value

> **value**: `Uint8Array`

Defined in: index.d.ts:155
