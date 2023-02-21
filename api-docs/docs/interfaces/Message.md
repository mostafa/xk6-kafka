# Interface: Message

Message format for producing messages to a topic.

**`note:`** The message format will be adopted by the reader at some point.

## Table of contents

### Properties

- [headers](Message.md#headers)
- [highwaterMark](Message.md#highwatermark)
- [key](Message.md#key)
- [offset](Message.md#offset)
- [partition](Message.md#partition)
- [time](Message.md#time)
- [topic](Message.md#topic)
- [value](Message.md#value)

## Properties

### headers

• **headers**: `Map`<`string`, `any`\>

#### Defined in

[index.d.ts:146](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L146)

---

### highwaterMark

• **highwaterMark**: `number`

#### Defined in

[index.d.ts:143](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L143)

---

### key

• **key**: `Uint8Array`

#### Defined in

[index.d.ts:144](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L144)

---

### offset

• **offset**: `number`

#### Defined in

[index.d.ts:142](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L142)

---

### partition

• **partition**: `number`

#### Defined in

[index.d.ts:141](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L141)

---

### time

• **time**: `Date`

#### Defined in

[index.d.ts:147](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L147)

---

### topic

• **topic**: `string`

#### Defined in

[index.d.ts:140](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L140)

---

### value

• **value**: `Uint8Array`

#### Defined in

[index.d.ts:145](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L145)
