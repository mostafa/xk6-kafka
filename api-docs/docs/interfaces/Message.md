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

[index.d.ts:136](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L136)

---

### highwaterMark

• **highwaterMark**: `number`

#### Defined in

[index.d.ts:133](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L133)

---

### key

• **key**: `Uint8Array`

#### Defined in

[index.d.ts:134](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L134)

---

### offset

• **offset**: `number`

#### Defined in

[index.d.ts:132](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L132)

---

### partition

• **partition**: `number`

#### Defined in

[index.d.ts:131](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L131)

---

### time

• **time**: `Date`

#### Defined in

[index.d.ts:137](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L137)

---

### topic

• **topic**: `string`

#### Defined in

[index.d.ts:130](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L130)

---

### value

• **value**: `Uint8Array`

#### Defined in

[index.d.ts:135](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L135)
