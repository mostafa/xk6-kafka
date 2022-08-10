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

index.d.ts:129

___

### highwaterMark

• **highwaterMark**: `number`

#### Defined in

index.d.ts:126

___

### key

• **key**: `Uint8Array`

#### Defined in

index.d.ts:127

___

### offset

• **offset**: `number`

#### Defined in

index.d.ts:125

___

### partition

• **partition**: `number`

#### Defined in

index.d.ts:124

___

### time

• **time**: `Date`

#### Defined in

index.d.ts:130

___

### topic

• **topic**: `string`

#### Defined in

index.d.ts:123

___

### value

• **value**: `Uint8Array`

#### Defined in

index.d.ts:128
