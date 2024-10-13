---
id: analytics-commands
sidebar_label: Analytics
title: Analytics
slug: analytics
---

## BITMAP

### BITCOUNT

#### Syntax

```bash
    BITCOUNT key [start end [BYTE | BIT]]
```

Count the number of set bits (population counting) in a string.

#### Resp Reply

Integer reply: the number of bits set to 1.

---
### BITFIELD

#### Syntax

```bash
BITFIELD key [GET encoding offset | [OVERFLOW <WRAP | SAT | FAIL>]
  <SET encoding offset value | INCRBY encoding offset increment>
  [GET encoding offset | [OVERFLOW <WRAP | SAT | FAIL>]
  <SET encoding offset value | INCRBY encoding offset increment>
  ...]]
```

The command treats a Garnet string as a array of bits, and is capable of addressing specific integer fields of varying bit widths and arbitrary non (necessary) aligned offset.\ 
In practical terms using this command you can set, for example, a signed 5 bits integer at bit offset 1234 to a specific value, retrieve a 31 bit unsigned integer from offset 4567.\
Similarly the command handles increments and decrements of the specified integers, providing guaranteed and well specified overflow and underflow behavior that the user can configure.

#### Resp Reply

Integer Reply: the bit value stored at offset.

---
### BITFIELD_RO

#### Syntax

```bash
BITFIELD_RO key [GET encoding offset [GET encoding offset ...]]
```

Read-only variant of the [BITFIELD](#bitfield) command. It is like the original [BITFIELD](#bitfield) but only accepts GET subcommand and can safely be used in read-only replicas.

#### Resp Reply

Array reply: each entry being the corresponding result of the sub-command given at the same position.

---
### BITOP AND

#### Syntax

```bash
BITOP AND destkey srckey1 srckey2 srckey3 ... srckeyN
```

Perform a bitwise AND operation between multiple keys (containing string values) and store the result in the destination key.

#### Resp Reply
Integer Reply: the size of the string stored in the destination key, that is equal to the size of the longest input string.

---
### BITOP NOT

#### Syntax

```bash
BITOP NOT destkey srckey
```

Perform a bitwise NOT operation between multiple keys (containing string values) and store the result in the destination key.

#### Resp Reply

Integer Reply: the size of the string stored in the destination key, that is equal to the size of the longest input string.

---
### BITPOS

#### Syntax

```bash
    BITPOS key bit [start [end [BYTE | BIT]]]    
```

Returns the position of the first bit set to 1 or 0 in a string.

#### Resp Reply

One of the following:

* Integer reply: the position of the first bit set to 1 or 0 according to the request
* Integer reply: -1. In case the bit argument is 1 and the string is empty or composed of just zero bytes

---
### GETBIT

#### Syntax

```bash
   GETBIT key offset 
```

Returns the bit value at offset in the string value stored at key.

#### Resp Reply

The bit value stored at offset, one of the following:

* Integer reply: 0.
* Integer reply: 1.

---
### SETBIT

#### Syntax

```bash
    SETBIT key offset value
```

Sets or clears the bit at offset in the string value stored at key. The bit is either set or cleared depending on value, which can be either 0 or 1. When key does not exist, a new string value is created. 

#### Resp Reply

Integer reply: the original bit value stored at offset.

---
## HYPERLOGLOG
### PFADD
#### Syntax

```bash
PFADD <key> <element-1> ... <element-n>
```

Adds all the element arguments to the HyperLogLog data structure stored at the variable name specified as first argument.

#### Resp Reply

Integer Reply: 1 if at least 1 HyperLogLog internal register was altered. 0 otherwise.

---
### PFCOUNT
#### Syntax

```bash
PFCOUNT key [key ...]
```
When called with a single key, returns the approximated cardinality computed by the HyperLogLog data structure stored at the specified variable, which is 0 if the variable does not exist.\
When called with multiple keys, returns the approximated cardinality of the union of the HyperLogLogs passed, by internally merging the HyperLogLogs stored at the provided keys into a temporary HyperLogLog.

#### Resp Reply
Integer Reply: the approximated number of unique elements observed via PFADD.

---
### PFMERGE
#### Syntax

```bash
PFMERGE <destination-key> <source-key-1> ... <source-key-n>
```

Merge multiple HyperLogLog values into an unique value that will approximate the cardinality of the union of the observed Sets of the source HyperLogLog structures.\
The computed merged HyperLogLog is set to the destination variable, which is created if does not exist (defaulting to an empty HyperLogLog).

#### Resp Reply

Simple String Reply: the command just returns OK.

---
## PUB/SUB

### PSUBSCRIBE
#### Syntax

```bash
PSUBSCRIBE <pattern-1> ... <pattern-n>
```

Subscribes the client to the given patterns.

Supported glob-style patterns:

```h?llo``` subscribes to ```hello```, ```hallo``` and ```hxllo```\
```h*llo``` subscribes to ```hllo``` and ```heeeello```\
```h[ae]llo``` subscribes to ```hello``` and ```hallo```, but not ```hillo```\
Use ```\``` to escape special characters if you want to match them verbatim.

#### Resp Reply

When successful, this command doesn't return anything. Instead, for each pattern, one message with the first element being the string ```psubscribe``` is pushed as a confirmation that the command succeeded.

---
### PUBLISH
#### Syntax

```bash
PUBLISH <channel> <message>
```

Posts a message to the given channel.

#### Resp Reply

Integer Reply: the number of clients that received the message.

---

### PUBSUB CHANNELS
#### Syntax

```bash
PUBSUB CHANNELS [pattern]
```

Lists the currently active channels. An active channel is a Pub/Sub channel with one or more subscribers (excluding clients subscribed to patterns).

#### Resp Reply

Array reply: a list of active channels, optionally matching the specified pattern.

---

### PUBSUB NUMPAT
#### Syntax

```bash
PUBSUB NUMPAT
```

Returns the number of unique patterns that are subscribed to by clients (that are performed using the PSUBSCRIBE command).

#### Resp Reply

Integer reply: the number of patterns all the clients are subscribed to.

---

### PUBSUB NUMSUB
#### Syntax

```bash
PUBSUB NUMSUB [channel [channel ...]]
```

Returns the number of subscribers (exclusive of clients subscribed to patterns) for the specified channels.

#### Resp Reply

Array reply: the number of subscribers per channel, each even element (including the 0th) is channel name, each odd element is the number of subscribers

---

### PUNSUBSCRIBE
#### Syntax

```bash
PUNSUBSCRIBE <pattern-1> ... <pattern-n>
```

Unsubscribes the client from the given patterns, or from all of them if none is given.

When no patterns are specified, the client is unsubscribed from all the previously subscribed patterns. In this case, a message for every unsubscribed pattern will be sent to the client.

#### Resp Reply

When successful, this command doesn't return anything. Instead, for each pattern, one message with the first element being the string ```punsubscribe``` is pushed as a confirmation that the command succeeded.

---
### SUBSCRIBE
#### Syntax

```bash
SUBSCRIBE channel [channel ...]
```

Subscribes the client to the specified channels.

#### Resp Reply

When successful, this command doesn't return anything. Instead, for each channel, one message with the first element being the string ```subscribe``` is pushed as a confirmation that the command succeeded.

---
### UNSUBSCRIBE
#### Syntax

```bash
UNSUBSCRIBE [channel [channel ...]]
```

Unsubscribes the client from the given channels, or from all of them if none is given.

#### Resp Reply

When successful, this command doesn't return anything. Instead, for each channel, one message with the first element being the string ```unsubscribe``` is pushed as a confirmation that the command succeeded.

---
