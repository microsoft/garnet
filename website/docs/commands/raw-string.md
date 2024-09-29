---
id: raw-string
sidebar_label: Raw String
title: Raw String
---

# Raw String Commands

### DECR

#### Syntax

```bash
    DECR key
```

Decrements the number stored at key by one. If the key does not exist, it is set to 0 before performing the operation. An error is returned if the key contains a value of the wrong type or contains a string that can not be represented as integer. 

#### RESP Reply

Integer reply: the value of the key after decrementing it.

---

### DECRBY

#### Syntax

```bash
    DECRBY key decrement
```

Decrements the number stored at key by the value of parameter `decrement`. If the key does not exist, it is set to 0 before performing the operation. An error is returned if the key contains a value of the wrong type or contains a string that can not be represented as integer.

#### Resp Reply

Integer reply: the value of the key after decrementing it.

---

### GET

#### Syntax

```bash
    GET key
```

Gets the value of key. If the key does not exist nil is returned.

#### Resp Reply

One of the following:

* Bulk string reply: the value of the key.
* Nil reply: if the key does not exist.

---

### GETEX

#### Syntax

```bash
    GETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST]
```

Get the value of key and optionally set its expiration. GETEX is similar to GET, but is a write command with additional options.

The GETEX command supports a set of options that modify its behavior:

* EX seconds -- Set the specified expire time, in seconds.
* PX milliseconds -- Set the specified expire time, in milliseconds.
* EXAT timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds.
* PXAT timestamp-milliseconds -- Set the specified Unix time at which the key will expire, in milliseconds.
* PERSIST -- Remove the time to live associated with the key.

#### Resp Reply

One of the following:

* Bulk string reply: the value of the key.
* Nil reply: if the key does not exist or if the key's value type is not a string.

---

### GETDEL

#### Syntax

```bash
    GETDEL key
```

Get the value of key and delete the key. This command is similar to GET, but that it also deletes the key on success (if and only if the key's value type is a string).

#### Resp Reply

One of the following:

* Bulk string reply: the value of the key.
* Nil reply: if the key does not exist or if the key's value type is not a string.

---

### GETRANGE

#### Syntax

```bash
    GETRANGE key start end
```

Returns the substring of the string value stored at key, determined by the offsets start and end (both are inclusive). 

#### Resp Reply

Bulk string reply: The substring of the string value stored at key, determined by the offsets start and end (both are inclusive).

---

### INCR

#### Syntax

```bash
    INCR key
```

Increments the number stored at key by one. If the key does not exist, it is set to 0 before performing the operation.

#### Resp Reply

Integer reply: the value of the key after the increment.

---

### INCRBY

#### Syntax

```bash
    INCRBY key increment
```

Increments the number stored at key by the value of the parameter increment. If the key does not exist, it is set to 0 before performing the operation.

#### Resp Reply

Integer reply: the value of the key after the increment.

---

### MGET

#### Syntax

```bash
    MGET key [key ...]
```

Returns the values of all specified keys. For every key that does not exist, the special value nil is returned. 

### MSET

#### Syntax

```bash
    MSET key value [key value ...]
```

Sets the given keys to their respective values. MSET replaces existing values with new values, just as regular SET. See MSETNX if you don't want to overwrite existing values.


#### Resp Reply

Array reply: a list of values at the specified keys.

---

### MSETNX

#### Syntax

```bash
    MSETNX key value [key value ...]
```

Sets the given keys to their respective values. MSETNX will not perform any operation at all even if just a single key already exists.

#### Resp Reply

One of the following:

* Integer reply: 0 if no key was set (at least one key already existed).
* Integer reply: 1 if all the keys were set.

---

### PSETEX

#### Syntax

```bash
    PSETEX key milliseconds value
```

PSETEX works exactly like SETEX with the sole difference that the expire time is specified in milliseconds instead of seconds.

#### Resp Reply

Simple string reply: OK.

---

### SET

#### Syntax

```bash
    SET key value [NX | XX] [GET] [EX seconds | PX milliseconds |
```

Set **key** to hold the string value. If key already holds a value, it is overwritten, regardless of its type. Any previous time to live associated with the **key** is discarded on successful SET operation.

**Options:**

* EX seconds -- Set the specified expire time, in seconds (a positive integer).
* PX milliseconds -- Set the specified expire time, in milliseconds (a positive integer).
* NX -- Only set the key if it does not already exist.
* XX -- Only set the key if it already exists.
* KEEPTTL -- Retain the time to live associated with the key.

#### Resp Reply

Any of the following:

* Nil reply: GET not given: Operation was aborted (conflict with one of the XX/NX options).
* Simple string reply: OK. GET not given: The key was set.
* Nil reply: GET given: The key didn't exist before the SET.
* Bulk string reply: GET given: The previous value of the key.

---

### SETEX

#### Syntax

```bash
    SETEX key value
```

Set **key** to hold the string value and set **key** to timeout after a given number of seconds.

#### Resp Reply

Simple string reply: OK.

---

### STRLEN

#### Syntax

```bash
    STRLEN key
```

Returns the length of the string value stored at **key**.

#### Resp Reply

* Integer reply: the length of the string stored at key, or 0 when the key does not exist.

---

### SETRANGE

#### Syntax

```bash
    SETRANGE key offset value
```

Overwrites part of the string stored at key, starting at the specified offset, for the entire length of value. 

#### Resp Reply

* Integer reply: the length of the string after it was modified by the command.

---
