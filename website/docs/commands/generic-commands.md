---
id: generic-commands
sidebar_label: Generic
title: Generic API
slug: generic
---

## Connection Commands

### AUTH

#### Syntax

```bash
    AUTH <placeholderforpassword>
```

Authenticates the connection.

#### Resp Reply

Simple string reply: OK, or an error if the password, or username/password pair, is invalid.

**Security notice:**

Only use strong and long passwords so an attack is infeasible. A better way to manage authentication will be using the ACL feature.

---

### ECHO

#### Syntax

```bash
    ECHO <message>
```

Returns message.

#### Resp Reply

Bulk string reply: the given string.

---

### HELLO

#### Syntax

```bash
    HELLO [protover [AUTH username password] [SETNAME clientname]]
```

Switch to a different protocol, optionally authenticating and setting the connection's name, or provide a contextual client report.

When called with the optional `protover` argument, this command switches the protocol to the specified version and also accepts the following options:

`AUTH <username> <password>`: directly authenticate the connection in addition to switching to the specified protocol version. This makes calling AUTH before HELLO unnecessary when setting up a new connection. Note that the username can be set to "default" to authenticate against a server that does not use ACLs, but rather the simpler requirepass mechanism of Redis prior to version 6.
`SETNAME <clientname>`: this is the equivalent of calling CLIENT SETNAME.

#### Resp Reply

Map reply: a list of server properties. Simple error reply: if the protover requested does not exist.

---

### PING

#### Syntax

```bash
    PING key
```

Returns PONG if no argument is provided, otherwise return a copy of the argument as a bulk string.

#### Resp Reply

Any of the following:

* Simple string reply: PONG when no argument is provided.
* Bulk string reply: the provided argument.

---

### QUIT

#### Syntax

```bash
    QUIT
```

Ask the server to close the connection. The connection is close when all pending replies have been written to the client.

#### Resp Reply

Simple string reply: OK.

---

### SELECT

#### Syntax

```bash
    SELECT
```

Select the Redis logical database having the specified zero-based numeric index. New connections always use the database 0.

#### Resp Reply

Simple string reply: OK.

---

## Generic Commands

### DEL

#### Syntax

```bash
    DEL key [key ...]
```

Removes the specified keys. A key is ignored if it does not exist.

#### Resp Reply

Integer reply: the number of keys that were removed.

---

### EXISTS

#### Syntax

```bash
    EXISTS key [key ...]
```

Determines whether one or more keys exist. If the same existing key is mentioned in the arguments multiple times, it will be counted multiple times. So if foo exists, EXISTS foo foo will return 2.

#### Resp Reply

Integer reply: the number of keys that exist from those specified as arguments.

---

### EXPIRE

#### Syntax

```bash
    EXPIRE key seconds [NX | XX | GT | LT]
```

Set a timeout on key in seconds. After the timeout has expired, the key will automatically be deleted.

The EXPIRE command supports a set of options:

* `NX` -- Set expiry only when the key has no expiry
* `XX` -- Set expiry only when the key has an existing expiry
* `GT` -- Set expiry only when the new expiry is greater than current one
* `LT` -- Set expiry only when the new expiry is less than current one

#### Resp Reply

One of the following:

* Integer reply: 0 if the timeout was not set; for example, the key doesn't exist, or the operation was skipped because of the provided arguments.

* Integer reply: 1 if the timeout was set.

---

### EXPIRETIME

#### Syntax

```bash
    EXPIRETIME key
```

Returns the absolute Unix timestamp (since January 1, 1970) in seconds at which the given key will expire.

#### Resp Reply

One of the following:

* Integer reply: Expiration Unix timestamp in milliseconds.
* Integer reply: -1 if the key exists but has no associated expiration time.
* Integer reply: -2 if the key does not exist.

---

### KEYS

#### Syntax

```bash
    KEYS pattern
```

Returns all keys matching pattern.

**Warning:** consider KEYS as a command that should only be used in production environments with extreme care. It may ruin performance when it is executed against large databases. 

Examples of supported patterns:

* h?llo matches hello, hallo and hxllo
* h*llo matches hllo and heeeello
* h[ae]llo matches hello and hallo, but not hillo
* h[^e]llo matches hallo, hbllo, ... but not hello
* h[a-b]llo matches hallo and hbllo

Use \ to escape special characters if you want to match them verbatim.

#### Resp Reply

Array reply: a list of keys matching pattern.

---

### MIGRATE

**TODO: Verify syntax and functionality**

#### Resp Reply

---

### PERSIST

#### Syntax

```bash
    PERSIST key
```

Removes the existing timeout on a key, turning the key from volatile (a key with an expire set) to persistent (a key that will never expire as no timeout is associated).

#### Resp Reply

---
### PEXPIRE

#### Syntax

```bash
    PEXPIRE key milliseconds [NX | XX | GT | LT]
```

This command works exactly like [EXPIRE](#expire) but the time to live of the key is specified in milliseconds instead of seconds.

#### Resp Reply

One of the following:

* Integer reply: 0 if key does not exist or does not have an associated timeout.
* Integer reply: 1 if the timeout has been removed.

---

### PEXPIRETIME

#### Syntax

```bash
    PEXPIRETIME key
```

Returns the absolute Unix timestamp (since January 1, 1970) in milliseconds at which the given key will expire.

#### Resp Reply

One of the following:

* Integer reply: Expiration Unix timestamp in milliseconds.
* Integer reply: -1 if the key exists but has no associated expiration time.
* Integer reply: -2 if the key does not exist.

---

### PTTL

#### Syntax

```bash
    PTTL  key
```

Like [TTL](#ttl) this command returns the remaining time to live of a key that has an expire set, with the sole difference that TTL returns the amount of remaining time in seconds while PTTL returns it in milliseconds.

#### Resp Reply

One of the following:

* Integer reply: TTL in milliseconds.
* Integer reply: -1 if the key exists but has no associated expiration.
* Integer reply: -2 if the key does not exist.

---

### RENAME

#### Syntax

```bash
    RENAME key newkey
```

Renames key to newkey. It returns an error when key does not exist. If newkey already exists it is overwritten, when this happens RENAME executes an implicit [DEL](#del) operation.

#### Resp Reply

Simple string reply: OK.

---

### RENAMENX

#### Syntax

```bash
    RENAME key newkey
```

Renames key to newkey if newkey does not yet exist. It returns an error when key does not exist.

#### Resp Reply

One of the following:

* Integer reply: 1 if key was renamed to newkey.
* Integer reply: 0 if newkey already exists.

---

### SCAN

#### Syntax

```bash
    SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
```

Iterates over the keys that exist in the store and returns only the ones matching the selected filters.

**The MATCH option**
It is possible to only iterate elements matching a given glob-style pattern, similarly to the behavior of the [KEYS](#keys) command that takes a pattern as its only argument.


**The TYPE option**
You can use the TYPE option to ask SCAN to only return objects that match a given type, allowing you to iterate through the database looking for keys of a specific type.

**The COUNT option**

The default COUNT value is 10, but the user is free to change it.

#### Resp Reply

Array reply: specifically, an array with two elements.

* The first element is a Bulk string reply that represents an unsigned 64-bit number, the cursor.
* The second element is an Array reply with the names of scanned keys.

---

### TTL

#### Syntax

```bash
    TTL key
```

Returns the remaining time to live of a key that has a timeout. 

#### Resp Reply

One of the following:

* Integer reply: TTL in seconds.
* Integer reply: -1 if the key exists but has no associated expiration.
* Integer reply: -2 if the key does not exist.

---
### TYPE

#### Syntax

```bash
    TYPE key
```

Returns the string representation of the type of the value stored at key. The different types that can be returned are: string, list, set, zset, and hash.

#### Resp Reply

Simple string reply: the type of key, or none when key doesn't exist.

---
### UNLINK

#### Syntax

```bash
```

This command is very similar to [DEL](#del): it removes the specified keys. Just like [DEL](#del) a key is ignored if it does not exist. However the command performs the actual memory reclaiming in a different thread, so it is not blocking, while [DEL](#del) is. 

#### Resp Reply

Integer reply: the number of keys that were unlinked.

---



