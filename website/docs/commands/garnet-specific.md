---
id: garnet-specific-commands
sidebar_label: Garnet-specific Commands
title: Garnet-specific Commands
slug: garnet-specific-commands
---

Apart from Garnet's [server extensions](../extensions/overview.md), we support many API calls that are not available
in other RESP servers. These are described below.

### FORCEGC

#### Syntax

```bash
    FORCEGC [generation]
```

Invoke garbage collection on the server side. Optionally, specify the generation level for the collection. For more
information, see [this article](https://learn.microsoft.com/en-us/dotnet/api/system.gc.collect).

#### Resp Reply

Simple string reply: OK.

---

### COMMITAOF

#### Syntax

```bash
    COMMITAOF [DBID]
```

Issues a manual commit of the append-only-file (for all active databases in the Garnet instance). This is useful when auto-commits are turned off, but you need the system to commit at specific times. If a DB ID is specified, a manual commit of the append-only-file of that specific database will be issues.

#### Resp Reply

Simple string reply: OK.

---

### HCOLLECT

#### Syntax

```bash
    HCOLLECT key [key ...]
```

Manualy trigger cleanup of expired field from memory for a given Hash set key.

Use `*` as the key to collect it from all hash keys.

#### Resp Reply

Simple reply: OK response
Error reply: ERR HCOLLECT scan already in progress

---

### ZCOLLECT

#### Syntax

```bash
    ZCOLLECT key [key ...]
```

Manualy trigger cleanup of expired member from memory for a given Hash set key.

Use `*` as the key to collect it from all sorted set keys.

#### Resp Reply

Simple reply: OK response
Error reply: ERR ZCOLLECT scan already in progress

---

### COSCAN

#### Syntax

```bash
    COSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
```

Custom Object Scan is similar to [HSCAN](data-structures.md#hscan) and [SSCAN](data-structures.md#sscan). It iterates 
over the fields and values of a custom object stored at a given key.

The match parameter allows to apply a filter to elements after they have been retrieved from the collection.
The count option sets a limit to the maximum number of items returned from the server to this command. This limit 
is also set in conjunction with the object-scan-count-limit of the global server settings.

You can use the NOVALUES option to make Garnet return only the keys without their corresponding values.

---

### SECONDARYOF

Configures a server as secondary of another, or promotes it to a primary. Same as [REPLICAOF](server.md#replicaof).

---

### REGISTERCS

This allows registering specific custom commands and transactions implemented in C\#, in a server side DLL library.
See [this page](../dev/custom-commands.md) for details.

---

### RUNTXP

#### Syntax

```bash
    RUNTXP txid [args]
```

Runs the specific custom transactional procedure indetified by its ID.

---

### WATCHMS

#### Syntax

```bash
    WATCHMS key [key ...]
```

Same as [WATCH](transactions.md#watch), but specifies that the key is only present in the main (raw string) store.

---

### WATCHOS

#### Syntax

```bash
    WATCHOS key [key ...]
```

Same as [WATCH](transactions.md#watch), but specifies that the key is only present in the object store.

---

### ASYNC

Async interface to Garnet when accessing larger-than-memory data. See [this link](https://github.com/microsoft/garnet/pull/387) for details.

---

### MODULE LOADCS

This the equivalent of `MODULE LOAD` in the original RESP protocol. This loads a self-contained module in which the module 
initialization code registers all relevant commands and transactions automatically. See [this page](../dev/custom-commands.md) 
for details.

---

## Native ETag Support

Garnet provides support for ETags on raw strings. By using the ETag-related commands outlined below, you can associate any **string based key-value pair** inserted into Garnet with an automatically updated ETag.

Compatibility with non-ETag commands and the behavior of data inserted with ETags are detailed at the end of this document.
To initialize a key value pair with an ETag you can use either the SET command with the newly added "WITHETAG" optional flag, or you can take any existing Key value pair and call SETIFMATCH with the ETag argument as 0 (Any key value pair without an explicit ETag has an ETag of 0 implicitly). Read more about Etag use cases and patterns [here](../../blog/etags-when-and-how)


---

### **SET (WITHETAG)**

#### **Syntax**

```bash
    SET key value [NX | XX] [EX seconds | PX milliseconds] [KEEPTTL] WITHETAG
```

Set **key** to hold the string value along with an ETag. If key already holds a value, it is overwritten, regardless of its type. Any previous time to live associated with the **key** is discarded on successful SET operation.

**Options:**

* EX seconds -- Set the specified expire time, in seconds (a positive integer).
* PX milliseconds -- Set the specified expire time, in milliseconds (a positive integer).
* NX -- Only set the key if it does not already exist.
* XX -- Only set the key if it already exists.
* KEEPTTL -- Retain the time to live associated with the key.
* WITHETAG -- **Adding this sets the Key Value pair with an initial ETag**, if called on an existing key value pair with an ETag, this command will update the ETag transparently.

#### Resp Reply

* Integer reply: WITHETAG given: The ETag associated with the value.


---

### **GETWITHETAG**

#### **Syntax**

```bash
GETWITHETAG key
```

Retrieves the value and the ETag associated with the given key.

#### **Response**

One of the following:

- **Array reply**: An array of two items returned on success. The first item is an integer representing the ETag, and the second is the bulk string value of the key. If called on a key-value pair without ETag, the etag will be 0.
- **Nil reply**: If the key does not exist.

---

### **SETIFMATCH**

#### **Syntax**

```bash
SETIFMATCH key value etag [EX seconds | PX milliseconds] [NOGET]
```

Sets/updates a key value pair with the given etag only if (1) the etag given in the request matches the already existing etag ; or (2) there was no existing value; or (3) the existing value was not associated with any etag and the sent etag was 0.

**Options:**
* EX seconds -- Set the specified expire time, in seconds (a positive integer).
* PX milliseconds -- Set the specified expire time, in milliseconds (a positive integer).
* NOGET -- The value is not returned even if the etag mismatched

#### **Response**

- **Array reply**: If the sent etag matches the existing etag the reponse will be an array where the first item is the updated etag, and the second value is nil. If the etags do not match then the response array will hold the latest etag, and the latest value in order.

---

### **SETIFGREATER**

#### **Syntax**

```bash
SETIFGREATER key value etag [EX seconds | PX milliseconds] [NOGET]
```
Sets/updates a key value pair with the given etag only if (1) the etag given in the request is greater than the already existing etag ; or (2) there was no existing value; or (3) the existing value was not associated with any etag and the sent etag was greater than 0.

**Options:**
* EX seconds -- Set the specified expire time, in seconds (a positive integer).
* PX milliseconds -- Set the specified expire time, in milliseconds (a positive integer).
* NOGET -- The value is not returned even if the sent etag was not greater to existing etag. 

#### **Response**

- **Array reply**: If the sent etag is greater than the existing etag then an array where the first item is the updated etag, and the second value is nil is returned. If the sentEtag is less than or equal to the existing etag then the response array will hold the latest etag, and the latest value in order.

---

### **GETIFNOTMATCH**

#### **Syntax**

```bash
GETIFNOTMATCH key etag
```

Retrieves the value if the ETag associated with the key has changed; otherwise, returns a response indicating no change.

#### **Response**

One of the following:

- **Array reply**: If the ETag does not match, an array of two items is returned. The first item is the latest ETag, and the second item is the value associated with the key. If the Etag matches  the first item in the response array is the etag and the second item is nil.
- **Nil reply**: If the key does not exist.

---

### **DELIFGREATER**

#### **Syntax**

```bash
DELIFGREATER key etag
```

Deletes a key only if the provided Etag is strictly greater than the existing Etag for the key.

#### **Response**

- **Integer reply**: Returns 1 if the key was successfully deleted; otherwise, returns 0.

---

### Compatibility and Behavior with Non-ETag Commands

ETags are currently not supported for servers running in Cluster mode. This will be supported soon.

Below is the expected behavior of ETag-associated key-value pairs when non-ETag commands are used.

- **MSET, BITOP**: These commands will replace an existing ETag-associated key-value pair with a non-ETag key-value pair, effectively removing the ETag.

- **SET**: Only if used with additional option "WITHETAG" will calling SET update the etag while inserting the new key-value pair over the existing key-value pair.

- **RENAME**: RENAME takes an option for WITHETAG. When called WITHETAG it will rename the key with an etag if the key being renamed to did not exist, else it will increment the existing etag of the key being renamed to.

- **Custom Commands**: While etag based key value pairs **can be used blindly inside of custom transactions and custom procedures**, ETag set key value pairs are **not supported to be used from inside of Custom Raw String Functions.**

All other commands will update the etag internally if they modify the underlying data, and any responses from them will not expose the etag to the client. To the users the etag and it's updates remain hidden in non-etag commands.

---

### ZEXPIRE

#### Syntax

```bash
    ZEXPIRE key seconds [NX | XX | GT | LT] MEMBERS nummembers member [member ...]
```

Sets a timeout on one or more members of a sorted set key. After the timeout has expired, the members will automatically be deleted. The timeout is specified in seconds.

The command supports several options to control when the expiration should be set:

* **NX:** Only set expiry on members that have no existing expiry
* **XX:** Only set expiry on members that already have an expiry set
* **GT:** Only set expiry when it's greater than the current expiry
* **LT:** Only set expiry when it's less than the current expiry

The **NX**, **XX**, **GT**, and **LT** options are mutually exclusive.

#### Resp Reply

Array reply: For each member, returns:

* 1 if the timeout was set
* 0 if the member doesn't exist
* -1 if timeout was not set due to condition not being met

---

### ZEXPIREAT

#### Syntax

```bash
    ZEXPIREAT key unix-time-seconds [NX | XX | GT | LT] MEMBERS nummembers member [member ...]
```

Sets an absolute expiration time (Unix timestamp in seconds) for one or more sorted set members. After the timestamp has passed, the members will automatically be deleted.

The command supports several options to control when the expiration should be set:

* **NX:** Only set expiry on members that have no existing expiry
* **XX:** Only set expiry on members that already have an expiry set
* **GT:** Only set expiry when it's greater than the current expiry
* **LT:** Only set expiry when it's less than the current expiry

The **NX**, **XX**, **GT**, and **LT** options are mutually exclusive.

#### Resp Reply

Array reply: For each member, returns:

* 1 if the timeout was set
* 0 if the member doesn't exist
* -1 if timeout was not set due to condition not being met

---

### ZPEXPIRE

#### Syntax

```bash
    ZPEXPIRE key milliseconds [NX | XX | GT | LT] MEMBERS nummembers member [member ...]
```

Similar to HEXPIRE but the timeout is specified in milliseconds instead of seconds.

The command supports several options to control when the expiration should be set:

* **NX:** Only set expiry on members that have no existing expiry
* **XX:** Only set expiry on members that already have an expiry set
* **GT:** Only set expiry when it's greater than the current expiry
* **LT:** Only set expiry when it's less than the current expiry

The **NX**, **XX**, **GT**, and **LT** options are mutually exclusive.

#### Resp Reply

Array reply: For each member, returns:

* 1 if the timeout was set
* 0 if the member doesn't exist
* -1 if timeout was not set due to condition not being met

---

### ZPEXPIREAT

#### Syntax

```bash
    ZPEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT] MEMBERS nummembers member [member ...]
```

Similar to HEXPIREAT but uses Unix timestamp in milliseconds instead of seconds.

The command supports several options to control when the expiration should be set:

* **NX:** Only set expiry on members that have no existing expiry
* **XX:** Only set expiry on members that already have an expiry set
* **GT:** Only set expiry when it's greater than the current expiry
* **LT:** Only set expiry when it's less than the current expiry

The **NX**, **XX**, **GT**, and **LT** options are mutually exclusive.

#### Resp Reply

Array reply: For each member, returns:

* 1 if the timeout was set
* 0 if the member doesn't exist
* -1 if timeout was not set due to condition not being met

---

### ZTTL

#### Syntax

```bash
    ZTTL key MEMBERS nummembers member [member ...]
```

Returns the remaining time to live in seconds for one or more sorted set members that have a timeout set.

#### Resp Reply

Array reply: For each member, returns:

* TTL in seconds if the member exists and has an expiry set
* -1 if the member exists but has no expiry set
* -2 if the member does not exist

---

### ZPTTL

#### Syntax

```bash
    ZPTTL key MEMBERS nummembers member [member ...]
```

Similar to HTTL but returns the remaining time to live in milliseconds instead of seconds.

#### Resp Reply

Array reply: For each member, returns:

* TTL in milliseconds if the member exists and has an expiry set
* -1 if the member exists but has no expiry set
* -2 if the member does not exist

---

### ZEXPIRETIME

#### Syntax

```bash
    ZEXPIRETIME key MEMBERS nummembers member [member ...]
```

Returns the absolute Unix timestamp (in seconds) at which the specified sorted set members will expire.

#### Resp Reply

Array reply: For each member, returns:

* Unix timestamp in seconds when the member will expire
* -1 if the member exists but has no expiry set
* -2 if the member does not exist

---

### ZPEXPIRETIME

#### Syntax

```bash
    ZPEXPIRETIME key MEMBERS nummembers member [member ...]
```

Similar to HEXPIRETIME but returns the expiry timestamp in milliseconds instead of seconds.

#### Resp Reply

Array reply: For each member, returns:

* Unix timestamp in milliseconds when the member will expire
* -1 if the member exists but has no expiry set
* -2 if the member does not exist

---

### ZPERSIST

#### Syntax

```bash
    ZPERSIST key MEMBERS nummembers member [member ...]
```

Removes the expiration from the specified sorted set members, making them persistent.

#### Resp Reply

Array reply: For each member, returns:

* 1 if the timeout was removed
* 0 if the member exists but has no timeout
* -1 if the member does not exist

---

### ZCOLLECT

#### Syntax

```bash
    ZCOLLECT key [key ...]
```

Manualy trigger cleanup of expired member from memory for a given Hash set key.

Use `*` as the key to collect it from all sorted set keys.

#### Resp Reply

Simple reply: OK response
Error reply: ERR ZCOLLECT scan already in progress

---