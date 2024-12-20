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
    COMMITAOF
```

Issues a manual commit of the append-only-file. This is useful when auto-commits are turned off, but you need the
system to commit at specific times.

#### Resp Reply

Simple string reply: OK.

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

---

### **GETWITHETAG**

#### **Syntax**

```bash
GETWITHETAG key
```

Retrieves the value and the ETag associated with the given key.

#### **Response**

One of the following:

- **Array reply**: An array of two items returned on success. The first item is an integer representing the ETag, and the second is the bulk string value of the key. If called on a key-value pair without ETag, the first item will be nil.
- **Nil reply**: If the key does not exist.

---

### **SETIFMATCH**

#### **Syntax**

```bash
SETIFMATCH key value etag [EX || PX expiration]
```

Updates the value of a key if the provided ETag matches the current ETag of the key.

#### **Response**

One of the following:

- **Array reply**: The updated ETag if the value was successfully updated, and no a
- **Nil reply**: If the key does not exist.

---

### **GETIFNOTMATCH**

#### **Syntax**

```bash
GETIFNOTMATCH key etag
```

Retrieves the value if the ETag associated with the key has changed; otherwise, returns a response indicating no change.

#### **Response**

One of the following:

- **Array reply**: If the ETag does not match, an array of two items is returned. The first item is the updated ETag, and the second item is the value associated with the key. If called on a record without an ETag the first item in the array will be 0. If the etag matches then we the first item on the array is the existing etag, but the second value is Nil
- **Nil reply**: If the key does not exist.

---

### Compatibility and Behavior with Non-ETag Commands

Below is the expected behavior of ETag-associated key-value pairs when non-ETag commands are used.

- **MSET, BITOP**: These commands will replace an existing ETag-associated key-value pair with a non-ETag key-value pair, effectively removing the ETag.

- **SET**: Only if used with additional option "WITHETAG" will calling SET update the etag while inserting the new key-value pair over the existing key-value pair.

- **RENAME**: Renaming an old ETag-associated key-value pair will create the newly renamed key with an initial etag of 0. If the key being renamed to already existed before hand, it will retain the etag of the existing key that was the target of the rename, and show it as an updated etag.

- **Custom Commands**: While etag based key value pairs **can be used blindly inside of custom transactions and custom procedures**, ETag set key value pairs are **not supported to be used from inside of Custom Raw String Functions.**

All other commands will update the etag internally if they modify the underlying data, and any responses from them will not expose the etag to the client. To the users the etag and it's updates remain hidden in non-etag commands.

---
