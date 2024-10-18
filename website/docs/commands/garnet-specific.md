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
