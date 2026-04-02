---
id: garnet-etags
sidebar_label: Garnet ETags
title: Garnet ETags
slug: garnet-etags
---

# Garnet ETags

ETags are a Garnet-specific feature developed to support cases like augmented outputs, 
conditional execution etc. ETags are a form of optimistic concurrency control that 
allow clients to perform operations conditionally based on the version of the data 
they are operating on. Each key in Garnet can have an associated ETag, which is 
a numeric value that increments whenever the value of the key changes. Clients 
can retrieve the ETag for a key and use it to ensure that remote operations operate on 
the expected version of the data.

## ETag Commands

Listed below are all the Garnet ETag-related commands. We call these meta-commands as they can envelop 
any single-key data command. These are summarized here:

| Meta-Command | Description |
|---|---|
| `EXECWITHETAG cmd [arg [arg ...]]` | Execute a command and return the ETag alongside the result |
| `EXECIFMATCH etag cmd [arg [arg ...]]` | Execute only if the current ETag matches the provided ETag |
| `EXECIFNOTMATCH etag cmd [arg [arg ...]]` | Execute only if the current ETag does _not_ match the provided ETag |
| `EXECIFGREATER etag cmd [arg [arg ...]]` | Execute only if the provided ETag is greater than the current ETag |

Finally, a `GETETAG` command is available as a lightweight way to retrieve only the ETag for a key. These
commands are described in detail below.


---

### EXECIFGREATER

#### Syntax

```bash
    EXECIFGREATER etag command [arg [arg ...]]
```

Execute the specified command if the eTag specified is greater than the current eTag of the record stored at the key it operates on.

#### Resp Reply

* Array reply: An array of size 2 containing the response to the command (or null if command was not executed) followed by the eTag of the key after the operation is completed.
    * If the record stored at key has no eTag, an eTag of zero is returned.
    * The meta-command syntax supports only single-key data commands (with the exception of `DEL` and `EXISTS` only if executed with a single key)
    * If command specified is `SET`, the first value is null if the command executed or the current value at key if command was skipped
    * If command specified is `DEL`, the eTag value returned is that of the record prior to its deletion.

---

### EXECIFMATCH

#### Syntax

```bash
    EXECIFMATCH etag command [arg [arg ...]]
```

Execute the specified command if the eTag specified matches the current eTag of the record stored at the key it operates on.

#### Resp Reply

* Array reply: An array of size 2 containing the response to the command (or null if command was not executed) followed by the eTag of the key after the operation is completed.
    * If the record stored at key has no eTag, an eTag of zero is returned.
    * The meta-command syntax supports only single-key data commands (with the exception of `DEL` and `EXISTS` only if executed with a single key)
    * If command specified is `SET`, the first value is null if the command executed or the current value at key if command was skipped
    * If command specified is `DEL`, the eTag value returned is that of the record prior to its deletion.

---

### EXECIFNOTMATCH

#### Syntax

```bash
    EXECIFNOTMATCH etag command [arg [arg ...]]
```

Execute the specified command if the eTag specified does not match the current eTag of the record stored at the key it operates on.

#### Resp Reply

* Array reply: An array of size 2 containing the response to the command (or null if command was not executed) followed by the eTag of the key after the operation is completed.
    * If the record stored at key has no eTag, an eTag of zero is returned.
    * The meta-command syntax supports only single-key data commands (with the exception of `DEL` and `EXISTS` only if executed with a single key)
    * If command specified is `SET`, the first value is null if the command executed or the current value at key if command was skipped
    * If command specified is `DEL`, the eTag value returned is that of the record prior to its deletion.

---

### EXECWITHETAG

#### Syntax

```bash
    EXECWITHETAG command [arg [arg ...]]
```

Execute the specified command and append the eTag of record stored at the key it operates on into the output.

#### Resp Reply

* Array reply: An array of size 2 containing the response to the command followed by the eTag of the record stored at key after the operation is completed.
    * If the record stored at key has no eTag, an eTag of zero is returned.
    * The meta-command syntax supports only single-key data commands (with the exception of `DEL` and `EXISTS` only if executed with a single key)
    * If command specified is `DEL`, the eTag value returned is that of the record prior to its deletion.

---

## Unsupported Commands

ETag meta-commands support **single-key data commands**. The following categories are explicitly blocked:

- **Multi-key commands** — `MGET`, `MSET`, multi-key `EXISTS`, multi-key `DEL` etc.
- **Cross-key commands** — `BITOP`, `PFCOUNT`, `PFMERGE`, `LCS`, `LMOVE`, `RPOPLPUSH`, `SMOVE`, `SINTERSTORE`, `ZUNIONSTORE`, etc.
- **Scatter-gather / async reads** — `GET` variants

**Important note**: ETags will advance accordingly if the log record was modified upon running this commands without a meta-command.

## ETag Semantics

### Supported Data Types

The original ETag feature was limited to raw strings (values accessed via `GET`/`SET`). This is no longer the case. ETags now work with **all** Garnet data types:

- **Strings** — `SET`, `APPEND`, `INCR`, `SETRANGE`, `SETBIT`, and more
- **Hashes** — `HSET`, `HDEL`, `HINCRBY`, `HSETNX`, and more
- **Lists** — `LPUSH`, `RPUSH`, `LPOP`, `LSET`, `LINSERT`, and more
- **Sets** — `SADD`, `SREM`, `SPOP`, and more
- **Sorted Sets** — `ZADD`, `ZREM`, `ZINCRBY`, `ZPOPMIN`, and more
- **Geo** — `GEOADD` and related commands
- **Unified** — `DEL`, `EXISTS`, `EXPIRE`, `PERSIST`, `TTL`, `RENAME`, and more

This was made possible by moving ETag tracking from value metadata into the **log record level** in Garnet's storage engine, allowing any record — string or object — to carry an ETag.

### Composability

Because meta-commands wrap existing commands, you get ETag semantics for free on any supported operation. For example:

```bash
# Increment a counter only if your ETag is current
EXECIFMATCH 5 INCRBY myCounter 10

# Add to a sorted set only if your ETag matches
EXECIFMATCH 3 ZADD leaderboard 100 player1

# Push to a list and get the updated ETag
EXECWITHETAG LPUSH myList newItem

# Set a key with expiration and ETag tracking
EXECWITHETAG SET myKey myValue EX 300
```

### Behavior Rules

The ETag increments when a command **mutates the value** of a key. Metadata-only changes (like `EXPIRE` or `PERSIST`) do **not** increment the ETag. Overwriting a key (e.g., a plain `SET` without a meta-command) removes its ETag. Here is a summary:

| Operation | ETag Behavior |
|---|---|
| Value mutation (e.g., `INCR`, `HSET`, `LPUSH`, `SET` with meta-command) | ETag increments |
| Metadata-only change (e.g., `EXPIRE`, `PERSIST`) | ETag unchanged |
| Key deletion (`DEL`, `UNLINK`) | ETag returned, then removed |
| Key overwrite without meta-command (e.g. `SET` without meta-command) | ETag removed |
| Read-only command (e.g., `GET`, `HGET`) | ETag unchanged |

## Migrating from ETags v1

Garnet's ETag support has been redesigned in v2. Previously, Garnet provided a set of dedicated 
ETag commands such as `GETWITHETAG`, `SETIFMATCH`, `GETIFNOTMATCH`, and `SETIFGREATER`. 
Each operated on a single command (`GET` or `SET`) and required its own parsing, storage,
and response logic. ETags now work with all data types — not just raw strings — and the 
old dedicated ETag commands have been replaced by a separate meta-command API that wraps 
traditional commands as described above. This was made possible by moving ETag tracking 
from value metadata into the **log record level** in Garnet's storage engine, allowing 
any record — string or object — to carry an ETag.

### Command Mapping

| Old Command | New Equivalent |
|---|---|
| `SET key value WITHETAG` | `EXECWITHETAG SET key value` |
| `GETWITHETAG key` | `EXECWITHETAG GET key` |
| `SETIFMATCH key value etag` | `EXECIFMATCH etag SET key value` |
| `SETIFGREATER key value etag` | `EXECIFGREATER etag SET key value` |
| `GETIFNOTMATCH key etag` | `EXECIFNOTMATCH etag GET key` |
| `DELIFGREATER key etag` | `EXECIFGREATER etag DEL key` |

### Response Format

The old dedicated commands returned `[etag, value]` (ETag first). The new meta-commands return `[commandResponse, etag]` (ETag last). This is the most important difference to account for when migrating.

Two commands have special response behavior:
- **`SET`** — The first element is `null` when the command _executed_. If the conditional check caused execution to be _skipped_, the first element is the current value at the key instead.
- **`DEL`** — The ETag value returned is that of the record _prior_ to its deletion.

### Migration Examples

#### SET with ETag

##### _Previous Syntax_
```csharp
// Command: SET key value WITHETAG
// Response: 1 (just the initial ETag)
long etag = (long)await db.ExecuteAsync("SET", key, value, "WITHETAG");
```

##### _Updated Syntax_
```csharp
// Command: EXECWITHETAG SET key value
// Response: [null, 1] — null + initial ETag
var result = (RedisResult[])(await db.ExecuteAsync("EXECWITHETAG", "SET", key, value));
long etag = (long)result[1];
```

#### GET with ETag

##### _Previous Syntax_
```csharp
// Command: GETWITHETAG key
// Response: [1, "hello"] - current ETag + current value
var result = (RedisResult[])(await db.ExecuteAsync("GETWITHETAG", key))!;
long etag = (long)result[0];       // ETag was first
string value = (string)result[1];  // value was second
```

##### _Updated Syntax_
```csharp
// Command: EXECWITHETAG GET key
// Response: ["hello", 1] - current value + current ETag
var result = (RedisResult[])(await db.ExecuteAsync("EXECWITHETAG", "GET", key));
string value = (string)result[0];  // command response is first
long etag = (long)result[1];       // ETag is second
```

#### Conditional SET

##### _Previous Syntax_
```csharp
// Command: SETIFMATCH GET key value etag
// Response:
// Condition met; [2, null] — updated ETag + null
// Condition not met; [1, "hello"] — current ETag + current value
var res = (RedisResult[])(await db.ExecuteAsync("SETIFMATCH", key, newValue, etag));
long newEtag = (long)res[0];    // ETag was first
bool success = res[1].IsNull;   // null value meant success
```

##### _Updated Syntax_
```csharp
// Command: EXECIFMATCH etag SET key value
// Response:
// Condition met; [null, 2] — null + updated ETag
// Condition not met; ["hello", 1] — current value + current ETag
var res = (RedisResult[])(await db.ExecuteAsync("EXECIFMATCH", etag, "SET", key, newValue));
bool success = res[0].IsNull;   // null command response means it executed
long newEtag = (long)res[1];    // ETag is second
```

#### Conditional DELETE

##### _Previous Syntax_
```csharp
// Command: DELIFGREATER key etag
// Response:
// Condition met; 1 - deleted
// Condition not met; 0 - not deleted
int deleted = (int)await db.ExecuteAsync("DELIFGREATER", key, etag);
```

##### _Updated Syntax_
```csharp
// Command: EXECIFGREATER etag DEL key
// Response:
// Condition met; [1, 1] — 1 deleted + ETag prior to deletion
// Condition not met; [0, 1] — 0 deleted + current ETag
var res = (RedisResult[])(await db.ExecuteAsync("EXECIFGREATER", etag, "DEL", key));
int deleted = (int)res[0];     // DEL's normal response (1 or 0)
long etag = (long)res[1]; // ETag (prior to deletion, or current if skipped)
```

