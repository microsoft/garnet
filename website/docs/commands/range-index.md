---
id: range-index
sidebar_label: Range Index
title: Range Index Commands
slug: range-index
---

# Range Index (Preview)

Range Index is a Garnet data type backed by [Bf-Tree](https://github.com/microsoft/bf-tree), a high-performance B-tree
optimized for range queries on byte-string keys. It enables ordered key-value storage within a single Garnet key,
supporting point reads, inserts, deletes, and efficient range scans — capabilities not available with standard Redis
data structures.

:::note
Range Index is a preview feature. Enable it with the `--enable-range-index-preview` server flag.
:::

## Overview

Each Range Index is an independent ordered key-value store identified by a Garnet key name. Within that index, entries
are sorted lexicographically by their field (key) bytes, enabling range scans and prefix queries.

All commands use the `RI.*` prefix. Deletion of the index itself uses the standard `DEL` / `UNLINK` commands.

### Storage Backends

Each Range Index is created with one of two storage backends:

- **`DISK`** (default) — Leaf pages are stored in a data file on disk, with an in-memory circular buffer as a
  hot-data cache. Total capacity is limited by disk space. Supports all operations including scan. This is the
  recommended mode for production use.
- **`MEMORY`** — All data lives in a bounded in-memory circular buffer. Total capacity is limited by `CACHESIZE`.
  Scan operations are **not supported** in this mode.

### Persistence

Range Index is fully integrated with Garnet's checkpoint and AOF mechanisms:

- **Checkpoint (BGSAVE):** BfTree data is snapshotted alongside the Tsavorite store checkpoint.
  Recovery restores the tree to the exact checkpoint state.
- **AOF:** Write operations (RI.SET, RI.DEL) are logged to the append-only file. On recovery,
  the checkpoint is restored first, then AOF entries are replayed to recover post-checkpoint mutations.
- **Eviction and lazy restore:** When memory pressure causes the stub to be evicted from the log,
  the BfTree data file is preserved. On next access, the tree is lazily restored from its snapshot.

---

## Lifecycle Commands

### RI.CREATE

Create a new Range Index.

#### Syntax

```bash
RI.CREATE key [DISK | MEMORY] [CACHESIZE bytes] [MINRECORD bytes] [MAXRECORD bytes] [MAXKEYLEN bytes] [PAGESIZE bytes]
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `DISK` / `MEMORY` | `DISK` | Storage backend |
| `CACHESIZE` | Library default | Circular buffer size in bytes (hot-data cache for DISK; total capacity for MEMORY) |
| `MINRECORD` | Library default | Minimum record size in the circular buffer |
| `MAXRECORD` | Library default | Maximum record size in the circular buffer |
| `MAXKEYLEN` | Library default | Maximum key (field) length in bytes |
| `PAGESIZE` | Auto-computed | Leaf page size (auto-computed from MAXRECORD if not specified) |

#### Examples

```bash
RI.CREATE myindex DISK CACHESIZE 67108864 MAXKEYLEN 64
RI.CREATE myindex MEMORY CACHESIZE 16777216 MINRECORD 8 MAXRECORD 4096
RI.CREATE myindex DISK
```

#### Resp Reply

Simple string reply: `OK` on success, or error if the key already exists or configuration is invalid.

---

### RI.EXISTS

Check if a key is a Range Index.

#### Syntax

```bash
RI.EXISTS key
```

Returns `1` if the key exists and is a Range Index, `0` otherwise (including if the key is a different type).

#### Resp Reply

Integer reply: `1` or `0`.

---

### RI.CONFIG

Return the configuration of a Range Index.

#### Syntax

```bash
RI.CONFIG key
```

#### Resp Reply

Array reply: alternating field-name and value pairs:

```
1) "storage_backend"
2) "DISK"
3) "cache_size"
4) "67108864"
5) "min_record_size"
6) "8"
7) "max_record_size"
8) "4096"
9) "max_key_len"
10) "64"
11) "leaf_page_size"
12) "4096"
```

Returns a WRONGTYPE error if the key is not a Range Index.

---

### RI.METRICS

Return runtime metrics for a Range Index.

#### Syntax

```bash
RI.METRICS key
```

#### Resp Reply

Array reply: alternating metric-name and value pairs. Available metrics depend on the BfTree native library.

Returns a WRONGTYPE error if the key is not a Range Index.

---

### DEL / UNLINK

Deleting a Range Index uses the standard `DEL` or `UNLINK` commands. The underlying BfTree is freed automatically.

```bash
DEL myindex
UNLINK myindex
```

---

## Write Commands

### RI.SET

Insert or update a field-value entry in the Range Index.

#### Syntax

```bash
RI.SET key field value
```

#### Examples

```bash
RI.SET myindex "user:1001" "Alice"
RI.SET myindex "emp:042" "Bob,Engineering,L5"
```

#### Resp Reply

Simple string reply: `OK` on success, or error if the key/value size violates the index constraints.

---

### RI.DEL

Delete a field from the Range Index.

#### Syntax

```bash
RI.DEL key field
```

#### Examples

```bash
RI.DEL myindex "user:1001"
```

#### Resp Reply

Integer reply: `:1` if the field was deleted, `:0` if not found.

---

## Read Commands

### RI.GET

Read a single field from the Range Index.

#### Syntax

```bash
RI.GET key field
```

#### Examples

```bash
RI.GET myindex "user:1001"
```

#### Resp Reply

Bulk string reply: the value associated with the field, or nil if not found.

---

## Scan / Range Query Commands

These are the core differentiating commands that leverage Bf-Tree's range scan capability.

### RI.SCAN

Scan entries starting at a key, returning up to `COUNT` entries in lexicographic order.

#### Syntax

```bash
RI.SCAN key start COUNT n [FIELDS KEY | VALUE | BOTH]
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `COUNT n` | Required | Maximum number of entries to return |
| `FIELDS` | `BOTH` | What to return: `KEY` (keys only), `VALUE` (values only), or `BOTH` (key-value pairs) |

#### Examples

```bash
RI.SCAN myindex "user:1000" COUNT 10
RI.SCAN myindex "a" COUNT 100 FIELDS KEY
RI.SCAN myindex "" COUNT 50 FIELDS VALUE
```

#### Resp Reply

With `FIELDS BOTH` (default): Array of 2-element arrays `[key, value]`.

```
1) 1) "user:1000"
   2) "Alice"
2) 1) "user:1001"
   2) "Bob"
```

With `FIELDS KEY`: Array of bulk strings (keys only).

With `FIELDS VALUE`: Array of bulk strings (values only).

#### Errors

- `-ERR memory-only mode does not support scan` if the index uses the MEMORY backend.

---

### RI.RANGE

Scan all entries in the closed range `[start, end]`.

#### Syntax

```bash
RI.RANGE key start end [FIELDS KEY | VALUE | BOTH]
```

#### Examples

```bash
RI.RANGE myindex "user:1000" "user:2000"
RI.RANGE myindex "a" "z" FIELDS KEY
RI.RANGE myindex "emp:001" "emp:100" FIELDS BOTH
```

#### Resp Reply

Same format as `RI.SCAN`.

---

## Type Safety

Range Index keys are type-safe:

- **`GET`** / **`SET`** on a Range Index key returns a `WRONGTYPE` error.
- **`RI.*`** commands on a non-Range-Index key return a `WRONGTYPE` error.
- **`DEL`** / **`UNLINK`** / **`TYPE`** work on any key type, including Range Index.
- **`TYPE`** on a Range Index key returns `rangeindex`.

---

## Example Session

```bash
> RI.CREATE r1 DISK CACHESIZE 33554432 MINRECORD 8
+OK

> RI.SET r1 "emp:001" "Alice,Engineering,L5"
+OK

> RI.SET r1 "emp:002" "Bob,Sales,L3"
+OK

> RI.SET r1 "emp:010" "Charlie,Engineering,L7"
+OK

> RI.GET r1 "emp:002"
"Bob,Sales,L3"

> RI.SCAN r1 "emp:001" COUNT 2 FIELDS BOTH
1) 1) "emp:001"
   2) "Alice,Engineering,L5"
2) 1) "emp:002"
   2) "Bob,Sales,L3"

> RI.RANGE r1 "emp:001" "emp:010" FIELDS KEY
1) "emp:001"
2) "emp:002"
3) "emp:010"

> RI.DEL r1 "emp:002"
(integer) 1

> RI.EXISTS r1
(integer) 1

> TYPE r1
rangeindex

> DEL r1
(integer) 1

> RI.EXISTS r1
(integer) 0
```

---

## Configuration

Enable Range Index with the server flag:

```bash
garnet-server --enable-range-index-preview
```

Or in `garnet.conf`:

```
EnableRangeIndexPreview true
```

Range Index is disabled by default. When disabled, all `RI.*` commands return an error.
