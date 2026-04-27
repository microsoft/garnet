---
id: roaring-bitmap
sidebar_label: Roaring Bitmap
title: Roaring Bitmap
slug: roaring-bitmap
---

# Roaring Bitmap

Garnet ships an extension implementing **[Roaring Bitmaps](https://roaringbitmap.org/)**:
a compressed bitmap encoding that scales from sparse to dense `uint32` sets while
remaining fast for membership and population-count queries.

The extension lives in `main/GarnetServer/Extensions/RoaringBitmap/` and is wired
into the default `GarnetServer` host. It introduces a new object type and four
RESP commands prefixed with `R.` (to avoid colliding with the existing
[`SETBIT` / `GETBIT` / `BITCOUNT` / `BITPOS`](raw-string) commands which operate
on raw byte strings).

## Why Roaring Bitmaps?

A naive bitmap covering the full `uint32` range is 512 MiB. Roaring partitions
the universe into 65 536 chunks of 65 536 bits and stores each chunk as either:

- An **array container** (sorted `ushort[]`) when the chunk holds at most 4 096
  set bits — memory ≈ `2·count` bytes.
- A **bitmap container** (`ulong[1024]`, 8 KiB exactly) once the chunk crosses
  the 4 096-element threshold.

Empty chunks consume zero bytes (no entry in the per-chunk dictionary). The
implementation transparently promotes (array → bitmap) and demotes
(bitmap → array) chunks as cardinality changes.

## Commands

| Command | Arity | Description |
| --- | --- | --- |
| `R.SETBIT key offset value` | 4 | Set the bit at `offset` (an unsigned 32-bit integer) to `0` or `1`. Returns the previous bit value. |
| `R.GETBIT key offset` | 3 | Returns the bit at `offset`. Returns `0` for missing keys. |
| `R.BITCOUNT key` | 2 | Returns the population count (number of `1` bits). Returns `0` for missing keys. |
| `R.BITPOS key bit [from]` | 3 or 4 | Returns the position of the first `bit` (`0` or `1`) at or after `from` (default `0`). Returns `-1` when no match exists in the `uint32` universe. |

### `R.SETBIT key offset value`

```text
> R.SETBIT counters 1000 1
(integer) 0
> R.SETBIT counters 1000 1
(integer) 1
```

`offset` must be a non-negative integer that fits in `uint32` (max `4294967295`).
`value` must be `0` or `1`.

### `R.GETBIT key offset`

```text
> R.GETBIT counters 1000
(integer) 1
> R.GETBIT counters 9999
(integer) 0
```

Returns `0` for any offset that has never been set or for a missing key. The
key is not created on a `R.GETBIT` of a missing key.

### `R.BITCOUNT key`

```text
> R.BITCOUNT counters
(integer) 1
```

Returns the number of bits set to `1`. Returns `0` for a missing key.

### `R.BITPOS key bit [from]`

```text
> R.BITPOS counters 1
(integer) 1000
> R.BITPOS counters 0
(integer) 0
> R.BITPOS counters 1 1001
(integer) -1
```

When `bit` is `1` and no set bit exists at or after `from`, returns `-1`.
When `bit` is `0`, scanning continues across the full `uint32` universe; the
result is `-1` only if every offset from `from` up to `2^32 - 1` is set.

## Persistence and replication

The Roaring Bitmap object plugs into Garnet's standard custom-object path: it
serializes through `Serialize` / `Deserialize` overrides, so RDB checkpoints,
AOF, and cluster migration all work without any extra wiring.

## Known limitations (v1)

The first cut focuses on a clean, well-tested foundation. The following items
are intentionally deferred:

- **Run container** — the canonical Roaring third container. Adding it improves
  compression on contiguous ranges by ~30%, but the current array/bitmap pair
  already captures the bulk of real-world space savings.
- **`R.BITOP AND/OR/XOR/NOT`** — multi-key set operations. The data structure
  supports these natively; only the command surface is missing.
- **Empty-key removal** — when `R.SETBIT key offset 0` clears the last bit, the
  key currently remains as an empty bitmap object instead of being removed.
  This is a property of the custom-object framework's tombstone path
  (`output.HasRemoveKey` is honoured only on the built-in path) and is tracked
  separately.

These are good candidates for follow-up PRs.

## See also

- Issue [`#1270`](https://github.com/microsoft/garnet/issues/1270) — original
  feature request.
- [`SETBIT` / `GETBIT` / `BITCOUNT` / `BITPOS`](raw-string) — Redis-compatible
  raw-string bit operations (these operate on byte strings, not Roaring
  bitmaps).
