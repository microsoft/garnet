---
id: vector-sets
sidebar_label: Vector Sets
title: Vector Sets
---

# Overview

Garnet has partial support for Vector Sets, implemented on top of the [DiskANN project](TODO).

This data is very strange when compared to others Garnet supports.

# Design

Vector Sets are a combination of one "index" key, which stores metadata and a pointer to the DiskANN data structure, and many "element" keys, which store vectors/quantized vectors/attributes/etc.  All Vector Set keys are kept in the main store, but only the index key is visible - this is accomplished by putting all element keys in different namespaces.

## Global Metadata

In order to track allocated Vector Sets and in progress cleanups, we keep a single `ContextMetadata` struct under the empty key in namespace 0.

This is loaded and cached on startup, and updated (both in memory and in Tsavorite) whenver a Vector Set is created or deleted.  Simple locking (on the `VectorManager` instance) is used to serialize these updates as they should be rare.

> [!IMPORTANT]
> Today `ContextMetadata` can track only 64 Vector Sets in some state of creation or cleanup.
> 
> The prartical limit is actually 31, because context must be &lt 256, divisible by 8, and not 0 (which is reserved).
>
> This limitation will be lifted eventually, perhaps after Store V2 lands.

## Indexes

The index key (represented by the [`Index`](TODO) struct) contains the following data:
 - `ulong Context` - used to derive namespaces, detailed below
 - `ulong IndexPtr` - a pointer to the DiskANN data structure, note this may be _dangling_ after a recovery or replication
 - `uint Dimensions` - the expected dimension of vectors in commands targetting the Vector Set, this is inferred based on the `VADD` that creates the Vector Set
 - `uint ReduceDims` - if a Vector Set was created with the `REDUCE` option that value, otherwise zero
   * > TODO: Today this ignored except for validation purposes, eventually DiskANN will use it.
 - `uint NumLinks` - the `M` used to create the Vector Set, or the default value of 16 if not specified
 - `uint BuildExplorationFactor` - the `EF` used to create the Vector Set, or the default value of 200 if not specified
 - `VectorQuantType QuantType` - the quantizier specified at creation time, or the default value of `Q8` if not specified
   * > [!NOTE]
     > We have an extension here, `XPREQ8` which is not 
   from Redis.
     > This is a quantizier for data sets which have already been 8-bit quantized or are otherwise naturally small byte vectors, and is extremely optimized for reducing reads during queries.
     > It forbids the `REDUCE` option and requires 4-byte element ids.
   * > [!IMPORTANT]
     > Today only `XPREQ` is actually implemented, eventually DiskANN will provide reasonable versions of all the Redis builtin quantizers.
 - `Guid ProcessInstanceId` - an identifier which is used distinguish the current process from previous instances, this is used after recovery or replication to detect if `IndexPtr` is dangling

The index key is in the main store alongside other binary values like strings, hyperloglogs, and so on.  It is distinguished for `WRONGTYPE` purposes with the `VectorSet` bit on `RecordInfo`.

> [!IMPORTANT]
> `RecordInfo.VectorSet` is checked in a few places to correctly produce `WRONGTYPE` responses, but we need more coverage for all commands.  Probably something akin to how ACLs required per-command tests.

> [!IMPORTANT]
> A generalization of the `VectorSet`-bit should be used for all data types, this can happen once we have Store V2.

## Elements

While the Vector Set API only concerns itself with top-level index keys, ids, vectors, and attributes; DiskANN has different storage needs.  To abstract around these needs a bit, we reserve a number of different "namespaces" for each Vector Set.

These namespaces are simple numbers, starting at the `Context` value stored in the `Index` struct - we currently reserve 8 namespaces per Vector Set.  What goes in which namespace is mostly hidden from Garnet, DiskANN indicates namespace (and index) to use with a modified `Context` passed to relevant callbacks.
> There are two cases where we "know" the namespace involved: attributes (+3) and full vectors (+0) which are used to implement the `WITHATTR` option and the `VEMB` command respectively.  These exceptions _may_ go away in the future, but don't have to.

Using namespaces prevents other commands from accessing keys which store element data.

To illustrate, this means that:
```
VADD vector-set-key VALUES 1 123 element-key
SET element-key string-value
```
Can work as expected.  Without namespacing, the `SET` would overwrite (or otherwiswe mangle) the element data of the Vector Set.

# Operations

We implement the [Redis Vector Set API](https://redis.io/docs/latest/commands/?group=vector_set):

Implemented commands:
 - [ ] VADD
 - [ ] VCARD
 - [x] VDIM
 - [x] VEMB
 - [ ] VGETATTR
 - [ ] VINFO
 - [ ] VISMEMBER
 - [ ] VLINKS
 - [ ] VRANDMEMBER
 - [x] VREM
 - [ ] VSETATTR
 - [x] VSIM

## Creation (via `VADD`)

[`VADD`](https://redis.io/docs/latest/commands/vadd/) implicitly creates a Vector Set when run on an empty key.

DiskANN index creation must be serialized, so this requires holding an exclusive lock ([more details on locking](#locking)) that covers just that key.  During `create_index` call to DiskANN, the read/write/delete callbacks provided may be invoked - accordingly creation is re-entrant and we cannot call `create_index` directly from any Tsavorite session functions.

> [!IMPORTANT]
> Today the `create_index` call _is_ trigger from session functions, but is moved onto the thread pool.  This is a hack to enable callbacks to function during index creation, and will be removed.

## Insertion (via `VADD`)

Once a Vector Set exists, insertions (which also use `VADD`) can proceed in parallel.

Every insertion begins with a Tsavorite read, to get the [`Index`](#indexes) metadata (for validation) and the pointer to DiskANN's index.  As a consequence, most `VADD` operations despite _semantically_ being writes are from Tsavorites perspective reads.  This has implications for replication, [which is discussed below](#replication).

To prevent the index from being deleted mid-insertion, we still hold a shared lock while calling DiskANN's `insert` function.  These locks are sharded for performance purposes, [which is discussed below](#locking).

## Removal (via `VREM`)

Removal works much the same as insertion, using shared locks so it can proceed in parallel.  The only meaningful difference is calling DiskANN's `remove` instead of `insert`.

> [!NOTE]
> Removing all elements from a Vector Set is not the same as deleting it.  While it is not possible to create an empty Vector Set with a single command, it is legal for one to exist after a `VREM`.

## Search (via `VSIM`)

Searching is a pure read operation, and so holds shared locks and proceeds in parallel like insertions and removals.

Great care is taken to avoid copying during `VSIM`.  In particular, values and element ids are passed directly from the receive buffer for all encodings except `VALUES`.  Callbacks from DiskANN to Garnet likewise take great care to avoid copying, and are detailed below.

## Element Data (via `VEMB` and `VGETATTR`)

This operations are handled purely on the Garnet side by first reading out the [`Index`](#indexes) structure, and then using the context value to look for data in the appropriate namespaces.

> [!NOTE]
> Strictly speaking we don't need the DiskANN index to access this data, but the current implementation does make sure the index is valid.

## Metadata (via `VDIM` and `VINFO`)

Metadata is handled purely on the Garnet side by reading out the [`Index`](#indexes) structure.

> [!NOTE]
> `VINFO` directly exposes Redis implementation details in addition to "normal" data.
> Because our implementation is different, we intentionally will not expose all the same information.
> To be concrete `max-level`, `vset-uid`, and `hnsw-max-node-uid` are not returned.

> [!IMPORTANT]
> We _may_ return more details of our own implementation.  What those are need to be documented, and why,
> when we implement `VINFO`.

## Deletion (via `DEL` and `UNLINK`)

`DEL` (and its equivalent `UNLINK`) is only non-Vector Set command to be routinely expected on a Vector Set key.  It is complicated by not knowing we're operating on a Vector Set until we get rather far into deletion.

We cope with this by _cancelling_ the Tsavorite delete operation once we have a `RecordInfo` with the `VectorSet`-bit set and a value which is not all zeros, detecting that cancellation in `MainStoreOps`, and shunting the delete attempt to `VectorManager`.

`VectorManager` performs the delete in five steps:
 - Acquire exclusive locks covering the Vector Set ([more locking details](#locking))
 - If the index was initialized in the current process (see recovery for more details), call DiskANN's `drop_index` function
 - Perform a write to zero out the index key in Tsavorite
 - Reperform the Tsavorite delete
 - Cleanup ancillary metadata and schedule element data for cleanup (more details below)

## FlushDB

`FLUSHDB` (and it's relative `FLUSHALL`) require special handling.

> [!IMPORTANT]
> This is not currently implemented.

# Locking

Vector Sets workloads require extreme parallelism, and so intricate locking protocols are required for both performance and correctness.

Concretely, there are 3 sorts of locks involved:
 - Tsavorite hashbucket locks
 - Vector Set sharded locks
   * > [!NOTE]
     > Today these are implemented as manual locks against the Object Store.
     > With Store V2 those locks go away, but before then we probably want to shift to something lighter weight anyway
 - `VectorManager` lock around `ContextMetadata`

## Tsavorite Locks

Whenver we read or write a key/value pair in the main store, we acquire locks in Tsavorite.  Importantly, we cannot start a new Tsavorite operation while still holding any lock - it is for this reason we must copy the index out before each operation.

> [!NOTE]
> Based on profiling, Tsavorite shared locks are a significant source of contention.  Even though reads will not block each other we still pay a cache coherency tax.  Accordingly, reducing the number of Tsavorite operations (even reads) can lead to significant performance gains.

> [!IMPORTANT]
> Some effort was spent early attempting to elide the initial index read in common cases.  This did not pay divdends on smaller clusters, but is worth exploring again on large SKUs.

## Vector Set Sharded Locks

As noted above, to prevent `DEL` from clobering in use Vector Sets and concurrent `VADD`s from calling `create_index` multiple times we have to hold locks based on the vector set key.  As every Vector Set operations starts by taking these locks, we have sharded them into `RoundUpToPowerOf2(Environment.ProcessorCount)` separate lock.  To derive many related keys from a single key, we mangle the low bits of a key's hash value - this is implemented in `VectorManager.PrepareReadLockHash`.

For operations we remain reads, we only acquire a single shared lock (based on the current processor number) to prevent destructive operations.

For operations which are always writes (like `DEL`) we acquire all shards in exclusively.

For operations which might be either (like `VADD`) we first acquire the usual single shared lock, then sweep the other shards (in order) acquiring them exclusively.  When we would normally acquire the shared lock exclusively in that sweep, we instead upgrade.  This logic is in `VectorManager.TryAcquireExclusiveLocks`.

> [!IMPORTANT]
> Today the locks are manual locks against the Object Store (but using the Main Store's hash functions).
>
> We will remove this eventually, as it won't work with Store V2.

## `VectorManager` Lock Around `ContextMetadata`

Whenever we need to allocate a new context or mark an old one for cleanup, we need to modify the cached `ContextMetadata` and write the new value to Tsavorite.  To simplify this, we take a simple `lock` around `VectorManager` while reparing a new `ContextMetadata`.

The `RMW` into Tsavorite still proceeds in parallel, outside of the lock, but a simple version counter in `ContextMetadata` allows us to keep only the latest version in the store.

> [!NOTE]
> Rapid creation or deletion of Vector Sets is expected to perform poorly due to this lock.
> This isn't a case we're very interested in right now, but if that changes this will need to be reworked.

# Replication

Replicating Vector Sets is tricky because of the unusual "writes are actually reads"-semantics of most operations.

## On Primaries

As noted above, inserts (via `VADD`) and deletes (via `VREM`) are reads from Tsavorite's perspective.  As a consequence, normal replication (which is triggered via `MainSessionFunctions.WriteLog(Delete|RMW|Upsert)`) does not happen on those operations.

To fix that, synthetic writes against related keys are made after an insert or remove.  These writes are against the same Vector Set key, but in namespace 0.  See `VectorManager.ReplicateVectorSetAdd` and `VectorManager.ReplicateVectorSetRemove` for details.

> [!IMPORTANT]
> There is a failure case here where we crash between the insert operation completing and the replication operation completing.
>
> This appears to simply extend a window that already existed between when a Tsavorite operation completed and an entry was written to the AOF.
> This needs to confirmed - if it is not the case, handling this failure needs to be figured out.

> [!IMPORTANT]
> This code assumes a Vector Set under the empty string is illegal.  That needs to be tested against Redis, and if it's not true we need to use
> one of the other reserved namespaces.

> [!NOTE]
> These syntetic writes might appear to double write volume, but that is not the case.  Actual inserts and deletes have extreme write applification (that is, each cause DiskANN to perform many writes against the Main Store), whereas the synthetic writes cause a single (no-op) modification to the Main Store plus an AOF entry.

> [!NOTE]
> The replication key is the same for all operations against the same Vector Set, this could be sharded which may improve performance.

## On Replicas

The synthetic writes on primary are intercepted on replicas and redirected to `VectorManager.HandleVectorSetAddReplication` and `VectorManager.HandleVectorSetRemoveReplication`, rather than being handled by directly by `AOFProcessor`.

For performance reasons, replicated `VADD`s are applied across many threads instead of serially.  This introduces a new source of non-determinism, since `VADD`s will occur in a different order than on the primary, but we believe this acceptable as Vector Sets are inherently non-deterministic.  While not _exactly_ the same Redis also permits a degree of non-determinism with its `CAS` option for `VADD`, so we're not diverging an incredible amount here.

While a `VADD` can proceed in parallel with respect to other `VADD`s, that is not the case for any other commands.  Accordingly, `AofProcessor` now calls `VectorManager.WaitForVectorOperationsToComplete()` before applying any other updates to maintain coherency.

# Cleanup

Deleting a Vector Set only drops the DiskANN index and removes the top-level keys (ie. the visible key and related hidden keys for replication).  This leaves all element, attribute, neighbor lists, etc. still in the Main Store.

To clean up the remaining data we record the deleted index context value in `ContextMetadata` and then schedule a full sweep of the Main Store looking for any keys under namespaces related to that context.  When we find those keys we delete them, see `VectorManager.RunCleanupTaskAsync()` and `VectorManager.PostDropCleanupFunctions` for details.

> [!NOTE]
> There isn't really an elegant way to avoid scanning the whole keyspace which can take awhile to free everything up.
>
> If we wanted to explore better options, we'd need to build something that can drop whole namespaces at once in Tsavorite.

> [!IMPORTANT]
> Today because we only have ~30 available Vector Set contexts, it is quite possible likely that deleting a Vector Set and then immediately creating a new one will fail if you're near the limit.
>
> This will be fixed once we have arbitrarily long namespaces in Store V2, and have updated `ContextMetadata` to track those.

# Recovery

Vector Sets represent a unique kind of recovery because most operations are mediated through DiskANN, for which we only ever have a pointer to a data structure.  This means that recovery needs to both deal with Vector Sets metadata AND the recreation of the DiskANN side of things.

## Vector Set Metadata

During startup we read any old `ContextMetadata` out of the Main Store, cache it, and resume any in progress cleanups.

## Vector Sets

While reading out [`Index`](#indexes) before any performing a DiskANN function call, we check the stored `ProcessInstanceId` against the (randomly generated) one in our `VectorManager` instance.  If they do not match, we know that the DiskANN `IndexPtr` is dangling and we need to recreate the index.

To recreate, we simply acquire exclusive locks (in the same way we would for `VADD` or `DEL`) and invoke `create_index` again.  From DiskANN's perspective, there's no difference between creating a new empty index and recreating an old one which has existing data.

This means we recreate indexes lazily after recovery.  Consequently the _first_ command (regardless of if it's a `VADD`, a `VSIM`, or whatever) against an index after recovery will be slower since it needs to do extra work, and will block other commands since it needs exclusive locking.

> [!NOTE]
> Today `ProcessInstanceId` is a `GUID`, which means we're paying for a 16-byte comparison on every command.
>
> This comparison is highly predictable, but we could try and remove the comparison (with caching, as mentioned for `Index` above).
> We could also make it cheaper by using a random `ulong` instead, but would need to do some math to convince ourselves collisions aren't possible in realistic scenarios.