---
id: vector-sets
sidebar_label: Vector Sets
title: Vector Sets
---

# Overview

Garnet has partial support for Vector Sets, implemented on top of the [DiskANN project](https://www.nuget.org/packages/diskann-garnet/).

This data type is very strange when compared to others Garnet supports.

> [!IMPORTANT]
> The DiskANN link needs to be updated once OSS'd.

# Design

Vector Sets are a combination of one "index" key, which stores metadata and a pointer to the DiskANN data structure, and many "element" keys, which store vectors/quantized vectors/attributes/etc.  All Vector Set keys are kept in the main store, but only the index key is visible - this is accomplished by putting all element keys in different namespaces.

## Global Metadata

In order to track allocated Vector Sets (and their respective hash slots), in progress cleanups, in progress migrations - we keep a single `ContextMetadata` struct under the empty key in namespace 0.

This is loaded and cached on startup, and updated (both in memory and in Tsavorite) whenever a Vector Set is created or deleted.  Simple locking (on the `VectorManager` instance) is used to serialize these updates as they should be rare.

> [!IMPORTANT]
> Today `ContextMetadata` can track only 64 Vector Sets in some state of creation or cleanup.
> 
> The practical limit is actually 31, because context must be &lt; 256, divisible by 8, and not 0 (which is reserved).
>
> This limitation will be lifted eventually, perhaps after Store V2 lands.

## Indexes

The index key (represented by the `Index` struct) contains the following data:
 - `ulong Context` - used to derive namespaces, detailed below
 - `ulong IndexPtr` - a pointer to the DiskANN data structure, note this may be _dangling_ after [recovery](#recovery) or [replication](#replication)
 - `uint Dimensions` - the expected dimension of vectors in commands targeting the Vector Set, this is inferred based on the `VADD` that creates the Vector Set
 - `uint ReduceDims` - if a Vector Set was created with the `REDUCE` option that value, otherwise zero
   * > [!NOTE]
     > Today this ignored except for validation purposes, eventually DiskANN will use it.
 - `uint NumLinks` - the `M` used to create the Vector Set, or the default value of 16 if not specified
 - `uint BuildExplorationFactor` - the `EF` used to create the Vector Set, or the default value of 200 if not specified
 - `VectorQuantType QuantType` - the quantizier specified at creation time, or the default value of `Q8` if not specified
   * > [!NOTE]
     > We have an extension here, `XPREQ8` which is not from Redis.
     > This is a quantizier for data sets which have already been 8-bit quantized or are otherwise naturally small byte vectors, and is extremely optimized for reducing reads during queries.
     > It forbids the `REDUCE` option and requires 4-byte element ids.
   * > [!IMPORTANT]
     > Today only `XPREQ` is actually implemented, eventually DiskANN will provide reasonable versions of all the Redis builtin quantizers.
 - `Guid ProcessInstanceId` - an identifier which is used distinguish the current process from previous instances, this is used after [recovery](#recovery) or [replication](#replication) to detect if `IndexPtr` is dangling

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
Can work as expected.  Without namespacing, the `SET` would overwrite (or otherwise mangle) the element data of the Vector Set.

# Operations

We implement the [Redis Vector Set API](https://redis.io/docs/latest/commands/?group=vector_set):

Implemented commands:
 - [x] VADD
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

DiskANN index creation must be serialized, so this requires holding an exclusive lock ([more details on locking](#locking)) that covers just that key.  During the `create_index` call to DiskANN the read/write/delete callbacks provided may be invoked - accordingly creation is re-entrant and we cannot call `create_index` directly from any Tsavorite session functions.

## Insertion (via `VADD`)

Once a Vector Set exists, insertions (which also use `VADD`) can proceed in parallel.

Every insertion begins with a Tsavorite read, to get the [`Index`](#indexes) metadata (for validation) and the pointer to DiskANN's index.  As a consequence, most `VADD` operations despite _semantically_ being writes are, from Tsavorite's perspective, reads.  This has implications for replication, [which is discussed below](#replication).

To prevent the index from being deleted mid-insertion, we hold a shared lock while calling DiskANN's `insert` function.  These locks are sharded for performance purposes, [which is discussed below](#locking).

## Removal (via `VREM`)

Removal works much the same as insertion, using shared locks so it can proceed in parallel.  The only meaningful difference is calling DiskANN's `remove` instead of `insert`.

> [!NOTE]
> Removing all elements from a Vector Set is not the same as deleting it.  While it is not possible to create an empty Vector Set with a single command, it is legal for one to exist after a `VREM`.

## Search (via `VSIM`)

Searching is a pure read operation, and so holds shared locks and proceeds in parallel like insertions and removals.

Great care is taken to avoid copying during `VSIM`.  In particular, values and element ids are passed directly from the receive buffer for all encodings except `VALUES`.  Callbacks from DiskANN to Garnet likewise take great care to avoid copying, and are [detailed below](#diskann-integration).

## Element Data (via `VEMB` and `VGETATTR`)

These operations are handled purely on the Garnet side by first reading out the [`Index`](#indexes) structure, and then using the context value to look for data in the appropriate namespaces.

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
 - Add the key to an `InProgressDeletes` key (namespace 0, key=0x01)
 - If the index was initialized in the current process ([see recovery for more details](#recovery)), call DiskANN's `drop_index` function
 - Perform a write to zero out the index key in Tsavorite
 - Reattempt the Tsavorite delete
 - Cleanup ancillary metadata and schedule element data for cleanup ([more details below](#cleanup))
 - Remove the key from the `InProgressDeletes` key

The `InProgressDeletes` key is necessary to recover from interrupted deletes.  At process start, `VectorManager` consults the `InProgressDeletes` key and completes any deletes that got as far as zero-ing out the index key.

> [!IMPORTANT] Interrupted deletes are expected only during process exits, but if they occur without the process exiting they will leave the Vector Set in a partially deleted state.  We detect that and return a new `GarnetStatus.BADSTATE` which returns an explanatory error.
>
> We _could_ resume the delete on `GarnetStatus.BADSTATE`, but like `GarnetStatus.WRONGTYPE` that needs to be done for _all_ commands not just Vector Set commands.  This work is likewise left for the future.

## FlushDB

`FLUSHDB` (and it's relative `FLUSHALL`) require special handling.

> [!IMPORTANT]
> This is not currently implemented.

# Locking

Vector Sets workloads require extreme parallelism, and so intricate locking protocols are required for both performance and correctness.

Concretely, there are 3 sorts of locks involved:
 - Tsavorite hashbucket locks
 - A `ReadOptimizedLock` instance
 - `VectorManager` lock around `ContextMetadata`

## Tsavorite Locks

Whenever we read or write a key/value pair in the main store, we acquire locks in Tsavorite.  Importantly, we cannot start a new Tsavorite operation while still holding these locks - we must copy the index out before each operation so Garnet can use the read/write/delete callbacks.

> [!NOTE]
> Based on profiling, Tsavorite shared locks are a significant source of contention.  Even though reads will not block each other we still pay a cache coherency tax.  Accordingly, reducing the number of Tsavorite operations (even reads) can lead to significant performance gains.

> [!IMPORTANT]
> Some effort was spent early attempting to elide the initial index read in common cases.  This did not pay dividends on smaller clusters, but is worth exploring again on large SKUs.

## `ReadOptimizedLock`

As noted above, to prevent `DEL` from clobbering in use Vector Sets and concurrent `VADD`s from calling `create_index` multiple times we have to hold locks based on the Vector Set key.  As every Vector Set operations starts by taking these locks, we have sharded them into separate locks.  To derive many related keys from a single key, we mangle the low bits of a key's hash value - this is implemented in the new (but not bound to Vector Sets) type `ReadOptimizedLock`.

For operations which remain reads, we only acquire a single shared lock (based on the current thread) to prevent destructive operations.

For operations which are always writes (like `DEL`) we acquire all sharded locks in exclusive mode.

For operations which might be either (like `VADD`) we first acquire the usual single sharded lock (in shared mode), then promote to an exclusive lock if needed.

## `VectorManager` Lock Around `ContextMetadata`

Whenever we need to allocate a new context or mark an old one for cleanup, we need to modify the cached `ContextMetadata` and write the new value to Tsavorite.  To simplify this, we take a plain `lock` around `VectorManager` while preparing a new `ContextMetadata`.

The `RMW` into Tsavorite still proceeds in parallel, outside of the lock, but a version counter in `ContextMetadata` allows us to keep only the latest version in the store.

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
> This code assumes a Vector Set under the empty string is illegal.  That does not seem to be true with Redis - so we will need to move these keys elsewhere.  For now, we just forbid the empty key for VADDs.

> [!NOTE]
> These synthetic writes might appear to double write volume, but that is not the case.  Actual inserts and deletes have extreme write amplification (that is, each cause DiskANN to perform many writes against the Main Store), whereas the synthetic writes cause a single (no-op) modification to the Main Store plus an AOF entry.

> [!NOTE]
> The replication key is the same for all operations against the same Vector Set, this could be sharded which may improve performance.

## On Replicas

The synthetic writes on primary are intercepted on replicas and redirected to `VectorManager.HandleVectorSetAddReplication` and `VectorManager.HandleVectorSetRemoveReplication`, rather than being handled directly by `AOFProcessor`.

For performance reasons, replicated `VADD`s are applied across many threads instead of serially.  This introduces a new source of non-determinism, since `VADD`s will occur in a different order than on the primary, but this is acceptable as Vector Sets are inherently non-deterministic.  While not _exactly_ the same Redis also permits a degree of non-determinism with its `CAS` option for `VADD`, so we're not diverging an incredible amount here.

While a `VADD` can proceed in parallel with respect to other `VADD`s, that is not the case for any other commands.  Accordingly, `AofProcessor` now calls `VectorManager.WaitForVectorOperationsToComplete()` before applying any other updates to maintain coherency.

## Migration

Migrating a Vector Set between two primaries (either as part of a `MIGRATE ... KEYS` or migration of a whole hash slot) is complicated by storing element data in namespaces.

Namespaces (intentionally) do not participate in hash slots or clustering, and are a node specific concept.  This means that migration must also update the namespaces of elements as they are migrated.

At a high level, migration between the originating primary a destination primary behaves as follows:
 1. Once target slots transition to `MIGRATING`...
    * An addition to `ClusterSession.SingleKeySlotVerify` causes all WRITE Vector Set commands to pause once a slot is `MIGRATING` or `IMPORTING` - this is necessary because we cannot block based on the key as Vector Sets are composed of many key-value pairs across several namespaces
 2. `VectorManager` on the originating primary enumerates all _namespaces_ and Vector Sets that are covered by those slots
 3. The originating primary contacts the destination primary and reserves enough new Vector Set contexts to handled those found in step 2
    * These Vector Sets are "in use" but also in a migrating state in `ContextMetadata`
 4. During the scan of main store in `MigrateOperation` any keys found with namespaces found in step 2 are migrated, but their namespace is updated prior to transmission to the appropriate new namespaces reserved in step 3
    * Unlike with normal keys, we do not _delete_ the keys in namespaces as we enumerate them
    * Also unlike with normal keys, we synthesize a write on the _destination_ (using a special arg and `VADD`) so replicas of the destination also get these writes
 5. Once all namespace keys are migrated, we migrate the Vector Set index keys, but mutate their values to have the appropriate context reserved in step 3
    * As in 4, we synthesize a write on the _destination_ to tell any replicas to also create the index key
 6. When the target slots transition back to `STABLE`, we do a delete of the Vector Set index keys, drop the DiskANN indexes, and schedule the original contexts for cleanup on the originating primary
    * Unlike in 4 & 5, we do no synthetic writes here.  The normal replication of `DEL` will cleanup replicas of the originating primary.

 `KEYS` migrations differ only in the slot discovery being omitted.  We still have to determine the migrating namespaces, reserve new ones on the destination primary, and schedule cleanup only once migration is completed.  This does mean that, if any of the keys being migrated is a Vector Set, `MIGRATE ... KEYS` now causes a scan of the main store.

> [!NOTE]
> This approach prevents the Vector Set from being visible when it is partially migrated, which has the desirable property of not returning weird results during a migration.

> [!NOTE]
> While we explicitly reserve contexts on primaries, they are implicit on replicas.  This is because a replica should always come up with the same determination of reserved contexts.
>
> To keep that determinism, the synthetic `VADD`s introduced by migration are not executed in parallel.

# Cleanup

Deleting a Vector Set only drops the DiskANN index and removes the top-level keys (ie. the visible key and related hidden keys for replication).  This leaves all element, attribute, neighbor lists, etc. still in the Main Store.

To clean up the remaining data we record the deleted index context value in `ContextMetadata` and then schedule a full sweep of the Main Store looking for any keys under namespaces related to that context.  When we find those keys we delete them, see `VectorManager.RunCleanupTaskAsync()` and `VectorManager.PostDropCleanupFunctions` for details.

> [!NOTE]
> There isn't really an elegant way to avoid scanning the whole keyspace which can take awhile to free everything up.
>
> If we wanted to explore better options, we'd need to build something that can drop whole namespaces at once in Tsavorite.

> [!IMPORTANT]
> Today because we only have ~30 available Vector Set contexts, it is quite likely that deleting a Vector Set and then immediately creating a new one will fail if you're near the limit.
>
> This will be fixed once we have arbitrarily long namespaces in Store V2, and have updated `ContextMetadata` to track those.

# Recovery

Vector Sets represent a unique kind of recovery because most operations are mediated through DiskANN, for which we only ever have a pointer to a data structure.  This means that recovery needs to both deal with Vector Sets metadata AND the recreation of the DiskANN side of things.

## Vector Set Metadata

During startup we read any old `ContextMetadata` out of the Main Store, cache it, and resume any in progress cleanups.

## Vector Sets

While reading out [`Index`](#indexes) before performing a DiskANN function call, we check the stored `ProcessInstanceId` against the (randomly generated) one in our `VectorManager` instance.  If they do not match, we know that the DiskANN `IndexPtr` is dangling and we need to recreate the index.

To recreate, we acquire exclusive locks (in the same way we would for `VADD` or `DEL`) and invoke `create_index` again.  From DiskANN's perspective, there's no difference between creating a new empty index and recreating an old one which has existing data.

This means we recreate indexes lazily after recovery.  Consequently the _first_ command (regardless of if it's a `VADD`, a `VSIM`, or whatever) against an index after recovery will be slower since it needs to do extra work, and will block other commands since it needs exclusive locking.

> [!NOTE]
> Today `ProcessInstanceId` is a `GUID`, which means we're paying for a 16-byte comparison on every command.
>
> This comparison is highly predictable, but we could try and remove the comparison (with caching, as mentioned for `Index` above).
> We could also make it cheaper by using a random `ulong` instead, but would need to do some math to convince ourselves collisions aren't possible in realistic scenarios.

# DiskANN Integration

Almost all of how Vector Sets actually function is handled by DiskANN.  Garnet just embeds it, translates between RESP commands and DiskANN functions, and manages storage.

In order for DiskANN to access and store data in Garnet, we provide a set of callbacks.  All callbacks are `[UnmanagedCallersOnly]` and converted to function pointers before they are passed to Garnet.

All callbacks take a `ulong context` parameter which identifies the Vector Set involved (the high 61-bits of the context) and the associated namespace (the low 3-bits of the context).  On the Garnet side, the whole `context` is effectively a namespace, but from DiskANN's perspective the top 61-bits are an opaque identifier.

> [!IMPORTANT]
> As noted elsewhere, we only have a byte's worth of namespaces today - so although `context` could handle quintillions of Vector Sets, today we're limited to just 31.
>
> This restriction will go away with Store V2, but we expect "lower" Vector Sets to out perform "higher" ones due to the need for intermediate data copies with longer namespaces.

## Read Callback

The most complicated of our callbacks, the signature is:
```csharp
void ReadCallbackUnmanaged(ulong context, uint numKeys, nint keysData, nuint keysLength, nint dataCallback, nint dataCallbackContext)
```

`context` identifies which Vector Set is being operated on AND the associated namespace, `numKeys` tells us how many keys have been encoded into `keysData`, `keysData` and `keysLength` define a `Span<byte>` of length prefixied keys, `dataCallback` is a `delegate* unmanaged[Cdecl, SuppressGCTransition]<int, nint, nint, nuint, void>` used to push found keys back into DiskANN, and `dataCallbackContext` is passed back unaltered to `dataCallback`.

In the `Span<byte>` defined by `keysData` and `keysLength` the keys are length prefixed with a 4-byte little endian `int`.  This is necessary to support variable length element ids, but also gives us some scratch space to store a namespace when we convert these to `SpanByte`s.  This mangling is done as part of the `IReadArgBatch` implementation we use to read keys from Tsavorite.

> [!NOTE]
> Once variable sized namespaces are supported we'll have to handle the case where the namespace can't fit in 4 bytes.  However, we expect that to be rare (4-bytes would give us ~53,000,000 Vector Sets) and the performance benefits of _not_ copying during querying are very large.

As we find keys, we invoke `dataCallback(index, dataCallbackContext, keyPointer, keyLength)`.  If a key is not found, its index is simply skipped.  The benefits of this is that we don't copy data out of the Tsavorite log as part of reads, DiskANN is able to do distance calculations and traversal over in-place data.

> [!NOTE]
> Each invocation of `dataCallback` is a managed -&gt; native transition, which can add up very quickly.  We've reduced that as much as possible with function points and `SuppressGCTransition`, but that comes with risks.
>
> In particular if DiskANN raises an error or blocks in the `dataCallback` expect very bad things to happen, up to the runtime corrupting itself.  Great care must be taken to keep the DiskANN side of this call cheap and reliable.

> [!IMPORTANT]
> Tsavorite has been extended with a `ContextReadWithPrefetch` method to accommodate this pattern, which also employs prefetching when we have batches of keys to lookup.
>
> Additionally, some experimentation to figure out good prefetch sizes (and if [AMAC](https://dl.acm.org/doi/10.14778/2856318.2856321) is useful) based on hardware is merited.  Right now we've chosen 12 based on testing with some 96-core Intel machines, but that is unlikely to be correct in all interesting circumstances.

## Write Callback

A simpler callback, the signature is:
```csharp
byte WriteCallbackUnmanaged(ulong context, nint keyData, nuint keyLength, nint writeData, nuint writeLength)
```

`context` identifies which Vector Set is being operated on AND the associated namespace,  `keyData` and `keyLength` represent a `Span<byte>` of the key to write, and `writeData` and `writeLength` represent a `Span<byte>` of the value to write.

DiskANN guarantees an extra 4-bytes BEFORE `keyData` that we can safely modify.  This is used to avoid copying the key value when we add a namespace to the `SpanByte` before invoking Tsavorite's `Upsert`.

This callback returns 1 if successful, and 0 otherwise.

## Delete Callback

Another simple callback, the signature is:
```csharp
byte DeleteCallbackUnmanaged(ulong context, nint keyData, nuint keyLength)
```

`context` identifies which Vector Set is being operated on AND the associated namespace,  and `keyData` and `keyLength` represent a `Span<byte>` of the key to delete.

As with the write callback, DiskANN guarantees an extra 4-bytes BEFORE `keyData` that we use to store a namespace, and thus avoid copying the key value before invoking Tsavorite's `Delete`.

This callback returns 1 if the key was found and removed, and 0 otherwise.

## Read Modify Write Callback

A more complicated callback, the signature is:
```csharp
byte ReadModifyWriteCallbackUnmanaged(ulong context, nint keyData, nuint keyLength, nuint writeLength, nint dataCallback, nint dataCallbackContext)
```

`context` identifies which Vector Set is being operated on AND the associated namespace,  and `keyData` and `keyLength` represent a `Span<byte>` of the key to create, read, or update.

`writeLength` is the desired number of bytes, this is only used used if we are creating a new key-value pair.

As with the write and delete callbacks, DiskANN guarantees an extra 4-bytes BEFORE `keyData` that we use to store a namespace, and thus avoid copying the key value before invoking Tsavorite's `RMW`.

After we allocate a new key-value pair or find an existing one, `dataCallback(nint dataCallbackContext, nint dataPointer, nuint dataLength)` is called.  Changes made to data in this callback are persisted.  This needs to be _fast_ to prevent gumming up Tsavorite, as we are under epoch protection.

Newly allocated values are guaranteed to be all zeros.

The callback returns 1 if the key-value pair was found or created, and 0 if some error occurred.

## DiskANN Functions

Garnet calls into the following DiskANN functions:

 - [x] `nint create_index(ulong context, uint dimensions, uint reduceDims, VectorQuantType quantType, uint buildExplorationFactor, uint numLinks, nint readCallback, nint writeCallback, nint deleteCallback, nint readModifyWriteCallback)`
 - [x] `void drop_index(ulong context, nint index)`
 - [x] `byte insert(ulong context, nint index, nint id_data, nuint id_len, VectorValueType vector_value_type, nint vector_data, nuint vector_len, nint attribute_data, nuint attribute_len)`
 - [x] `byte remove(ulong context, nint index, nint id_data, nuint id_len)`
 - [ ] `byte set_attribute(ulong context, nint index, nint id_data, nuint id_len, nint attribute_data, nuint attribute_len)`
 - [x] `int search_vector(ulong context, nint index, VectorValueType vector_value_type, nint vector_data, nuint vector_len, float delta, int search_exploration_factor, nint filter_data, nuint filter_len, nuint max_filtering_effort, nint output_ids, nuint output_ids_len, nint output_distances, nuint output_distances_len, nint continuation)`
 - [x] `int search_element(ulong context, nint index, nint id_data, nuint id_len, float delta, int search_exploration_factor, nint filter_data, nuint filter_len, nuint max_filtering_effort, nint output_ids, nuint output_ids_len, nint output_distances, nuint output_distances_len, nint continuation)`
 - [ ] `int continue_search(ulong context, nint index, nint continuation, nint output_ids, nuint output_ids_len, nint output_distances, nuint output_distances_len, nint new_continuation)`
 - [ ] `ulong card(ulong context, nint index)`
 - [ ] `byte check_internal_id_valid(ulong context, nint index, nint internal_id, nuint internal_id_len)`

 Some non-obvious subtleties:
  - The number of results _requested_ from `search_vector` and `search_element` is indicated by `output_distances_len`
  - `output_distances_len` is the number of _floats_ in `output_distances`, not bytes
  - When inserting, if `vector_value_type == FP32` then `vector_len` is the number of _floats_ in `vector_data`, otherwise it is the number of bytes
  - `byte` returning functions are effectively returning booleans, `0 == false` and `1 == true`
  - `index` is always a pointer created by DiskANN and returned from `create_index`
  - `context` is always the `Context` value created by Garnet and stored in [`Index`](#indexes) for a Vector Set, this implies it is always a non-0 multiple of 8
  - `search_vector`, `search_element`, and `continue_search` all return the number of ids written into `output_ids`, and if there are more values to return they set the `nint` _pointed to by_ `continuation` or `new_continuation`

> [!IMPORTANT]
> These p/invoke definitions are all a little rough and should be cleaned up.
>
> They were defined very loosely to ease getting the .NET &lt;-&gt; Rust interface working quickly.