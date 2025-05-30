---
id: locking
sidebar_label: Locking
title: Locking
---

# Locking

Locking is always on in Tsavorite. It is done by locking the HashIndex bucket. There are two modes of locking; these are automatic based on what sessions the caller uses:
  - **Manual**: In this mode, the Garnet processing layer calls a `Lock` method on `TransactionalContext` or `TransactionalUnsafeContext` (hereafter referred to collectively as `Transactional*Context`) at the beginning of a transaction, passing an ordered array of keys, and must call `Unlock` when the transaction is complete. Tsavorite does not try to lock during individual operations on these session contexts.
  - **Transient**: Tsavorite acquires and releases locks for individual keys for the duration of a data operation: Upsert, RMW, Read, or Delete. Collectively, these are referred to here as `InternalRUMD` for the internal methods that implement them: Read, Upsert, rMw, and Delete.

All locks are obtained via spinning on `Interlocked.CompareExchange` and `Thread.Yield()` and have limited spin count, to avoid deadlocks; if they fail to acquire the desired lock in this time, the operation retries.

As noted above, manual locking is done by obtaining the `Transactional*Context` instance from a `ClientSession`. There are currently 4 `*Context` implementations; all are `struct` for inlining. All `*Context` are obtained as properties on the `ClientSession` named for the type (e.g. `clientSession.TransactionalContext`). The characteristics of each `*Context` are:
- **`BasicContext`**: This is exactly the same as `ClientSession`, internally calling directly through to `ClientSession`'s methods and reusing `ClientSession`'s `TsavoriteSession`. It provides safe epoch management (acquiring and releasing the epoch on each call) and Transient locking.
- **`UnsafeContext : IUnsafeContext`**: This provides Transient locking, but rather than safe epoch management handled per-operation by Tsavorite, this supports "unsafe" manual epoch management controlled by the client via `BeginUnsafe()` and `EndUnsafe()`; it is the client's responsibility to make these calls correctly. `UnsafeContext` API methods call the internal ContextRead etc. methods without doing the Resume and Suspend (within try/finally) of epoch protection as is done by the "Safe" API methods.
- **`TransactionalContext : ITransactionalContext`**: This provides safe epoch management, but rather than Transient locking, this requires Manual locks via `BeginTransactional` and `EndTransactional`. This requirement ensures that all locks are acquired before any methods accessing those keys are called.
- **`TransactionalUnsafeContext : ITransactionalContext, IUnsafeContext`**: This combines manual epoch management and manual locking, exposing both sets of methods.

In addition to the `Lock` methods, Tsavorite supports:
- `TryLock`: Accepts an array of keys and returns true if all locks were acquired, else false (and any locks that were acquired are released)
- `TryPromoteLock`: Accepts a single key and returns true if the key's lock could be promoted from Read to Exclusive.

## Considerations

All manual locking of keys must lock the keys in a deterministic order, and unlock in the reverse order, to avoid deadlocks.

Lock spinning is limited in order to avoid deadlocks such as the following:
  - `Transactional*Context` LC1 exclusively locks k1
  - `BasicContext` BC1 tries to acquire an exclusive Transient lock on k1, and spins while holding the epoch
  - LC1 does an RMW on k1 resulting in a CopyUpdate; this does a BlockAllocate that finds it must flush pages from the head of the log in order to make room at the tail. 
    - LC1 therefore calls BumpCurrentEpoch(... OnPagesClosed)
    - Because BC1 holds the epoch, the OnPagesClosed() call is never drained, so we have deadlock
By ensuring that locks are limited in spins, we force one or both of the above sessions to release any locks it has already aquired and return up the callstack to retry the operation via RETRY_LATER (which refreshes the epoch, allowing other operations such as the OnPagesClosed() mentioned above to complete).

Transient locks are never held across pending I/O or other Wait operations. All the data operations' low-level implementors (`InternalRead`, `InternalUpsert`, `InternalRMW`, and `InternalDelete`--collectively known as `InternalRUMD`) release these locks when the call is exited; if the operations must be retried, the locks are reacquired as part of the normal operation there.

## Example
Here is an example of the above two use cases, condensed from the unit tests in `TransactionalUnsafeContextTests.cs`:

```cs
    var luContext = session.GetTransactionalUnsafeContext();
    luContext.BeginUnsafe();
    luContext.BeginTransaction();

    var keys = new[]
    {
        new FixedLengthTransactionalKeyStruct(readKey24, LockType.Shared, luContext),      // Source, shared
        new FixedLengthTransactionalKeyStruct(readKey51, LockType.Shared, luContext),      // Source, shared
        new FixedLengthTransactionalKeyStruct(resultKey, LockType.Exclusive, luContext),   // Destination, exclusive
    };

    // Sort the keys to guard against deadlock
    luContext.SortKeyHashes(keys);

    Assert.IsTrue(luContext.TryLock(keys));

    luContext.Read(key24, out var value24);
    luContext.Read(key51, out var value51);
    luContext.Upsert(resultKey, value24 + value51);

    luContext.Unlock(keys);

    luContext.EndTransaction();
    luContext.EndUnsafe();
```

## Internal Design

This section covers the internal design and implementation of Tsavorite's locking.

### Operation Data Structures
There are a number of variables necessary to track the main hash table entry information, the 'source' record as defined above, and other stack-based data relevant to the operation. These variables are placed within structs that live on the stack at the `InternalRUMD` level.

#### HashEntryInfo
This is used for hash-chain traversal and CAS updates. It consists primarily of:
- The key's hash code and associated tag.
- a stable copy of the `HashBucketEntry` at the time the `HashEntryInfo` was populated.
  - This has both `Address` (which may or may not include the readcache bit) and `AbsoluteAddress` (`Address` stripped of the readcache bit) accessors.
- a pointer (in the unsafe "C/C++ *" sense) to the live `HashBucketEntry` that may be updated by other sessions as our current operation proceeds.
  - As with the stable copy, this has two address accessors: `CurrentAddress` (which may or may not include the readcache bit) and `AbsoluteCurrentAddress` (`CurrentAddress` stripped of the readcache bit).
- A method to update the stable copy of the `HashBucketEntry` with the current information from the 'live' pointer.

#### RecordSource
This is implemented as `RecordSource<TKey, TValue>` and carries the information identifying the source record and lock information for that record:
- Whether there is an in-memory source record, and if so: 
  - its logical and physical addresses
  - whether it is in the readcache or main log
  - whether it is locked
- The latest logical address of this key hash in the main log. If there are no readcache records, then this is the same as the `HashEntryInfo` Address; 
- If there are readcache records in the chain, then `RecordSource` contains the lowest readcache logical and physical addresses. These are used for 'splicing' a new main-log record into the gap between the readcache records and the main log recoreds; see [ReadCache](#readcache) below.
- The log (readcache or hlog) in which the source record, if any, resides. This is hlog unless there is a source readcache record.
- Whether a LockTable lock was acquired. This is exclusive with in-memory locks; only one should be set.

#### OperationStackContext
This contains all information on the stack that is necessary for the operation, making parameter lists much smaller. It contains:
- The `HashEntryInfo`  and `RecordSource<TKey, TValue>` for this operation. These are generally used together, and in some situations, such as when hlog.HeadAddress has changed due to an epoch refresh, `RecordSource<TKey, TValue>` is reinitialized from `HashEntryInfo` during the operation.
- the logical address of a new record created for the operation. This is passed back up the chain so the try/finally can set it invalid and non-tentative on exceptions, without having to pay the cost of a nested try/finally in the `CreateNewRecord*` method.

### Lock Locations

This section discusses where locks are actually stored.

#### LockTable
For HashTable locking, the key is hashed to its code which is modulo'd to find the hash table bucket index. This bucket consists of a vector of 8 `HashBucketEntries` (cache-aligned, as each HashBucketEntry contains only a 'long'). The entries are organized by 'tags', which are the upper 14 bits of the hashcode. The 8th `HashBucketEntry` is used as a linked list to an overflow bucket; its Address property is a separate allocation that points to an overflow bucket. Thus, a bucket contains 7 entries and a pointer to another bucket if more than 7 tags have been found.

Following is a simplified overfire of the 'tag' logic, using `FindOrCreateTag` as an example:
- Iterate the first 7 entries of all buckets, including overflow. If the tag is found, use that entry.
- Otherwise, if an empty entry was found, assign this tag to the first empty entry and use that entry.
- otherwise, add an overflow bucket and repeat the first two steps in that bucket.
(`FindTag` is simpler, since it does not have to add an entry; it returns false if the tag is not found).

For the first bucket, the tag bits of its overflow entry are used for locking; see the `HashBucket` class for detailed comments. There are 16 tag bits; for locking, 15 bits are used for shared (Read) locking and one bit for exclusive locking. 

Thus, a key is locked indirectly, by having its `HashBucket` locked. This may result in multiple keys being locked (all keys that hash to that bucket) rather than just the required key.

#### RecordInfo
Some relevant `RecordInfo` bits:
  - **Sealed**: A record marked Sealed has been superseded by an update (and the record may be in the Revivification FreeList). Sealing is necessary because otherwise one thread could do an RCU (Read, Copy, Update) with a value too large to be an IPU (In-Place Update), while at the same time another thread could do an IPU with a value small enough to fit. A thread encountering a Sealed record should immediately return RETRY_LATER (it must be RETRY_LATER instead of RETRY_NOW, because the thread owning the Seal must do another operation, such as an Insert to tail, to bring things into a consistent state; this operation may require epoch refresh). 
    - Sealing is done via `RecordInfo.Seal`. This is only done when the `LockTable` entry is already exclusively locked, so `Seal()` does a simple bitwise operation.
  - **Invalid**: This indicates that the record is to be skipped, using its `.PreviousAddress` to move along the chain, rather than restarted. This "skipping" semantic is primarily relevant to the readcache; we should not have invalid records in the main log's hash chain. Indeed, any main-log record that is not in the hash chain *must* be marked Invalid.
    - In the `ReadCache` we do not Seal records; the semantics of Seal are that the operation is restarted when a Sealed record is found. For non-`readcache` records, this causes the execution to restart at the tail of the main-log records. However, `readcache` records are at the beginning of the hash chain, *before* the main-log records; thus, restarting would start again at the hash bucket, traverse the readcache records, and hit the Sealed record again, ad infinitum. 
    - Thus, we instead set `readcache` records to Invalid when they are no longer current; this allows the traversal to continue until the readcache chain links to the first main-log record.
    
    Additionally, records may be elided (removed) from the tag chain for one of the following reasons:
      - The record was deleted
      - The record was a "source" for RCU
    
    In these cases, if the record's `PreviousAddress` points below `hlog.BeginAddress`, we remove the record so we do not have to waste an IO to determine that it is a superseded record.
      - Such elided records are put into the FreeList if it is enabled for Revivification.

### Locking Flow

We obtain the key hash at the start of the operation, so we lock its bucket if we are not in a `Transactional*Context` (if we are, we later Assert that the key is already locked).

Following this, the requested operation is performed within a try/finally block whose 'finally' releases the lock.

For RCU-like operations such as RMW or Upsert of an in-memory (including in-ReadCache) record, the source record is sealed (and may invalidate it to allow it to be freelisted for Revivification).

Similar locking logic applies to the pending IO completion routines: `ContinuePendingRead`, `ContinuePendingRMW`, and `ContinuePendingConditionalCopyToTail`.

### ReadCache
The readcache is a cache for records that are read from disk. In the case of record that become 'hot' (read multiple times) this saves multiple IOs. It is of fixed size determined at TsavoriteKV startup, and has no on-disk component; records in the ReadCache are evicted from the head without writing to disk when enough new records are added to the tail to exceed the memory-size specification.

When the `ReadCache` is enabled, records from the `ReadCache` are inserted into the tag chain starting at the `HashBucketEntry` (these records are identified as `ReadCache` by a combination of `TsavoriteKV.UseReadCache` being set *and* the ReadCache bit in the `RecordInfo` is set). All `ReadCache` records come before any main log record. So (using r#### to indicate a `ReadCache` record and m#### to indicate a main log record):
  - When there are no `ReadCache` entries in a hash chain, it looks like: `HashTable` -> m4000 -> m3000 -> m...
  - When there are `ReadCache` entries in a hash chain, it looks like: `HashTable` -> r8000 -> r7000 -> m4000 -> m3000 -> m...

As a terminology note, the sub-chain of r#### records is referred to as the `ReadCache` prefix chain of that hash chain.

If the key is found in the readcache, then the `RecordSource<TKey, TValue>.Log` is set to the readcache, and the logicalAddress is set to the readcache record's logicalAddress (which has the ReadCache bit). If the operation is a successful update (which will always be an RCU; readcache records are never themselves updated), the following steps happen:
- The new version of the record is "spliced in" to the tag chain after the readcache prefix
- The readcache "source" record is invalidated.

Using the above example and assuming an update of r8000, the resulting chain would be:
- `HashTable` -> r8000 (invalid) -> r7000 -> mxxxx (new) -> m4000 -> m3000 -> m...
- In this example, note that the record address spaces are totally different between the main log and readcache; "xxxx" is used as the "new address" to symbolize this.

This splicing operation requires that we deal with updates at the tail of the tag chain (in the `HashEntryInfo`) as well as at the splice point. This cannot be done as a single atomic operation. To handle this, we check if another session added a readcache entry from a pending read while we were inserting a record at the tail of the log. If so, the new readcache record must be invalidated (see `ReadCacheCheckTailAfterSplice`).
