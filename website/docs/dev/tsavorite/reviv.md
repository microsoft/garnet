---
id: reviv
sidebar_label: Revivification
title: Revivification
---

# Revivification
Revivification in Tsavorite refers to reusing ("revivifying") Tombstoned (deleted) records, as well as in-memory source records for RCUs done by Upsert or RMW. This minimizes log growth (and unused space) for high-delete scenarios. It is used in the following cases:
- An Upsert, RMW, or Delete that adds a new record
- CopyToTail from Read or RMW that did IO (or a read from the immutable region of the log that is copied to tail)
- Revivification is *not* used for the ReadCache

There are two forms of revivification:
- **In-Chain**: Tombstoned records are left in the hash chain. A subsequent Upsert or RMW of the same key will revivify the record if it has sufficient value length.
- **FreeList**: A free list is maintained, and records that are Tombstoned and are at the tail of the hash chain (pointed to directly from the hash table) are removed from the hash chain and kept in a free list (a binned (by power of 2, by default), multiple-segment-per-bin set of circular buffers).

These are separate from the reuse of a record due to a RETRY return from `CreateNewRecordXxx`. In that case the record address is stored in the `PendingContext` and `CreateNewRecordXxx` returns either RETRY_LATER or RETRY_NOW; when the `CreateNewRecordXxx` is attempted again, it will retrieve this record (if the length is still sufficient and it has not fallen below the minimal required address). Retry reuse is always enabled; revivification might not be.

## External Interface
This section describes the external API and command-line arguments for Revivification.

### `RevivificationSettings`
This struct indicates whether revivification is to be active:
- `EnableRevivification`: If this is true, then at least in-chain revivification is done; otherwise, record revivification is not done (but Retry reuse still is).
- `FreeListBins`: If this array of [`RevivificationBin`](#revivificationbin) is non-null, then revivification will include a freelist of records, as defined below.
- `SearchNextHigherBin`: By default, when looking for FreeRecords we search only the bin for the specified size. If this is set, then if there are no records in the initial bin, then next-higher bin is search as well.
- `RevivifiableFraction`: It may be desirable not to use a record that is too close to the `ReadOnlyAddress`; some apps will prefer that a newly-inserted record remain in the mutable region as long as possible. `RevivifiableFraction` limits the eligible range of revivification to be within this fraction of memory immediately belowthe `TailAddress`; it has the same semantics as `LogSettings.MutablePercent`, and cannot be greater than that. For example, if `RevivifiableFraction` is .2, TailAddress is 100,000, and HeadAddress is 50,000, then the .2 x 50,000 = 10,000 records closest to the tail will be eligible for reuse. This is done on an address-space basis, not record count, so the actual number of records that can be revivified will vary for variable-length records.
- `RestoreDeletedRecordsIfBinIsFull` Deleted records that are to be added to a RevivificationBin are elided from the hash chain. If the bin is full, this option controls whether the record is restored (if possible) to the hash chain. This preserves them as in-chain revivifiable records, at the potential cost of having the record evicted to disk while part of the hash chain, and thus having to do an I/O only to find that the record is deleted and thus potentially unnecessary. For applications that add and delete the same keys repeatedly, this option should be set true if the FreeList is used.
- `UseFreeRecordPoolForCopyToTail` When doing explicit CopyToTail operations such as Compaction, CopyToTail when reading from the immutable in-memory region, or disk IO, this controls whether the allocation for the retrieved records may be satisfied from the FreeRecordPool. These operations may require that the records be allocated at the tail of the log, to remain in memory as long as possible.

#### `RevivificationBin`
This struct contains the definition of the free-list bins, which are lists of free addresses within a certain range of sizes.
- `RecordSize`: The maximum size of records in this partition (the minimum size is either 16 or the previous bin's RecordSize + 8). These should be partitioned according to the anticipated record sizes for your app. Ignored if the TsavoriteKV uses fixed-length data; in that case there is only a single bin for fixed-length records.
- `NumberOfRecords`: The number of records (addresses) to be held in this bin. Tsavorite makes a best-effort attempt to partition the bin into segments to reduce sequential search among the various sizes.
- `BestFitScanLimit`: The maximum number of entries to scan for best fit after finding first fit; may be the special `UseFirstFit` value to use first-fit. Ignored for fixed-length datatypes.

### GarnetServer.exe Commandline Arguments
GarnetServer.exe supports the following commandline arguments for Revivification:
- `--reviv`: A shortcut to specify revivification with default power-of-2-sized bins. This default can be overridden by `--reviv-in-chain-only` or by the combination of `--reviv-bin-record-sizes` and `--reviv-bin-record-counts`.
- `--reviv-bin-record-sizes`: For the main store, the sizes of records in each revivification bin, in order of increasing size. Supersedes the default `--reviv`; cannot be used with `--reviv-in-chain-only`.
- `--reviv-bin-record-counts`: For the main store, the number of records in each bin:
    - Default (not specified): If reviv-bin-record-sizes is specified, each bin has `RevivificationBin.DefaultRecordsPerBin` records.
    - One number: If `--reviv-bin-record-sizes` is specified, then all bins have this number of records, else error.
    - Multiple comma-delimited numbers: If reviv-bin-record-sizes is specified, then it must be the same size as that array, else error. This defines the number of records per bin and supersedes the default `--reviv`; cannot be used with `--reviv-in-chain-only`.
- `--reviv-fraction`: Fraction of mutable in-memory log space, from the highest log address down to the read-only region, that is eligible for revivification. Applies to both main and object store.
- `reviv-search-next-higher-bins`: Search this number of next-higher bins if the search cannot be satisfied in the best-fitting bin. Requires `--reviv` or the combination of `--reviv-bin-record-sizes` and `--reviv-bin-record-counts`.
- `--reviv-bin-best-fit-scan-limit`: Number of records to scan for best fit after finding first fit. Requires `--reviv` or the combination of `--reviv-bin-record-sizes` and `--reviv-bin-record-counts`. Values are:
    - `RevivificationBin.UseFirstFit`: Return the first address whose record is large enough to satisfy the request.
    - `RevivificationBin.BestFitScanAll`: Scan all records in the bin for best fit, stopping early if we find an exact match.
    - Other number: Limit scan to this many records after first fit, up to the record count of the bin.
- `--reviv-in-chain-only`: Revivify tombstoned records in tag chains only (do not use free list). Cannot be used with reviv-bin-record-sizes or reviv-bin-record-counts. Also applies to object store.
- `--reviv-obj-bin-record-count`: Number of records in the single free record bin for the object store. The Object store has only a single bin, unlike the main store. Ignored unless the main store is using the free record list.

## Internal Implementation
This section describes the internal design and implementation of Revivification.

### Maintaining Extra Value Length
Because variable-length record Values can grow and shrink, we must store the actual length of the value in addition to the Value data. Fixed-length datatypes (including objects) do not need to store this, because their Value length does not change.

This storage of record lengths applies to both Revification and non-Revivification scenarios. `ConcurrentWriter` and `InPlaceUpdater` may change the record size, and need to know how much space they have available if they are to to be able to grow in place later:
- In-place updates in `ConcurrentWriter` and `InPlaceUpdater` use this to set the `UpsertInfo` and `RMWInfo` properties `UsedValueLength` and `FullValueLength`. These properties are discussed below.
- Revivification needs this to satisfy requests from the FreeRecordPool, and then also to set the `UpsertInfo` and `RMWInfo` properties `UsedValueLength` and `FullValueLength`.

Storing the extra value length is done by rounding up the used value length (the current length of the value) to int boundary, and then if the allocated Value space is 4 bytes or more greater, storing the extra length at (start of Value plus rounded-up used space). Thus, the full value size is the rounded-up used value size plus the extra value size. If this extra length can be stored then the RecordInfo.Filler bit is set to indicate the extra length is present. Variable-length log allocations are 8-byte (RecordInfo size) aligned, so any extra length less than sizeof(int) can be inferred from this rounding up.

### Ensuring Log Integrity
Storing the extra value length must be done carefully so we maintain the invariant that unused space is zeroed. This is necessary because log scans assume any non-zero data they land on past the previous record length is a valid RecordInfo header. If the extra length is set before the Filler bit is, or if the Filler bit is cleared before the extra length is zeroed, then a log scan could momentarily see a short length, land on the nonzero extra length, and think it is a valid RecordInfo. The other direction is safe: the filler bit may be set but the extra length is zero. In this case the scan sees a zeroed RecordInfo (`RecordInfo.IsNull()`) and assumes it is uninitialized space and steps forward by the size of a RecordInfo (8 bytes).

This invariant is also followed when variable-length values are shrunk, even in the absence of the Filler bit. When a value is shrunk, the data beyond the new (shorter) size must be zeroed before the shorter size is set.

Combining these two, when we adjust a variable-length value length, we must:
- Clear the extra value length.
- Remove Filler bit.
- Zero extra value data space past the new (shorter) size.
- Set the new (shorter) value length.
- Set the Filler bit.
- Set the extra value length to the correct new value.

The Tsavorite-provided variable-length implementation is `SpanByte`, and `SpanByteFunctions` variants do the right thing here.

### UpdateInfo Record Length Management
For variable-length datatypes, the `UpsertInfo`, `RMWInfo`, and `DeleteInfo` have two fields that let the relevant `IFunctions` callbacks know about the record length (without having to know the internals of value storage):
- UsedValueLength: the amount of value space that is actually in use.
- FullValueLength: full allocated space for the value; this is the requested Value size for the initial Insert of the record rounded up to 8-byte alignment.

For SpanByte, the `VariableLenghtBlittableAllocator.GetValue` overload with (physicalAddress, physicalAddress + allocatedSize) that calls the default SpanByte implementation of `IVariableLengthStructureSettings` actually initializes the value space to be a valid SpanByte that includes the requested Value length (up to the maximum number of elements that can fit). However, other variable-length data types may not do this.

### DisposeForRevivification
In general, the `IFunctions` `Dispose*` functions are intended for data that does not get written to the log (usually due to CAS failure or a failure in `SingleWriter`, `InitialUpdater`, or `CopyUpdater`). Data written to the log will be seen by `OnEvictionObserver` if one is registered. However, revivified records are taken from the log and reused; thus they must give the application an opportunity to Dispose().

To handle this a new `DisposeForRevivification` `IFunctions` method has been added. When a record is moved to the FreeList, revivified from either in-chain or freelist, or is reused from Retry, it has a Key and potentially a Value. Tsavorite calls `DisposeForRevivification` twice:
- At the time a record is put on the FreeList. In this case the `newKeySize` parameter is -1, indicating that we are not reusing it yet. The main reason for this call is to release no-longer-used objects as soon as possible. For non-object types, there is minimal cost to this; fixed-length types do nothing, and the only Garnet variable-length type is SpanByte, which also does not need to do anything at this time (there are no allocations).
    - `DisposeForRevivification` is not called for In-Chain tombstoned records; the key must remain valid, and the Value will be disposed at eviction.
    - `DisposeForRevivification` is not called for Retry-recycled records at the time they are added to the `PendingContext`, because it is called when retrieving them and that is always done (we always retry).
- At the time of reuse, either from the FreeList or In-Chain, or from the Retry reuse, with `newKeySize` set to the actual size of the new key if being reused from the FreeList. This is only a concern for variable-length types and for Garnet that means `SpanByte` only, which ensures that the record is always zero-initialized such that it is "valid" for scan at any point. At reuse time, Tsavorite guarantees a valid Key and Value (even if the Value is default) are in the record, and calls `DisposeForRevivification` in the following way:
- Clears the extra value length and filler (the extra value length is retained in local variables).
- Calls `DisposeForRevivification` with `newKeySize` > 0 if the record was retrieved from the freelist
    - If `DisposeForRevivification` clears the Value and possibly Key, it must do so in accordance with the protocol for zeroing data after used space as described in [Maintaining Extra Value Length](#maintaining-extra-value-length). `SpanByte` does not need to clear anything as a `SpanByte` contains nothing that needs to be freed.
    - In a potentially breaking change from earlier versions, any non-`SpanByte` variable-length implementation must either implement `DisposeForRevivification` to zero-init the space or must revise its `SingleWriter`, `InitialUpdater`, and `CopyUpdater` implementations to recognize an existing value is there and handle it similarly to `ConcurrentWriter` or `InPlaceUpdater`.
        - For `SpanByte`, `SingleWriter` et al. will have a valid target value in the destination for non-revivified records, because `VariableLengthBlittableAllocator.GetAndInitializeValue` (called after record allocation) calls the default SpanByte implementation of `IVariableLengthStructureSettings` and actually initializes the value space to be a valid SpanByte that includes the entire requested value size. For newly-allocated (not revivified) records the value data initialized to zero; for revivified records this is not guaranteed; only the value space after the usedValueLength is guaranteed to be zeroed, so `SingleWriter` et al. must be prepared to shrink the destination value.

## In-Chain Revivification
If the FreeList is not active, all Tombstoned records are left in the hash chain; and even if the FreeList is active, a deleted record may not be elidable. A subsequent Upsert or RMW of the same key will revivify the record if it has sufficient allocated value length.

In-Chain revivification is always active if `RevivificationSettings.EnableRevivification` is true. It functions as follows:
- `Delete()` is done in the normal way; the Tombstone is set. If the Tombstoned record is at the tail of the tag chain (i.e. is the record in the `HashBucketEntry`) and the FreeList is enabled, then it will be moved to the FreeList. Otherwise (or if this move fails), it remains in the tag chain.
- `Upsert()` and `RMW()` will try to revivify a Tombstoned record:
    - If the record is large enough, we Reinitialize its Value by:
        - Clearing the extra value length and filler and calls `DisposeForRevivification` as described in [Maintaining Extra Value Length](#maintaining-extra-value-length).
        - Removing the Tombstone.

## FreeList Revivification
If `RevivificationSettings.FreeBinList` is not null, this creates the freelist according to the `RevivificationBin` elements of the array. If the data types are fixed, then there must be one element of the array; otherwise there can be many.

When the FreeList is active and a record is deleted, if it is at the tail of the hash chain, is not locked, and its PreviousAddress points below BeginAddress, then it can be CASed (Compare-And-Swapped) out of the hash chain. If this succeeds, it is added to the freelist. Similar considerations apply to freelisting the source records of RCUs done by Upsert and RMW.

FreeList revivification functions as follows:
- `Delete()` checks to see if the record is at the tail of the hash chain (i.e. is the record in the `HashBucketEntry`). 
    - If it is not, then we do not try to freelist it due to complexity: TracebackForKeyMatch would need to return the next-higher record whose .PreviousAddress points to this one, *and* we would need to check whether that next-higher record had also been freelisted (and possibly revivified).
    - Otherwise, `Delete()` checks to see if its PreviousAddress points below BeginAddress. If not, then we must leave it; it may be the marker for a deleted record below it, and removing it from the tag chain would allow the earlier record to be reached erroneously.
    - Otherwise, we try to CAS the newly-Tombstoned record's `.PreviousAddress` into the `HashBucketEntry`. 
        - It is possible to fail the CAS due to a concurrent insert
    - If this succeeds, then we `Add` the record onto the freelist. This Seals it, in case other threads are traversing the tag chain.
        - If this `Add` fails, it will be due to the bin being full. Rather than lose the record entirely, Tsavorite attempts to re-insert it as a deleted record.
- `Upsert` and `RMW` which perform RCU will check *before* doing the CAS to see that the source record is in the `HashBucketEntry`. If so and all other conditions apply, the source record is freelisted as for `Delete` (except that no attempt is made to reinsert it if `Add` to the freelist fails).
- `Upsert()` and `RMW()` will try to revivify a freelist record if they must create a new record:
    - They call `TryTakeFreeRecord` to remove a record from the freelist.
    - If successful, `TryTakeFreeRecord` initializes the record by:
        - Clearing the extra value length and filler and calls `DisposeForRevivification` as described in [Maintaining Extra Value Length](#maintaining-extra-value-length).
        - Unsealing the record; epoch management guarantees nobody is still executing who saw this record before it went into the free record pool.

### `FreeRecordPool` Design
The FreeList hierarchy consists of:
- The `FreeRecordPool`, which maintains the bins, deciding which bin should be used for Enqueue and Dequeue.
- Multiple `FreeRecordBins`, one for each `RevivificationBin` in the `RevivificationSettings` with the corresponding number of records, max record size, and best-fit scan specification.
- Each element of a bin is a `FreeRecord`

Each bin is a separate allocation, so the pool uses a size index which is a cache-aligned separate vector of ints of the bin sizes that is sequentially searched to determine the bin size. This avoids pulling in each bin’s separate cache line to check its size.

#### `FreeRecord` Design
A `FreeRecord` contains a free log address and the epoch in which it was freed; its data is a 'long' containing:
- the address (48 bits) and 
- the size (16 bits) of the record at the address. If the size is > 2^16, then the record is "oversize" and its exact size is obtained from the hlog.
    - The Address is stored similarly to `RecordInfo.PreviousAddress`, with the same number of bits
    - The Size is shifted above the address, using the remaining bits. Thus it is limited to 16 bits; any free records over that size go into the oversize bin.

Because this is only a long, we have atomic compare and exchange when setting and taking the address.

#### `FreeRecordBin` Design
The records of the bin are of sizes between [previous bin's max record size + 8] and [current bin's max record size]. As a shortcut we refer to bins by their max record size, i.e. "the 64-byte bin" means "the bin whose min record size is 8 more than the max size of the previous bin, and whose max record size is 64."

Each bin has a cache-aligned vector of `FreeRecord` that operates as a circular buffer. Unlike FIFO queues, we don't maintain a read/write pointer, for the following reasons:
- Some records in the bin will be smaller than the size requirement for a given allocation request; this could mean skipping over a number of records before finding a fit. With the pure FIFO approach this would lose a lot of records. The only solutions that keep the read/write pointer are to re-enqueue the skipped record(s), or some variation of peeking ahead of the read pointer, which would introduce complexity such as potentially blocking subsequent writes (the read pointer could be “frozen” but with many open elements beyond it that the write pointer can’t advance to). 
- Maintaining the read and write pointers entails an interlock on every call, in addition to the Add/Remove interlock on the FreeRecord itself.

We wish to obtain a solution that has only one interlock on read and write operations, does not iterate the entire segment, and makes a best effort at best-fit. To accomplish this we use a strategy of segmenting the bin by record sizes.

##### Variable-Length Bin Segmenting
For varlen Key and Value types, we segment bins based on the range of sizes at 8-byte alignment.

First, the max number of records in the bin is rounded up to a cache-aligned size, and may be further rounded up to ensure that each segment starts on a cache boundary. `FreeRecord`s are 16 bytes as stated above so there are 4 per cache line. However, 4 elements is too small for a segment, so we will require a minimum segment size of 8 (2 cache lines). This is a defined constant `MinSegmentSize` so can be modified if desired.

We calculate bin segments by evenly dividing the number of records in the by by the range of possible 8-byte-aligned record sizes in the bin, with a minimum of two cache lines (8 items) per segment. Segments do not have read/write pointers; they are just the index to start at in the bin’s record array, calculated on the fly from the record size (if Adding) or the requested record size (if Taking).

Here are 3 examples of determining the size ranges, using bin definitions as follow:
- A 32-byte bin and a 64-byte bin, both with 1024 records
- Intervening bins we will ignore but which end with a max record size of 2k
- A bin whose max record size is 4k and has 256 records.
This example takes from the 

The following segmenting strategies are used:
- The 32-byte bin’s minimum size is 16 because it is the first bin. Because the sizes are considered in multiples of 8 or greater, it has 3 possible record sizes: 16, 24, 32. Thus we create internal segments of size 1024/3, then round this up so each segment has a record count that is a multiple of 4. Thus, each segment is 344 elements (1024/3 is 341.33...; round this up to multiple of `MinSegmentSize`), and there are 1032 total elements in the bin. These segments start at indexes 0, 344, 688.
- The 64-byte bin’s min record size is 40 (8 greater than the previous bin’s maximum). By the foregoing, it has 4 possible record sizes: 40, 48, 56, 64. 1024 is evenly divisible by 4 to `MinSegmentSize`-aligned 256, and therefore the segments start at 0, 256, 512, 768.
- The the size range of the larger bin has too many possible sizes to create one segment for each size, and therefore uses a different segment-calculation strategy. In this case the size range is 2k which divided by 8 is 256 possible record sizes within the bin. Dividing the bin record count by this yields 4 which is below the required `MinSegmentSize`, so instead we set the bin's segment count to `MinSegementSize` and divide the bin record count by that to get the number of segments (rounded up to integer), and then set the segment size increment to the size range divided by segment count. We therefore have 16 segments, and thus we divide the size range by 16 to get the record size ranges for the segments. The number of records is the segment size multiplied by the segment count. The segments are:
    - Segment 0 starts at index 0 with max record size 2k+16 (thus, the segment may contain a mix of records of the following sizes: 2k+8, 2k+16)
    - Segment 1 starts at index 8 with max record size 2k + 16*2
    - Segment 2 starts at index 16 with max record size, 2k+16*3
    - And so on
- Essentially the partitions are mini-bins that we can efficiently calculate the start offsets for based on record or request size.

An alternative to size-based partitioning would be to use the thread id, as `LightEpoch` does. However, for threadId-based partitioning there is no organization to the size; we could have a random distribution of sizes. With size-based partitioning, we will much more likely land on the size (or close to it) that we want, and overflowing will put us into the next-highest record-size partition half the time, potentially giving us near-best-fit with minimal effort. Additionally, size-based partitioning makes best-fit search for a request easier.

###### Best-Fit and First-Fit
As the names imply, we have the option of first-fit or best-fit to satisfy a request. We allow both, via `RevivificationBin.BestFitScanLimit`:
- `UseFirstFit`: The value is zero, so we do not scan; when a record is requested from the bin, it returns the first one that has a size >= the requested size, has an addedEpoch >= SafeToReclaimEpoch, and has an address >= the minAddress passed to the Take call.
- `BestFitScanAll`: The value is Int.MaxValue, so we scan the entire bin (possibly wrapping), keeping track of the best fit, then try to return that (which may fail if another thread returned it first, in which case we retry). If at any point there is an exact fit, that bin attempts to return that record.
- Some other number < bin record count: Similar to `BestFitScanAll` except that it limits the number of records to scan.

##### Fixed-Length Bins
For fixed-length Key and Value datatypes there is only one bin and of course no size-based partitioning. In this case we could use thread-id-based partitioning (as could any sufficiently large partition) to determine the index within the partition to start iterating from. However, threadId-based partitioning suffers from the possibility that the writers write to different partitions than the readers read from; in the worst case the readers must wrap all the way around through all partitions to get to the records. This may be offset by the reduced cache-line ping-ponging between processor caches if the first 4 records of the partition are repeatedly updated by different threads, but this has not been tried.

##### Checking for Empty Bins
We do not want to maintain a per-operation counter of records in the `FreeRecordPool` or each `FreeRecordBin` because this would be an additional interlock per `Add` or `Take`. Because of this, we cannot have an "accurate at all times" indication of whether the pool or a bin are empty. However, checking empty bins is expensive, so we want to optimize this.

There are numerous "performance vs. accuracy" trade-offs for setting one or more coarse-grained flags indicating whether the pool and/or individual bins are empty:
- Doing so at the pool level requires coordinating with all bins
- Doing so at the bin level requires knowing which bin a request maps to, which takes some calculations
- Obviously, an "empty" flag is set false by Add. However, it is not easy to set it true when Take has removed the last flag, since there may be an Add happening at the same time.

The approach selected is to maintain bin-level isEmpty flags.
- Add always sets isEmpty to false.
- We do not clear isEmpty on Take() because that could lead to lost "isEmpty = false" flags due to Add.
- We maintain a worker Task that periodically iterates all bins to set IsEmpty true.

This Task to iterate bins to set isEmpty is done by the `CheckEmptyWorker` class, which uses a separate on-demand `Task` to do the bump. It does this in a loop that performs:
- Wait for 1 second
- Scan: Scan each bin in the entire pool:
    - If the bin is already marked isEmpty, do nothing
    - Otherwise, iterate all entries in the bin:
        - If an is found with a valid address, exit that bin's loop
        - Otherwise, set that bin's isEmpty flag.

`CheckEmptyWorker` has a Start method that is called by `FreeRecordPool` `Add` or `Take`. This Start method checks whether the `CheckEmptyWorkerWorker` has been started, and starts it if not.

Because this is a persistent Task, it has a CancellationTokenSource that is signaled by `CheckEmptyWorker.Dispose()` when we are shutting down the TsavoriteKV.

##### Fixed vs. VarLen
For non-variable-length types, the record size is fixed, so the FreeRecordPool has only a single bin for it. Otherwise, it has the full range of variable-length bins.

#### Adding a Record to the Pool
When Adding a record to the pool:
- The caller calls `FreeRecordPool.TryAdd`.
- TryAdd scans the size index to find the bin index
- The bin at that index is called to Add the record
    - The bin calculates the segment offset based on the record size, then does a linear scan to look for a free space (or a space whose address is < hlog.ReadOnlyAddress).
    - If it finds one, the record is added with its address, size (if below 16 bits), and the currentEpoch at the time of the `TryAdd` call. `TryAdd` then returns true.
    - else `TryAdd` returns false

#### Taking a Record from the Pool
When Taking a record, we must:
- Determine the bin corresponding to the requested size
- If the bin is empty, return
- Otherwise, any returned address must maintain the invariant that hash chains point downward; the `HashTableEntry.Address` is passed as a minimum required address. This is either a valid lower address or an invalid address (below BeginAddress).

The operation then proceeds as:
- The caller calls `FreeRecordPool.TryTake`.
- TryTake scans the size index to find the bin index
- The bin at that index is called to try to Take the record
    - The bin calculates the segment offset based on the record size, then does a linear scan to look for a record that is >= the required size and has an address >= the minAddress passed to the call. See [Best-Fit and First-Fit](#best-fit-and-first-fit) for a discussion of which viable records are returned.
    - If it finds a record matching this and can successfully CAS the bin to empty, the record is returned and `TryTake` returns true.
    - Otherwise `TryTake` returns false





