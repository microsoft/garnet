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

These are separate from the reuse of a record due to a RETRY return from `CreateNewRecordXxx`. In that case the record address is stored in the `OperationState` and `CreateNewRecordXxx` returns either RETRY_LATER or RETRY_NOW; when the `CreateNewRecordXxx` is attempted again, it will retrieve this record (if the length is still sufficient and it has not fallen below the minimal required address). Retry reuse is always enabled; revivification might not be.

## External Interface
This section describes the external API and command-line arguments for Revivification.

### `RevivificationSettings`
This struct indicates whether revivification is to be active:
- `EnableRevivification`: If this is true, then at least in-chain revivification is done; otherwise, record revivification is not done (but Retry reuse still is).
- `FreeRecordBins`: If this array of [`RevivificationBin`](#revivificationbin) is non-null and non-empty, then revivification will include a freelist of records, as defined below.
- `NumberOfBinsToSearch`: By default, when looking for FreeRecords we search only the bin for the specified size. This is the number of additional next-higher bins to search if the initial bin has no available records.
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
- `--reviv-bin-record-sizes`: The sizes of records in each revivification bin, in order of increasing size. Supersedes the default `--reviv`; cannot be used with `--reviv-in-chain-only`.
- `--reviv-bin-record-counts`: The number of records in each bin:
    - Default (not specified): If reviv-bin-record-sizes is specified, each bin has `RevivificationBin.DefaultRecordsPerBin` records.
    - One number: If `--reviv-bin-record-sizes` is specified, then all bins have this number of records, else error.
    - Multiple comma-delimited numbers: If reviv-bin-record-sizes is specified, then it must be the same size as that array, else error. This defines the number of records per bin and supersedes the default `--reviv`; cannot be used with `--reviv-in-chain-only`.
- `--reviv-fraction`: Fraction of mutable in-memory log space, from the highest log address down to the read-only region, that is eligible for revivification.
- `reviv-search-next-higher-bins`: Search this number of next-higher bins if the search cannot be satisfied in the best-fitting bin. Requires `--reviv` or the combination of `--reviv-bin-record-sizes` and `--reviv-bin-record-counts`.
- `--reviv-bin-best-fit-scan-limit`: Number of records to scan for best fit after finding first fit. Requires `--reviv` or the combination of `--reviv-bin-record-sizes` and `--reviv-bin-record-counts`. Values are:
    - `RevivificationBin.UseFirstFit`: Return the first address whose record is large enough to satisfy the request.
    - `RevivificationBin.BestFitScanAll`: Scan all records in the bin for best fit, stopping early if we find an exact match.
    - Other number: Limit scan to this many records after first fit, up to the record count of the bin.
- `--reviv-in-chain-only`: Revivify tombstoned records in tag chains only (do not use free list). Cannot be used with reviv-bin-record-sizes or reviv-bin-record-counts.

## Internal Implementation
This section describes the internal design and implementation of Revivification.

### Maintaining Extra Value Length
Because inline (string) record Values can grow and shrink, a record can have unused space between its current Value length and the space that was allocated for it. This unused space is tracked so that `InPlaceWriter` and `InPlaceUpdater` know how much room is available to grow in place later, and so that revivification can size records taken from the FreeRecordPool. Object values do not need this, because the record only stores a fixed-size object reference (an `ObjectId`) whose length does not change.

Each record carries this unused space as a *filler length* recorded in the record's `RecordDataHeader` (RDH). Specifically, the RDH's `FillerWords` field is an 8-bit count of 8-byte filler words that follow the Value (and any optional fields) at the end of the record. The record's total length is fully derived from the RDH — key length, value length, optionals, and `FillerWords` all sum to the allocated record size — so there is no separate length stored inside the Value data, and the filler bytes themselves are never read. Records that need more filler than `FillerWords` can represent are *split*: the record retains a portion of the filler and the excess becomes a separate invalid record.

Because the RDH is the authoritative source of record length, `InPlaceWriter`/`InPlaceUpdater` grow or shrink an inline Value by moving space between the Value and the filler: growing the Value decreases `FillerWords` by the same amount, and shrinking it increases `FillerWords`. The allocated record size is therefore unchanged.

### Ensuring Log Integrity
Adjusting a record's Value length must be done carefully so we maintain the invariant that a log scan always sees a consistent record extent. A scan walks the log by computing each record's length and stepping to the next record; if it ever observed a partially-updated record it could miscompute the length and land in the middle of record data, mistaking it for a valid `RecordInfo` header.

This invariant is preserved because everything that defines a record's length lives in the single 8-byte RDH word (key length, value length, optional-field flags, and `FillerWords`). Length-affecting changes are built up in a local copy of the RDH and then published with one atomic word write, so a concurrent scanner observes either the pre-update or post-update state, never an intermediate. When an inline Value grows or shrinks, the change in value length is offset by an equal, opposite change in `FillerWords`, so the derived record length is identical before and after and the scan extent is stable throughout. Any space a value grows into is zero-initialized so no stale bytes are exposed.

The Tsavorite-provided inline value type is `SpanByte`, and the `SpanByteFunctions` variants do the right thing here.

### Conveying Record Length to Callbacks
The `ISessionFunctions` writer and updater callbacks operate on a `LogRecord`, which exposes the record's Key and Value directly, so a callback does not need to know the internals of value storage. When a new or reused record is being written (`InitialWriter`, `InitialUpdater`, `CopyUpdater`), the callback also receives a `RecordSizeInfo` (`in RecordSizeInfo sizeInfo`) describing the record's layout — including `AllocatedInlineRecordSize` (the total allocated inline size), `MaxInlineValueSize` (the largest inline Value the record can hold), and the `IsRevivifiedRecord` flag indicating the record is being reused rather than freshly allocated. In-place callbacks (`InPlaceWriter`, `InPlaceUpdater`) read the currently allocated and used sizes from the `LogRecord`/RDH to decide whether an update fits in place.

For `SpanByte`, the value space of a freshly-allocated record is initialized to a valid, zero-length `SpanByte` sized to the requested Value length, so the writer callbacks always see a valid destination Value. For a revivified record, the previous record's larger allocation is preserved (see [Disposing Revivified Records](#disposing-revivified-records)), so the writer callbacks must be prepared to shrink the destination Value to the size they actually write.

### Disposing Revivified Records
The `ISessionFunctions` writer, updater, and deleter callbacks are the point at which an application disposes a Value. For object Values, `Dispose()` may be called from these callbacks; in particular, when `logRecord.Info.Invalid` (or `recordInfo.Invalid`) is true the callback is being invoked for a record that was allocated and populated but could not be appended to the log, so the application can release the object there.

Records that are removed from the log for reuse follow a separate path. When a record is put on the FreeList, revivified in-chain, or elided, Tsavorite invokes the allocator's record-disposal trigger, `IRecordTriggers.OnDispose(ref LogRecord, DisposeReason)`, with a `DisposeReason` that identifies the event (for example `RevivificationFreeList` when the record is moved to the FreeList, `Deleted` when a record is tombstoned in place, and `Elided` when a record is removed from the hash chain without being freelisted). This gives the application the opportunity to release no-longer-needed objects as soon as possible, and lets Tsavorite adjust its heap-size accounting. For `SpanByte` there is nothing to free, so this is a no-op. Records materialized transiently from disk are disposed through the companion `OnDisposeDiskRecord` trigger instead.

Disposal-at-eviction (as opposed to reuse) is handled by a different trigger: when a page is evicted past `HeadAddress`, Tsavorite calls `IRecordTriggers.OnEvict(ref LogRecord, EvictionSource)` per non-tombstoned record, provided the store's `CallOnEvict` is true. In-chain tombstoned records keep a valid Key (needed for chain traversal) and are not put through the reuse-dispose path; their Value is disposed at delete time and their remaining resources are accounted for when the record is eventually elided or evicted.

When a record is actually reused (from the FreeList, in-chain, or from a Retry), Tsavorite hands the writer/updater callbacks a valid, consistent record. Preparing a reused record clears the filler, namespace, and record-type fields while preserving the inline Key/Value length information, and sets the record to a valid state — all published with a single atomic RDH word write so a concurrent scanner never sees an intermediate state (see [Ensuring Log Integrity](#ensuring-log-integrity)). The reused record's `RecordSizeInfo.AllocatedInlineRecordSize` retains the previous (possibly larger) record length and its `IsRevivifiedRecord` flag is set, so the callbacks know they are writing into a reused allocation and may shrink the destination Value as needed.

## In-Chain Revivification
If the FreeList is not active, all Tombstoned records are left in the hash chain; and even if the FreeList is active, a deleted record may not be elidable. A subsequent Upsert or RMW of the same key will revivify the record if it has sufficient allocated value length.

In-Chain revivification is always active if `RevivificationSettings.EnableRevivification` is true. It functions as follows:
- `Delete()` is done in the normal way; the Tombstone is set. If the Tombstoned record is at the tail of the tag chain (i.e. is the record in the `HashBucketEntry`) and the FreeList is enabled, then it will be moved to the FreeList. Otherwise (or if this move fails), it remains in the tag chain.
- `Upsert()` and `RMW()` will try to revivify a Tombstoned record:
    - If the record is large enough, we Reinitialize its Value by:
        - Clearing the filler and preparing the record for reuse as described in [Disposing Revivified Records](#disposing-revivified-records).
        - Removing the Tombstone.

## FreeList Revivification
If `RevivificationSettings.FreeRecordBins` is non-null and non-empty, this creates the freelist according to the `RevivificationBin` elements of the array. If the data types are fixed, then there must be one element of the array; otherwise there can be many.

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
        - Clearing the filler and preparing the record for reuse as described in [Disposing Revivified Records](#disposing-revivified-records).
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
- The size range of the larger bin has too many possible sizes to create one segment for each size, and therefore uses a different segment-calculation strategy. In this case the size range is 2k which divided by 8 is 256 possible record sizes within the bin. Dividing the bin record count by this yields 4 which is below the required `MinSegmentSize`, so instead we set the bin's segment count to `MinSegmentSize` and divide the bin record count by that to get the number of segments (rounded up to integer), and then set the segment size increment to the size range divided by segment count. We therefore have 16 segments, and thus we divide the size range by 16 to get the record size ranges for the segments. The number of records is the segment size multiplied by the segment count. The segments are:
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

`CheckEmptyWorker` has a `Start` method that is called by `FreeRecordPool.TryAdd`. This `Start` method checks whether the worker task is already active, and launches it (via `Task.Run`) if not.

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





