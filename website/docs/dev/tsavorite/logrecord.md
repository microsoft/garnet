---
id: logrecord
sidebar_label: LogRecord
title: LogRecord
---

# LogRecord

The `LogRecord` struct is a major revision in the Tsavorite `ISessionFunctions` design. It replaces individual `ref key` and `ref value` parameters in the `ISessionFunctions` methods with a single LogRecord, which may be either `LogRecord<TValue>` for in-memory log records, or `DiskLogRecord<TValue>` for on-disk records. There are a number of additional changes in this design as well, as shown in the following sections.

## All Keys are now `SpanByte`

Originally, Tsavorite was templated on the key type. In this revision, all keys are now `SpanByte`. Any key structure must be converted to a stream of bytes and a SpanByte created over this. This can be a stack variable (which is not subject to GC) or a pinned pointer. 

This has simplified the signature and internal implementation of TsavoriteKV itself, the sessions, allocators, ISessionFunctions, Iterators, and so on. And we now have only two allocators, `SpanByteAllocator` and the new `ObjectAllocator`.

## Removal of `BlittableAllocator`

As part of the migration to `SpanByte`-only keys, `BlittableAllocator` has been removed. Tsavorite Unit Tests such as `BasicTests` and the YCSB benchmark's fixed-length test illustrate simple ways to use stack-based 'long' keys and values with `SpanByte`. This does incur some log record space overhead for the key's or value's length prefix.

## Replace `GenericAllocator` with `ObjectAllocator`

With the move to `SpanByte`-only keys we also created a new `ObjectAllocator` for a store that uses an object value type. `GenericAllocator` is not able to take SpanByte keys, and it also uses a separate object log file. `ObjectAllocator` uses a single log file; see the separate `ObjectAllocator` documentation for more details.

## Overflow Keys and Values

To keep the size of the main log record tractable, we provide an option to have large SpanByte keys and values "overflow"; rather than being stored inline in the log record, they are allocated separately by the `OverflowAllocator`, and a pointer is stored in the log record (with the key or value length being `sizeof(IntPtr)`). This does incur some performance overhead. The initial reason for this was to keep `ObjectAllocator` pages small enough that the page-level object size tracking would be sufficiently granular; if those pages are large enough to support large keys, then it is possible there are a large number of records with small keys and large objects, making it impossible to control object space budgets with sufficient granularity. By providing this for SpanByte values as well, it allows similar control of the number of records per `SpanByteAllocator` page.

### OverflowAllocator

The `OverflowAllocator` provides native-memory pointers (using the NativeMemory APIs) to support overflow keys and values. It has two sub-allocators, both of which use a `NativePageAllocator` to provide the actual memory:
    - `FixedSizePages` for keys and values up to `FixesSizePages.MaxExternalBlockSize`. This sub-divides a native allocation into smaller blocks via pointer-advancement, with additional pages allocated as needed. The size of the page is configurable. It has a list of `freeBins` that are linked lists of pointers in power-of-2 sizes from `FixedSizePages.MinInternalBlockSize` to `FixedSizePages.MaxInternalBlockSize`. All allocations in this allocator are done in power-of-2 sizes, similar to a buddy allocator, but bounded by size to minimize wasted space.
      - The difference between `MaxExternalBlockSize` and `MaxInternalBlockSize` is the size of the block header, which is used to track the allocations sizes and to link blocks together in the free list.
    - `OversizePages` for keys that are larger than `FixesSizePages.MaxExternalBlockSize`

`OverflowAllocator` is a partial class, grouping together additional classes described below.

There is currently one `OverflowAllocator` for each main-log page, but this could be changed to handle multiple pages ("segments") with a single `OverflowAllocator` if needed, at the cost of some complexity.

`OverflowAllocator` has a finalizer, so it will free all its memory even if the caller forgets to call `Dispose()` on the `TsavoriteKV`.

One terminology point to note: The term "overflow" as used here refers to Keys or Values being longer than the maximum inline space allowed in the log record; these fields then "overflow" into the `OverflowAllocator`. The term "oversize" refers to the distinction between the `FixedSizePages` and `OversizePages` allocators; "oversize" here means "too large to fit into a `FixedSizePages` bin".

### BlockHeader

This is the basic block header for a NativeMemory allocation. It contains information about the allocation size of the block and the user size. The user size is used to populate the Length field of Key and Value SpanBytes. The returned pointer to the caller (FixedSizePages or OversizePages) is the start of the user data which immediately follows the block header.

### MultiLevelPageArray

The `MultiLevelPageArray` is the data structure used to manage the pages of the NativePageAllocator, as well as to provide a sturcture for simple stacks such as for free lists.

This `MultiLevelPageArray` is a 3-d array of page vectors. Because `NativePageAllocator` allocates pages for caller use, this can be envisioned as a book, where the first two dimensions are infrastructure, and the third is where the user-visible allocations are created.
  - The first dimension is the "book", which is a collection of "chapters". Think of the book as a spine, which can be reallocated--but when it is reallocated, the individual chapters, and references within them, are not moved, so may be accessed by other threads.
  - The second dimension is the "chapters", which are collections of pages.
  - The third dimension is the actual pages of data which are returned to the user.

This structure is chosen so that only the "book" is grown; individual chapters are allocated as a fixed size. This means that getting and clearing items in the chapter does not have to take a latch to prevent a lost update as the array is grown, as would be necessary if there was only a single level of infrastructure (i.e. a growable single vector).

The `MultiLevelPageArray` is a managed structure, because it is also used to hold the .NET objects for the `ObjectAllocator` to manage their lifetimes.

In the initial implementation `MultiLevelPageArray` has fixed-size book (1024) and chapters (64k), but this can be made configurable.

### SimpleConcurrentStack

The `SimpleConcurrentStack` sits on top of the `MultiLevelPageArray` and provides a simple stack interface for the free lists.

### NativePageAllocator

This is the underlying allocator for the `FixedSizePages` and `OversizePages` sub-allocators. It allocates pages of memory via the NativeMemory APIs and provides them to the caller either directly as pages (e.g. `OversizePages` allocator) or as pointer-advancement within a single page (as in the `FixedSizePages` allocator).

The `NativePageAllocator` maintains its list of allocated pages in its pageArray `MultiLevelPageArray`, and frees these at shutdown.

### FixedSizePages

This is the sub-allocator for keys and values up to `FixesSizePages.MaxExternalBlockSize`. It uses a `NativePageAllocator` to provide the actual memory via pointer advancement. Its free list is the `freeBins` vector, which are power-of-2 sized bins up to `FixesSizePages.MaxInternalBlockSize`. All allocations in this allocator are done in power-of-2 sizes, similar to a buddy allocator, but bounded by `MaxInternalBlockSize` to minimize wasted space.

Allocation is done by requesting an allocation from the OverflowAllocator, which sees that the size is within the range of `MaxInternalBlockSize` and calls `FixedSizePages.Allocate` with the size. `FixedSizePages.Allocate` will first "promote" the size to the next-highest power of 2; this size includes the length prefix and `BlockHeader` so the page is always subdivided by a power of 2.

`FixedSizePages` first looks for a free block in the `freeBins` list for that size. If it finds one it pops and returns it. Otherwise it continues to higher bins, currently scanning the entire freeBins list (which is only 11 bins). If it finds a block in a higher bin, it succesively splits it into two blocks, moving back down the freeBins list and pushing the "spare" block onto the bin, until it gets to the requested size bin and then returns the final split.

If there is no block of the requested or larger size in the freeBins list, it will call `NativePageAllocator` to either pointer-advance within the current "last" page, or allocate a new page, and return the block at that pointer (again, as the pointer immediately following the `BlockHeader`).

On `Free()`, the block is added to the appropriate bin in the `freeBins` list. No attempt is made to coalesce blocks, because the lifetime of the allocator's allocations is bounded by the lifetime of the main log page.

### OversizePages

This is for keys and values larger than `FixesSizePages.MaxExternalBlockSize`. It is mostly for Values as Keys should be much smaller. It uses a `NativePageAllocator` to provide the actual memory as pages and returns these pages; as such it is simpler than `FixedSizePages` because it does not subdivide the memory by pointer advancement.

Its free list is the `freeSlots` `SimpleConcurrentStack`. When a page is deallocated, its entry in the `NativePageAllocator` (the slot index is tracked in the `BlockHeader`) is freed and the slot zero'd, so it does not double-free. The slot is then added to the `freeSlots` list. On allocation, it looks for a free slot in the `freeSlots` list, and if it finds one, it tells `NativePageAllocator` to allocate a new page in that slot and return its pointer. 

If it does not find one, it calls `NativePageAllocator` to allocate a new page and returns that pointer.

### ObjectIdMap

The `ObjectIdMap` manages the lifetime of .NET objects for the `ObjectAllocator`. Because we cannot store objects in the unmanaged log (or IntPtrs, as they will become obsolete), we store ObjectIds in the log record. These IDs are simply integer indices into the `MultiLevelPageArray` `objectArray` of the `ObjectIdMap`, so it is not truly a "map".

The `ObjectIdMap` has a freeList which, like the `OversizePages` free list, is a `SimpleConcurrentStack` called `freeSlots`. When an object is deallocated, its slot is nulled and the slot is added to the free list. When an object is allocated, it looks for a free slot in the free list, and if it finds one, it returns that slot. If it does not find one, it allocates a new slot and returns that.

## ISourceLogRecord

In this revision of Tsavorite, the individual "ref key" and "ref value" (as well as "ref recordInfo") parameters to `ISessionFunctions` methods have been replaced by a single `LogRecord` parameter. Not only does this consolidate those record attributes, it also encapsulate the "optional" record attributes of `ETag` and `Expiration`, as well as managing the `FillerLength` that allows records to shrink and re-expand in place. Previously the `ISessionFunctions` implementation had to manage the "extra" length; that is now automatically handled by the `LogRecord`. Similarly, `ETag` and `Expiration` previously were encoded into the Value `SpanByte` or a field of the object; these too are now managed by the `LogRecord` through simple calls.

As part of this change, keys are now always `SpanByte`.

Although we have two allocators, there is only one `LogRecord` family; we do not have separate `StringLogRecord` and `ObjectLogRecord`. There are several reasons for this:
  - It would be more complex to maintain them, especially as we have multiple implementations of `ISourceLogRecord`.
  - Iterators would no longer be able to iterate both stores.
  - The `ObjectAllocator` can have either `SpanByte` or object values, so the `LogRecord` must be able to handle both.

`ISourceLogRecord` defines the common operations among a number of `LogRecord` implementations. These common operations are summarized here, and the implementations are described below.
  - Obtaining the RecordInfo header. There is both a "ref" (mutable) and non-"ref" (readonly) form.
  - Obtaining the SpanByte of the Key. This is always a non-ref SpanByte. Tsavorite no longer passes keys or other SpanBytes by ref.
  - Obtaining the ValueSpan `SpanByte` (for both `SpanByteAllocator` and `ObjectAllocator`; this may be either inline or overflow).
  - Obtaining the ValueObject (for `ObjectAllocator` only. This is intended to be an `IHeapObject`, although this is not enforced and the Tsavorite Unit Tests use object types that do not implement `IHeapObject`. The Garnet processing layer uses `IGarnetObject`, which inherits from `IHeapObject`).
  - Obtaining the "optionals": ETag and Expiration. Note that while `FillerLength` is also optional in the record (it may or may not be present), it is now completely handled by the `LogRecord` and thus is unknown to the caller.
  - Setting a new Value Span or Object.
  - Setting the Value field's length.
    - This is done automatically when setting the Value Span or Object.
    - It may also be called directly and then the ValueSpan obtained and operated on directly, rather than creating a separate `SpanByte` and copying.
  - Utilities such as clearing the Value Object, obtaining record size info, and so on.

### LogRecord<TValue> struct

This is the primary implementation which wraps a log record. It carries the log record's physical address, the `OverflowAllocator` for that log record's log page, and an `ObjectIdMap` if this is an `ObjectAllocator` record. See `LogRecord.cs` for more details, including the record layout and comments.

There are two structs:
  - `LogRecord` (non-generic): Contains physicalAddress only and provides common operations for the `RecordInfo` and Key elements, as well as finding the Value address. These do not need to know specifics of the Value type and thus are useful for quick operations, using either an instance or the static methods the instance calls through to.
  - `LogRecord<TValue>`: This provides the full set of `ISourceLogRecord` operations, as well as the mutators such as `TrySetValueLength`, `TrySetValueSpan`, `TrySetValueObject`, `TryCopyRecordValues`, etc. (including the optionals, `TrySetETag`, `TrySetExpiration`, etc.)

`TrySetValueLength` also manages conversion between the three Value "styles". Both Keys and Values may be inline or overflow, and values additionally may be object in the `ObjectAllocator`. Keys are not mutable, so there is no `LogRecord` method to change them. Values, however, may move between any of the three (or two, if `SpanByteAllocator`). Using the `ObjectAllocator` for a full description:
  - Initially, a Value in the `ObjectAllocator` may be a small inline value, such as the first couple strings of a list. This is stored as a length-prefixed byte stream "inline" in the record.
  - Depending on the inline size limit, such a value may overflow, and become a pointer to an `OverflowAllocator` allocation. In this case, `TrySetValueLength` will handle converting the inline field value to an overflow pointer, shrinking the record, moving the optionals, and adjusting the `FillerLength` as needed. The record has no length prefix for this; its inline size cost is simply an 8-byte pointer (`SpanField.OverflowInlineSize` is the constant used).
  - Finally, the value may be "promoted" to an actual object; e.g., allocating a `ListObject` and populating it from the `ValueSpan`. Again, `TrySetValueLength` will handle this conversion, resizing the Value, moving the optionals, and adjusting the `FillerLength` as needed. The record has no length prefix for this; its inline size cost is simply a 4-byte int containing the `ObjectId` for the `ObjectIdMap`(`ObjectIdMap.ObjectIdSize` is the constant used).

  `TrySetValueLength` handles this switching between inline, overflow, and object values automatically, based upon settings in the `RecordSizeInfo` that is also passed to `ISessionFunctions` methods and then to the `LogRecord`. When `LogRecord` converts between these styles, it handles all the necessary freeing and allocations (for example, if an overflow Value becomes an Object, the overflow allocation is freed (to go to the appropriate free list) and an `ObjectIdMap` slot is allocated). Similarly, if either an overflow or object Value becomes inline, the allocation or ObjectId is freed for reuse, and the record is resized to inline.

  Although `TrySetValueLength` allocates the `ObjectId` slot, it does not know the actual object, so the `ISessionFunctions` implementation must create the object and call `TrySetValueObject`.

#### RecordSizeInfo

This structure is populated prior to record allocation (it is necesasry to know what size to allocate from the log), and then is passed through to `ISessionFunctions` implementation and subsequently to the `LogRecord`. The flow is:
- The `RecordSizeInfo` is populated prior to record allocation:
  - This is done by calling the appropriate `IVariableLengthInput` method to populate the `RecordFieldInfo` part of the structure with Key and Value sizes, whether the Value is to be an object, and whether there are optionals present:
    - `GetRMWModifiedFieldInfo`: For in-place or copy updates via RMW
    - `GetRMWInitialFieldInfo`: For initial update via RMW
    - `GetUpsertFieldInfo`: For writing via Upsert
  - The allocator's `PopulateRecordSizeInfo` method is called to fill in the `RecordSizeInfo` fields based upon the `RecordFieldInfo` fields and other information such as maximum inline size and so on:
    - Whether the Key or Value are inline or overflow
    - Utility methods to make it easy for the Tsavorite allocators to calculate the allocation size
    - Other utility methods to allow `LogRecord.TrySetValueLength` to operate efficiently.

### DiskLogRecord<TValue> struct

The DiskLogRecord is an `ISourceLogRecord` that is backed by a record in on-disk format. See `DiskLogRecord.cs` for more details, inluding the record layout and comments. It is a read-only record.

In on-disk format, Keys and Values are always stored inline, as byte streams. However, the Value may be an object over 2MB, so its length is stored as a long rather than an int. The `DiskLogRecord` also has the full record length (as a long) stored immediately after the `RecordInfo` so IO can know much to read if the entire record is not present. Also, the optionals are stored up front, to allow evaluating them without additional IO, for example to determine if a record is expired (this is not yet implemented).

### PendingContext

`PendingContext` implements `ISourceLogRecord` because it carries a information through the IO process and provides the source record for RMW copy updates.

### RecordScanIterator

`RecordScanIterator` must copy in-memory source records for Pull iterations, so it implements `ISourceLogRecord` by delegating to a `DiskLogRecord` that is instantiated over its copy buffer.

### TsavoriteKVIterator

`TsavoriteKVIterator` must copy in-memory source records for Pull iterations, so it implements `ISourceLogRecord` by delegating its internal `ITsavoriteScanIterator`s.
