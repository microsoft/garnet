---
id: logrecord
sidebar_label: LogRecord
title: LogRecord
---

# `LogRecord`

The `LogRecord` struct is a major revision in the Tsavorite `ISessionFunctions` design. It replaces individual `ref key` and `ref value` parameters in the `ISessionFunctions` methods (as well as endoding optional `ETag` and `Expiration` into the Value) with a single `LogRecord`, which may be either `LogRecord` for in-memory log records, or `DiskLogRecord` for on-disk records. These `LogRecord` have properties for `Key` and `Value` as well as making `Etag` and `Expiration` first-class properties. There are a number of additional changes in this design as well, as shown in the following sections.

Much of the record-related logic of the allocators (e.g. `SpanByteAllocator`) has been moved into the `LogRecord` structs.

See [RecordDataHeader](#recorddataheader) for details of the layout, including `RecordType`, `Namespace`, and the ObjectLogPosition ulong if the record is not inline (has an Overflow Key and/or an Overflow or Object value).

## `SpanByte` and `ArgSlice` are now `PinnedSpanByte` or `ReadOnlySpan<byte>`

To clarify that the element must be a pointer to a pinned span of bytes, the `SpanByte` and `ArgSlice` types have been replaced with `PinnedSpanByte` and `ReadOnlySpan<byte>`. The `PinnedSpanByte` is similar to the earlier `SpanByte`; a struct that wraps a pointer to a pinned span of bytes. Its construction has been changed from direct constructor calls to static `FromPinned*` calls, e.g. `FromLengthPrefixedPinnedPointer`. This is mostly used for cases where `(ReadOnly)Span<byte>` are not possible due to restrictions on their use; further work could reduce these areas.

There are still areas where direct `byte*` are used, such as number parsing. Later work can revisit this to use `(ReadOnly)Span<byte>` instead if there is no performance impact.

The `SpanByte` class now exists only as a static utility class that provides extension functions `(ReadOnly)Span<byte>`.

## All Keys are now `ReadOnlySpan<byte>` at the Tsavorite Level

Originally, Tsavorite was templated on the `TKey` generic type, which was either `SpanByte` for the string store or `byte[]` for the object store. In this revision, all keys at the Tsavorite level are now `ReadOnlySpan<byte>`. At the Garnet processing level, they may be `PinnedSpanByte` at the `GarnetApi` layer and above. Any key structure must be converted to a stream of bytes and a `ReadOnlySpan<byte>` or `PinnedSpanByte` created over this. This can be a stack variable (which is not subject to GC) or a pinned pointer. 

This has simplified the signature and internal implementation of TsavoriteKV itself, the sessions, allocators, ISessionFunctions, Compaction, Iterators, and so on. And we now have only two allocators, `SpanByteAllocator` and the new `ObjectAllocator`.

## Removal of `BlittableAllocator`

As part of the migration to `SpanByte`-only keys, `BlittableAllocator` has been removed. Tsavorite Unit Tests such as `BasicTests` and the YCSB benchmark's fixed-length test illustrate simple ways to use stack-based 'long' keys and values with `SpanByte`. This does incur some log record space overhead for the key's or value's length bytes, described below under `LogRecord`.

A reduced form of `BlittableAllocator`, renamed `TsavoriteLogAllocator`, is still used by `TsavoriteLog`.

## Replace `GenericAllocator` with `ObjectAllocator`

With the move to `SpanByte`-only keys we also created a new `ObjectAllocator` for a store that uses an object value type. `GenericAllocator` is not able to take SpanByte keys, and stored both key and value in a separate managed array; `ObjectAllocator` uses native allocations, the same as `SpanByteAllocator`.

### IHeapObject

An object field's object must inherit from `IHeapObject`. The Garnet processing layer uses `IGarnetObject`, which inherits from `IHeapObject`). The Tsavorite Unit Tests use object types that implement `IHeapObject`.

`IHeapObject` provides methods for object management by core Tsavorite and Garnet processing. One significant property is `MemorySize`, the size the object takes in memory. This includes .NET object overhead as well as the size of the actual data. It is used in object size tracking.

There are a number of other methods on IHeapObject, mostly to handle serialization.

### `ObjectIdMap`
In `ObjectAllocator` we have an `ObjectIdMap` that provides a GC root for objects (and overflows, as discussed next). In the log record itself, there is a 4-byte `ObjectId` that is an index into the `ObjectIdMap`.

The `ObjectIdMap` manages the lifetime of .NET objects for the `ObjectAllocator`. Because we cannot store objects in the unmanaged log (or IntPtrs, as they will become obsolete), we store a 4-byte `ObjectId` in the log record; this is an index into the `ObjectIdMap`. These IDs are simply integer indices into the `MultiLevelPageArray` `objectArray` of the `ObjectIdMap`, so it is not truly a "map".

The `ObjectIdMap` has a freeList in a `SimpleConcurrentStack` called `freeSlots`. When an object is deallocated, its slot is nulled and the slot is added to the free list. When an object is allocated, it looks for a free slot in the free list, and if it finds one, it returns that slot. If it does not find one, it allocates a new slot and returns that. This freelist keeps the `ObjectIdMap` from having to grow more than necessary.

#### MultiLevelPageArray

The `MultiLevelPageArray` is a data structure to provide efficiently growable arrays with direct indexing. It is a managed structure, as it is used by the `ObjectIdMap` to root .NET objects. It is also used to provide a structure for simple stacks such as for free lists.

This `MultiLevelPageArray` is a 3-d array of page vectors. Because `NativePageAllocator` allocates pages for caller use, this can be envisioned as a book, where the first two dimensions are infrastructure, and the third is where the user-visible allocations are created.
  - The first dimension is the "book", which is a collection of "chapters". Think of the book as a spine, which can be reallocated--but when it is reallocated, the individual chapters, and references within them, are not moved, so may be accessed by other threads.
  - The second dimension is the "chapters", which are collections of pages.
  - The third dimension is the actual pages of data which are returned to the user. "Page" is somewhat of a misnomer, as the purpose has changed slightly from its initial intent; currently these are object slots for `IHeapObject` and `byte[]`, as well as integer indexes for freelists.

This structure is chosen so that only the "book" is grown; individual chapters are allocated as a fixed size. This means that getting and clearing items in the chapter does not have to take a latch to prevent a lost update as the array is grown, as would be necessary if there was only a single level of infrastructure (i.e. a growable single vector).

The `MultiLevelPageArray` is a managed structure, because it is also used to hold the .NET objects for the `ObjectAllocator` to manage their lifetimes.

In the initial implementation `MultiLevelPageArray` has fixed-size book (1024) and chapters (64k), but this can be made configurable.

#### `SimpleConcurrentStack`

The `SimpleConcurrentStack` sits on top of the `MultiLevelPageArray` and provides a simple stack interface for the free lists.

## Overflow Keys and Values

To keep the size of the main log record tractable, we provide an option for `ObjectAllocator` to have large `Span<byte>` keys and values "overflow"; rather than being stored inline in the log record, they are allocated separately as `byte[]`, and an integer `ObjectId` is stored in the log record (with the key or value having no explicit length, and the data being the `ObjectId` of size `sizeof(int)`). This redirection does incur some performance overhead. The initial reason for this was to keep `ObjectAllocator` pages small enough that the page-level object size tracking would be sufficiently granular; if those pages are large enough to support large keys, then it is possible there are a large number of records with small keys and large objects, making it impossible to control object space budgets with sufficient granularity. By providing this for `Span<byte>` values as well, it allows similar control of the number of records per page.

## `ISourceLogRecord`

In this revision of Tsavorite, the individual "ref key" and "ref value" (as well as "ref recordInfo") parameters to `ISessionFunctions` methods have been replaced by a single `LogRecord` parameter. Not only does this consolidate those record attributes, it also encapsulate the "optional" record attributes of `ETag` and `Expiration`, as well as managing the `FillerLength` that allows records to shrink and re-expand in place. Previously the `ISessionFunctions` implementation had to manage the "extra" length; that is now automatically handled by the `LogRecord`. Similarly, `ETag` and `Expiration` previously were encoded into the Value `SpanByte` or a field of the object and this required tracking additional metadata and shifting when these values were added/removed; these too are now managed by the `LogRecord` as first-class properties.

As part of this change, keys are now always `ReadOnlySpan<byte>` at the Tsavorite level. At the processing layer, they are initially `PinnedSpanBytes`; these have a `ReadOnlySpan` property that is called to convert them to `ReadOnlySpan<byte>` at the GarnetApi/StorageApi boundary.

Although we have two allocators, there is only one `LogRecord` family; we do not have separate `StringLogRecord` and `ObjectLogRecord`. There are a couple reasons for this:
  - It would be more complex to maintain them, especially as we have multiple implementations of `ISourceLogRecord`.
  - Iterators would no longer be able to iterate both stores.
  - The `ObjectAllocator` can have `SpanByte`, overflow `byte[]`, or `IHeapObject` values, so the `LogRecord` must be able to handle both.
This decision may be revisited in the future; for example, `SpanByteAllocator` currently cannot have overflow keys or values, so a much leaner implementation could be used for that case. This would require a `TLogRecord` generic type in place of the earlier `TKey` and `TValue` types that have been removed in this revision.

`ISourceLogRecord` defines the common operations among a number of `LogRecord` implementations. These common operations are summarized here, and the implementations are described below.
  - Obtaining the RecordInfo header. There is both a "ref" (mutable) and non-"ref" (readonly) form.
  - Obtaining the `ReadOnlySpan<byte>` of the Key.
  - Obtaining the ValueSpan `Span<byte>` (for both `SpanByteAllocator` and `ObjectAllocator`; this may be either inline or overflow).
  - Obtaining the ValueObject (for `ObjectAllocator` only. This is intended to be an `IHeapObject`.
  - Obtaining the "optionals": ETag and Expiration. Note that while `FillerLength` is also optional in the record (it may or may not be present), it is now completely handled by the `LogRecord` and thus is unknown to the caller.
  - Setting a new Value Span or Object.
  - Setting the Value field's length.
    - This is done automatically when setting the Value Span or Object.
    - It may also be called directly and then the ValueSpan obtained and operated on directly, rather than creating a separate `Span<byte>` and copying.
  - Utilities such as clearing the Value Object, obtaining record size info, and so on.

For operations that take an input log record, such as `ISessionFunctions.CopyUpdater`, the input log record is of type `TSourceLogRecord`, which may be either `LogRecord` or `DiskLogRecord`. No code outside Tsavorite should need to know the actual type. Within Tsavorite, it is sometimes useful to call `AsLogRecord` or `AsDiskLogRecord` and operate accordingly.

### `LogRecord` struct

This is the primary implementation which wraps a log record. It carries the log record's physical address and, if this is an `ObjectAllocator` record, an `ObjectIdMap` for that log record's log page. See `LogRecord.cs` for more details, including the record layout and comments.

For `ObjectAllocator` records, `TrySetContentLengthsAndPrepareOptionals` also manages conversion between the three Value "styles". Both Keys and Values may be inline or overflow, and values additionally may be object. Keys are not mutable, so there is no `LogRecord` method to change them. Values, however, may move between any of the three:
  - Initially, a Value in the `ObjectAllocator` may be a small inline value, such as the first couple strings of a list. This is stored as a byte stream "inline" in the record.
  - Depending on the inline size limit, such a value may overflow, and become a pointer to an `OverflowAllocator` allocation. In this case, `TrySetContentLengthsAndPrepareOptionals` will handle converting the inline field value to an overflow pointer, shrinking the record, moving the optionals, and adjusting the `FillerLength` as needed. The value length becomes `ObjectIdMap.ObjectIdSize`.
  - Finally, the value may be "promoted" to an actual object; e.g., allocating a `ListObject` and populating it from the `ValueSpan`. Again, `TrySetContentLengthsAndPrepareOptionals` will handle this conversion, resizing the Value, moving the optionals, and adjusting the `FillerLength` as needed. The value becomes a 4-byte int containing the `ObjectId` for the `ObjectIdMap` and its length becomes `ObjectIdMap.ObjectIdSize`.

  `TrySetContentLengthsAndPrepareOptionals` handles this switching between inline, overflow, and object values automatically, based upon settings in the `RecordSizeInfo` that is also passed to `ISessionFunctions` methods and then to the `LogRecord`. When `LogRecord` converts between these styles, it handles all the necessary freeing and allocations. For example, when growing a Value causes allows it to move from inline to overflow, the `byte[]` slot is allocated in `ObjectIdMap`; if it shrinks enough to return to inline, the `ObjectIdMap` slot element is nulled and the slot is added to the freelist, and the record is resized to inline.

  Although `TrySetContentLengthsAndPrepareOptionals` allocates the `ObjectId` slot, it does not know the actual object, so the `ISessionFunctions` implementation must create the object and call `TrySetValueObject`.

  Performance note: `TrySetContentLengthsAndPrepareOptionals` handles all conversions. It should be beneficial to provide some leaner versions, for example string-only when lengths are unlikely to change.

#### RecordDataHeader

This is a struct wrapper to manage the variable-length record lengths. At a high level, it manages an indicator byte for lengths, as well as fixed information such as Namespace and RecordType.
For details of the layout, see RecordDataHeader.cs; in summary, the bytes of this header are laid out in sequence as:
- Indicator byte: flags, number of bytes in record length (immutable), number of bytes in key length (also immutable)
- Namespace byte (immutable): indicates the namespace value (if 127 or less) or the number of bytes in the ExtendedNamespace field
- RecordType byte (immutable): indicates the type of the record
- RecordLength (variable # of bytes; immutable): The allocated size of the record
- KeyLength: (variable # of bytes; immutable until revivification): The size of the key
- ExtendedNamespace (variable # of bytes; immutable until revivification): The bytes of the namespace if it is longer than 127 bytes
- Key bytes (immutable until revivification)
- Value bytes, or Overflow or Object id for the `ObjectIdMap`
- Optionals, if present, in this order:
  - ETag
  - Expiration
  - ObjectLogPosition (pseudo-optional; always present if the record Key or Value is Overflow, or the Value is an Object; used only for serialization)
- Filler (the amount of extra space in the record, e.g. if the initial length was less than the "rounded to record alignment" or if the value shrank)

Note that we do not store Value length directly. Because we must ensure that the Record length is available for Scan even when the record is actively being edited,
we store the complete record length; for space efficiency, we then calculate the Value length from the immutable other fields rather than storing it explicitly.

#### RecordSizeInfo

This structure is populated prior to record allocation (it is necessary to know what size to allocate from the log), and then is passed through to `ISessionFunctions` implementation and subsequently to the `LogRecord`. The flow is:
- The `RecordSizeInfo` is populated prior to record allocation:
  - This is done by calling the appropriate `IVariableLengthInput` method to populate the `RecordFieldInfo` part of the structure with Key and Value sizes, whether the Value is to be an object, and whether there are optionals present:
    - `GetRMWModifiedFieldInfo`: For in-place or copy updates via RMW
    - `GetRMWInitialFieldInfo`: For initial update via RMW
        - `GetUpsertFieldInfo`: For writing via Upsert. There are three overloads of this, because Upsert takes a Value which may be one of `ReadOnlySpan<byte>`, `IHeapObject`, or a source `TSourceLogRecord`.
  - The allocator's `PopulateRecordSizeInfo` method is called to fill in the `RecordSizeInfo` fields based upon the `RecordFieldInfo` fields and other information such as maximum inline size and so on:
    - Whether the Key or Value are inline or overflow
    - Utility methods to make it easy for the Tsavorite allocators to calculate the allocation size
    - Other utility methods to allow `LogRecord.TrySetContentLengthsAndPrepareOptionals` to operate efficiently.

#### LogField

This is a static class that provides utility functions for `LogRecord` to operate on a Key or Value field at a certain address.

As a terminology note, `LogField` (and `RecordSizeInfo` and `LogRecord`) use the following terms for field layout. In all cases, the field length is stored separately from the data, in the [RecordDataHeader](#recorddataheader):
- Inline: The field is stored inline in the record.
- Overflow: The field is a byte stream that exceeds the limit to remain inline, so is stored in as an `OverflowByteArray` wrapping a `byte[]`. The "Inline size" is `sizeof(ObjectId)`, which is an int. The "Data size" is the length of the byte stream.
- Object: The field is an object implementing `IHeapObject`. As with overflow, the "Inline size" is `sizeof(ObjectId)`, which is an int. The "Data size" is only relevant during serialization; however, the `HeapMemorySize` property of the `IHeapObject` is used in object size tracking.

### DiskLogRecord struct

The DiskLogRecord is an `ISourceLogRecord` that is backed by a `LogRecord`. See `DiskLogRecord.cs` for more details. Its main purpose is to act as a container for a `LogRecord` and the `SectorAlignedMemory` buffer and value-object disposer associated with that `LogRecord`.

### PendingContext

`PendingContext` implements `ISourceLogRecord` because it carries a information through the IO process and provides the source record for RMW copy updates.

Previously `PendingContext` had separate `HeapContainers` for keys and values. However, for operations such as conditional insert for Copy-To-Tail or Compaction, we need to carry through the entire log record (including optionals). In the case of records read from disk (e.g. Compaction), it is easiest to pass the `LogRecord` in its entirety, including its `SectorAlignedMemory` buffer, in the `DiskLogRecord`. So now PendingContext will also serialize the Key passed to Upsert or RMW, and the value passed to Upsert, as a `DiskLogRecord`. `PendingContext` still carries the `HeapContainer` for Input, and `CompletedOutputs` must still retain the Key's `HeapContainer`. 

For Compaction or other operations that must carry an in-memory record's data through the pending process, `PendingContext` serializes that in-memory `LogRecord` to its `DiskLogRecord`.

### ObjectScanIterator

`ObjectScanIterator` must copy in-memory source records for Pull iterations, so it implements `ISourceLogRecord` by delegating to a `DiskLogRecord` that is instantiated over its copy buffer.

### TsavoriteKVIterator

`TsavoriteKVIterator` must copy in-memory source records for Pull iterations, so it implements `ISourceLogRecord` by delegating its internal `ITsavoriteScanIterator`s.

## Migration and Replication

Key migration and diskless Replication have been converted to serialize the record to a `DiskLogRecord` on the sending side, and on the receiving side call one of the new `Upsert` overloads that take a `TSourceLogRecord` as the Value. This serialization mimics the writing to disk, but instead of a separate file or memory allocation, it allocates one chunk large enough for the entire inline portion followed by the out-of-line portions appended after the inline portion. Note that this limits the capacity of out-of-line allocations to a single network buffer; there is a pending work item to provide "chunked" output to (and read from) the network buffer.