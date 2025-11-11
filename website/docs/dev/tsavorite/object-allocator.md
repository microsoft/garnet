---
id: object-allocator
sidebar_label: ObjectAllocator
title: ObjectAllocator
---

# ObjectAllocator

The `ObjectAllocator` replaces the `GenericAllocator` to provide two important improvements:
- It supports `ReadOnlySpan<byte>` keys. With this change Tsavorite now uses only `ReadOnlySpan<byte>` keys; the Garnet processing layer uses `PinnedSpanByte` keys initially, which are converted to `ReadOnlySpan<byte>` at the GarnetApi/StorageSession boundary. (The `GenericAllocator` did not support `SpanByte` keys.)
- It replaces the separate managed array of `GenericAllocator` with native allocations for the log pages, exactly like `SpanByteAllocator`.

Tsavorite provides two primary allocators:
- Strings are stored in a version of `TsavoriteKV` that uses a `SpanByteAllocator`.
- Objects are stored in a version of `TsavoriteKV` that uses an `ObjectAllocator`. This is also the "unified allocator" used by Garnet.

The former `BlittableAllocator` is now the `TsavoriteLogAllocator`, used only by the `TsavoriteLog`.

One big difference between the two is that `SpanByteAllocator` allows larger pages for strings, while `ObjectAllocator` allows using a smaller page size because objects use only 4 byte identifiers in the inline log record. Thus, the page size can be much smaller, allowing finer control over Object size tracking and memory-budget enforcement. Either allocator can also set the Key and Value max inline sizes to cause the field to be stored in an overflow allocation, although this is less performant.

## Two Log Files

ObjectAllocator supports two log files simlarly to `GenericAllocator`. However, `GenericAllocator` was limited to a fixed 100MB buffer size for object serialization. `ObjectAllocator` uses a circular buffer system, currently 4 buffers of 4MB each, and writes one to disk while populating the next (or reads the following buffers from disk while processing the current one, e.g. deserializing objects.

The key points in the code here are AsyncWrite, AsyncRead, AsyncGetFromDisk, and iterators. The basic flow is:
- Flush:
  - This is AsyncFlushPagesForReadOnly and Checkpointing. AsyncFlushPagesForReadOnly is a loop on partial or complete pages; it creates a CircularDiskWriteBuffer that is reused throughout the entire call to AsyncFlushPagesForReadOnly, for all the partial ranges. The main driver is an ObjectLogWriter, which handles writing for Overflow (large, out-of-line allocations that are byte[]; this writing is optimized to minimize copies) strings and serialization of objects. The basic operation of the circular buffer is to track the current DiskWriteBuffer's current position, last flushed position, and end position (e.g. if it ends in the middle of a segment).
- Read:
  - This may be either AsyncGetFromDisk for RUMD operations or Scan, either for iteration or recovery. Similar to Flush, it creates a CircularDiskReadBuffer composed of multiple DiskReadBuffers, and issues disk reads ahead of the operation on the buffer (such as object deserialization).

### Adaptive Fields in LogRecord
When we serialize a LogRecord to disk, as we write the object log file we also modify the disk-image of the LogRecord (but, importantly, do not modify the length of the logRecord, to keep the LogicalAddress space consistent) to maintain a "pointer" and length in the ObjectLog file:
- We record the position in the Log file in the ObjectLogPosition ulong that is allocated for the record when we specify non-inline.
- We reuse the ObjectId space for the length of that field. For Value objects, we also use the top byte of the `ObjectLogPosition` field, giving a total of 40 bits or 1 TB of address for a single object, and 72 PB for the entire object log.

### ObjectIdMap Remapping
The ObjectAllocator carries a second ObjectIdMap, a transient one intended for IO and iterator and pending operations. (These are the same scenarios that embed a LogRecord into a DiskLogRecord.) In these cases we remap the ObjectIds in the temporary image to use the TransientObjectIdMap, so pages can be evicted and their ObjectIdMaps reused.