---
id: object-allocator
sidebar_label: ObjectAllocator
title: ObjectAllocator
---

# ObjectAllocator

The `ObjectAllocator` replaces the `GenericAllocator` to provide two important improvements:
- It supports `ReadOnlySpan<byte>` keys. With this change Tsavorite now uses only `ReadOnlySpan<byte>` keys; the Garnet processing layer uses `PinnedSpanByte` keys initially, which are converted to `ReadOnlySpan<byte>` at the GarnetApi/StorageSession boundary. (The `GenericAllocator` did not support `SpanByte` keys.)
- It replaces the "two log file" approach of `GenericAllocator` with an "expand inline when flushing" approach:
    - If the key or value is inline, it remains so
    - If the key or value is overflow, it is written inline into the main log record on flush, and addresses are modified.
    - If the value is an object, it is serialized inline.

Garnet uses a two-allocator scheme:
- Strings are stored in a version of `TsavoriteKV` that uses a `SpanByteAllocator`.
- Objects are stored in a version of `TsavoriteKV` that uses an `ObjectAllocator`.

One big difference between the two is that `SpanByteAllocator` allows larger pages for strings, while `ObjectAllocator` allows using a smaller page size because objects use only 4 byte identifiers in the inline log record. Thus, the page size can be much smaller, allowing finer control over Object size tracking and memory-budget enforcement. Either allocator can also set the Key and Value max inline sizes to cause the field to be stored in an overflow allocation, although this is less performant.

## Address Expansion

To be filled in when the implementation is underway.
