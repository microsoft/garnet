// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>An interface to cover either an in-memory or on-disk log record for RCU</summary>
    public unsafe interface ISourceLogRecord : IKey
    {
        /// <summary>The physical address of the record data</summary>
        unsafe long PhysicalAddress => (long)Unsafe.AsPointer(ref InfoRef);

        /// <summary>A ref to the record header</summary>
        ref RecordInfo InfoRef { get; }

        /// <summary>Fast access returning a copy of the record header</summary>
        RecordInfo Info { get; }

        /// <summary>Type of the record. Should be set on creation of the <see cref="LogRecord"/> and then immutable.</summary>
        byte RecordType { get; }

        /// <summary>Namespace of the record. Should be set on creation of the <see cref="LogRecord"/> and then immutable.</summary>
        ReadOnlySpan<byte> Namespace { get; }

        /// <summary>The <see cref="ObjectIdMap"/> for this instance. May be the allocator's or transient (for <see cref="DiskLogRecord"/>).</summary>
        ObjectIdMap ObjectIdMap { get; }

        /// <summary>Whether there is actually a record here</summary>
        bool IsSet { get; }

        /// <summary>The key, which may be inline in this record or an overflow byte[]</summary>
        /// <remarks>Not a ref return as it cannot be changed</remarks>
        ReadOnlySpan<byte> Key { get; }

        /// <summary>Whether the record's key is pinned in memory, e.g. inline in the log vs an overflow byte[]. If this is true, <see cref="PinnedKeyPointer"/> is non-null.</summary>
        bool IsPinnedKey { get; }

        /// <summary>The pointer to the pinned memory if <see cref="IsPinnedKey"/> is true, else null.</summary>
        byte* PinnedKeyPointer { get; }

        /// <summary>Get and set the <see cref="OverflowByteArray"/> if this Key is Overflow; an exception is thrown if it is a pinned pointer (e.g. to a <see cref="SectorAlignedMemory"/>.</summary>
        OverflowByteArray KeyOverflow { get; set; }

        /// <summary>The value <see cref="Span{_byte_}"/>, if this is a String LogRecord; an assertion is raised if it is an Object LogRecord.</summary>
        /// <remarks>Not a ref return as it cannot be changed directly; use <see cref="LogRecord.TrySetValueSpanAndPrepareOptionals(ReadOnlySpan{byte}, in RecordSizeInfo, bool)"/> instead.</remarks>
        Span<byte> ValueSpan { get; }

        /// <summary>The value object, if the value in this record is an IHeapObject; an exception is thrown if it is a Span, either inline or overflow byte[].</summary>
        IHeapObject ValueObject { get; }

        /// <summary>Whether the record's value is pinned in memory, e.g. inline in the log vs an overflow byte[]. If this is true, <see cref="PinnedValuePointer"/> is non-null.</summary>
        bool IsPinnedValue { get; }

        /// <summary>The pointer to the pinned memory if <see cref="IsPinnedValue"/> is true, else null.</summary>
        byte* PinnedValuePointer { get; }

        /// <summary>Get and set the <see cref="OverflowByteArray"/> if this Value is not Overflow; an exception is thrown if it is a pinned pointer (e.g. to a <see cref="SectorAlignedMemory"/>.</summary>
        OverflowByteArray ValueOverflow { get; set; }

        /// <summary>
        /// Expose the value bytes through the <see cref="SpanByteAndMemory"/> abstraction.
        /// </summary>
        /// <remarks>
        /// <para>The shape and lifetime of the returned <see cref="SpanByteAndMemory"/> depend on
        /// the source record:
        /// <list type="bullet">
        /// <item><description><b>In-memory <see cref="LogRecord"/>, inline value</b> — returns a
        /// <see cref="SpanByteAndMemory.SpanByte"/> pointing directly at the allocator's main log
        /// memory (no copy). The pointer is valid <em>only while the enclosing epoch / unsafe
        /// context is held</em>; once the epoch is released the page may be evicted and the
        /// pointer becomes invalid.</description></item>
        /// <item><description><b><see cref="DiskLogRecord"/>, inline value</b> — the bytes are
        /// copied into a pooled <see cref="System.Buffers.IMemoryOwner{T}"/> returned in
        /// <see cref="SpanByteAndMemory.Memory"/>. The underlying
        /// <see cref="SectorAlignedMemory"/> <c>recordBuffer</c> is returned to its pool when the
        /// <see cref="DiskLogRecord"/> is disposed (e.g. at the end of pending completion or when
        /// a scan iterator advances), so the inline pointer would otherwise dangle; the copy
        /// makes the returned <see cref="SpanByteAndMemory.Memory"/> safe beyond the callback /
        /// iterator scope.</description></item>
        /// <item><description><b>Overflow value</b> (either record type) — returns a no-copy
        /// <see cref="BorrowedMemoryOwner"/> wrapping the underlying GC-managed byte[]. The array
        /// stays rooted via the <see cref="System.Memory{T}"/> reference inside the owner, so the
        /// contents survive disposal of the source record (and do not require epoch protection).
        /// </description></item>
        /// </list>
        /// </para>
        /// <para>Consumers that need a stable native pointer (e.g. for SIMD operations) into the
        /// <see cref="SpanByteAndMemory.Memory"/> path MUST call <see cref="System.Memory{T}.Pin"/>
        /// on it and hold the resulting <see cref="System.Buffers.MemoryHandle"/> for the duration
        /// of the operation, otherwise GC compaction may relocate the underlying array.</para>
        /// <para>The caller owns the returned <see cref="SpanByteAndMemory.Memory"/> (when set)
        /// and is responsible for disposing it. <see cref="BorrowedMemoryOwner"/>'s
        /// <see cref="System.IDisposable.Dispose"/> is a no-op; pooled owners returned for the
        /// inline <see cref="DiskLogRecord"/> path return their buffer to the pool on dispose.</para>
        /// <para>Throws if the value is an object.</para>
        /// </remarks>
        SpanByteAndMemory ValueSpanByteAndMemory { get; }

        /// <summary>The ETag of the record, if any (see <see cref="RecordInfo.HasETag"/>; 0 by default.</summary>
        long ETag { get; }

        /// <summary>The Expiration of the record, if any (see <see cref="RecordInfo.HasExpiration"/>; 0 by default.</summary>
        long Expiration { get; }

        /// <summary>If requested by CopyUpdater or InPlaceDeleter, the source ValueObject or ValueOverflow will be cleared immediately (to manage object size tracking most effectively).
        ///     This is called after we have either ensured there is a newer record inserted at tail, or after we have tombstoned the record; either way, we won't be accessing its value.
        ///     The cleared <see cref="IHeapObject"/>, if any, has its <see cref="IDisposable.Dispose"/> invoked.</summary>
        void ClearValueIfHeap();

        /// <summary>Whether this is an instance of <see cref="LogRecord"/></summary>
        bool IsMemoryLogRecord { get; }

        /// <summary>Return this as a ref <see cref="LogRecord"/>, or throw if not <see cref="IsMemoryLogRecord"/></summary>
        ref LogRecord AsMemoryLogRecordRef();

        /// <summary>Whether this is an instance of <see cref="DiskLogRecord"/></summary>
        bool IsDiskLogRecord { get; }

        /// <summary>Return this as a ref <see cref="DiskLogRecord"/>, or throw if not <see cref="IsDiskLogRecord"/></summary>
        ref DiskLogRecord AsDiskLogRecordRef();

        /// <summary>Get the record's field info, for use in calculating required record size</summary>
        RecordFieldInfo GetRecordFieldInfo();

        /// <summary>The total allocated inline size of the main-log record; includes filler length.</summary>
        int AllocatedSize { get; }

        /// <summary>The total used inline portion of the size of the main-log portion of the record; does not include filler length.</summary>
        int ActualSize { get; }

        /// <summary>Calculate the heap memory size of this log record</summary>
        public long CalculateHeapMemorySize();
    }
}