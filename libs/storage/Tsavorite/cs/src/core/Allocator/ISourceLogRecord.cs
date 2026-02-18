// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>An interface to cover either an in-memory or on-disk log record for RCU</summary>
    public unsafe interface ISourceLogRecord
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

        /// <summary>The ETag of the record, if any (see <see cref="RecordInfo.HasETag"/>; 0 by default.</summary>
        long ETag { get; }

        /// <summary>The Expiration of the record, if any (see <see cref="RecordInfo.HasExpiration"/>; 0 by default.</summary>
        long Expiration { get; }

        /// <summary>If requested by CopyUpdater or InPlaceDeleter, the source ValueObject or ValueOverflow will be cleared immediately (to manage object size tracking most effectively).
        ///     This is called after we have either ensured there is a newer record inserted at tail, or after we have tombstoned the record; either way, we won't be accessing its value.</summary>
        /// <remarks>The disposer is not inlined, but this is called after object cloning, so the perf hit won't matter</remarks>
        /// <returns>True if we did clear a heap object or overflow, else false</returns>
        void ClearValueIfHeap(Action<IHeapObject> disposer);

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