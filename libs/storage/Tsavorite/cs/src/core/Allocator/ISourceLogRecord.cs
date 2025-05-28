// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>An interface to cover either an in-memory or on-disk log record for RCU</summary>
    public unsafe interface ISourceLogRecord
    {
        /// <summary>A ref to the record header</summary>
        ref RecordInfo InfoRef { get; }

        /// <summary>Fast access returning a copy of the record header</summary>
        RecordInfo Info { get; }

        /// <summary>Whether there is actually a record here</summary>
        bool IsSet { get; }

        /// <summary>The key, which may be inline in this record or an overflow byte[]</summary>
        /// <remarks>Not a ref return as it cannot be changed</remarks>
        ReadOnlySpan<byte> Key { get; }

        /// <summary>Whether the record's key is pinned in memory, e.g. inline in the log vs an overflow byte[]. If this is true, <see cref="PinnedKeyPointer"/> is non-null.</summary>
        bool IsPinnedKey { get; }

        /// <summary>The pointer to the pinned memory if <see cref="IsPinnedKey"/> is true, else null.</summary>
        byte* PinnedKeyPointer { get; }

        /// <summary>The value <see cref="Span{_byte_}"/>, if this is a String LogRecord; an assertion is raised if it is an Object LogRecord.</summary>
        /// <remarks>Not a ref return as it cannot be changed directly; use <see cref="LogRecord.TrySetValueSpan(ReadOnlySpan{byte}, in RecordSizeInfo)"/> instead.</remarks>
        Span<byte> ValueSpan { get; }

        /// <summary>The value object, if the value in this record is an IHeapObject; an exception is thrown if it is a Span, either inline or overflow byte[].</summary>
        IHeapObject ValueObject { get; }

        /// <summary>The span of the entire record, if <see cref="RecordInfo.RecordIsInline"/>, else an exception is thrown.</summary>
        ReadOnlySpan<byte> RecordSpan { get; }

        /// <summary>Whether the record's value is pinned in memory, e.g. inline in the log vs an overflow byte[]. If this is true, <see cref="PinnedValuePointer"/> is non-null.</summary>
        bool IsPinnedValue { get; }

        /// <summary>The pointer to the pinned memory if <see cref="IsPinnedValue"/> is true, else null.</summary>
        byte* PinnedValuePointer { get; }

        /// <summary>The ETag of the record, if any (see <see cref="RecordInfo.HasETag"/>; 0 by default.</summary>
        long ETag { get; }

        /// <summary>The Expiration of the record, if any (see <see cref="RecordInfo.HasExpiration"/>; 0 by default.</summary>
        long Expiration { get; }

        /// <summary>If requested by CopyUpdater, the source ValueObject will be cleared immediately (to manage object size tracking most effectively).</summary>
        /// <remarks>The disposer is not inlined, but this is called after object cloning, so the perf hit won't matter</remarks>
        void ClearValueObject(Action<IHeapObject> disposer);

        /// <summary>A shim to "convert" a TSourceLogRecord generic type that is an instance of <see cref="LogRecord"/> to a <see cref="LogRecord"/> type.</summary>
        /// <returns>True if this is a <see cref="LogRecord"/>, with the output <paramref name="logRecord"/> set; else false.</returns>
        bool AsLogRecord(out LogRecord logRecord);

        /// <summary>A shim to "convert" a TSourceLogRecord generic type this is an instance of <see cref="DiskLogRecord"/> to a <see cref="DiskLogRecord"/> type.</summary>
        /// <returns>True if this is a <see cref="DiskLogRecord"/>, with the output <paramref name="diskLogRecord"/> set; else false.</returns>
        bool AsDiskLogRecord(out DiskLogRecord diskLogRecord);

        /// <summary>Get the record's field info, for use in calculating required record size</summary>
        RecordFieldInfo GetRecordFieldInfo();
    }
}