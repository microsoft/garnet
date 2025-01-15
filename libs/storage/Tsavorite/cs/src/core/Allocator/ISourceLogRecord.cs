// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>An interface to cover either an in-memory or on-disk log record for RCU</summary>
    public interface ISourceLogRecord
    {
        /// <summary>Whether this is a record for an object or a SpanByte value</summary>
        bool IsObjectRecord { get; }

        /// <summary>A ref to the record header</summary>
        ref RecordInfo InfoRef { get; }

        /// <summary>Fast access returning a copy of the record header</summary>
        RecordInfo Info { get; }

        /// <summary>Whether there is actually a record here</summary>
        bool IsSet { get; }

        /// <summary>The key:
        ///     <list type="bullet">
        ///     <item>If serialized, then the key is inline in this record (i.e. is below the overflow size).</item>
        ///     <item>If not serialized, then it is a pointer to the key in <see cref="OverflowAllocator"/>.</item>
        ///     </list>
        /// </summary>
        /// <remarks>Not a ref return as it cannot be changed</remarks>
        SpanByte Key { get; }

        /// <summary>The value <see cref="SpanByte"/>, if this is a String LogRecord; an assertion is raised if it is an Object LogRecord.</summary>
        /// <remarks>Not a ref return as it cannot be changed</remarks>
        SpanByte ValueSpan { get; }

        /// <summary>The value object, if this is an Object LogRecord; an exception is thrown if it is a String LogRecord.</summary>
        IHeapObject ValueObject { get; }

        /// <summary>Get a reference to the value; useful when the generic type is needed.</summary>
        ref TValue GetValueRef<TValue>();

        /// <summary>The ETag of the record, if any (see <see cref="RecordInfo.HasETag"/>; 0 by default.</summary>
        long ETag { get; }

        /// <summary>The Expiration of the record, if any (see <see cref="RecordInfo.HasExpiration"/>; 0 by default.</summary>
        long Expiration { get; }

        /// <summary>A shim to "convert" a TSourceLogRecord generic type that is a <see cref="LogRecord"/> to a <see cref="LogRecord"/> type.
        /// Should throw if the TSourceLogRecord is not a <see cref="LogRecord"/>.</summary>
        LogRecord AsLogRecord();

        /// <summary>Get the record's field info, for use in calculating required record size</summary>
        RecordFieldInfo GetRecordFieldInfo();
    }
}