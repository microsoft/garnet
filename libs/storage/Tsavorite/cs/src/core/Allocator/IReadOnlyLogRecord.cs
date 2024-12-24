// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>An interface to cover either an in-memory or on-disk log record for RCU</summary>
    public interface IReadOnlyLogRecord
    {
        /// <summary>A ref to the record header</summary>
        public ref RecordInfo InfoRef { get; }

        /// <summary>Fast access returning a copy of the record header</summary>
        public RecordInfo Info { get; }

        /// <summary>The key, which may be inline serialized or overflow.</summary>
        /// <remarks>Not a ref return as it cannot be changed</remarks>
        public SpanByte Key { get; }

        /// <summary>The value <see cref="SpanByte"/>, if this is a <see cref="StringLogRecord"/>; an exception is thrown if it is an <see cref="ObjectLogRecord"/>.</summary>
        /// <remarks>Not a ref return as it cannot be changed</remarks>
        public SpanByte ValueSpan { get; }

        /// <summary>The value object, if this is an <see cref="ObjectLogRecord"/>; an exception is thrown if it is a <see cref="StringLogRecord"/>.</summary>
        public IHeapObject ValueObject { get; }

        /// <summary>The DBID of the record, if any (see <see cref="RecordInfo.HasDBId"/>; 0 by default.</summary>
        public int DBId { get; }

        /// <summary>The ETag of the record, if any (see <see cref="RecordInfo.HasETag"/>; 0 by default.</summary>
        public long ETag { get; }

        /// <summary>The Expiration of the record, if any (see <see cref="RecordInfo.HasExpiration"/>; 0 by default.</summary>
        public long Expiration { get; }
    }
}