// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Interface to implement the Disposer component of <see cref="IStoreFunctions"/>
    /// </summary>
    public interface IRecordTrigger
    {
        /// <summary>
        /// If true, <see cref="DisposeRecord(ref LogRecord, DisposeReason)"/> is called per record
        /// during page evictions from both readcache and main log, allowing cleanup of external resources.
        /// </summary>
        public bool DisposeOnPageEviction { get; }

        /// <summary>
        /// If true, <see cref="OnFlushRecord(ref LogRecord)"/> is called per valid record on the
        /// original in-memory page before it is flushed to disk. Allows snapshotting external resources
        /// and setting flags on the live record.
        /// </summary>
        public bool CallOnFlush { get; }

        /// <summary>
        /// If true, <see cref="OnDiskReadRecord(ref LogRecord)"/> is called per record loaded from
        /// disk into memory (recovery, delta log apply, pending reads, push scans).
        /// </summary>
        public bool CallOnDiskRead { get; }

        /// <summary>
        /// Dispose the Key and Value of a record, if necessary. See comments in <see cref="IStoreFunctions.DisposeValueObject(IHeapObject, DisposeReason)"/> for details.
        /// </summary>
        void DisposeValueObject(IHeapObject valueObject, DisposeReason reason);

        /// <summary>
        /// Called during record disposal to allow the application to clean up external resources.
        /// The application can inspect RecordType, ValueIsObject, or any other record property
        /// to decide what cleanup is needed. Default implementation is a no-op.
        /// </summary>
        void DisposeRecord(ref LogRecord logRecord, DisposeReason reason) { }

        /// <summary>
        /// Called per valid record on the original in-memory page before flush to disk.
        /// Allows the application to snapshot external resources and set flags on the live record
        /// so the next operation can promote it to tail.
        /// Only called when <see cref="CallOnFlush"/> is true. Default implementation is a no-op.
        /// </summary>
        void OnFlushRecord(ref LogRecord logRecord) { }

        /// <summary>
        /// Called per record loaded from disk into memory. Allows the application to invalidate
        /// stale external resource handles (e.g. native pointers from a previous process).
        /// Only called when <see cref="CallOnDiskRead"/> is true. Default implementation is a no-op.
        /// </summary>
        void OnDiskReadRecord(ref LogRecord logRecord) { }
    }

    /// <summary>
    /// Default no-op implementation if <see cref="IRecordTrigger"/>
    /// </summary>
    /// <remarks>It is appropriate to call methods on this instance as a no-op.</remarks>
    public struct DefaultRecordTrigger : IRecordTrigger
    {
        /// <summary>
        /// Default instance
        /// </summary>
        public static readonly DefaultRecordTrigger Instance = new();

        /// <summary>
        /// Assumes the key and value have no need of Dispose(), and does nothing.
        /// </summary>
        public readonly bool DisposeOnPageEviction => false;

        /// <inheritdoc/>
        public readonly bool CallOnFlush => false;

        /// <inheritdoc/>
        public readonly bool CallOnDiskRead => false;

        /// <summary>
        /// Assumes the key and value have no need of Dispose(), and does nothing.
        /// </summary>
        public readonly void DisposeValueObject(IHeapObject valueObject, DisposeReason reason) { }
    }

    /// <summary>
    /// No-op implementation of <see cref="IRecordTrigger"/> for SpanByte
    /// </summary>
    public struct SpanByteRecordTrigger : IRecordTrigger    // TODO remove for dual
    {
        /// <summary>
        /// Default instance
        /// </summary>
        public static readonly SpanByteRecordTrigger Instance = new();

        /// <summary>
        /// Assumes the key and value have no need of Dispose(), and does nothing.
        /// </summary>
        public readonly bool DisposeOnPageEviction => false;

        /// <inheritdoc/>
        public readonly bool CallOnFlush => false;

        /// <inheritdoc/>
        public readonly bool CallOnDiskRead => false;

        /// <summary>No-op implementation because SpanByte values have no need for disposal.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void DisposeValueObject(IHeapObject valueObject, DisposeReason reason) { }
    }
}