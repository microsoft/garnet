// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Per-record lifecycle callbacks invoked by the store at key events:
    /// flush to disk, eviction, disposal, and disk read.
    /// </summary>
    public interface IRecordTriggers
    {
        /// <summary>
        /// If true, <see cref="OnFlush(ref LogRecord)"/> is called per valid record on the
        /// original in-memory page before it is flushed to disk.
        /// </summary>
        public bool CallOnFlush { get; }

        /// <summary>
        /// If true, <see cref="OnEvict(ref LogRecord)"/> is called per non-tombstoned record
        /// when a page is evicted past HeadAddress.
        /// </summary>
        public bool CallOnEvict { get; }

        /// <summary>
        /// If true, <see cref="OnDiskRead(ref LogRecord)"/> is called per record loaded from
        /// disk into memory (recovery, delta log apply, pending reads, push scans).
        /// </summary>
        public bool CallOnDiskRead { get; }

        /// <summary>
        /// Called when a heap value object needs to be disposed (e.g. during ClearHeapFields).
        /// </summary>
        void OnDisposeValueObject(IHeapObject valueObject, DisposeReason reason);

        /// <summary>
        /// Called when a record is disposed due to delete, CAS failure, or other store-internal reasons.
        /// NOT called for page eviction (use <see cref="OnEvict"/> instead).
        /// Default implementation is a no-op.
        /// </summary>
        void OnDispose(ref LogRecord logRecord, DisposeReason reason) { }

        /// <summary>
        /// Called per valid record on the original in-memory page before flush to disk.
        /// Allows the application to snapshot external resources and set flags on the live record.
        /// Only called when <see cref="CallOnFlush"/> is true. Default implementation is a no-op.
        /// </summary>
        void OnFlush(ref LogRecord logRecord) { }

        /// <summary>
        /// Called per non-tombstoned record when a page is evicted past HeadAddress.
        /// Allows the application to free external resources (e.g. native memory).
        /// Only called when <see cref="CallOnEvict"/> is true. Default implementation is a no-op.
        /// </summary>
        void OnEvict(ref LogRecord logRecord) { }

        /// <summary>
        /// Called per record loaded from disk into memory. Allows the application to invalidate
        /// stale external resource handles (e.g. native pointers from a previous process).
        /// Only called when <see cref="CallOnDiskRead"/> is true. Default implementation is a no-op.
        /// </summary>
        void OnDiskRead(ref LogRecord logRecord) { }
    }

    /// <summary>
    /// Default no-op implementation of <see cref="IRecordTriggers"/>.
    /// </summary>
    /// <remarks>It is appropriate to call methods on this instance as a no-op.</remarks>
    public struct DefaultRecordTrigger : IRecordTriggers
    {
        /// <summary>Default instance.</summary>
        public static readonly DefaultRecordTrigger Instance = new();

        /// <inheritdoc/>
        public readonly bool CallOnFlush => false;

        /// <inheritdoc/>
        public readonly bool CallOnEvict => false;

        /// <inheritdoc/>
        public readonly bool CallOnDiskRead => false;

        /// <inheritdoc/>
        public readonly void OnDisposeValueObject(IHeapObject valueObject, DisposeReason reason) { }
    }

    /// <summary>
    /// No-op implementation of <see cref="IRecordTriggers"/> for SpanByte.
    /// </summary>
    public struct SpanByteRecordTrigger : IRecordTriggers    // TODO remove for dual
    {
        /// <summary>Default instance.</summary>
        public static readonly SpanByteRecordTrigger Instance = new();

        /// <inheritdoc/>
        public readonly bool CallOnFlush => false;

        /// <inheritdoc/>
        public readonly bool CallOnEvict => false;

        /// <inheritdoc/>
        public readonly bool CallOnDiskRead => false;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void OnDisposeValueObject(IHeapObject valueObject, DisposeReason reason) { }
    }
}