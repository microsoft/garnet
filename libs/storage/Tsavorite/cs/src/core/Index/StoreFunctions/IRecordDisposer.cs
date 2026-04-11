// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Interface to implement the Disposer component of <see cref="IStoreFunctions"/>
    /// </summary>
    public interface IRecordDisposer
    {
        /// <summary>
        /// If true, <see cref="DisposeRecord(ref LogRecord, DisposeReason)"/> is called per record
        /// during page evictions from both readcache and main log, allowing cleanup of external resources.
        /// </summary>
        public bool DisposeOnPageEviction { get; }

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
    }

    /// <summary>
    /// Default no-op implementation if <see cref="IRecordDisposer"/>
    /// </summary>
    /// <remarks>It is appropriate to call methods on this instance as a no-op.</remarks>
    public struct DefaultRecordDisposer : IRecordDisposer
    {
        /// <summary>
        /// Default instance
        /// </summary>
        public static readonly DefaultRecordDisposer Instance = new();

        /// <summary>
        /// Assumes the key and value have no need of Dispose(), and does nothing.
        /// </summary>
        public readonly bool DisposeOnPageEviction => false;

        /// <summary>
        /// Assumes the key and value have no need of Dispose(), and does nothing.
        /// </summary>
        public readonly void DisposeValueObject(IHeapObject valueObject, DisposeReason reason) { }
    }

    /// <summary>
    /// No-op implementation of <see cref="IRecordDisposer"/> for SpanByte
    /// </summary>
    public struct SpanByteRecordDisposer : IRecordDisposer    // TODO remove for dual
    {
        /// <summary>
        /// Default instance
        /// </summary>
        public static readonly SpanByteRecordDisposer Instance = new();

        /// <summary>
        /// Assumes the key and value have no need of Dispose(), and does nothing.
        /// </summary>
        public readonly bool DisposeOnPageEviction => false;

        /// <summary>No-op implementation because SpanByte values have no need for disposal.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void DisposeValueObject(IHeapObject valueObject, DisposeReason reason) { }
    }
}