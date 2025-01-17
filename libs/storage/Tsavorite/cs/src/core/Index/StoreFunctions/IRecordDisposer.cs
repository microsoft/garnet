// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Interface to implement the Disposer component of <see cref="IStoreFunctions{TValue}"/>
    /// </summary>
    public interface IRecordDisposer<TValue>
    {
        /// <summary>
        /// If true, <see cref="DisposeValueObject(IHeapObject, DisposeReason)"/> with <see cref="DisposeReason.PageEviction"/> 
        /// is called on page evictions from both readcache and main log. Otherwise, the user can register an Observer and do any needed disposal there.
        /// </summary>
        public bool DisposeOnPageEviction { get; }

        /// <summary>
        /// Dispose the Key and Value of a record, if necessary. See comments in <see cref="IStoreFunctions{TValue}.DisposeValueObject(TValue, DisposeReason)"/> for details.
        /// </summary>
        void DisposeValueObject(TValue valueObject, DisposeReason reason);
    }

    /// <summary>
    /// Default no-op implementation if <see cref="IRecordDisposer{TValue}"/>
    /// </summary>
    /// <remarks>It is appropriate to call methods on this instance as a no-op.</remarks>
    public struct DefaultRecordDisposer<TValue> : IRecordDisposer<TValue>
    {
        /// <summary>
        /// Default instance
        /// </summary>
        public static readonly DefaultRecordDisposer<TValue> Instance = new();

        /// <summary>
        /// Assumes the key and value have no need of Dispose(), and does nothing.
        /// </summary>
        public readonly bool DisposeOnPageEviction => false;

        /// <summary>
        /// Assumes the key and value have no need of Dispose(), and does nothing.
        /// </summary>
        public readonly void DisposeValueObject(TValue valueObject, DisposeReason reason) { }
    }

    /// <summary>
    /// No-op implementation of <see cref="IRecordDisposer{TValue}"/> for SpanByte
    /// </summary>
    public struct SpanByteRecordDisposer : IRecordDisposer<SpanByte>    // TODO remove for dual
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
        public unsafe void DisposeValueObject(SpanByte valueObject, DisposeReason reason) { }
    }
}