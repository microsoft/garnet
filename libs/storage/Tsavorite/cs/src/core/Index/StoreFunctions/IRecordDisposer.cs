// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Interface to implement the Disposer component of <see cref="IStoreFunctions{Key, Value, TKeyComparer, TKeySerializer, TValueSerializer, TRecordDisposer}"/>
    /// </summary>
    public interface IRecordDisposer<Key, Value>
    {
        /// <summary>
        /// If true, <see cref="DisposeRecord(ref Key, ref Value, DisposeReason)"/> with <see cref="DisposeReason.PageEviction"/> 
        /// is called on page evictions from both readcache and main log. Otherwise, the user can register an Observer and
        /// do any needed disposal there.
        /// </summary>
        public bool DisposeOnPageEviction { get; }

        /// <summary>
        /// Dispose the Key and Value of a record, if necessary.
        /// </summary>
        void DisposeRecord(ref Key key, ref Value value, DisposeReason reason);
    }

    /// <summary>
    /// Default no-op implementation if <see cref="IRecordDisposer{Key, Value}"/>
    /// </summary>
    /// <remarks>It is appropriate to call methods on this instance as a no-op.</remarks>
    public class DefaultRecordDisposer<Key, Value> : IRecordDisposer<Key, Value>
    {
        /// <summary>
        /// Default instance
        /// </summary>
        public static readonly DefaultRecordDisposer<Key, Value> Default = new();

        /// <summary>
        /// Assumes the key and value have no need of Dispose(), and does nothing.
        /// </summary>
        public bool DisposeOnPageEviction => false;

        /// <summary>
        /// Assumes the key and value have no need of Dispose(), and does nothing.
        /// </summary>
        public void DisposeRecord(ref Key key, ref Value value, DisposeReason reason)
        {
        }
    }

    /// <summary>
    /// Default no-op implementation if <see cref="IRecordDisposer{Key, Value}"/> for SpanByte
    /// </summary>
    public class SpanByteRecordDisposer : DefaultRecordDisposer<SpanByte, SpanByte>
    {
    }
}
