// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// The interface to define functions on the TsavoriteKV store itself (rather than a session).
    /// </summary>
    /// <remarks>
    /// The implementation takes instances of the supported interfaces (e.g. <see cref="IObjectSerializer{T}"/>) to allow custom
    /// implementation of any/all. We also provide standard implementations for standard types. The design exposes the instances
    /// because there is no need to wrap calls to them with additional functionality. This can be changed to redirect if such wrapper
    /// functionality is needed.
    /// </remarks>
    public interface IStoreFunctions<TKey, TValue, TKeyComparer, TKeySerializer, TValueSerializer, TRecordDisposer>
        where TKeyComparer: IKeyComparer<TKey> 
        where TKeySerializer: IObjectSerializer<TKey>
        where TValueSerializer : IObjectSerializer<TValue>
        where TRecordDisposer : IRecordDisposer<TKey, TValue>
    {
        /// <summary>
        /// Compare two keys for equality, and get a key's hash code.
        /// </summary>
        TKeyComparer KeyComparer { get; }

        /// <summary>
        /// Serialize a Key to persistent storage
        /// </summary>
        TKeySerializer KeySerializer { get; }

        /// <summary>
        /// Serialize a Value to persistent storage
        /// </summary>
        TValueSerializer ValueSerializer { get; }

        /// <summary>
        /// Dispose a record
        /// </summary>
        TRecordDisposer RecordDisposer { get; }
    }
}
