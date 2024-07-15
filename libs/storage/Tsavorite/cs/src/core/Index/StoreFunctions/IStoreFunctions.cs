// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.IO;

namespace Tsavorite.core
{
    /// <summary>
    /// The interface to define functions on the TsavoriteKV store itself (rather than a session).
    /// </summary>
    public interface IStoreFunctions<TKey, TValue>
    {
        #region Key Comparer
        /// <summary>Get a 64-bit hash code for a key</summary>
        long GetKeyHashCode64(ref TKey key);

        /// <summary>Compare two keys for equality</summary>
        bool KeysEqual(ref TKey k1, ref TKey k2);
        #endregion Key Comparer

        #region Key Serializer
        /// <summary>Indicates whether the Key Serializer is to be used</summary>
        bool HasKeySerializer { get; }

        /// <summary>Begin Key serialization to given stream</summary>
        IObjectSerializer<TKey> BeginSerializeKey(Stream stream);

        /// <summary>Begin Key deserialization from stream</summary>
        IObjectSerializer<TKey> BeginDeserializeKey(Stream stream);
        #endregion Key Serializer

        #region Value Serializer
        /// <summary>Indicates whether the Value Serializer is to be used</summary>
        bool HasValueSerializer { get; }

        /// <summary>Begin Value serialization to given stream</summary>
        IObjectSerializer<TValue> BeginSerializeValue(Stream stream);

        /// <summary>Begin Value deserialization from stream</summary>
        IObjectSerializer<TValue> BeginDeserializeValue(Stream stream);
        #endregion Value Serializer

        #region Record Disposer
        /// <summary>
        /// If true, <see cref="DisposeRecord(ref TKey, ref TValue, DisposeReason)"/> with <see cref="DisposeReason.PageEviction"/> 
        /// is called on page evictions from both readcache and main log. Otherwise, the user can register an Observer and
        /// do any needed disposal there.
        /// </summary>
        bool DisposeOnPageEviction { get; }

        /// <summary>Dispose the Key and Value of a record, if necessary.</summary>
        void DisposeRecord(ref TKey key, ref TValue value, DisposeReason reason);
        #endregion Record Disposer
    }
}
