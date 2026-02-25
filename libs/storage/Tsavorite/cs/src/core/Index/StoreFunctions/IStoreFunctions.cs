// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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

        /// <summary>Instatiate a KeySerializer and begin Key serialization to the given stream.</summary>
        /// <remarks>This must instantiate a new serializer as multiple threads may be serializing or deserializing.</remarks>
        IObjectSerializer<TKey> BeginSerializeKey(Stream stream);

        /// <summary>Instatiate a KeySerializer and begin Key deserialization from the given stream.</summary>
        /// <remarks>This must instantiate a new serializer as multiple threads may be serializing or deserializing.</remarks>
        IObjectSerializer<TKey> BeginDeserializeKey(Stream stream);
        #endregion Key Serializer

        #region Value Serializer
        /// <summary>Indicates whether the Value Serializer is to be used</summary>
        bool HasValueSerializer { get; }

        /// <summary>Instatiate a ValueSerializer and begin Value serialization to the given stream.</summary>
        /// <remarks>This must instantiate a new serializer as multiple threads may be serializing or deserializing.</remarks>
        IObjectSerializer<TValue> BeginSerializeValue(Stream stream);

        /// <summary>Instatiate a ValueSerializer and begin Value deserialization from the given stream.</summary>
        /// <remarks>This must instantiate a new serializer as multiple threads may be serializing or deserializing.</remarks>
        IObjectSerializer<TValue> BeginDeserializeValue(Stream stream);
        #endregion Value Serializer

        #region Record Disposer
        /// <summary>
        /// If true, <see cref="DisposeRecord(ref TKey, ref TValue, DisposeReason, int)"/> with <see cref="DisposeReason.PageEviction"/> 
        /// is called on page evictions from both readcache and main log. Otherwise, the user can register an Observer and
        /// do any needed disposal there.
        /// </summary>
        bool DisposeOnPageEviction { get; }

        /// <summary>Dispose the Key and Value of a record, if necessary.</summary>
        /// <param name="key">The key for the record</param>
        /// <param name="value">The value for the record</param>
        /// <param name="newKeySize">For <see cref="DisposeReason.RevivificationFreeList"/> only, this is a record from the freelist and we may be disposing the key as well as value
        ///     (it is -1 when revivifying a record in the hash chain or when doing a RETRY; for these the key does not change)</param>
        void DisposeRecord(ref TKey key, ref TValue value, DisposeReason reason, int newKeySize = -1);
        #endregion Record Disposer

        #region Checkpoint Completion
        /// <summary>Set the parameterless checkpoint completion callback.</summary>
        void SetCheckpointCompletedCallback(Action callback);

        /// <summary>Called when a checkpoint has completed.</summary>
        void OnCheckpointCompleted();
        #endregion Checkpoint Completion
    }
}