// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;

namespace Tsavorite.core
{
    /// <summary>
    /// The interface to define functions on the TsavoriteKV store itself (rather than a session).
    /// </summary>
    public interface IStoreFunctions
    {
        #region Key Comparer
        /// <summary>Get a 64-bit hash code for a key</summary>
        long GetKeyHashCode64(ReadOnlySpan<byte> key);

        /// <summary>Get a 64-bit hash code for a key and namespace</summary>
        long GetKeyHashCode64(ReadOnlySpan<byte> key, ReadOnlySpan<byte> namespaceBytes);

        /// <summary>Compare two keys for equality</summary>
        bool KeysEqual(ReadOnlySpan<byte> k1, ReadOnlySpan<byte> k2);

        /// <summary>Compare two keys in namespaces for equality</summary>
        bool KeysEqual(ReadOnlySpan<byte> k1, ReadOnlySpan<byte> ns1, ReadOnlySpan<byte> k2, ReadOnlySpan<byte> ns2);
        #endregion Key Comparer

        #region Value Serializer
        /// <summary>Creates an instance of the Value Serializer</summary>
        IObjectSerializer<IHeapObject> CreateValueObjectSerializer();

        /// <summary>Indicates whether the Value Serializer is to be used</summary>
        bool HasValueSerializer { get; }

        /// <summary>Instatiate a ValueSerializer and begin Value serialization to the given stream.</summary>
        /// <remarks>This must instantiate a new serializer as multiple threads may be serializing or deserializing.</remarks>
        IObjectSerializer<IHeapObject> BeginSerializeValue(Stream stream);

        /// <summary>Instatiate a ValueSerializer and begin Value deserialization from the given stream.</summary>
        /// <remarks>This must instantiate a new serializer as multiple threads may be serializing or deserializing.</remarks>
        IObjectSerializer<IHeapObject> BeginDeserializeValue(Stream stream);
        #endregion Value Serializer

        #region Record Disposer
        /// <summary>
        /// If true, <see cref="DisposeValueObject(IHeapObject, DisposeReason)"/> with <see cref="DisposeReason.PageEviction"/> 
        /// is called on page evictions from both readcache and main log. Otherwise, the user can register an Observer and
        /// do any needed disposal there.
        /// </summary>
        bool DisposeOnPageEviction { get; }

        /// <summary>Dispose the Value of a record, if necessary.</summary>
        void DisposeValueObject(IHeapObject valueObject, DisposeReason reason);
        #endregion Record Disposer

        #region Checkpoint Completion
        /// <summary>Set the parameterless checkpoint completion callback.</summary>
        void SetCheckpointCompletedCallback(Action callback);

        /// <summary>Called when a checkpoint has completed.</summary>
        void OnCheckpointCompleted();
        #endregion Checkpoint Completion
    }
}