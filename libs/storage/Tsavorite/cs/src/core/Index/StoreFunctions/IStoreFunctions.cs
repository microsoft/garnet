// Copyright (c) Microsoft Corporation.
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
        long GetKeyHashCode64<TKey>(TKey key)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            ;

        /// <summary>Compare two keys for equality</summary>
        bool KeysEqual<TFirstKey, TSecondKey>(TFirstKey k1, TSecondKey k2)
            where TFirstKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TSecondKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            ;
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
        /// If true, <see cref="DisposeRecord(ref LogRecord, DisposeReason)"/> is called per record
        /// during page evictions from both readcache and main log, allowing cleanup of external resources.
        /// </summary>
        bool DisposeOnPageEviction { get; }

        /// <inheritdoc cref="IRecordTrigger.CallOnFlush"/>
        bool CallOnFlush { get; }

        /// <inheritdoc cref="IRecordTrigger.CallOnDiskRead"/>
        bool CallOnDiskRead { get; }

        /// <summary>Dispose the Value of a record, if necessary.</summary>
        void DisposeValueObject(IHeapObject valueObject, DisposeReason reason);

        /// <summary>Called during page eviction to allow the application to clean up external resources.</summary>
        void DisposeRecord(ref LogRecord logRecord, DisposeReason reason);

        /// <summary>Called per valid record during page flush to allow snapshotting external resources.</summary>
        void OnFlushRecord(ref LogRecord logRecord);

        /// <summary>Called per record loaded from disk to allow invalidating stale resource handles.</summary>
        void OnDiskReadRecord(ref LogRecord logRecord);
        #endregion Record Disposer

        #region Checkpoint Completion
        /// <summary>Set the parameterless checkpoint completion callback.</summary>
        void SetCheckpointCompletedCallback(Action callback);

        /// <summary>Called when a checkpoint has completed.</summary>
        void OnCheckpointCompleted();
        #endregion Checkpoint Completion
    }
}