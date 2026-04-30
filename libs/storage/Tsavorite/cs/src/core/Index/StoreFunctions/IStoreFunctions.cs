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

        #region Record Triggers
        /// <inheritdoc cref="IRecordTriggers.CallOnFlush"/>
        bool CallOnFlush { get; }

        /// <inheritdoc cref="IRecordTriggers.CallOnEvict"/>
        bool CallOnEvict { get; }

        /// <inheritdoc cref="IRecordTriggers.CallOnDiskRead"/>
        bool CallOnDiskRead { get; }

        /// <inheritdoc cref="IRecordTriggers.OnDispose"/>
        void OnDispose(ref LogRecord logRecord, DisposeReason reason);

        /// <inheritdoc cref="IRecordTriggers.OnDisposeDiskRecord"/>
        void OnDisposeDiskRecord(ref DiskLogRecord logRecord, DisposeReason reason);

        /// <inheritdoc cref="IRecordTriggers.OnFlush"/>
        void OnFlush(ref LogRecord logRecord);

        /// <inheritdoc cref="IRecordTriggers.OnEvict"/>
        void OnEvict(ref LogRecord logRecord, EvictionSource source);

        /// <inheritdoc cref="IRecordTriggers.OnDiskRead"/>
        void OnDiskRead(ref LogRecord logRecord);

        /// <inheritdoc cref="IRecordTriggers.OnRecovery"/>
        void OnRecovery(System.Guid checkpointToken);

        /// <inheritdoc cref="IRecordTriggers.OnRecoverySnapshotRead"/>
        void OnRecoverySnapshotRead(ref LogRecord logRecord);

        /// <inheritdoc cref="IRecordTriggers.OnCheckpoint"/>
        void OnCheckpoint(CheckpointTrigger trigger, System.Guid checkpointToken);
        #endregion Record Triggers

        #region Checkpoint Completion
        /// <summary>Set the parameterless checkpoint completion callback.</summary>
        void SetCheckpointCompletedCallback(Action callback);

        /// <summary>Called when a checkpoint has completed.</summary>
        void OnCheckpointCompleted();
        #endregion Checkpoint Completion
    }
}