// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Store functions for an instance of TsavoriteKV.
    /// </summary>
    /// <remarks>
    /// The implementation takes instances of the supported interfaces (e.g. <see cref="IObjectSerializer{T}"/>) to allow custom
    /// implementation of any/all. We also provide standard implementations for standard types. The design exposes the instances
    /// because there is no need to wrap calls to them with additional functionality. This can be changed to redirect if such wrapper
    /// functionality is needed.
    /// </remarks>
    public struct StoreFunctions<TKeyComparer, TRecordTriggers>(TKeyComparer keyComparer, Func<IObjectSerializer<IHeapObject>> valueSerializerCreator, TRecordTriggers recordTriggers) : IStoreFunctions
        where TKeyComparer : IKeyComparer
        where TRecordTriggers : IRecordTriggers
    {
        #region Fields
        /// <summary>Compare two keys for equality, and get a key's hash code.</summary>
        readonly TKeyComparer keyComparer = keyComparer;

        /// <summary>Serialize a Value to persistent storage</summary>
        readonly Func<IObjectSerializer<IHeapObject>> valueSerializerCreator = valueSerializerCreator;

        /// <summary>Dispose a record</summary>
        readonly TRecordTriggers recordTriggers = recordTriggers;

        /// <summary>Optional checkpoint completion callback, set separately from ctor.</summary>
        Action checkpointCompletionCallback = () => { };
        #endregion Fields

        #region Key Comparer
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetKeyHashCode64<TKey>(TKey key)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            => keyComparer.GetHashCode64(key);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool KeysEqual<TFirstKey, TSecondKey>(TFirstKey k1, TSecondKey k2)
            where TFirstKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TSecondKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            => keyComparer.Equals(k1, k2);
        #endregion Key Comparer

        #region Value Serializer
        /// <inheritdoc/>
        public readonly IObjectSerializer<IHeapObject> CreateValueObjectSerializer() => valueSerializerCreator is null ? default : valueSerializerCreator();

        /// <inheritdoc/>
        public readonly bool HasValueSerializer => valueSerializerCreator is not null;

        /// <inheritdoc/>
        public readonly IObjectSerializer<IHeapObject> BeginSerializeValue(Stream stream)
        {
            var valueSerializer = CreateValueObjectSerializer();
            valueSerializer.BeginSerialize(stream);
            return valueSerializer;
        }

        /// <inheritdoc/>
        public readonly IObjectSerializer<IHeapObject> BeginDeserializeValue(Stream stream)
        {
            var valueSerializer = CreateValueObjectSerializer();
            valueSerializer.BeginDeserialize(stream);
            return valueSerializer;
        }
        #endregion Value Serializer

        #region Record Triggers
        /// <inheritdoc/>
        public readonly bool CallOnFlush => recordTriggers.CallOnFlush;

        /// <inheritdoc/>
        public readonly bool CallOnEvict => recordTriggers.CallOnEvict;

        /// <inheritdoc/>
        public readonly bool CallOnDiskRead => recordTriggers.CallOnDiskRead;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void OnDispose(ref LogRecord logRecord, DisposeReason reason) => recordTriggers.OnDispose(ref logRecord, reason);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void OnDisposeDiskRecord(ref DiskLogRecord logRecord, DisposeReason reason) => recordTriggers.OnDisposeDiskRecord(ref logRecord, reason);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void OnFlush(ref LogRecord logRecord) => recordTriggers.OnFlush(ref logRecord);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void OnEvict(ref LogRecord logRecord, EvictionSource source) => recordTriggers.OnEvict(ref logRecord, source);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void OnDiskRead(ref LogRecord logRecord) => recordTriggers.OnDiskRead(ref logRecord);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void OnRecovery(System.Guid checkpointToken) => recordTriggers.OnRecovery(checkpointToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void OnRecoverySnapshotRead(ref LogRecord logRecord) => recordTriggers.OnRecoverySnapshotRead(ref logRecord);

        /// <inheritdoc/>
        public readonly void OnCheckpoint(CheckpointTrigger trigger, System.Guid checkpointToken) => recordTriggers.OnCheckpoint(trigger, checkpointToken);
        #endregion Record Triggers

        #region Checkpoint Completion
        /// <inheritdoc/>
        public void SetCheckpointCompletedCallback(Action callback) => checkpointCompletionCallback = callback;

        /// <inheritdoc/>
        public readonly void OnCheckpointCompleted() => checkpointCompletionCallback();
        #endregion Checkpoint Completion
    }

    /// <summary>
    /// A minimally-parameterized version of StoreFunctions that provides type-reduced Create() methods.
    /// </summary>
    public struct StoreFunctions
    {
        /// <summary>
        /// Construct a StoreFunctions instance with all types specified and contained instances passed, e.g. for custom objects.
        /// </summary>
        public static StoreFunctions<TKeyComparer, TRecordTriggers> Create<TKeyComparer, TRecordTriggers>
                (TKeyComparer keyComparer, Func<IObjectSerializer<IHeapObject>> valueSerializerCreator, TRecordTriggers recordTriggers)
            where TKeyComparer : IKeyComparer
            where TRecordTriggers : IRecordTriggers
            => new(keyComparer, valueSerializerCreator, recordTriggers);

        /// <summary>
        /// Construct a StoreFunctions instance with all types specified and contained instances passed, e.g. for custom objects.
        /// </summary>
        public static StoreFunctions<TKeyComparer, DefaultRecordTriggers> Create<TKeyComparer>(TKeyComparer keyComparer, Func<IObjectSerializer<IHeapObject>> valueSerializerCreator)
            where TKeyComparer : IKeyComparer
            => new(keyComparer, valueSerializerCreator, DefaultRecordTriggers.Instance);

        /// <summary>
        /// Construct a StoreFunctions instance with all types specified and contained instances passed, e.g. for custom objects.
        /// </summary>
        public static StoreFunctions<TKeyComparer, TRecordTriggers> Create<TKeyComparer, TRecordTriggers>(TKeyComparer keyComparer, TRecordTriggers recordTriggers)
            where TKeyComparer : IKeyComparer
            where TRecordTriggers : IRecordTriggers
            => new(keyComparer, valueSerializerCreator: null, recordTriggers);

        /// <summary>
        /// Store functions that take only the <paramref name="keyComparer"/>
        /// </summary>
        public static StoreFunctions<TKeyComparer, DefaultRecordTriggers> Create<TKeyComparer>(TKeyComparer keyComparer)
            where TKeyComparer : IKeyComparer
            => new(keyComparer, valueSerializerCreator: null, DefaultRecordTriggers.Instance);

        /// <summary>
        /// Store functions for <see cref="Span{_byte_}"/> Key and Value
        /// </summary>
        public static StoreFunctions<SpanByteComparer, DefaultRecordTriggers> Create()
            => new(SpanByteComparer.Instance, valueSerializerCreator: null, DefaultRecordTriggers.Instance);
    }
}