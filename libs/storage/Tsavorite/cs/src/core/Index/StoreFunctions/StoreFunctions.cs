// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Store functions for <typeparamref name="TKey"/> and <typeparamref name="TValue"/>.
    /// </summary>
    /// <remarks>
    /// The implementation takes instances of the supported interfaces (e.g. <see cref="IObjectSerializer{T}"/>) to allow custom
    /// implementation of any/all. We also provide standard implementations for standard types. The design exposes the instances
    /// because there is no need to wrap calls to them with additional functionality. This can be changed to redirect if such wrapper
    /// functionality is needed.
    /// </remarks>
    public struct StoreFunctions<TKey, TValue, TKeyComparer, TRecordDisposer>
            (TKeyComparer keyComparer, Func<IObjectSerializer<TKey>> keySerializerCreator, Func<IObjectSerializer<TValue>> valueSerializerCreator, TRecordDisposer recordDisposer)
            : IStoreFunctions<TKey, TValue>
        where TKeyComparer : IKeyComparer<TKey>
        where TRecordDisposer : IRecordDisposer<TKey, TValue>
    {
        #region Fields
        /// <summary>Compare two keys for equality, and get a key's hash code.</summary>
        readonly TKeyComparer keyComparer = keyComparer;

        /// <summary>Serialize a Key to persistent storage</summary>
        readonly Func<IObjectSerializer<TKey>> keySerializerCreator = keySerializerCreator;

        /// <summary>Serialize a Value to persistent storage</summary>
        readonly Func<IObjectSerializer<TValue>> valueSerializerCreator = valueSerializerCreator;

        /// <summary>Dispose a record</summary>
        readonly TRecordDisposer recordDisposer = recordDisposer;

        /// <summary>Optional checkpoint completion callback, set separately from ctor.</summary>
        Action checkpointCompletionCallback = () => { };
        #endregion Fields

        #region Key Comparer
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetKeyHashCode64(ref TKey key) => keyComparer.GetHashCode64(ref key);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool KeysEqual(ref TKey k1, ref TKey k2) => keyComparer.Equals(ref k1, ref k2);
        #endregion Key Comparer

        #region Key Serializer
        /// <inheritdoc/>
        public readonly bool HasKeySerializer => keySerializerCreator is not null;

        /// <inheritdoc/>
        public readonly IObjectSerializer<TKey> BeginSerializeKey(Stream stream)
        {
            var keySerializer = keySerializerCreator();
            keySerializer.BeginSerialize(stream);
            return keySerializer;
        }

        /// <inheritdoc/>
        public readonly IObjectSerializer<TKey> BeginDeserializeKey(Stream stream)
        {
            var keySerializer = keySerializerCreator();
            keySerializer.BeginDeserialize(stream);
            return keySerializer;
        }
        #endregion Key Serializer

        #region Value Serializer
        /// <inheritdoc/>
        public readonly bool HasValueSerializer => valueSerializerCreator is not null;

        /// <inheritdoc/>
        public readonly IObjectSerializer<TValue> BeginSerializeValue(Stream stream)
        {
            var valueSerializer = valueSerializerCreator();
            valueSerializer.BeginSerialize(stream);
            return valueSerializer;
        }

        /// <inheritdoc/>
        public readonly IObjectSerializer<TValue> BeginDeserializeValue(Stream stream)
        {
            var valueSerializer = valueSerializerCreator();
            valueSerializer.BeginDeserialize(stream);
            return valueSerializer;
        }
        #endregion Value Serializer

        #region Record Disposer
        /// <inheritdoc/>
        public readonly bool DisposeOnPageEviction => recordDisposer.DisposeOnPageEviction;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void DisposeRecord(ref TKey key, ref TValue value, DisposeReason reason, int newKeySize) => recordDisposer.DisposeRecord(ref key, ref value, reason, newKeySize);
        #endregion Record Disposer

        #region Checkpoint Completion
        /// <inheritdoc/>
        public void SetCheckpointCompletedCallback(Action callback) => checkpointCompletionCallback = callback;

        /// <inheritdoc/>
        public readonly void OnCheckpointCompleted() => checkpointCompletionCallback();
        #endregion Checkpoint Completion
    }

    /// <summary>
    /// A non-parameterized version of StoreFunctions that provides type-reduced Create() methods.
    /// </summary>
    public struct StoreFunctions<TKey, TValue>
    {
        /// <summary>
        /// Construct a StoreFunctions instance with all types specified and contained instances passed, e.g. for custom objects.
        /// </summary>
        public static StoreFunctions<TKey, TValue, TKeyComparer, TRecordDisposer> Create<TKeyComparer, TRecordDisposer>
                (TKeyComparer keyComparer, Func<IObjectSerializer<TKey>> keySerializerCreator, Func<IObjectSerializer<TValue>> valueSerializerCreator, TRecordDisposer recordDisposer)
            where TKeyComparer : IKeyComparer<TKey>
            where TRecordDisposer : IRecordDisposer<TKey, TValue>
            => new(keyComparer, keySerializerCreator, valueSerializerCreator, recordDisposer);

        /// <summary>
        /// Construct a StoreFunctions instance with all types specified and contained instances passed, e.g. for custom objects.
        /// </summary>
        public static StoreFunctions<TKey, TValue, TKeyComparer, DefaultRecordDisposer<TKey, TValue>> Create<TKeyComparer>
                (TKeyComparer keyComparer, Func<IObjectSerializer<TKey>> keySerializerCreator, Func<IObjectSerializer<TValue>> valueSerializerCreator)
            where TKeyComparer : IKeyComparer<TKey>
            => new(keyComparer, keySerializerCreator, valueSerializerCreator, new DefaultRecordDisposer<TKey, TValue>());

        /// <summary>
        /// Construct a StoreFunctions instance with all types specified and contained instances passed, e.g. for custom objects.
        /// </summary>
        public static StoreFunctions<TKey, TValue, TKeyComparer, TRecordDisposer> Create<TKeyComparer, TRecordDisposer>
                (TKeyComparer keyComparer, TRecordDisposer recordDisposer)
            where TKeyComparer : IKeyComparer<TKey>
            where TRecordDisposer : IRecordDisposer<TKey, TValue>
            => new(keyComparer, keySerializerCreator: null, valueSerializerCreator: null, recordDisposer);

        /// <summary>
        /// Store functions for <typeparamref name="TKey"/> and <typeparamref name="TValue"/> that take only the <paramref name="keyComparer"/>
        /// </summary>
        public static StoreFunctions<TKey, TValue, TKeyComparer, DefaultRecordDisposer<TKey, TValue>> Create<TKeyComparer>
                (TKeyComparer keyComparer)
            where TKeyComparer : IKeyComparer<TKey>
            => new(keyComparer, keySerializerCreator: null, valueSerializerCreator: null, DefaultRecordDisposer<TKey, TValue>.Instance);

        /// <summary>
        /// Store functions for <see cref="SpanByte"/> Key and Value
        /// </summary>
        public static StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer> Create()
            => new(SpanByteComparer.Instance, keySerializerCreator: null, valueSerializerCreator: null, SpanByteRecordDisposer.Instance);
    }
}