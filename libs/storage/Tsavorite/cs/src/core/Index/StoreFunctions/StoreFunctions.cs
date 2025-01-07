// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Store functions for <typeparamref name="TValue"/>.
    /// </summary>
    /// <remarks>
    /// The implementation takes instances of the supported interfaces (e.g. <see cref="IObjectSerializer{T}"/>) to allow custom
    /// implementation of any/all. We also provide standard implementations for standard types. The design exposes the instances
    /// because there is no need to wrap calls to them with additional functionality. This can be changed to redirect if such wrapper
    /// functionality is needed.
    /// </remarks>
    public struct StoreFunctions<TValue, TKeyComparer, TRecordDisposer>
            (TKeyComparer keyComparer, Func<IObjectSerializer<TValue>> valueSerializerCreator, TRecordDisposer recordDisposer)
            : IStoreFunctions<TValue>
        where TKeyComparer : IKeyComparer
        where TRecordDisposer : IRecordDisposer<TValue>
    {
        #region Fields
        /// <summary>Compare two keys for equality, and get a key's hash code.</summary>
        readonly TKeyComparer keyComparer = keyComparer;

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
        public readonly long GetKeyHashCode64(SpanByte key) => keyComparer.GetHashCode64(key);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool KeysEqual(SpanByte k1, SpanByte k2) => keyComparer.Equals(k1, k2);
        #endregion Key Comparer

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
        public readonly void DisposeValueObject(IHeapObject valueObject, DisposeReason reason) => recordDisposer.DisposeValueObject(valueObject, reason);
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
        public static StoreFunctions<TValue, TKeyComparer, TRecordDisposer> Create<TKeyComparer, TRecordDisposer>
                (TKeyComparer keyComparer, Func<IObjectSerializer<TValue>> valueSerializerCreator, TRecordDisposer recordDisposer)
            where TKeyComparer : IKeyComparer
            where TRecordDisposer : IRecordDisposer<TValue>
            => new(keyComparer, valueSerializerCreator, recordDisposer);

        /// <summary>
        /// Construct a StoreFunctions instance with all types specified and contained instances passed, e.g. for custom objects.
        /// </summary>
        public static StoreFunctions<TValue, TKeyComparer, DefaultRecordDisposer<TValue>> Create<TKeyComparer>(TKeyComparer keyComparer, Func<IObjectSerializer<TValue>> valueSerializerCreator)
            where TKeyComparer : IKeyComparer
            => new(keyComparer, valueSerializerCreator, new DefaultRecordDisposer<TValue>());

        /// <summary>
        /// Construct a StoreFunctions instance with all types specified and contained instances passed, e.g. for custom objects.
        /// </summary>
        public static StoreFunctions<TValue, TKeyComparer, TRecordDisposer> Create<TKeyComparer, TRecordDisposer>(TKeyComparer keyComparer, TRecordDisposer recordDisposer)
            where TKeyComparer : IKeyComparer
            where TRecordDisposer : IRecordDisposer<TValue>
            => new(keyComparer, valueSerializerCreator: null, recordDisposer);

        /// <summary>
        /// Store functions for <typeparamref name="TValue"/> that take only the <paramref name="keyComparer"/>
        /// </summary>
        public static StoreFunctions<TValue, TKeyComparer, DefaultRecordDisposer<TValue>> Create<TKeyComparer>(TKeyComparer keyComparer)
            where TKeyComparer : IKeyComparer
            => new(keyComparer, valueSerializerCreator: null, DefaultRecordDisposer<TValue>.Instance);

        /// <summary>
        /// Store functions for <see cref="SpanByte"/> Key and Value
        /// </summary>
        public static StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer> Create()
            => new(SpanByteComparer.Instance, keySerializerCreator: null, valueSerializerCreator: null, SpanByteRecordDisposer.Instance);
    }
}