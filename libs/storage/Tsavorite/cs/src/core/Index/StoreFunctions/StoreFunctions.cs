// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

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
    public struct StoreFunctions<TKey, TValue, TKeyComparer, TKeySerializer, TValueSerializer, TRecordDisposer>
            (TKeyComparer keyComparer, TKeySerializer keySerializer, TValueSerializer valueSerializer, TRecordDisposer recordDisposer)
            : IStoreFunctions<TKey, TValue>
        where TKeyComparer : IKeyComparer<TKey>
        where TKeySerializer : IObjectSerializer<TKey>
        where TValueSerializer : IObjectSerializer<TValue>
        where TRecordDisposer : IRecordDisposer<TKey, TValue>
    {
        #region Fields
        /// <summary>Compare two keys for equality, and get a key's hash code.</summary>
        readonly TKeyComparer keyComparer = keyComparer;

        /// <summary>Serialize a Key to persistent storage</summary>
        readonly TKeySerializer keySerializer = keySerializer;

        /// <summary>Serialize a Value to persistent storage</summary>
        readonly TValueSerializer valueSerializer = valueSerializer;

        /// <summary>Dispose a record</summary>
        readonly TRecordDisposer recordDisposer = recordDisposer;
        #endregion Fields

        #region Key Comparer
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        /// <inheritdoc/>
        public readonly long GetKeyHashCode64(ref TKey key) => keyComparer.GetHashCode64(ref key);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool KeysEqual(ref TKey k1, ref TKey k2) => keyComparer.Equals(ref k1, ref k2);
        #endregion Key Comparer

        #region Key Serializer
        /// <inheritdoc/>
        public readonly bool HasKeySerializer => typeof(TKeySerializer).IsClass && keySerializer is null ? false : keySerializer.IsNull;

        /// <inheritdoc/>
        public readonly void BeginSerializeKey(Stream stream) => keySerializer.BeginSerialize(stream);

        /// <inheritdoc/>
        public readonly void SerializeKey(ref TKey key) => keySerializer.Serialize(ref key);

        /// <inheritdoc/>
        public readonly void EndSerializeKey() => keySerializer.EndSerialize();

        /// <inheritdoc/>
        public readonly void BeginDeserializeKey(Stream stream) => keySerializer.BeginDeserialize(stream);

        /// <inheritdoc/>
        public readonly void DeserializeKey(out TKey key) => keySerializer.Deserialize(out key);

        /// <inheritdoc/>
        public readonly void EndDeserializeKey() => keySerializer.EndDeserialize();
        #endregion Key Serializer

        #region Value Serializer
        /// <inheritdoc/>
        public readonly bool HasValueSerializer => typeof(TValueSerializer).IsClass && valueSerializer is null ? false : valueSerializer.IsNull;

        /// <inheritdoc/>
        public readonly void BeginSerializeValue(Stream stream) => valueSerializer.BeginSerialize(stream);

        /// <inheritdoc/>
        public readonly void SerializeValue(ref TValue value) => valueSerializer.Serialize(ref value);

        /// <inheritdoc/>
        public readonly void EndSerializeValue() => valueSerializer.EndSerialize();

        /// <inheritdoc/>
        public readonly void BeginDeserializeValue(Stream stream) => valueSerializer.BeginDeserialize(stream);

        /// <inheritdoc/>
        public readonly void DeserializeValue(out TValue value) => valueSerializer.Deserialize(out value);

        /// <inheritdoc/>
        public readonly void EndDeserializeValue() => valueSerializer.EndDeserialize();
        #endregion Value Serializer

        #region Record Disposer
        /// <inheritdoc/>
        public readonly bool DisposeOnPageEviction => recordDisposer.DisposeOnPageEviction;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void DisposeRecord(ref TKey key, ref TValue value, DisposeReason reason) => recordDisposer.DisposeRecord(ref key, ref value, reason);
        #endregion Record Disposer
    }

    /// <summary>
    /// A non-parameterized version of StoreFunctions that provides type-reduced Create() methods.
    /// </summary>
    public struct StoreFunctions<TKey, TValue>
    {
        /// <summary>
        /// Construct a StoreFunctions instance with all types specified and contained instances passed, e.g. for custom objects.
        /// </summary>
        public static StoreFunctions<TKey, TValue, TKeyComparer, TKeySerializer, TValueSerializer, TRecordDisposer> Create<TKeyComparer, TKeySerializer, TValueSerializer, TRecordDisposer>
                (TKeyComparer keyComparer, TKeySerializer keySerializer, TValueSerializer valueSerializer, TRecordDisposer recordDisposer)
            where TKeyComparer : IKeyComparer<TKey>
            where TKeySerializer : IObjectSerializer<TKey>
            where TValueSerializer : IObjectSerializer<TValue>
            where TRecordDisposer : IRecordDisposer<TKey, TValue>
            => new(keyComparer, keySerializer, valueSerializer, recordDisposer);

        /// <summary>
        /// Store functions for <typeparamref name="TKey"/> and <typeparamref name="TValue"/> that take only the <paramref name="keyComparer"/>
        /// </summary>
        public static StoreFunctions<TKey, TValue, TKeyComparer, NoSerializer<TKey>, NoSerializer<TValue>, DefaultRecordDisposer<TKey, TValue>> Create<TKeyComparer>
                (TKeyComparer keyComparer)
            where TKeyComparer : IKeyComparer<TKey>
            => new(keyComparer, NoSerializer<TKey>.Instance, NoSerializer<TValue>.Instance, DefaultRecordDisposer<TKey, TValue>.Instance);

        /// <summary>
        /// Store functions for <see cref="SpanByte"/> Key and Value
        /// </summary>
        StoreFunctions<SpanByte, SpanByte, SpanByteComparer, NoSerializer<SpanByte>, NoSerializer<SpanByte>, SpanByteRecordDisposer> Create()
            => new(SpanByteComparer.Instance, NoSerializer<SpanByte>.Instance, NoSerializer<SpanByte>.Instance, SpanByteRecordDisposer.Instance);
    }
}
