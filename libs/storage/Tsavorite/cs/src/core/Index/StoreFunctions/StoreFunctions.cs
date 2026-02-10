// Copyright (c) Microsoft Corporation. All rights reserved.
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
    public struct StoreFunctions<TKeyComparer, TRecordDisposer>(TKeyComparer keyComparer, Func<IObjectSerializer<IHeapObject>> valueSerializerCreator, TRecordDisposer recordDisposer) : IStoreFunctions
        where TKeyComparer : IKeyComparer
        where TRecordDisposer : IRecordDisposer
    {
        #region Fields
        /// <summary>Compare two keys for equality, and get a key's hash code.</summary>
        readonly TKeyComparer keyComparer = keyComparer;

        /// <summary>Serialize a Value to persistent storage</summary>
        readonly Func<IObjectSerializer<IHeapObject>> valueSerializerCreator = valueSerializerCreator;

        /// <summary>Dispose a record</summary>
        readonly TRecordDisposer recordDisposer = recordDisposer;

        /// <summary>Optional checkpoint completion callback, set separately from ctor.</summary>
        Action checkpointCompletionCallback = () => { };
        #endregion Fields

        #region Key Comparer
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetKeyHashCode64(ReadOnlySpan<byte> key) => keyComparer.GetHashCode64(key);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetKeyHashCode64(ReadOnlySpan<byte> key, ReadOnlySpan<byte> namespaceBytes) => keyComparer.GetHashCode64(key, namespaceBytes);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool KeysEqual(ReadOnlySpan<byte> k1, ReadOnlySpan<byte> k2) => keyComparer.Equals(k1, k2);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool KeysEqual(ReadOnlySpan<byte> k1, ReadOnlySpan<byte> ns1, ReadOnlySpan<byte> k2, ReadOnlySpan<byte> ns2) => keyComparer.Equals(k1, ns1, k2, ns2);
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
    /// A minimally-parameterized version of StoreFunctions that provides type-reduced Create() methods.
    /// </summary>
    public struct StoreFunctions
    {
        /// <summary>
        /// Construct a StoreFunctions instance with all types specified and contained instances passed, e.g. for custom objects.
        /// </summary>
        public static StoreFunctions<TKeyComparer, TRecordDisposer> Create<TKeyComparer, TRecordDisposer>
                (TKeyComparer keyComparer, Func<IObjectSerializer<IHeapObject>> valueSerializerCreator, TRecordDisposer recordDisposer)
            where TKeyComparer : IKeyComparer
            where TRecordDisposer : IRecordDisposer
            => new(keyComparer, valueSerializerCreator, recordDisposer);

        /// <summary>
        /// Construct a StoreFunctions instance with all types specified and contained instances passed, e.g. for custom objects.
        /// </summary>
        public static StoreFunctions<TKeyComparer, DefaultRecordDisposer> Create<TKeyComparer>(TKeyComparer keyComparer, Func<IObjectSerializer<IHeapObject>> valueSerializerCreator)
            where TKeyComparer : IKeyComparer
            => new(keyComparer, valueSerializerCreator, DefaultRecordDisposer.Instance);

        /// <summary>
        /// Construct a StoreFunctions instance with all types specified and contained instances passed, e.g. for custom objects.
        /// </summary>
        public static StoreFunctions<TKeyComparer, TRecordDisposer> Create<TKeyComparer, TRecordDisposer>(TKeyComparer keyComparer, TRecordDisposer recordDisposer)
            where TKeyComparer : IKeyComparer
            where TRecordDisposer : IRecordDisposer
            => new(keyComparer, valueSerializerCreator: null, recordDisposer);

        /// <summary>
        /// Store functions that take only the <paramref name="keyComparer"/>
        /// </summary>
        public static StoreFunctions<TKeyComparer, DefaultRecordDisposer> Create<TKeyComparer>(TKeyComparer keyComparer)
            where TKeyComparer : IKeyComparer
            => new(keyComparer, valueSerializerCreator: null, DefaultRecordDisposer.Instance);

        /// <summary>
        /// Store functions for <see cref="Span{_byte_}"/> Key and Value
        /// </summary>
        public static StoreFunctions<SpanByteComparer, DefaultRecordDisposer> Create()
            => new(SpanByteComparer.Instance, valueSerializerCreator: null, DefaultRecordDisposer.Instance);
    }
}