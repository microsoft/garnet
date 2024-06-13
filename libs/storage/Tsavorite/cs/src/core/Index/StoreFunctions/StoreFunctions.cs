// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Default compositor of the StoreFunctions components.
    /// </summary>
    public class StoreFunctions<TKey, TValue, TKeyComparer, TKeySerializer, TValueSerializer, TRecordDisposer> : IStoreFunctions<TKey, TValue, TKeyComparer, TKeySerializer, TValueSerializer, TRecordDisposer>
        where TKeyComparer : ITsavoriteEqualityComparer<TKey>
        where TKeySerializer : IObjectSerializer<TKey>
        where TValueSerializer : IObjectSerializer<TValue>
        where TRecordDisposer : IRecordDisposer<TKey, TValue>
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public StoreFunctions(TKeyComparer keyComparer, TKeySerializer keySerializer, TValueSerializer valueSerializer, TRecordDisposer recordDisposer)
        {
            KeyComparer = keyComparer;
            KeySerializer = keySerializer;
            ValueSerializer = valueSerializer;
            RecordDisposer = recordDisposer;
        }

        /// <inheritdoc/>
        public TKeyComparer KeyComparer { get; private set; }

        /// <inheritdoc/>
        public TKeySerializer KeySerializer { get; private set; }

        /// <inheritdoc/>
        public TValueSerializer ValueSerializer { get; private set; }

        /// <inheritdoc/>
        public TRecordDisposer RecordDisposer { get; private set; }
    }

    /// <summary>
    /// Store functions for <see cref="SpanByte"/> Key and Value, using <see cref="SpanByteAllocator"/>
    /// </summary>
    public sealed class StoreFunctions_SpanByte : StoreFunctions<SpanByte, SpanByte, SpanByteComparer, NoSerializer<SpanByte>, NoSerializer<SpanByte>, SpanByteRecordDisposer>
    {
        /// <summary>Default instance</summary>
        public static readonly StoreFunctions_SpanByte Default = new();

        /// <summary>Constructor</summary>
        public StoreFunctions_SpanByte()
            : base(new SpanByteComparer(), new NoSerializer<SpanByte>(), new NoSerializer<SpanByte>(), new SpanByteRecordDisposer())
        { }
    }

    /// <summary>
    /// Store functions for object Key and Value, using <see cref="GenericAllocator{TKey, TValue}"/>
    /// </summary>
    public sealed class StoreFunctions_Generic<TKey, TValue, TKeyComparer, TKeySerializer, TValueSerializer, TRecordDisposer> : StoreFunctions<TKey, TValue, TKeyComparer, TKeySerializer, TValueSerializer, TRecordDisposer>
        where TKeyComparer : ITsavoriteEqualityComparer<TKey>
        where TKeySerializer : IObjectSerializer<TKey>
        where TValueSerializer : IObjectSerializer<TValue>
        where TRecordDisposer : IRecordDisposer<TKey, TValue>
    {
        // There is no default instance because there is no parameterless ctor.

        /// <summary>Constructor</summary>
        public StoreFunctions_Generic(TKeyComparer keyComparer, TKeySerializer keySerializer, TValueSerializer valueSerializer, TRecordDisposer recordDisposer)
            : base(keyComparer, keySerializer, valueSerializer, recordDisposer)
        { }
    }

    /// <summary>
    /// Store functions for <typeparamref name="TKey"/> and <typeparamref name="TValue"/>, using <see cref="BlittableAllocatorImpl{Key, Value, TKeyComparer}"/>
    /// </summary>
    public sealed class StoreFunctions_FixedLenBlittable<TKey, TValue, TKeyComparer> : StoreFunctions<TKey, TValue, TKeyComparer, NoSerializer<TKey>, NoSerializer<TValue>, DefaultRecordDisposer<TKey, TValue>>
        where TKeyComparer : ITsavoriteEqualityComparer<TKey>
    {
        // There is no default instance because there is no parameterless ctor.

        /// <summary>Constructor</summary>
        public StoreFunctions_FixedLenBlittable(TKeyComparer keyComparer)
            : base(keyComparer, new NoSerializer<TKey>(), new NoSerializer<TValue>(), new DefaultRecordDisposer<TKey, TValue>())
        { }
    }
}
