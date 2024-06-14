// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Store functions for <see cref="SpanByte"/> Key and Value
    /// </summary>
    public struct StoreFunctions_SpanByte()
            : IStoreFunctions<SpanByte, SpanByte, SpanByteComparer, NoSerializer<SpanByte>, NoSerializer<SpanByte>, SpanByteRecordDisposer>
    {
        /// <inheritdoc/>
        public SpanByteComparer KeyComparer { get; private set; } = SpanByteComparer.Instance;

        /// <inheritdoc/>
        public NoSerializer<SpanByte> KeySerializer { get; private set; } = NoSerializer<SpanByte>.Instance;

        /// <inheritdoc/>
        public NoSerializer<SpanByte> ValueSerializer { get; private set; } = NoSerializer<SpanByte>.Instance;

        /// <inheritdoc/>
        public SpanByteRecordDisposer RecordDisposer { get; private set; } = SpanByteRecordDisposer.Instance;
    }

    /// <summary>
    /// Store functions for <typeparamref name="TKey"/> and <typeparamref name="TValue"/> that take only the <paramref name="keyComparer"/>
    /// </summary>
    public struct StoreFunctions_KeyComparerOnly<TKey, TValue, TKeyComparer>(TKeyComparer keyComparer)
            : IStoreFunctions<TKey, TValue, TKeyComparer, NoSerializer<TKey>, NoSerializer<TValue>, DefaultRecordDisposer<TKey, TValue>>
        where TKeyComparer : IKeyComparer<TKey>
    {
        /// <inheritdoc/>
        public TKeyComparer KeyComparer { get; private set; } = keyComparer;

        /// <inheritdoc/>
        public NoSerializer<TKey> KeySerializer { get; private set; } = NoSerializer<TKey>.Instance;

        /// <inheritdoc/>
        public NoSerializer<TValue> ValueSerializer { get; private set; } = NoSerializer<TValue>.Instance;

        /// <inheritdoc/>
        public DefaultRecordDisposer<TKey, TValue> RecordDisposer { get; private set; } = DefaultRecordDisposer<TKey, TValue>.Instance;
    }

    /// <summary>
    /// Store functions for <typeparamref name="TKey"/> and <typeparamref name="TValue"/> that take parameters for all fields. This is usually Object types.
    /// </summary>
    /// <remarks>Constructor</remarks>
    public struct StoreFunctions_All<TKey, TValue, TKeyComparer, TKeySerializer, TValueSerializer, TRecordDisposer>(TKeyComparer keyComparer, TKeySerializer keySerializer, TValueSerializer valueSerializer, TRecordDisposer recordDisposer) 
            : IStoreFunctions<TKey, TValue, TKeyComparer, TKeySerializer, TValueSerializer, TRecordDisposer>
        where TKeyComparer : IKeyComparer<TKey>
        where TKeySerializer : IObjectSerializer<TKey>
        where TValueSerializer : IObjectSerializer<TValue>
        where TRecordDisposer : IRecordDisposer<TKey, TValue>
    {
        /// <inheritdoc/>
        public TKeyComparer KeyComparer { get; private set; } = keyComparer;

        /// <inheritdoc/>
        public TKeySerializer KeySerializer { get; private set; } = keySerializer;

        /// <inheritdoc/>
        public TValueSerializer ValueSerializer { get; private set; } = valueSerializer;

        /// <inheritdoc/>
        public TRecordDisposer RecordDisposer { get; private set; } = recordDisposer;
    }
}
