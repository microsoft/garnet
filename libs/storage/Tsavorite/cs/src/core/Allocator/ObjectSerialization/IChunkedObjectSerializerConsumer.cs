// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Consumes the chunked serialized output produced by a <c>ChunkedObjectSerializer</c>. Because the serializer's buffer is
    /// circular, the available bytes arrive as up to two spans, <c>first</c> then <c>second</c>; the total available is
    /// <c>first.Length + second.Length</c>. Both forms return the number of bytes consumed (counted from the front of
    /// <c>first</c>, then <c>second</c>) so the serializer can free that much of the ring.
    /// </summary>
    public interface IChunkedObjectSerializerConsumer
    {
        /// <summary>
        /// Consume serialized value bytes only (no key or input). Used by the read side to reassemble an object value from its
        /// chunks. <paramref name="isComplete"/> is true on the final call (its combined span length may be zero).
        /// </summary>
        /// <typeparam name="TContext">Caller state type passed through unchanged (e.g. the write-side chunk state).</typeparam>
        /// <param name="first">The first (contiguous) run of available bytes.</param>
        /// <param name="second">The wrapped-around run of available bytes; empty when the ring is not wrapped.</param>
        /// <param name="isComplete">True on the final chunk of the value.</param>
        /// <param name="context">Caller state, passed through unchanged.</param>
        /// <returns>The number of bytes consumed.</returns>
        int Consume<TContext>(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, bool isComplete, TContext context);

        /// <summary>
        /// Consume serialized value bytes together with the record's key and input (the write side, which packs key, value, and
        /// input into chunk records).
        /// </summary>
        /// <typeparam name="TContext">Caller state type passed through unchanged (e.g. the write-side chunk state).</typeparam>
        /// <typeparam name="TKey">The record key type.</typeparam>
        /// <typeparam name="TInput">The record input type.</typeparam>
        /// <param name="first">The first (contiguous) run of available value bytes.</param>
        /// <param name="second">The wrapped-around run of available value bytes; empty when the ring is not wrapped.</param>
        /// <param name="isComplete">True on the final chunk of the value.</param>
        /// <param name="key">The record's key.</param>
        /// <param name="input">The record's input.</param>
        /// <param name="context">Caller state, passed through unchanged.</param>
        /// <returns>The number of value bytes consumed.</returns>
        int Consume<TContext, TKey, TInput>(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, bool isComplete, TKey key, ref TInput input, TContext context)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            where TInput : IStoreInput;
    }
}
