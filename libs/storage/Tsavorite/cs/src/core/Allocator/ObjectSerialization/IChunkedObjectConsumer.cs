// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    public interface IChunkedObjectConsumer
    {
        /// <summary>
        /// Called when a chunk's serialized data is available to be consumed. The span is pinned if we're serializing from pinned data
        /// (e.g. inline log record), else unpinned. Length may be zero, in which case <paramref name="isComplete"/> will be true.
        /// </summary>
        /// <param name="data">The data to consume. Length may be zero, in which case <paramref name="isComplete"/> will be true.</param>
        /// <param name="isComplete">Indicates whether this is the final chunk of data.</param>
        /// <param name="key">The key of the record being written.</param>
        /// <param name="input">The input of the record being written.</param>
        /// <returns>The number of bytes consumed, so the circular buffer can refill.</returns>
        long Consume<TKey, TInput>(ReadOnlySpan<byte> data, bool isComplete, TKey key, ref TInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            where TInput : IStoreInput;
    }
}