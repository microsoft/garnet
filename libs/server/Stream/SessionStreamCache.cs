// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace Garnet.server
{
    internal class SessionStreamCache
    {
        const int DefaultCacheSize = 16;
        readonly Dictionary<byte[], StreamObject> streamCache = new Dictionary<byte[], StreamObject>(DefaultCacheSize, ByteArrayComparer.Instance);
        readonly byte[][] streamKeysCache = new byte[DefaultCacheSize][];
        int cachedStreamsCount = 0;
        int front = 0;

        public SessionStreamCache()
        { }

        /// <summary>
        /// Lookup a stream in the cache. Since the cache is expected to be small, we can sequentially scan.
        /// Stale entries (StreamObjects that were disposed by FLUSHDB/FLUSHALL while still cached
        /// here) are evicted on detection and reported as a miss so callers re-resolve through the
        /// StreamManager.
        /// </summary>
        /// <param name="key">name of stream to lookup</param>
        /// <param name="stream">stream found from the cache</param>
        /// <returns>true if stream exists in cache</returns>
        public bool TryGetStreamFromCache(ReadOnlySpan<byte> key, out StreamObject stream)
        {
            var keyArr = key.ToArray();
            if (!streamCache.TryGetValue(keyArr, out stream))
                return false;

            // FLUSHDB / FLUSHALL disposes StreamObjects out from under us. The cache holds a
            // direct reference, so we have to validate liveness here.
            if (stream.IsDisposed)
            {
                streamCache.Remove(keyArr);
                stream = null;
                return false;
            }

            return true;
        }

        /// <summary>
        /// Add a stream to the cache. If the cache is full, we don't add the stream.
        /// </summary>
        /// <param name="key">name of stream</param>
        /// <param name="stream">reference to stream object</param>
        /// <returns>true if successfully added</returns>
        public bool TryAddStreamToCache(byte[] key, StreamObject stream)
        {
            if (cachedStreamsCount < DefaultCacheSize)
            {
                streamCache.Add(key, stream);
                // add to circular array and update front
                streamKeysCache[front] = key;
                front = (front + 1) % DefaultCacheSize;
                cachedStreamsCount++;
                return true;
            }

            streamCache.Remove(streamKeysCache[front]);
            streamCache.Add(key, stream);
            // add to circular array where we removed the oldest stream
            streamKeysCache[front] = key;
            front = (front + 1) % DefaultCacheSize;
            // we don't need to update cachedStreamsCount since we added and removed a stream
            return true;

        }
    }
}