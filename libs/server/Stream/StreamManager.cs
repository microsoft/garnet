// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
    using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    public sealed class StreamManager : IDisposable
    {
        private Dictionary<byte[], StreamObject> streams;
        long defPageSize;
        long defMemorySize;
        int safeTailRefreshFreqMs;

        SingleWriterMultiReaderLock _lock = new SingleWriterMultiReaderLock();

        public StreamManager(long pageSize, long memorySize, int safeTailRefreshFreqMs)
        {
            streams = new Dictionary<byte[], StreamObject>(ByteArrayComparer.Instance);
            defPageSize = pageSize;
            defMemorySize = memorySize;
            this.safeTailRefreshFreqMs = safeTailRefreshFreqMs;
        }

        /*
        SCAN semantics:
        Eventually returns all stable keys: Keys that exist from start to finish of the full scan will be returned at least once
        No duplicates for stable keys: Keys that don't change during the scan won't be returned multiple times (though this isn't guaranteed if rehashing occurs)
        May return deleted keys: A key deleted after being scanned but before the cursor is returned can still appear in results
        May miss new keys: Keys added during the scan may or may not be returned
        May return modified keys multiple times: If keys are added/deleted causing rehash, some keys might be returned more than once
        Full scan always terminates: Returns cursor 0 eventually, even with ongoing modifications.
        Note: Naive locking is okay till I see something in the profiler that suggests otherwise.
        */
        public unsafe void KeyScan(byte* patternPtr, int length, ref long cursor, long remainingCount, List<byte[]> keys)
        {
            _lock.ReadLock();
            try
            {
                int cursorFromStart = 0;
                Dictionary<byte[], StreamObject>.KeyCollection streamKeys = streams.Keys;
                foreach (byte[] key in streams.Keys)
                {
                    // skip till we reach the cursor position, better to tradeoff some CPU for memory here by avoiding having to store all keys in an array
                    if (cursorFromStart < cursor)
                    {
                        cursorFromStart++;
                        continue;
                    }

                    if (patternPtr != null)
                    {
                        fixed (byte* keyPtr = key)
                            if (!GlobUtils.Match(patternPtr, length, keyPtr, key.Length, true))
                                continue;
                    }

                    keys.Add(key);
                    cursorFromStart++;
                    remainingCount--;
                    if (remainingCount == 0)
                    {
                        break;
                    }
                }

                cursor = cursorFromStart < streams.Count ? cursorFromStart : 0;
            }
            finally
            {
                _lock.ReadUnlock();
            }
        }

        /// <summary>
        /// Get all the stream keys
        /// </summary>
        /// <returns>Array of stream keys as strings</returns>
        public unsafe byte[][] GetKeys(byte* pattern, int len)
        {
            _lock.ReadLock();
            byte[][] keys = new byte[streams.Count][];
            try
            {
                int i = 0;
                foreach (var key in streams.Keys)
                {
                    if (pattern != null)
                    {
                        fixed (byte* keyPtr = key)
                        {
                            if (!GlobUtils.Match(pattern, len, keyPtr, key.Length, true))
                            {
                                continue;
                            }
                        }
                    }

                    keys[i] = key;
                    i++;
                }
                return keys;
            }
            finally
            {
                _lock.ReadUnlock();
            }
        }

        /// <summary>
        /// Add a new entry to the stream
        /// </summary>
        /// <param name="keySlice">key/name of the stream</param>
        /// <param name="idSlice">id of the stream entry</param>
        /// <param name="noMkStream">if true, do not create a new stream if it does not exist</param>
        /// <param name="value">payload to the stream</param>
        /// <param name="valueLength">length of payload to the stream</param>
        /// <param name="numPairs"># k-v pairs in the payload</param>
        /// <param name="output"></param>
        /// <param name="streamKey">key of last stream accessed (for cache)</param>
        /// <param name="lastStream">reference to last stream accessed (for cache)</param>
        /// <param name="respProtocolVersion">RESP protocol version</param>
        public unsafe void StreamAdd(ArgSlice keySlice, ArgSlice idSlice, bool noMkStream, ReadOnlySpan<byte> value, int valueLength, int numPairs, ref SpanByteAndMemory output, out byte[] streamKey, out StreamObject lastStream, byte respProtocolVersion)
        {
            // copy key store this key in the dictionary
            byte[] key = new byte[keySlice.Length];
            fixed (byte* keyPtr = key)
                Buffer.MemoryCopy(keySlice.ptr, keyPtr, keySlice.Length, keySlice.Length);
            bool foundStream = false;
            StreamObject stream;
            lastStream = null;
            streamKey = null;
            _lock.ReadLock();
            try
            {
                foundStream = streams.TryGetValue(key, out stream);
                if (foundStream)
                {
                    stream.AddEntry(value, valueLength, idSlice, numPairs, ref output, respProtocolVersion);
                    // update last accessed stream key 
                    lastStream = stream;
                    streamKey = key;
                }
            }
            finally
            {
                _lock.ReadUnlock();
            }
            if (foundStream)
            {
                return;
            }
            // take a write lock 
            _lock.WriteLock();
            try
            {
                // retry to validate if some other thread has created the stream
                foundStream = streams.TryGetValue(key, out stream);
                if (!foundStream && !noMkStream)
                {
                    // stream was not found with this key so create a new one 
                    StreamObject newStream = new StreamObject(null, defPageSize, defMemorySize, safeTailRefreshFreqMs);
                    newStream.AddEntry(value, valueLength, idSlice, numPairs, ref output, respProtocolVersion);
                    streams.TryAdd(key, newStream);
                    streamKey = key;
                    lastStream = newStream;
                }
                else if (!foundStream && noMkStream)
                {
                    // stream was not found and noMkStream is set so return an error
                    using var writer = new RespMemoryWriter(respProtocolVersion, ref output);
                    writer.WriteNull();
                    return;
                }
                else
                {
                    stream.AddEntry(value, valueLength, idSlice, numPairs, ref output, respProtocolVersion);
                    lastStream = stream;
                    streamKey = key;
                }
            }
            finally
            {
                _lock.WriteUnlock();
            }
            return;
        }

        /// <summary>
        /// Get the length of a particular stream
        /// </summary>
        /// <param name="keySlice">key of the stream we want to obtain the length</param>
        /// <returns>length of the stream</returns>
        public unsafe ulong StreamLength(ArgSlice keySlice)
        {
            var key = keySlice.ToArray();
            if (streams != null)
            {
                bool foundStream = streams.TryGetValue(key, out StreamObject stream);
                if (foundStream)
                {
                    return stream.Length();
                }
                else
                {
                    // return 0 if stream does not exist, as if it was empty
                    return 0;
                }
            }
            return 0;
        }

        /// <summary>
        /// Perform range scan in a stream
        /// </summary>
        /// <param name="keySlice">key/name of stream</param>
        /// <param name="start">start of range</param>
        /// <param name="end">end of range</param>
        /// <param name="count">threshold to limit scanning</param>
        /// <param name="output"></param>
        /// <param name="respProtocolVersion">RESP protocol version</param>
        public unsafe bool StreamRange(ArgSlice keySlice, string start, string end, int count, ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var key = keySlice.ToArray();
            if (streams != null && streams.Count > 0)
            {
                bool foundStream = streams.TryGetValue(key, out StreamObject stream);
                if (foundStream)
                {
                    stream.ReadRange(start, end, count, ref output, respProtocolVersion);
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Delete an entry from a stream
        /// </summary>
        /// <param name="keySlice">key/name of stream to delete</param>
        /// <param name="idSlice">id of stream entry to delete</param>
        /// <param name="lastSeenStream">last accessed stream in cache</param>
        /// <returns></returns>
        public bool StreamDelete(ArgSlice keySlice, ArgSlice idSlice, out StreamObject lastSeenStream)
        {
            var key = keySlice.ToArray();
            StreamObject stream;
            lastSeenStream = null;
            if (streams != null)
            {
                if (streams.TryGetValue(key, out stream))
                {
                    lastSeenStream = stream;
                    return stream.DeleteEntry(idSlice);
                }
            }
            return false;
        }

        public bool StreamTrim(ArgSlice keySlice, ArgSlice trimArg, StreamTrimOpts optType, out ulong validKeysRemoved, bool approximate = false)
        {
            bool foundStream;
            var key = keySlice.ToArray();
            StreamObject stream;
            validKeysRemoved = 0;
            if (streams != null)
            {
                foundStream = streams.TryGetValue(key, out stream);

                if (foundStream)
                {
                    return stream.Trim(trimArg, optType, out validKeysRemoved, approximate);
                }
            }
            return true; // no keys removed so return true
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (streams != null)
            {
                _lock.WriteLock();
                try
                {
                    foreach (var stream in streams.Values)
                    {
                        stream.Dispose();
                    }

                    streams.Clear();
                }
                finally
                {
                    _lock.WriteUnlock();
                }
            }

        }
    }
}