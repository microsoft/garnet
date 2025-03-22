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

        SingleWriterMultiReaderLock _lock = new SingleWriterMultiReaderLock();

        public StreamManager(long pageSize, long memorySize)
        {
            streams = new Dictionary<byte[], StreamObject>(new ByteArrayComparer());
            defPageSize = pageSize;
            defMemorySize = memorySize;
        }

        /// <summary>
        /// Add a new entry to the stream
        /// </summary>
        /// <param name="keySlice">key/name of the stream</param>
        /// <param name="idSlice">id of the stream entry</param>
        /// <param name="value">payload to the stream</param>
        /// <param name="valueLength">length of payload to the stream</param>
        /// <param name="numPairs"># k-v pairs in the payload</param>
        /// <param name="output"></param>
        /// <param name="streamKey">key of last stream accessed (for cache)</param>
        /// <param name="lastStream">reference to last stream accessed (for cache)</param>
        public unsafe void StreamAdd(ArgSlice keySlice, ArgSlice idSlice, byte* value, int valueLength, int numPairs, ref SpanByteAndMemory output, out byte[] streamKey, out StreamObject lastStream)
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
                    stream.AddEntry(value, valueLength, idSlice, numPairs, ref output);
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
                if (!foundStream)
                {
                    // stream was not found with this key so create a new one 
                    StreamObject newStream = new StreamObject(null, defPageSize, defMemorySize);
                    newStream.AddEntry(value, valueLength, idSlice, numPairs, ref output);
                    streams.TryAdd(key, newStream);
                    streamKey = key;
                    lastStream = newStream;
                }
                else
                {
                    stream.AddEntry(value, valueLength, idSlice, numPairs, ref output);
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
        public void StreamRange(ArgSlice keySlice, string start, string end, int count, ref SpanByteAndMemory output)
        {
            var key = keySlice.ToArray();
            if (streams != null && streams.Count > 0)
            {
                bool foundStream = streams.TryGetValue(key, out StreamObject stream);
                if (foundStream)
                {
                    // stream.ReadRange(start, end, count, ref output);
                }
            }
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
            bool foundStream;
            var key = keySlice.ToArray();
            StreamObject stream;
            lastSeenStream = null;
            if (streams != null)
            {
                foundStream = streams.TryGetValue(key, out stream);

                if (foundStream)
                {
                    lastSeenStream = stream;
                    return stream.DeleteEntry(idSlice);
                }
            }
            return false;
        }

        /// <summary>
        /// Trim a stream
        /// </summary>
        /// <param name="keySlice">key/name of stream</param>
        /// <param name="trimValue">parameter to trim by</param>
        /// <param name="optType">option to trim by (length or ID)</param>
        /// <param name="validKeysRemoved"># valid keys removed</param>
        /// <returns></returns>
        public bool StreamTrim(ArgSlice keySlice, ArgSlice trimValue, XTRIMOpts optType, out ulong validKeysRemoved)
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
                    // return stream.Trim(trimValue, optType, out validKeysRemoved);
                }
            }
            // we return true if the stream does not exist (as Resp expects a 0 result)
            return true;
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