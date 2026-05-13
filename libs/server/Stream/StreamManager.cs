// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    public sealed class StreamManager : IDisposable
    {
        private readonly Dictionary<byte[], StreamObject> streams;

        readonly string streamsRootDir;
        long defPageSize;
        long defMemorySize;
        readonly bool waitForCommit;
        readonly ILogger logger;

        SingleWriterMultiReaderLock _lock = new SingleWriterMultiReaderLock();

        /// <summary>
        /// Creates a stream manager.
        /// </summary>
        /// <param name="streamsRootDir">Root directory under which per-stream subdirectories are
        ///     created. When null, all streams are kept in-memory only (no durability).</param>
        /// <param name="waitForCommit">When true, every write to a stream's log synchronously
        ///     flushes and waits for the commit to complete before returning — matching the
        ///     server-wide <c>--wait-for-commit</c> AOF behaviour.</param>
        public StreamManager(string streamsRootDir, long pageSize, long memorySize, bool waitForCommit = false, ILogger logger = null)
        {
            streams = new Dictionary<byte[], StreamObject>(ByteArrayComparer.Instance);
            this.streamsRootDir = streamsRootDir;
            defPageSize = pageSize;
            defMemorySize = memorySize;
            this.waitForCommit = waitForCommit;
            this.logger = logger;
        }

        /// <summary>
        /// Recover streams from <see cref="streamsRootDir"/>: enumerate hex-named subdirectories,
        /// decode their names back into stream keys, and replay each log to rebuild its BTree.
        /// Must be called once at startup, before any traffic is served.
        /// </summary>
        public void Recover()
        {
            if (streamsRootDir == null) return;
            if (!Directory.Exists(streamsRootDir)) return;

            foreach (var dir in Directory.EnumerateDirectories(streamsRootDir))
            {
                var hexName = Path.GetFileName(dir);
                byte[] keyBytes;
                try
                {
                    keyBytes = Convert.FromHexString(hexName);
                }
                catch (FormatException)
                {
                    // Not one of ours — skip.
                    logger?.LogWarning("Skipping non-hex stream directory '{dir}'", dir);
                    continue;
                }

                var stream = new StreamObject(streamsRootDir, hexName, defPageSize, defMemorySize, waitForCommit, recover: true);
                streams[keyBytes] = stream;
                logger?.LogInformation("Recovered stream '{key}' from '{dir}'", BitConverter.ToString(keyBytes), dir);
            }
        }

        /// <summary>
        /// Asynchronously commit every stream's log to disk. Snapshots the current set of streams
        /// under a brief read lock and then awaits all <see cref="StreamObject.CommitAsync"/> tasks
        /// concurrently — readers/writers continue to operate against the BTrees throughout.
        /// </summary>
        public async Task CommitAsync(CancellationToken token = default)
        {
            StreamObject[] snapshot;
            _lock.ReadLock();
            try
            {
                snapshot = streams.Values.ToArray();
            }
            finally
            {
                _lock.ReadUnlock();
            }

            // No streams (or all in-memory): the awaits below are still cheap (NullDevice commit returns immediately).
            if (snapshot.Length == 0) return;

            // ValueTask awaits don't compose with Task.WhenAll; use one Task per stream so we can fan-out.
            var tasks = new Task[snapshot.Length];
            for (int i = 0; i < snapshot.Length; i++)
            {
                tasks[i] = snapshot[i].CommitAsync(token).AsTask();
            }
            await Task.WhenAll(tasks).ConfigureAwait(false);
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
                int currentPosition = 0;  // Tracks absolute position in dictionary
                int matchedCount = 0;     // Tracks number of keys added to results

                foreach (byte[] key in streams.Keys)
                {
                    // Skip keys before cursor position
                    if (currentPosition < cursor)
                    {
                        currentPosition++;
                        continue;
                    }

                    // Check pattern matching
                    bool matches = true;
                    if (patternPtr != null)
                    {
                        fixed (byte* keyPtr = key)
                            matches = GlobUtils.Match(patternPtr, length, keyPtr, key.Length, true);
                    }

                    // If key matches pattern, add it to results
                    if (matches)
                    {
                        keys.Add(key);
                        matchedCount++;

                        // Stop if we've reached the requested count
                        if (matchedCount >= remainingCount)
                        {
                            currentPosition++;
                            break;
                        }
                    }

                    currentPosition++;
                }

                // If we've processed all keys, set cursor to 0 (scan complete)
                // Otherwise, set cursor to current position for next iteration
                cursor = currentPosition >= streams.Count ? 0 : currentPosition;
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
        /// <param name="numPairs"># k-v pairs in the payload</param>
        /// <param name="output"></param>
        /// <param name="streamKey">key of last stream accessed (for cache)</param>
        /// <param name="lastStream">reference to last stream accessed (for cache)</param>
        /// <param name="respProtocolVersion">RESP protocol version</param>
        public void StreamAdd(PinnedSpanByte keySlice, PinnedSpanByte idSlice, bool noMkStream, ReadOnlySpan<byte> value, int numPairs, ref SpanByteAndMemory output, out byte[] streamKey, out StreamObject lastStream, byte respProtocolVersion)
        {
            // copy key store this key in the dictionary
            byte[] key = keySlice.ToArray();

            bool foundStream = false;
            StreamObject stream;
            lastStream = null;
            streamKey = null;
            _lock.ReadLock();
            try
            { // HK TODO: wth is this code block doing? Where it calls AddEntry seems weird
                foundStream = streams.TryGetValue(key, out stream);
                if (foundStream)
                {
                    stream.AddEntry(idSlice, numPairs, value, ref output, respProtocolVersion);
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
                    // stream was not found with this key so create a new one. Encode the key as hex
                    // for the on-disk directory name so arbitrary key bytes are filesystem-safe and
                    // the encoding is reversible during recovery.
                    var dirName = streamsRootDir != null ? Convert.ToHexString(key) : null;
                    StreamObject newStream = new StreamObject(streamsRootDir, dirName, defPageSize, defMemorySize, waitForCommit);
                    newStream.AddEntry(idSlice, numPairs, value, ref output, respProtocolVersion);
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
                    stream.AddEntry(idSlice, numPairs, value, ref output, respProtocolVersion);
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
        public ulong StreamLength(PinnedSpanByte keySlice)
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
        public bool StreamRange(PinnedSpanByte keySlice, string start, string end, int count, ref SpanByteAndMemory output, byte respProtocolVersion, bool isReverse)
        {
            var key = keySlice.ToArray();
            if (streams != null && streams.Count > 0)
            {
                bool foundStream = streams.TryGetValue(key, out StreamObject stream);
                if (foundStream)
                {
                    stream.ReadRange(start, end, count, ref output, respProtocolVersion, isReverse);
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
        public bool StreamDelete(PinnedSpanByte keySlice, PinnedSpanByte idSlice, out StreamObject lastSeenStream)
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

        /// <summary>
        /// Drop a single stream by name — the per-key counterpart to <see cref="FlushAll"/>. Used
        /// by <c>DEL</c> / <c>UNLINK</c> so that removing a stream key also disposes its
        /// <see cref="StreamObject"/> (deallocating the BTree, closing the TsavoriteLog and device,
        /// and dropping all consumer groups + PELs that hung off it) and deletes the on-disk
        /// subdirectory so a subsequent recovery sees nothing. Session-level caches detect the
        /// stale reference via <see cref="StreamObject.IsDisposed"/> on their next lookup.
        /// </summary>
        /// <param name="keySlice">name of the stream to delete</param>
        /// <returns>true if a stream with this key existed and was removed; false if no such stream</returns>
        public bool DeleteStream(PinnedSpanByte keySlice)
        {
            if (streams == null) return false;
            var key = keySlice.ToArray();

            _lock.WriteLock();
            try
            {
                // Remove from the dictionary first under the write lock — once we've taken the
                // entry out, no future cache miss can resolve it. Dispose() and the directory
                // delete are allowed to throw without leaving the dict inconsistent.
                if (!streams.Remove(key, out var stream))
                    return false;

                try
                {
                    stream.Dispose();
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "Error disposing stream during DEL/UNLINK");
                }

                if (streamsRootDir != null)
                {
                    try
                    {
                        var dir = Path.Combine(streamsRootDir, Convert.ToHexString(key));
                        if (Directory.Exists(dir))
                            Directory.Delete(dir, recursive: true);
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "Error deleting stream directory during DEL/UNLINK");
                    }
                }

                return true;
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }

        public bool StreamTrim(PinnedSpanByte keySlice, PinnedSpanByte trimArg, StreamTrimOpts optType, out ulong validKeysRemoved, bool approximate = false)
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

        public bool StreamLast(PinnedSpanByte key, ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var keyArr = key.ToArray();
            if (streams != null && streams.Count > 0)
            {
                bool foundStream = streams.TryGetValue(keyArr, out StreamObject stream);
                if (foundStream)
                {
                    stream.ReadLastEntry(ref output, respProtocolVersion);
                    return true;
                }
            }
            return false;
        }

        #region Consumer Group Forwarding

        /// <summary>
        /// Look up a stream by key. Returns null if not found.
        /// Caller must handle the "stream not found" error.
        /// </summary>
        StreamObject FindStream(byte[] key)
        {
            _lock.ReadLock();
            try
            {
                streams.TryGetValue(key, out var stream);
                return stream;
            }
            finally
            {
                _lock.ReadUnlock();
            }
        }

        public bool StreamGroupCreate(PinnedSpanByte keySlice, string groupName, StreamID startId, long entriesRead, bool mkStream, ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var key = keySlice.ToArray();
            var stream = FindStream(key);

            if (stream == null && mkStream)
            {
                // Create the stream first
                _lock.WriteLock();
                try
                {
                    if (!streams.TryGetValue(key, out stream))
                    {
                        var dirName = streamsRootDir != null ? Convert.ToHexString(key) : null;
                        stream = new StreamObject(streamsRootDir, dirName, defPageSize, defMemorySize, waitForCommit);
                        streams[key] = stream;
                    }
                }
                finally
                {
                    _lock.WriteUnlock();
                }
            }

            if (stream == null)
                return false; // stream doesn't exist

            return stream.CreateGroup(groupName, startId, entriesRead);
        }

        public bool StreamGroupDestroy(PinnedSpanByte keySlice, string groupName)
        {
            var stream = FindStream(keySlice.ToArray());
            if (stream == null) return false;
            return stream.DestroyGroup(groupName);
        }

        public bool StreamGroupSetId(PinnedSpanByte keySlice, string groupName, StreamID id, long entriesRead)
        {
            var stream = FindStream(keySlice.ToArray());
            if (stream == null) return false;
            return stream.SetGroupId(groupName, id, entriesRead);
        }

        public bool? StreamGroupCreateConsumer(PinnedSpanByte keySlice, string groupName, string consumerName)
        {
            var stream = FindStream(keySlice.ToArray());
            if (stream == null) return null;
            return stream.CreateConsumer(groupName, consumerName);
        }

        public int StreamGroupDeleteConsumer(PinnedSpanByte keySlice, string groupName, string consumerName)
        {
            var stream = FindStream(keySlice.ToArray());
            if (stream == null) return -1;
            return stream.DeleteConsumer(groupName, consumerName);
        }

        public bool StreamReadGroup(PinnedSpanByte keySlice, string groupName, string consumerName,
            string id, int count, bool noAck, ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var stream = FindStream(keySlice.ToArray());
            if (stream == null) return false;
            return stream.ReadGroup(groupName, consumerName, id, count, noAck, ref output, respProtocolVersion);
        }

        public int StreamAcknowledge(PinnedSpanByte keySlice, string groupName, ReadOnlySpan<StreamID> ids)
        {
            var stream = FindStream(keySlice.ToArray());
            if (stream == null) return -1;
            return stream.Acknowledge(groupName, ids);
        }

        public bool StreamPending(PinnedSpanByte keySlice, string groupName, string start, string end,
            int count, long minIdleTime, string consumerFilter,
            ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var stream = FindStream(keySlice.ToArray());
            if (stream == null) return false;
            return stream.GetPending(groupName, start, end, count, minIdleTime, consumerFilter, ref output, respProtocolVersion);
        }

        public bool StreamClaim(PinnedSpanByte keySlice, string groupName, string consumerName,
            long minIdleTime, StreamID[] ids, long? idleMs, long? timeMs, int? retryCount,
            bool force, bool justId, ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var stream = FindStream(keySlice.ToArray());
            if (stream == null) return false;
            return stream.ClaimEntries(groupName, consumerName, minIdleTime, ids, idleMs, timeMs, retryCount, force, justId, ref output, respProtocolVersion);
        }

        public bool StreamAutoClaim(PinnedSpanByte keySlice, string groupName, string consumerName,
            long minIdleTime, StreamID start, int count, bool justId,
            ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var stream = FindStream(keySlice.ToArray());
            if (stream == null) return false;
            return stream.AutoClaim(groupName, consumerName, minIdleTime, start, count, justId, ref output, respProtocolVersion);
        }

        public bool StreamInfo(PinnedSpanByte keySlice, ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var stream = FindStream(keySlice.ToArray());
            if (stream == null) return false;
            stream.GetStreamInfo(ref output, respProtocolVersion);
            return true;
        }

        public bool StreamInfoGroups(PinnedSpanByte keySlice, ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var stream = FindStream(keySlice.ToArray());
            if (stream == null) return false;
            return stream.GetGroupsInfo(ref output, respProtocolVersion);
        }

        public bool StreamInfoConsumers(PinnedSpanByte keySlice, string groupName, ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var stream = FindStream(keySlice.ToArray());
            if (stream == null) return false;
            return stream.GetConsumersInfo(groupName, ref output, respProtocolVersion);
        }

        public void StreamRead(PinnedSpanByte keySlice, StreamID afterId, int count, ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var stream = FindStream(keySlice.ToArray());
            if (stream == null)
            {
                // Stream not found — write empty array
                using var writer = new RespMemoryWriter(respProtocolVersion, ref output);
                writer.WriteNull();
                return;
            }
            stream.ReadAfter(afterId, count, ref output, respProtocolVersion);
        }

        #endregion Consumer Group Forwarding

        /// <summary>
        /// Drop every stream — disposes each <see cref="StreamObject"/> (which deallocates its
        /// BTree index and closes its TsavoriteLog + device) and deletes the per-stream on-disk
        /// subdirectory so a subsequent recovery does not replay the data. Used by
        /// <c>FLUSHDB</c> / <c>FLUSHALL</c>. Streams are not per-database, so flushing any
        /// database wipes the entire stream namespace.
        /// </summary>
        public void FlushAll()
        {
            if (streams == null) return;

            _lock.WriteLock();
            try
            {
                foreach (var kv in streams)
                {
                    var key = kv.Key;
                    var stream = kv.Value;

                    try
                    {
                        stream.Dispose();
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "Error disposing stream during FLUSHDB/FLUSHALL");
                    }

                    // Remove the on-disk artifacts. The directory name matches the encoding used
                    // when the stream was created (see StreamAdd / StreamGroupCreate).
                    if (streamsRootDir != null)
                    {
                        try
                        {
                            var dir = Path.Combine(streamsRootDir, Convert.ToHexString(key));
                            if (Directory.Exists(dir))
                                Directory.Delete(dir, recursive: true);
                        }
                        catch (Exception ex)
                        {
                            logger?.LogError(ex, "Error deleting stream directory during FLUSHDB/FLUSHALL");
                        }
                    }
                }

                streams.Clear();
            }
            finally
            {
                _lock.WriteUnlock();
            }
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