// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;
using Garnet.server.BTreeIndex;
using Garnet.common;
using System.Threading;
using System.Diagnostics;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace Garnet.server
{
    public enum XTRIMOpts
    {
        MAXLEN,
        MINID,
        NONE
    }

    public class StreamObject : IDisposable
    {
        readonly IDevice device;
        readonly TsavoriteLog log;
        readonly BTree index;
        StreamID lastId;
        long totalEntriesAdded;
        SingleWriterMultiReaderLock _lock;
        private bool _disposed;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="logDir">Directory where the log will be stored</param>
        /// <param name="pageSize">Page size of the log used for the stream</param>
        public StreamObject(string logDir, long pageSize, long memorySize)
        {
            device = logDir == null ? new NullDevice() : Devices.CreateLogDevice("streamLogs/" + logDir + "/streamLog", preallocateFile: false);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSize = pageSize, MemorySize = memorySize });
            index = new BTree(device.SectorSize);
            totalEntriesAdded = 0;
            lastId = default;
            _lock = new SingleWriterMultiReaderLock();
        }

        /// <summary>
        /// Increment the stream ID
        /// </summary>
        /// <param name="incrementedID">carries the incremented stream id</param>
        public void IncrementID(ref GarnetStreamID incrementedID)
        {
            while (true)
            {
                var originalMs = lastId.ms;
                var originalSeq = lastId.seq;

                if (originalMs == long.MaxValue)
                {
                    incrementedID = default;
                    return;
                }

                var newMs = originalMs;
                var newSeq = originalSeq + 1;

                // if seq overflows, increment timestamp and reset seq
                if (newSeq == 0)
                {
                    newMs = originalMs + 1;
                    newSeq = 0;
                }

                // Use Interlocked.CompareExchange to ensure atomic update
                var updatedMs = Interlocked.CompareExchange(ref lastId.ms, newMs, originalMs);
                if (updatedMs == originalMs)
                {
                    // Successfully updated ms, now update seq
                    Interlocked.Exchange(ref lastId.seq, newSeq);
                    incrementedID.setMS(newMs);
                    incrementedID.setSeq(newSeq);
                    return;
                }

                // If we reach here, it means another thread has updated lastId.ms
                // Retry the operation
            }
        }

        /// <summary>
        /// Generate the next stream ID
        /// </summary>
        /// <returns>StreamID generated</returns>
        public void GenerateNextID(ref GarnetStreamID id)
        {
            // ulong timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            ulong timestamp = (ulong)Stopwatch.GetTimestamp() / (ulong)(Stopwatch.Frequency / 1000);

            // if this is the first entry or timestamp is greater than last added entry 
            if (totalEntriesAdded == 0 || timestamp > lastId.ms)
            {
                id.setMS(timestamp);
                id.setSeq(0);
                return;
            }
            // if timestamp is same as last added entry, increment the sequence number
            // if seq overflows, increment timestamp and reset the sequence number 
            IncrementID(ref id);
        }

        // TODO: implement this using parseState functions without operating with RespReadUtils
        unsafe bool parseIDString(ArgSlice idSlice, ref GarnetStreamID id)
        {
            // if we have to auto-generate the whole ID
            if (*idSlice.ptr == '*' && idSlice.length == 1)
            {
                GenerateNextID(ref id);
                return true;
            }

            // we have to parse user-defined ID
            // this can be of following formats: 
            // 1. ts (seq = 0)
            // 2. ts-* (auto-generate seq number)
            // 3. ts-seq

            // check if last character is a *
            if (*(idSlice.ptr + idSlice.length - 1) == '*')
            {
                // this has to be of format ts-*, so check if '-' is the preceding character
                if (*(idSlice.ptr + idSlice.length - 2) != '-')
                {
                    return false;
                }
                // parse the timestamp
                // slice the id to remove the last two characters
                var slicedId = new ArgSlice(idSlice.ptr, idSlice.length - 2);
                var idEnd = idSlice.ptr + idSlice.length - 2;
                if (!RespReadUtils.ReadUlong(out ulong timestamp, ref idSlice.ptr, idEnd))
                {
                    return false;
                }

                // check if timestamp is greater than last added entry
                if (totalEntriesAdded != 0 && timestamp < lastId.getMS())
                {
                    return false;
                }
                else if (totalEntriesAdded != 0 && timestamp == lastId.getMS())
                {
                    IncrementID(ref id);
                }
                else
                {
                    id.setMS(timestamp);
                    id.setSeq(0);
                }
            }
            else
            {
                // find index of '-' in the id
                int index = -1;
                for (int i = 0; i < idSlice.length; i++)
                {
                    if (*(idSlice.ptr + i) == '-')
                    {
                        index = i;
                        break;
                    }
                }
                // if '-' is not found, it has to be of format ts
                if (index == -1)
                {
                    if (!RespReadUtils.ReadUlong(out ulong timestamp, ref idSlice.ptr, idSlice.ptr + idSlice.length))
                    {
                        return false;
                    }
                    // check if timestamp is greater than last added entry
                    if (totalEntriesAdded != 0 && timestamp < lastId.getMS())
                    {
                        return false;
                    }
                    else if (totalEntriesAdded != 0 && timestamp == lastId.getMS())
                    {
                        IncrementID(ref id);
                    }
                    else
                    {
                        id.setMS(timestamp);
                        id.setSeq(0);
                    }
                }
                else
                {
                    // parse the timestamp
                    // slice the id to remove everything after '-'
                    var slicedId = new ArgSlice(idSlice.ptr, index);
                    var slicedSeq = new ArgSlice(idSlice.ptr + index + 1, idSlice.length - index - 1);
                    if (!RespReadUtils.ReadUlong(out ulong timestamp, ref idSlice.ptr, idSlice.ptr + index))
                    {
                        return false;
                    }
                    var seqBegin = idSlice.ptr + index + 1;
                    var seqEnd = idSlice.ptr + idSlice.length;
                    if (!RespReadUtils.ReadUlong(out ulong seq, ref seqBegin, seqEnd))
                    {
                        return false;
                    }

                    if (totalEntriesAdded != 0 && timestamp < lastId.getMS())
                    {
                        return false;
                    }
                    else if (totalEntriesAdded != 0 && timestamp == lastId.getMS())
                    {
                        if (seq <= lastId.getSeq())
                        {
                            return false;
                        }
                    }
                    id.setMS(timestamp);
                    id.setSeq(seq);
                }
            }

            return true;
        }

        /// <summary>
        /// Adds an entry or item to the stream
        /// </summary>
        /// <param name="value">byte array of the entry to store in the stream</param>
        /// <returns>True if entry is added successfully</returns>
        public unsafe void AddEntry(byte* value, int valueLength, ArgSlice idSlice, int numPairs, ref SpanByteAndMemory output)
        {
            byte* ptr = output.SpanByte.ToPointer();
            var curr = ptr;
            var end = curr + output.Length;
            MemoryHandle ptrHandle = default;
            bool isMemory = false;
            byte* tmpPtr = null;
            GarnetStreamID id = default;
            // take a lock to ensure thread safety
            _lock.WriteLock();
            try
            {
                bool canParseID = parseIDString(idSlice, ref id);
                if (!canParseID)
                {
                    while (!RespWriteUtils.WriteError("ERR Syntax", ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }

                // add the entry to the log
                {
                    bool enqueueInLog = log.TryEnqueueStreamEntry(id.idBytes, sizeof(GarnetStreamID), numPairs, value, valueLength, out long retAddress);
                    if (!enqueueInLog)
                    {
                        while (!RespWriteUtils.WriteError("ERR StreamAdd failed", ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        return;
                    }

                    var streamValue = new Value((ulong)retAddress);

                    bool added = index.Insert((byte*)Unsafe.AsPointer(ref id.idBytes[0]), streamValue);
                    // bool added = true;
                    if (!added)
                    {
                        while (!RespWriteUtils.WriteError("ERR StreamAdd failed", ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        return;
                    }
                    lastId.setMS(id.ms);
                    lastId.setSeq(id.seq);

                    totalEntriesAdded++;
                    // write back the ID of the entry added
                    string idString = $"{id.getMS()}-{id.getSeq()}";
                    while (!RespWriteUtils.WriteSimpleString(idString, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
            }
            finally
            {
                // log.Commit();

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr) + sizeof(ObjectOutputHeader);
                _lock.WriteUnlock();

            }

        }

        /// <summary>
        /// Get current length of the stream (number of entries in the stream)
        /// </summary>
        /// <returns>length of stream</returns>
        public ulong Length()
        {
            ulong len = 0;
            _lock.ReadLock();
            try
            {
                // get length of the stream from the index excluding tombstones
                len = index.ValidCount;
            }
            finally
            {
                _lock.ReadUnlock();
            }
            return len;
        }

        /// <summary>
        /// Deletes an entry fromt the stream
        /// </summary>
        /// <param name="idSlice">id of the stream entry to delete</param>
        /// <returns>true if entry was deleted successfully</returns>
        public unsafe bool DeleteEntry(ArgSlice idSlice)
        {
            // first parse the idString
            if (!parseCompleteID(idSlice, out GarnetStreamID entryID))
            {
                return false;
            }
            bool deleted = false;
            // take a lock to delete from the index
            _lock.WriteLock();
            try
            {
                deleted = index.Delete((byte*)Unsafe.AsPointer(ref entryID.idBytes[0]));
            }
            finally
            {
                _lock.WriteUnlock();
            }
            return deleted;
        }

        unsafe bool parseCompleteID(ArgSlice idSlice, out GarnetStreamID streamID)
        {
            streamID = default;
            // complete ID is of the format ts-seq in input where both ts and seq are ulong
            // find the index of '-' in the id
            int index = -1;
            for (int i = 0; i < idSlice.length; i++)
            {
                if (*(idSlice.ptr + i) == '-')
                {
                    index = i;
                    break;
                }
            }
            // parse the timestamp
            if (!RespReadUtils.ReadUlong(out ulong timestamp, ref idSlice.ptr, idSlice.ptr + index))
            {
                return false;
            }

            // after reading the timestamp, the pointer will be at the '-' character
            var seqBegin = idSlice.ptr + 1;
            // parse the sequence number
            if (!RespReadUtils.ReadUlong(out ulong seq, ref seqBegin, idSlice.ptr + idSlice.length - 1))
            {
                return false;
            }

            streamID.setMS(timestamp);
            streamID.setSeq(seq);
            return true;
        }



        /// <inheritdoc/>
        public void Dispose()
        {
            try
            {
                log.Dispose();
                device.Dispose();
            }
            finally
            {

            }
        }
    }
}