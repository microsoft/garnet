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
using System.Buffers.Binary;

namespace Garnet.server
{
    public enum XTRIMOpts
    {
        MAXLEN,
        MINID,
        NONE
    }

    public enum XADDOpts
    {
        
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
        public StreamObject(string logDir, long pageSize, long memorySize, int safeTailRefreshFreqMs)
        {
            device = logDir == null ? new NullDevice() : Devices.CreateLogDevice("streamLogs/" + logDir + "/streamLog", preallocateFile: false);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSize = pageSize, MemorySize = memorySize, SafeTailRefreshFrequencyMs = safeTailRefreshFreqMs});
            index = new BTree(device.SectorSize);
            totalEntriesAdded = 0;
            lastId = default;
            _lock = new SingleWriterMultiReaderLock();
        }

        /// <summary>
        /// Increment the stream ID
        /// </summary>
        /// <param name="incrementedID">carries the incremented stream id</param>
        public void IncrementID(ref StreamID incrementedID)
        {
            var originalMs = lastId.getMS();
            var originalSeq = lastId.getSeq();

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
                newMs += 1;
                newSeq = 0;
            }

            incrementedID.setMS(newMs);
            incrementedID.setSeq(newSeq);
            
        }

        /// <summary>
        /// Generate the next stream ID
        /// </summary>
        /// <returns>StreamID generated</returns>
        public unsafe void GenerateNextID(ref StreamID id)
        {
            ulong timestamp = (ulong)Stopwatch.GetTimestamp() / (ulong)(Stopwatch.Frequency / 1000);

            // read existing timestamp in big endian format
            var lastTs = lastId.getMS();
            // if this is the first entry or timestamp is greater than last added entry 
            if (totalEntriesAdded == 0 || timestamp > lastTs)
            {
                // this will write timestamp in big endian format 
                id.setMS(timestamp);
                id.setSeq(0);
                return;
            }
            // if timestamp is same as last added entry, increment the sequence number
            // if seq overflows, increment timestamp and reset the sequence number 
            IncrementID(ref id);
        }

        // TODO: implement this using parseState functions without operating with RespReadUtils
        unsafe bool parseIDString(ArgSlice idSlice, ref StreamID id)
        {
            // if we have to auto-generate the whole ID
            if (*idSlice.ptr == '*' && idSlice.length == 1)
            {
                GenerateNextID(ref id);
                return true;
            }

            var lastIdDecodedTs = lastId.getMS();

            // parse user-defined ID
            // can be of following formats: 
                // 1. ts (seq = 0)
                // 2. ts-* (auto-generate seq number)
                // 3. ts-seq

            // last character is a *
            if (*(idSlice.ptr + idSlice.length - 1) == '*')
            {
                // has to be of format ts-*,  check if '-' is the preceding character
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
                
                // check if timestamp is greater than last added entry's decoded ts
                if (totalEntriesAdded != 0 && timestamp < lastIdDecodedTs)
                {
                    return false;
                }
                else if (totalEntriesAdded != 0 && timestamp == lastIdDecodedTs)
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
                // if '-' is not found, format should be just ts
                if (index == -1)
                {
                    if (!RespReadUtils.ReadUlong(out ulong timestamp, ref idSlice.ptr, idSlice.ptr + idSlice.length))
                    {
                        return false;
                    }
                    // check if timestamp is greater than last added entry
                    if (totalEntriesAdded != 0 && timestamp < lastIdDecodedTs)
                    {
                        return false;
                    }
                    else if (totalEntriesAdded != 0 && timestamp == lastIdDecodedTs)
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

                    if (totalEntriesAdded != 0 && timestamp < lastIdDecodedTs)
                    {
                        return false;
                    }
                    else if (totalEntriesAdded != 0 && timestamp == lastIdDecodedTs)
                    {
                        if (seq <= lastId.seq)
                        {
                            return false;
                        }
                    }
                    // use ID and seq given by user 
                    // encode while storing
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
            StreamID id = default;
            // take a lock to ensure thread safety
            _lock.WriteLock();
            try
            {
                bool canParseID = parseIDString(idSlice, ref id);
                if (!canParseID)
                {
                    while (!RespWriteUtils.TryWriteError("ERR Syntax", ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }

                // add the entry to the log
                {
                    bool enqueueInLog = log.TryEnqueueStreamEntry(id.idBytes, sizeof(StreamID), numPairs, value, valueLength, out long retAddress);
                    if (!enqueueInLog)
                    {
                        while (!RespWriteUtils.TryWriteError("ERR StreamAdd failed", ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        return;
                    }

                    var streamValue = new Value((ulong)retAddress);

                    bool added = index.Insert((byte*)Unsafe.AsPointer(ref id.idBytes[0]), streamValue);
                    // bool added = true;
                    if (!added)
                    {
                        while (!RespWriteUtils.TryWriteError("ERR StreamAdd failed", ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        return;
                    }
                    // copy encoded ms and seq
                    lastId.ms = (id.ms);
                    lastId.seq = (id.seq);

                    totalEntriesAdded++;
                    // write back the decoded ID of the entry added
                    string idString = $"{id.getMS()}-{id.getSeq()}";
                    while (!RespWriteUtils.TryWriteSimpleString(idString, ref curr, end))
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
            if (!parseCompleteID(idSlice, out StreamID entryID))
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

        public bool ParseCompleteStreamIDFromString(string idString, out StreamID id)
        {
            id = default;
            string[] parts = idString.Split('-');
            if (parts.Length != 2)
            {
                return false;
            }
            if (!ulong.TryParse(parts[0], out ulong timestamp))
            {
                return false;
            }
            if (!ulong.TryParse(parts[1], out ulong seq))
            {
                return false;
            }

            id.setMS(timestamp);
            id.setSeq(seq);
            return true;
        }

        public bool ParseStreamIDFromString(string idString, out StreamID id)
        {
            id = default;
            if (idString == "-" || idString == "+")
            {
                return false;
            }
            if (!idString.Contains('-'))
            {
                
                if (!ulong.TryParse(idString, out ulong ms))
                {
                    return false;
                }
                id.setMS(ms);
                id.setSeq(0);   
                return true;
            }
            return ParseCompleteStreamIDFromString(idString, out id);
        }

        /// <summary>
        /// Read entries from the stream from given range
        /// </summary>
        /// <param name="min">start of range</param>
        /// <param name="max">end of range</param>
        /// <param name="limit">threshold to scanning</param>
        /// <param name="output"></param>
        public unsafe void ReadRange(string min, string max, int limit, ref SpanByteAndMemory output)
        {
            _lock.ReadLock();
            try
            {
                if (index.Count() == 0)
                {
                    return;
                }

                long startAddr, endAddr;
                StreamID startID, endID;
                if (min == "-")
                {
                    byte[] idBytes = index.First().Key;
                    startID = new StreamID(idBytes);
                }
                else if (!ParseStreamIDFromString(min, out startID))
                {
                    return;
                }
                if (max == "+")
                {
                    byte[] idBytes = index.Last().Key;
                    endID = new StreamID(idBytes);
                }
                else
                {
                    if (!ParseStreamIDFromString(max, out endID))
                    {
                        return;
                    }
                    //endID.seq = long.MaxValue;
                    endID.setSeq(long.MaxValue);
                }

                int count = index.Get((byte*)Unsafe.AsPointer(ref startID.idBytes[0]), (byte*)Unsafe.AsPointer(ref endID.idBytes[0]), out Value startVal, out Value endVal, out var tombstones, limit);
                startAddr = (long)startVal.address;
                endAddr = (long)endVal.address + 1;

                byte* ptr = output.SpanByte.ToPointer();
                var curr = ptr;
                var end = curr + output.Length;
                MemoryHandle ptrHandle = default;
                bool isMemory = false;
                byte* tmpPtr = null;
                int tmpSize = 0;
                long readCount = 0;

                try
                {
                    using (var iter = log.Scan(startAddr, endAddr, scanUncommitted: true))
                    {

                        // write length of how many entries we will print out 
                        while (!RespWriteUtils.TryWriteArrayLength(count, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                        byte* e;
                        while (iter.GetNext(out var entry, out _, out long currentAddress, out long nextAddress))
                        {

                            var current = new Value((ulong)currentAddress);
                            // check if any tombstone t.address matches current
                            var tombstoneFound = false;
                            foreach (var tombstone in tombstones)
                            {
                                if (tombstone.address == current.address)
                                {
                                    tombstoneFound = true;
                                    break;
                                }
                            }
                            if (tombstoneFound)
                            {
                                continue;
                            }

                            var entryBytes = entry.AsSpan();
                            // check if the entry is actually one of the qualified keys 
                            // parse ID for the entry which is the first 16 bytes
                            var idBytes = entryBytes.Slice(0, 16);
                            var ts = BinaryPrimitives.ReadUInt64BigEndian(idBytes.Slice(0, 8));
                            var seq = BinaryPrimitives.ReadUInt64BigEndian(idBytes.Slice(8, 8));
                            // var ts = BitConverter.ToUInt64(idBytes.Slice(0, 8));
                            // var seq = BitConverter.ToUInt64(idBytes.Slice(8, 8));
                            string idString = $"{ts}-{seq}";
                            Span<byte> numPairsBytes = entryBytes.Slice(16, 4);
                            int numPairs = BitConverter.ToInt32(numPairsBytes);
                            Span<byte> value = entryBytes.Slice(20);

                            // we can already write back the ID that we read 
                            while (!RespWriteUtils.TryWriteArrayLength(2, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            if (!RespWriteUtils.TryWriteSimpleString(idString, ref curr, end))
                            {
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            }

                            // print array length for the number of key-value pairs in the entry
                            while (!RespWriteUtils.TryWriteArrayLength(numPairs, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                            // write key-value pairs
                            fixed (byte* p = value)
                            {
                                e = p;
                                int read = 0;
                                read += (int)(e - p);
                                while (value.Length - read >= 4)
                                {
                                    var orig = e;
                                    if (!RespReadUtils.TryReadPtrWithLengthHeader(ref tmpPtr, ref tmpSize, ref e, e + entry.Length))
                                    {
                                        return;
                                    }
                                    var o = new Span<byte>(tmpPtr, tmpSize).ToArray();
                                    while (!RespWriteUtils.TryWriteBulkString(o, ref curr, end))
                                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                                    read += (int)(e - orig);
                                }
                            }
                            readCount++;
                            if (limit != -1 && readCount == limit)
                            {
                                break;
                            }
                        }
                    }
                }
                finally
                {
                    if (isMemory) ptrHandle.Dispose();
                    output.Length = (int)(curr - ptr) + sizeof(ObjectOutputHeader);
                }
            }
            finally
            {
                _lock.ReadUnlock();
            }
        }


        unsafe bool parseCompleteID(ArgSlice idSlice, out StreamID streamID)
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