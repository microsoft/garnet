// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;
using Garnet.server.BTreeIndex;
using Garnet.common;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Garnet.server
{
    public enum StreamTrimOpts
    {
        MAXLEN,
        MINID,
        NONE
    }

    public enum XADDOpts
    {
        NOMKSTREAM,
        NONE
    }

    public enum ParsedStreamEntryID
    {
        VALID,
        INVALID,
        NOT_GREATER,
    }

    // This is the layout that is put in the log for each stream entry
    [StructLayout(LayoutKind.Sequential, Pack = 1, Size = 20)]
    public struct StreamLogEntryHeader
    {
        public StreamID id;
        public int numPairs;
    }

    public class StreamObject : IDisposable
    {
        readonly IDevice device;
        readonly TsavoriteLog log;
        readonly BTree index;
        StreamID lastId;
        long totalEntriesAdded;
        SingleWriterMultiReaderLock _lock;

        public StreamID LastId
        {
            get
            {
                // Need locking to prevent torn reads from AddEntry
                _lock.ReadLock();
                try
                {
                    return lastId;
                }
                finally
                {
                    _lock.ReadUnlock();
                }
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="logDir">Directory where the log will be stored</param>
        /// <param name="pageSize">Page size of the log used for the stream</param>
        public StreamObject(string logDir, long pageSize, long memorySize, int safeTailRefreshFreqMs)
        {
            device = logDir == null ? new NullDevice() : Devices.CreateLogDevice("streamLogs/" + logDir + "/streamLog", preallocateFile: false);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSize = pageSize, MemorySize = memorySize, SafeTailRefreshFrequencyMs = safeTailRefreshFreqMs });
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

        unsafe ParsedStreamEntryID parseIDString(PinnedSpanByte idSlice, ref StreamID id)
        {
            // if we have to auto-generate the whole ID
            if (*idSlice.ptr == '*' && idSlice.length == 1)
            {
                GenerateNextID(ref id);
                return ParsedStreamEntryID.VALID;
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
                    return ParsedStreamEntryID.INVALID;
                }
                // parse the timestamp
                // slice the id to remove the last two characters
                var slicedId = PinnedSpanByte.FromPinnedPointer(idSlice.ptr, idSlice.length - 2);
                var idEnd = idSlice.ptr + idSlice.length - 2;
                if (!RespReadUtils.ReadUlong(out ulong timestamp, ref idSlice.ptr, idEnd))
                {
                    return ParsedStreamEntryID.INVALID;
                }

                // check if timestamp is greater than last added entry's decoded ts
                if (totalEntriesAdded != 0 && timestamp < lastIdDecodedTs)
                {
                    return ParsedStreamEntryID.NOT_GREATER;
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
                        return ParsedStreamEntryID.INVALID;
                    }
                    // check if timestamp is greater than last added entry
                    if (totalEntriesAdded != 0 && timestamp < lastIdDecodedTs)
                    {
                        return ParsedStreamEntryID.NOT_GREATER;
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
                    var slicedId = PinnedSpanByte.FromPinnedPointer(idSlice.ptr, index);
                    var slicedSeq = PinnedSpanByte.FromPinnedPointer(idSlice.ptr + index + 1, idSlice.length - index - 1);
                    if (!RespReadUtils.ReadUlong(out ulong timestamp, ref idSlice.ptr, idSlice.ptr + index))
                    {
                        return ParsedStreamEntryID.INVALID;
                    }
                    var seqBegin = idSlice.ptr + index + 1;
                    var seqEnd = idSlice.ptr + idSlice.length;
                    if (!RespReadUtils.ReadUlong(out ulong seq, ref seqBegin, seqEnd))
                    {
                        return ParsedStreamEntryID.INVALID;
                    }

                    if (totalEntriesAdded != 0 && timestamp < lastIdDecodedTs)
                    {
                        return ParsedStreamEntryID.NOT_GREATER;
                    }
                    else if (totalEntriesAdded != 0 && timestamp == lastIdDecodedTs)
                    {
                        if (seq <= lastId.seq)
                        {
                            return ParsedStreamEntryID.INVALID;
                        }
                    }
                    // use ID and seq given by user 
                    // encode while storing
                    id.setMS(timestamp);
                    id.setSeq(seq);
                }
            }

            return ParsedStreamEntryID.VALID;
        }

        /// <summary>
        /// Adds an entry or item to the stream
        /// </summary>
        public unsafe void AddEntry(PinnedSpanByte idSlice, int numPairs, ReadOnlySpan<byte> rawFieldValuePairs, ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            byte* tmpPtr = null;
            StreamID id = default;
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output);

            // take a lock to ensure thread safety
            _lock.WriteLock();
            try
            {
                var parsedIDStatus = parseIDString(idSlice, ref id);
                if (parsedIDStatus == ParsedStreamEntryID.INVALID)
                {
                    writer.WriteError(CmdStrings.RESP_ERR_XADD_INVALID_STREAM_ID);
                    return;
                }
                else if (parsedIDStatus == ParsedStreamEntryID.NOT_GREATER)
                {
                    writer.WriteError(CmdStrings.RESP_ERR_XADD_ID_NOT_GREATER);
                    return;
                }

                // add the entry to the log
                StreamLogEntryHeader header = new StreamLogEntryHeader
                {
                    id = id,
                    numPairs = numPairs,
                };

                log.Enqueue<StreamLogEntryHeader>(header, item: rawFieldValuePairs, out long returnedLogicalAddr);

                var streamValue = new Value((ulong)returnedLogicalAddr);

                bool added = index.Insert((byte*)Unsafe.AsPointer(ref id.idBytes[0]), streamValue);

                if (!added)
                {
                    writer.WriteNull();
                    return;
                }

                // copy encoded ms and seq
                lastId.ms = id.ms;
                lastId.seq = id.seq;

                totalEntriesAdded++;

                ulong idMS = id.getMS();
                ulong idSeq = id.getSeq();
                Span<byte> outputBuffer = stackalloc byte[(NumUtils.MaximumFormatInt64Length * 2) + 1];
                int len = NumUtils.WriteInt64((long)idMS, outputBuffer);
                outputBuffer[len++] = (byte)'-';
                len += NumUtils.WriteInt64((long)idSeq, outputBuffer.Slice(len));

                writer.WriteBulkString(outputBuffer.Slice(0, len));
            }
            finally
            {
                // log.Commit();
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
        /// Deletes an entry from the stream
        /// </summary>
        /// <param name="idSlice">id of the stream entry to delete</param>
        /// <returns>true if entry was deleted successfully</returns>
        public unsafe bool DeleteEntry(PinnedSpanByte idSlice)
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


        // Read the last entry in the stream and into output
        internal unsafe void ReadLastEntry(ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var writer = new RespMemoryWriter(respProtocolVersion, ref output);
            try
            {
                _lock.ReadLock();
                try
                {
                    if (index.Count() == 0)
                    {
                        writer.WriteNull();
                        return;
                    }

                    // LastAlive to skip tombstoned entries
                    long addressOnLog = (long)index.LastAlive().Value.address;
                    (byte[] entry, int len) = (null, 0); // log.Read(addressOnLog, readUncommitted: true);

                    if (entry == null)
                    {
                        writer.WriteNull();
                        return;
                    }

                    ReadOnlySpan<byte> entrySp = entry.AsSpan(sizeof(long), len - sizeof(long)); // skip the previousEntryAddress part
                    // HK TODO: this is broken atm
                    //WriteEntryToWriter(entrySp, ref writer, len);
                }
                finally
                {
                    _lock.ReadUnlock();
                }
            }
            finally
            {
                writer.Dispose();
            }
        }

        /// <summary>
        /// Read entries from the stream from given range
        /// </summary>
        /// <param name="min">start of range</param>
        /// <param name="max">end of range</param>
        /// <param name="limit">threshold to scanning</param>
        /// <param name="output"></param>
        public unsafe void ReadRange(string min, string max, int limit, ref SpanByteAndMemory output, byte respProtocolVersion, bool isReverse = false)
        {
            var writer = new RespMemoryWriter(respProtocolVersion, ref output);
            try
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
                    else if (min == "+") // this can happen in reverse range queries
                    {
                        byte[] idBytes = index.Last().Key;
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
                    else if (max == "-") // this can happen in reverse range queries
                    {
                        byte[] idBytes = index.First().Key;
                        endID = new StreamID(idBytes);
                    }
                    else if (!ParseStreamIDFromString(max, out endID))
                    {
                        return;
                    }

                    int count = index.Get((byte*)Unsafe.AsPointer(ref startID.idBytes[0]), (byte*)Unsafe.AsPointer(ref endID.idBytes[0]), out Value startVal, out Value endVal, out var tombstones, limit, isReverse);

                    if (isReverse)
                    {
                        startAddr = (long)startVal.address;
                        endAddr = (long)endVal.address;
                    }
                    else
                    {
                        startAddr = (long)startVal.address;
                        endAddr = (long)endVal.address + 1;
                    }

                    long readCount = 0;
                    try
                    {
                        using (var iter = log.Scan(startAddr, endAddr, scanUncommitted: true)) // isReverseStreamIter: isReverse))
                        {
                            writer.WriteArrayLength(count);

                            while (iter.GetNext(out byte[] entry, out _, out long currentAddress, out long nextAddress))
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

                                WriteEntryToWriter(entry, ref writer);

                                readCount++;
                                if (limit != -1 && readCount == limit)
                                {
                                    break;
                                }
                            }
                        }
                    }
                    finally
                    { }
                }
                finally
                {
                    _lock.ReadUnlock();
                }
            }
            finally
            {
                writer.Dispose();
            }
        }

        /// <summary>
        /// Trims the stream based on the specified options.
        /// </summary>
        /// <param name="trimArg">length or ID specifying the threshold</param>
        /// <param name="optType">MAXLEN or MINID</param>
        /// <param name="entriesTrimmed">number of keys trimmed</param>
        /// <returns></returns>
        public unsafe bool Trim(PinnedSpanByte trimArg, StreamTrimOpts optType, out ulong entriesTrimmed, bool approximate = false)
        {
            uint numLeavesDeleted = 0;
            Value headValue = default;
            _lock.WriteLock();
            try
            {
                switch (optType)
                {
                    case StreamTrimOpts.MAXLEN:
                        if (!RespReadUtils.ReadUlong(out ulong maxLen, ref trimArg.ptr, trimArg.ptr + trimArg.length))
                        {
                            entriesTrimmed = 0;
                            return false;
                        }
                        index.TrimByLength(maxLen, out entriesTrimmed, out headValue, out var headValidKey, out numLeavesDeleted, approximate);
                        break;
                    case StreamTrimOpts.MINID:
                        if (!parseCompleteID(trimArg, out StreamID minID))
                        {
                            entriesTrimmed = 0;
                            return false;
                        }
                        index.TrimByID((byte*)Unsafe.AsPointer(ref minID.idBytes[0]), out entriesTrimmed, out headValue, out headValidKey, out numLeavesDeleted);
                        break;
                    default:
                        entriesTrimmed = 0;
                        break;
                }

                if (numLeavesDeleted == 0)
                {
                    // didn't delete any leaf nodes so done here 
                    return true;
                }
                // truncate log to new head 
                var newHeadAddress = (long)headValue.address;
                log.TruncateUntil(newHeadAddress);
            }
            finally
            {
                _lock.WriteUnlock();
            }
            return true;
        }

        unsafe void WriteEntryToWriter(ReadOnlySpan<byte> entryBytes, ref RespMemoryWriter writer)
        {
            // each response entry is an array of two items: ID and array of key-value pairs
            writer.WriteArrayLength(2);

            // Read the first 20 bytes into our StreamLogEntryHeader struct
            StreamLogEntryHeader streamLogEntryHeader = MemoryMarshal.Read<StreamLogEntryHeader>(entryBytes.Slice(0, sizeof(StreamLogEntryHeader)));
            StreamID entryID = streamLogEntryHeader.id;

            // first item in the array is the ID
            WriteStreamIdToWriter(entryID, ref writer);

            // Second item is an array so write the subarray length
            int numPairs = streamLogEntryHeader.numPairs;
            writer.WriteArrayLength(numPairs);

            // this is a serialized ReadOnlySpan<byte> of field-value pairs, we want to copy it directly into the writer
            int serializedSpanLength = MemoryMarshal.Read<int>(entryBytes.Slice(sizeof(StreamLogEntryHeader)));
            int valueOffset = sizeof(StreamLogEntryHeader) + sizeof(int);
            ReadOnlySpan<byte> value = entryBytes.Slice(valueOffset, serializedSpanLength);
            writer.WriteDirect(value);
        }


        unsafe bool parseCompleteID(PinnedSpanByte idSlice, out StreamID streamID)
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

        public static bool ParseCompleteStreamIDFromString(ReadOnlySpan<char> idString, out StreamID id)
        {
            id = default;
            int hyphenIdx = -1;
            for (int i = 0; i < idString.Length; i++)
            {
                if (idString[i] == '-')
                {
                    if (hyphenIdx != -1)
                    {
                        // more than 1 occurence of hypen
                        return false;
                    }
                    hyphenIdx = i;
                }
            }

            // no occurence of hypen
            if (hyphenIdx == -1)
                return false;

            if (!ulong.TryParse(idString.Slice(0, hyphenIdx), out ulong timestamp))
            {
                return false;
            }
            if (!ulong.TryParse(idString.Slice(hyphenIdx + 1), out ulong seq))
            {
                return false;
            }

            id.setMS(timestamp);
            id.setSeq(seq);
            return true;
        }

        public static bool ParseStreamIDFromString(ReadOnlySpan<char> idString, out StreamID id)
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

        // Util to write without doing temp heap allocations
        private static void WriteStreamIdToWriter(StreamID id, ref RespMemoryWriter writer)
        {
            Span<byte> outputBuffer = stackalloc byte[(NumUtils.MaximumFormatInt64Length * 2) + 1];
            ulong idMS = id.getMS();
            ulong idSeq = id.getSeq();
            int len = NumUtils.WriteInt64((long)idMS, outputBuffer);
            outputBuffer[len++] = (byte)'-';
            len += NumUtils.WriteInt64((long)idSeq, outputBuffer.Slice(len));
            writer.WriteBulkString(outputBuffer.Slice(0, len));
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
            { }
        }
    }
}