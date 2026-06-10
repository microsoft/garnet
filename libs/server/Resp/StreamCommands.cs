// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        readonly StreamManager streamManager;

        /// <summary>
        /// Adds a new entry to the stream.
        /// </summary>
        /// <returns>true if stream was added successfully; error otherwise</returns> 
        private bool StreamAdd(byte respProtocolVersion)
        {
            if (parseState.Count < 4)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_XADD_WRONG_NUM_ARGS);
            }

            // Parse the stream key.
            var key = parseState.GetArgSliceByRef(0);

            var idIndex = 1;
            var options = StreamAddOptions.None;
            if (parseState.GetArgSliceByRef(idIndex).ReadOnlySpan.SequenceEqual("NOMKSTREAM"u8))
            {
                options = StreamAddOptions.NoMkStream;
                idIndex++;
            }

            // The object receives [id, field1, value1, ...]; NOMKSTREAM rides in arg1.
            var header = new RespInputHeader(GarnetObjectType.Stream) { StreamOp = StreamOperation.XADD };
            var input = new ObjectInput(header, ref parseState, startIdx: idIndex, arg1: (int)options);
            var output = new ObjectOutput { SpanByteAndMemory = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr)) };

            var status = storageSession.StreamObjectRMW(key, ref input, ref output);
            if (status == GarnetStatus.NOTFOUND)
            {
                // NOMKSTREAM and the stream does not exist — reply with null.
                while (!RespWriteUtils.TryWriteNull(ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            ProcessOutput(output.SpanByteAndMemory);
            return true;
        }

        /// <summary>
        /// Retrieves the length of the stream.
        /// </summary>
        /// <returns>true if stream length was retrieved successfully; error otherwise</returns>
        private bool StreamLength()
        {
            if (parseState.Count != 1)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_XLEN_WRONG_NUM_ARGS);
            }
            // parse the stream key. 
            var key = parseState.GetArgSliceByRef(0);

            var header = new RespInputHeader(GarnetObjectType.Stream) { StreamOp = StreamOperation.XLEN };
            var input = new ObjectInput(header, ref parseState);
            var output = new ObjectOutput { SpanByteAndMemory = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr)) };

            var status = storageSession.StreamObjectRead(key, ref input, ref output);
            if (status == GarnetStatus.NOTFOUND)
            {
                // Non-existent stream is treated as empty.
                while (!RespWriteUtils.TryWriteInt64(0, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            ProcessOutput(output.SpanByteAndMemory);
            return true;
        }

        /// <summary>
        ///  Retrieves a range of stream entries.
        /// </summary>
        /// <returns>true if range of stream entries were retrieved successfully; error otherwise</returns>
        public bool StreamRange(byte respProtocolVersion, bool isReverse = false)
        {
            // command is of format: XRANGE key start end [COUNT count]
            // and for XREVRANGE key end start [COUNT count]

            // we expect at least 3 arguments
            if (parseState.Count < 3)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_XRANGE_WRONG_NUM_ARGS);
            }

            // parse the stream key 
            var key = parseState.GetArgSliceByRef(0);

            var header = new RespInputHeader(GarnetObjectType.Stream) { StreamOp = isReverse ? StreamOperation.XREVRANGE : StreamOperation.XRANGE };
            var input = new ObjectInput(header, ref parseState);
            var output = new ObjectOutput { SpanByteAndMemory = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr)) };

            var status = storageSession.StreamObjectRead(key, ref input, ref output);
            if (status == GarnetStatus.NOTFOUND)
            {
                // return empty array
                while (!RespWriteUtils.TryWriteArrayLength(0, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            ProcessOutput(output.SpanByteAndMemory);
            return true;
        }

        /// <summary>
        /// Deletes stream entry(s).
        /// </summary>
        /// <returns>true if stream entry(s) was deleted successfully; error otherwise</returns>
        public bool StreamDelete()
        {
            // command is of format: XDEL key id [id ...]
            // we expect at least 2 arguments
            if (parseState.Count < 2)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_XDEL_WRONG_NUM_ARGS);
            }

            // parse the stream key
            var key = parseState.GetArgSliceByRef(0);

            var header = new RespInputHeader(GarnetObjectType.Stream) { StreamOp = StreamOperation.XDEL };
            var input = new ObjectInput(header, ref parseState);
            var output = new ObjectOutput { SpanByteAndMemory = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr)) };

            var status = storageSession.StreamObjectRMW(key, ref input, ref output);
            if (status == GarnetStatus.NOTFOUND)
            {
                // Nothing to delete from a non-existent stream.
                while (!RespWriteUtils.TryWriteInt64(0, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            ProcessOutput(output.SpanByteAndMemory);
            return true;
        }

        /// <summary>
        /// Trims the stream to the specified length or ID.
        /// </summary>
        /// <returns>returns true if stream was trimmed successfully; error otherwise</returns>
        public bool StreamTrim()
        {
            if (parseState.Count < 3)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_XTRIM_WRONG_NUM_ARGS);
            }

            var key = parseState.GetArgSliceByRef(0);

            var header = new RespInputHeader(GarnetObjectType.Stream) { StreamOp = StreamOperation.XTRIM };
            var input = new ObjectInput(header, ref parseState);
            var output = new ObjectOutput { SpanByteAndMemory = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr)) };

            var status = storageSession.StreamObjectRMW(key, ref input, ref output);
            if (status == GarnetStatus.NOTFOUND)
            {
                // Trimming a non-existent stream removes nothing.
                while (!RespWriteUtils.TryWriteInt64(0, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            ProcessOutput(output.SpanByteAndMemory);
            return true;
        }

        /// <summary>
        ///  Gets last entry in the stream.
        /// XLAST key
        /// </summary>
        /// <returns></returns>
        public bool StreamLast(byte respProtocolVersion)
        {
            if (parseState.Count != 1)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS);
            }

            var key = parseState.GetArgSliceByRef(0);

            var header = new RespInputHeader(GarnetObjectType.Stream) { StreamOp = StreamOperation.XLAST };
            var input = new ObjectInput(header, ref parseState);
            var output = new ObjectOutput { SpanByteAndMemory = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr)) };

            var status = storageSession.StreamObjectRead(key, ref input, ref output);
            if (status == GarnetStatus.NOTFOUND)
            {
                // return empty array
                while (!RespWriteUtils.TryWriteArrayLength(0, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            ProcessOutput(output.SpanByteAndMemory);
            return true;
        }

        /// <summary>
        /// XGROUP CREATE|SETID|DESTROY|CREATECONSUMER|DELCONSUMER
        /// </summary>
        private bool StreamGroup(byte respProtocolVersion)
        {
            if (parseState.Count < 2)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            var subCommand = parseState.GetArgSliceByRef(0).ToString().ToUpperInvariant();

            switch (subCommand)
            {
                case "CREATE":
                    return StreamGroupCreate(respProtocolVersion);
                case "SETID":
                    return StreamGroupSetId(respProtocolVersion);
                case "DESTROY":
                    return StreamGroupDestroy();
                case "CREATECONSUMER":
                    return StreamGroupCreateConsumer();
                case "DELCONSUMER":
                    return StreamGroupDelConsumer();
                default:
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
            }
        }

        // XGROUP CREATE key group id [MKSTREAM] [ENTRIESREAD n]
        private bool StreamGroupCreate(byte respProtocolVersion)
        {
            if (parseState.Count < 4)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            PinnedSpanByte key = parseState.GetArgSliceByRef(1);
            string groupName = parseState.GetArgSliceByRef(2).ToString();
            string idStr = parseState.GetArgSliceByRef(3).ToString();

            bool mkStream = false;
            long entriesRead = -1;
            int argIdx = 4;

            while (argIdx < parseState.Count)
            {
                var opt = parseState.GetArgSliceByRef(argIdx).ToString().ToUpperInvariant();
                if (opt == "MKSTREAM")
                {
                    mkStream = true;
                    argIdx++;
                }
                else if (opt == "ENTRIESREAD" && argIdx + 1 < parseState.Count)
                {
                    if (!long.TryParse(parseState.GetArgSliceByRef(argIdx + 1).ToString(), out entriesRead))
                        return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                    argIdx += 2;
                }
                else
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                }
            }

            // Resolve "$" to the stream's last ID
            StreamID startId;
            if (idStr == "$")
            {
                startId = default;
                // Get stream's last ID. FindStream returns null if not found.
                // For $, we want whatever the last ID is — if stream doesn't exist yet,
                // 0-0 is fine (everything will be "new").
                // We'll let StreamGroupCreate resolve it.
            }
            else if (idStr == "0" || idStr == "0-0")
            {
                startId = new StreamID(0, 0);
                if (entriesRead < 0) entriesRead = 0;
            }
            else if (!StreamObject.ParseCompleteStreamIDFromString(idStr, out startId))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_XADD_INVALID_STREAM_ID);
            }

            SpanByteAndMemory output = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr));
            bool created = streamManager.StreamGroupCreate(key, groupName, startId, entriesRead, mkStream, ref output, respProtocolVersion);

            if (!created)
            {
                // Group already exists → BUSYGROUP, or stream doesn't exist
                while (!RespWriteUtils.TryWriteError("ERR no such key"u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            while (!RespWriteUtils.TryWriteSimpleString("OK"u8, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        // XGROUP SETID key group id [ENTRIESREAD n]
        private bool StreamGroupSetId(byte respProtocolVersion)
        {
            if (parseState.Count < 4)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            var key = parseState.GetArgSliceByRef(1);
            var groupName = parseState.GetArgSliceByRef(2).ToString();
            var idStr = parseState.GetArgSliceByRef(3).ToString();

            long entriesRead = -1;
            if (parseState.Count >= 6)
            {
                var opt = parseState.GetArgSliceByRef(4).ToString().ToUpperInvariant();
                if (opt == "ENTRIESREAD")
                {
                    if (!long.TryParse(parseState.GetArgSliceByRef(5).ToString(), out entriesRead))
                        return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                }
            }

            StreamID id;
            if (idStr == "$")
            {
                id = default; // Will resolve to stream's last ID
            }
            else if (!StreamObject.ParseCompleteStreamIDFromString(idStr, out id))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_XADD_INVALID_STREAM_ID);
            }

            if (streamManager.StreamGroupSetId(key, groupName, id, entriesRead))
            {
                while (!RespWriteUtils.TryWriteSimpleString("OK"u8, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteError("NOGROUP No such consumer group"u8, ref dcurr, dend))
                    SendAndReset();
            }
            return true;
        }

        // XGROUP DESTROY key group
        private bool StreamGroupDestroy()
        {
            if (parseState.Count < 3)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            var key = parseState.GetArgSliceByRef(1);
            var groupName = parseState.GetArgSliceByRef(2).ToString();

            bool destroyed = streamManager.StreamGroupDestroy(key, groupName);
            while (!RespWriteUtils.TryWriteInt64(destroyed ? 1 : 0, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        // XGROUP CREATECONSUMER key group consumer
        private bool StreamGroupCreateConsumer()
        {
            if (parseState.Count < 4)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            var key = parseState.GetArgSliceByRef(1);
            var groupName = parseState.GetArgSliceByRef(2).ToString();
            var consumerName = parseState.GetArgSliceByRef(3).ToString();

            var result = streamManager.StreamGroupCreateConsumer(key, groupName, consumerName);
            if (result == null)
            {
                while (!RespWriteUtils.TryWriteError("NOGROUP No such consumer group"u8, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteInt64(result.Value ? 1 : 0, ref dcurr, dend))
                    SendAndReset();
            }
            return true;
        }

        // XGROUP DELCONSUMER key group consumer
        private bool StreamGroupDelConsumer()
        {
            if (parseState.Count < 4)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            var key = parseState.GetArgSliceByRef(1);
            var groupName = parseState.GetArgSliceByRef(2).ToString();
            var consumerName = parseState.GetArgSliceByRef(3).ToString();

            int removed = streamManager.StreamGroupDeleteConsumer(key, groupName, consumerName);
            if (removed < 0)
            {
                while (!RespWriteUtils.TryWriteError("NOGROUP No such consumer group"u8, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteInt64(removed, ref dcurr, dend))
                    SendAndReset();
            }
            return true;
        }

        /// <summary>
        /// XREADGROUP GROUP group consumer [COUNT count] [NOACK] STREAMS key [key...] id [id...]
        /// </summary>
        private bool StreamReadGroup(byte respProtocolVersion)
        {
            if (parseState.Count < 6)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            int argIdx = 0;

            // Parse "GROUP group consumer"
            var groupKw = parseState.GetArgSliceByRef(argIdx++).ToString().ToUpperInvariant();
            if (groupKw != "GROUP")
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            var groupName = parseState.GetArgSliceByRef(argIdx++).ToString();
            var consumerName = parseState.GetArgSliceByRef(argIdx++).ToString();

            int count = -1;
            bool noAck = false;

            // Parse optional COUNT and NOACK
            while (argIdx < parseState.Count)
            {
                var opt = parseState.GetArgSliceByRef(argIdx).ToString().ToUpperInvariant();
                if (opt == "COUNT" && argIdx + 1 < parseState.Count)
                {
                    if (!int.TryParse(parseState.GetArgSliceByRef(argIdx + 1).ToString(), out count))
                        return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                    argIdx += 2;
                }
                else if (opt == "NOACK")
                {
                    noAck = true;
                    argIdx++;
                }
                else if (opt == "STREAMS")
                {
                    argIdx++; // consume STREAMS keyword
                    break;
                }
                else
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                }
            }

            // Remaining args are key [key...] id [id...] — equal count of keys and IDs
            int remaining = parseState.Count - argIdx;
            if (remaining < 2 || remaining % 2 != 0)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            int numStreams = remaining / 2;
            var keys = new PinnedSpanByte[numStreams];
            var ids = new string[numStreams];

            for (int i = 0; i < numStreams; i++)
                keys[i] = parseState.GetArgSliceByRef(argIdx + i);
            for (int i = 0; i < numStreams; i++)
                ids[i] = parseState.GetArgSliceByRef(argIdx + numStreams + i).ToString();

            // XREADGROUP always returns multi-stream format: *N [*2 $key *M entries...]
            var _output = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr));
            var writer = new RespMemoryWriter(respProtocolVersion, ref _output);
            writer.WriteArrayLength(numStreams);
            for (int i = 0; i < numStreams; i++)
            {
                writer.WriteArrayLength(2);
                writer.WriteBulkString(keys[i].ReadOnlySpan);

                // Collect entries into a temp buffer
                var streamOutput = new SpanByteAndMemory();
                bool found = streamManager.StreamReadGroup(keys[i], groupName, consumerName, ids[i], count, noAck, ref streamOutput, respProtocolVersion);
                if (!found)
                {
                    // Group not found — discard partial output and write error
                    writer.Dispose();
                    while (!RespWriteUtils.TryWriteError("NOGROUP No such consumer group"u8, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                // Inline the entries sub-array
                if (streamOutput.IsSpanByte)
                {
                    writer.WriteDirect(streamOutput.SpanByte.ReadOnlySpan);
                }
                else if (streamOutput.Memory != null)
                {
                    writer.WriteDirect(streamOutput.Memory.Memory.Span.Slice(0, streamOutput.Length));
                    streamOutput.Memory.Dispose();
                }
            }
            writer.Dispose();
            ProcessOutput(_output);
            return true;
        }

        /// <summary>
        /// XACK key group id [id ...]
        /// </summary>
        private bool StreamAck(byte respProtocolVersion)
        {
            if (parseState.Count < 3)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            var key = parseState.GetArgSliceByRef(0);
            var groupName = parseState.GetArgSliceByRef(1).ToString();

            var ids = new StreamID[parseState.Count - 2];
            for (int i = 2; i < parseState.Count; i++)
            {
                var idStr = parseState.GetArgSliceByRef(i).ToString();
                if (!StreamObject.ParseCompleteStreamIDFromString(idStr, out ids[i - 2]))
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_XADD_INVALID_STREAM_ID);
            }

            int acked = streamManager.StreamAcknowledge(key, groupName, ids);
            if (acked < 0)
            {
                while (!RespWriteUtils.TryWriteError("NOGROUP No such consumer group"u8, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteInt64(acked, ref dcurr, dend))
                    SendAndReset();
            }
            return true;
        }

        /// <summary>
        /// XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
        /// </summary>
        private bool StreamPending(byte respProtocolVersion)
        {
            if (parseState.Count < 2)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            var key = parseState.GetArgSliceByRef(0);
            var groupName = parseState.GetArgSliceByRef(1).ToString();

            string start = null, end = null;
            int count = -1;
            long minIdleTime = -1;
            string consumerFilter = null;

            if (parseState.Count > 2)
            {
                int argIdx = 2;

                // Check for IDLE option
                if (argIdx < parseState.Count)
                {
                    var opt = parseState.GetArgSliceByRef(argIdx).ToString().ToUpperInvariant();
                    if (opt == "IDLE" && argIdx + 1 < parseState.Count)
                    {
                        if (!long.TryParse(parseState.GetArgSliceByRef(argIdx + 1).ToString(), out minIdleTime))
                            return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                        argIdx += 2;
                    }
                }

                // Parse start end count
                if (argIdx + 2 < parseState.Count)
                {
                    start = parseState.GetArgSliceByRef(argIdx).ToString();
                    end = parseState.GetArgSliceByRef(argIdx + 1).ToString();
                    if (!int.TryParse(parseState.GetArgSliceByRef(argIdx + 2).ToString(), out count))
                        return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                    argIdx += 3;
                }

                // Optional consumer filter
                if (argIdx < parseState.Count)
                {
                    consumerFilter = parseState.GetArgSliceByRef(argIdx).ToString();
                }
            }

            var _output = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr));
            bool found = streamManager.StreamPending(key, groupName, start, end, count, minIdleTime, consumerFilter, ref _output, respProtocolVersion);
            if (!found)
            {
                while (!RespWriteUtils.TryWriteError("NOGROUP No such consumer group"u8, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                ProcessOutput(_output);
            }
            return true;
        }

        /// <summary>
        /// XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms] [RETRYCOUNT count] [FORCE] [JUSTID]
        /// </summary>
        private bool StreamClaim(byte respProtocolVersion)
        {
            if (parseState.Count < 5)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            var key = parseState.GetArgSliceByRef(0);
            var groupName = parseState.GetArgSliceByRef(1).ToString();
            var consumerName = parseState.GetArgSliceByRef(2).ToString();

            if (!long.TryParse(parseState.GetArgSliceByRef(3).ToString(), out long minIdleTime))
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            // Parse IDs and options
            var idList = new System.Collections.Generic.List<StreamID>();
            long? idleMs = null;
            long? timeMs = null;
            int? retryCount = null;
            bool force = false;
            bool justId = false;

            int argIdx = 4;
            // First collect all IDs (they come before options)
            while (argIdx < parseState.Count)
            {
                var argStr = parseState.GetArgSliceByRef(argIdx).ToString();
                var upper = argStr.ToUpperInvariant();

                if (upper == "IDLE" || upper == "TIME" || upper == "RETRYCOUNT" || upper == "FORCE" || upper == "JUSTID")
                    break; // Hit an option, stop collecting IDs

                if (!StreamObject.ParseCompleteStreamIDFromString(argStr, out StreamID id))
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_XADD_INVALID_STREAM_ID);

                idList.Add(id);
                argIdx++;
            }

            // Parse options
            while (argIdx < parseState.Count)
            {
                var opt = parseState.GetArgSliceByRef(argIdx).ToString().ToUpperInvariant();
                switch (opt)
                {
                    case "IDLE":
                        if (argIdx + 1 >= parseState.Count || !long.TryParse(parseState.GetArgSliceByRef(argIdx + 1).ToString(), out long idle))
                            return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                        idleMs = idle;
                        argIdx += 2;
                        break;
                    case "TIME":
                        if (argIdx + 1 >= parseState.Count || !long.TryParse(parseState.GetArgSliceByRef(argIdx + 1).ToString(), out long time))
                            return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                        timeMs = time;
                        argIdx += 2;
                        break;
                    case "RETRYCOUNT":
                        if (argIdx + 1 >= parseState.Count || !int.TryParse(parseState.GetArgSliceByRef(argIdx + 1).ToString(), out int retry))
                            return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                        retryCount = retry;
                        argIdx += 2;
                        break;
                    case "FORCE":
                        force = true;
                        argIdx++;
                        break;
                    case "JUSTID":
                        justId = true;
                        argIdx++;
                        break;
                    default:
                        return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                }
            }

            var _output = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr));
            bool found = streamManager.StreamClaim(key, groupName, consumerName, minIdleTime,
                idList.ToArray(), idleMs, timeMs, retryCount, force, justId, ref _output, respProtocolVersion);
            if (!found)
            {
                while (!RespWriteUtils.TryWriteError("NOGROUP No such consumer group"u8, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                ProcessOutput(_output);
            }
            return true;
        }

        /// <summary>
        /// XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
        /// </summary>
        private bool StreamAutoClaim(byte respProtocolVersion)
        {
            if (parseState.Count < 5)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            var key = parseState.GetArgSliceByRef(0);
            var groupName = parseState.GetArgSliceByRef(1).ToString();
            var consumerName = parseState.GetArgSliceByRef(2).ToString();

            if (!long.TryParse(parseState.GetArgSliceByRef(3).ToString(), out long minIdleTime))
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            var startStr = parseState.GetArgSliceByRef(4).ToString();
            if (!StreamObject.ParseStreamIDFromString(startStr, out StreamID start))
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_XADD_INVALID_STREAM_ID);

            int count = 100; // Redis default
            bool justId = false;
            int argIdx = 5;

            while (argIdx < parseState.Count)
            {
                var opt = parseState.GetArgSliceByRef(argIdx).ToString().ToUpperInvariant();
                if (opt == "COUNT" && argIdx + 1 < parseState.Count)
                {
                    if (!int.TryParse(parseState.GetArgSliceByRef(argIdx + 1).ToString(), out count))
                        return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                    argIdx += 2;
                }
                else if (opt == "JUSTID")
                {
                    justId = true;
                    argIdx++;
                }
                else
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                }
            }

            var _output = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr));
            bool found = streamManager.StreamAutoClaim(key, groupName, consumerName, minIdleTime,
                start, count, justId, ref _output, respProtocolVersion);
            if (!found)
            {
                while (!RespWriteUtils.TryWriteError("NOGROUP No such consumer group"u8, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                ProcessOutput(_output);
            }
            return true;
        }

        /// <summary>
        /// XINFO STREAM|GROUPS|CONSUMERS key [group]
        /// </summary>
        private bool StreamInfoCmd(byte respProtocolVersion)
        {
            if (parseState.Count < 2)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            var subCommand = parseState.GetArgSliceByRef(0).ToString().ToUpperInvariant();
            var key = parseState.GetArgSliceByRef(1);

            var _output = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            switch (subCommand)
            {
                case "STREAM":
                    if (!streamManager.StreamInfo(key, ref _output, respProtocolVersion))
                    {
                        while (!RespWriteUtils.TryWriteError("ERR no such key"u8, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                    break;
                case "GROUPS":
                    if (!streamManager.StreamInfoGroups(key, ref _output, respProtocolVersion))
                    {
                        while (!RespWriteUtils.TryWriteError("ERR no such key"u8, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                    break;
                case "CONSUMERS":
                    if (parseState.Count < 3)
                        return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                    var groupName = parseState.GetArgSliceByRef(2).ToString();
                    if (!streamManager.StreamInfoConsumers(key, groupName, ref _output, respProtocolVersion))
                    {
                        while (!RespWriteUtils.TryWriteError("NOGROUP No such consumer group"u8, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                    break;
                default:
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
            }

            ProcessOutput(_output);
            return true;
        }

        /// <summary>
        /// XREAD [COUNT count] STREAMS key [key ...] id [id ...]
        /// </summary>
        private bool StreamRead(byte respProtocolVersion)
        {
            if (parseState.Count < 3)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            int argIdx = 0;
            int count = -1;

            // Parse optional COUNT and BLOCK (BLOCK not supported yet)
            while (argIdx < parseState.Count)
            {
                var opt = parseState.GetArgSliceByRef(argIdx).ToString().ToUpperInvariant();
                if (opt == "COUNT" && argIdx + 1 < parseState.Count)
                {
                    if (!int.TryParse(parseState.GetArgSliceByRef(argIdx + 1).ToString(), out count))
                        return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                    argIdx += 2;
                }
                else if (opt == "STREAMS")
                {
                    argIdx++;
                    break;
                }
                else if (opt == "BLOCK")
                {
                    // BLOCK not supported yet — skip the timeout arg
                    argIdx += 2;
                }
                else
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                }
            }

            int remaining = parseState.Count - argIdx;
            if (remaining < 2 || remaining % 2 != 0)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            int numStreams = remaining / 2;

            // Build response: array of [key, entries] pairs
            var _output = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr));
            var writer = new RespMemoryWriter(respProtocolVersion, ref _output);
            try
            {
                bool anyResults = false;
                var streamResults = new (PinnedSpanByte key, SpanByteAndMemory output)[numStreams];

                for (int i = 0; i < numStreams; i++)
                {
                    var streamKey = parseState.GetArgSliceByRef(argIdx + i);
                    var idStr = parseState.GetArgSliceByRef(argIdx + numStreams + i).ToString();

                    StreamID afterId;
                    if (idStr == "$")
                    {
                        // $ means only new entries — for non-blocking, return empty
                        afterId = new StreamID(ulong.MaxValue, ulong.MaxValue);
                    }
                    else if (!StreamObject.ParseStreamIDFromString(idStr, out afterId))
                    {
                        writer.Dispose();
                        return AbortWithErrorMessage(CmdStrings.RESP_ERR_XADD_INVALID_STREAM_ID);
                    }

                    var streamOutput = new SpanByteAndMemory();
                    streamManager.StreamRead(streamKey, afterId, count, ref streamOutput, respProtocolVersion);
                    streamResults[i] = (streamKey, streamOutput);
                }

                // Check if any stream returned results
                for (int i = 0; i < numStreams; i++)
                {
                    if (streamResults[i].output.Memory != null && streamResults[i].output.Memory.Memory.Length > 0)
                    {
                        anyResults = true;
                        break;
                    }
                }

                if (!anyResults)
                {
                    writer.WriteNull();
                }
                else
                {
                    writer.WriteArrayLength(numStreams);
                    for (int i = 0; i < numStreams; i++)
                    {
                        writer.WriteArrayLength(2);
                        writer.WriteBulkString(streamResults[i].key.ReadOnlySpan);
                        if (streamResults[i].output.Memory != null)
                        {
                            writer.WriteDirect(streamResults[i].output.Memory.Memory.Span);
                            streamResults[i].output.Memory.Dispose();
                        }
                        else
                        {
                            writer.WriteArrayLength(0);
                        }
                    }
                }
                writer.Dispose();
                ProcessOutput(_output);
            }
            catch
            {
                writer.Dispose();
                throw;
            }
            return true;
        }
    }
}