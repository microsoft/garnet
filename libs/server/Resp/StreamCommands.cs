// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
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

            var key = parseState.GetArgSliceByRef(1);

            // Detect MKSTREAM up front: it controls whether the stream is created on demand.
            var mkStream = 0;
            for (var i = 4; i < parseState.Count; i++)
            {
                if (parseState.GetArgSliceByRef(i).ToString().ToUpperInvariant() == "MKSTREAM")
                {
                    mkStream = 1;
                    break;
                }
            }

            var header = new RespInputHeader(GarnetObjectType.Stream) { StreamOp = StreamOperation.XGROUP_CREATE };
            var input = new ObjectInput(header, ref parseState, arg1: mkStream);
            var output = new ObjectOutput { SpanByteAndMemory = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr)) };

            var status = storageSession.StreamObjectRMW(key, ref input, ref output);
            if (status == GarnetStatus.NOTFOUND)
            {
                // Stream does not exist and MKSTREAM was not specified.
                while (!RespWriteUtils.TryWriteError("ERR no such key"u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            ProcessOutput(output.SpanByteAndMemory);
            return true;
        }

        // XGROUP SETID key group id [ENTRIESREAD n]
        private bool StreamGroupSetId(byte respProtocolVersion)
        {
            if (parseState.Count < 4)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            var key = parseState.GetArgSliceByRef(1);

            var header = new RespInputHeader(GarnetObjectType.Stream) { StreamOp = StreamOperation.XGROUP_SETID };
            var input = new ObjectInput(header, ref parseState);
            var output = new ObjectOutput { SpanByteAndMemory = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr)) };

            var status = storageSession.StreamObjectRMW(key, ref input, ref output);
            if (status == GarnetStatus.NOTFOUND)
            {
                while (!RespWriteUtils.TryWriteError("NOGROUP No such consumer group"u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            ProcessOutput(output.SpanByteAndMemory);
            return true;
        }

        // XGROUP DESTROY key group
        private bool StreamGroupDestroy()
        {
            if (parseState.Count < 3)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            var key = parseState.GetArgSliceByRef(1);

            var header = new RespInputHeader(GarnetObjectType.Stream) { StreamOp = StreamOperation.XGROUP_DESTROY };
            var input = new ObjectInput(header, ref parseState);
            var output = new ObjectOutput { SpanByteAndMemory = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr)) };

            var status = storageSession.StreamObjectRMW(key, ref input, ref output);
            if (status == GarnetStatus.NOTFOUND)
            {
                while (!RespWriteUtils.TryWriteInt64(0, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            ProcessOutput(output.SpanByteAndMemory);
            return true;
        }

        // XGROUP CREATECONSUMER key group consumer
        private bool StreamGroupCreateConsumer()
        {
            if (parseState.Count < 4)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            var key = parseState.GetArgSliceByRef(1);

            var header = new RespInputHeader(GarnetObjectType.Stream) { StreamOp = StreamOperation.XGROUP_CREATECONSUMER };
            var input = new ObjectInput(header, ref parseState);
            var output = new ObjectOutput { SpanByteAndMemory = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr)) };

            var status = storageSession.StreamObjectRMW(key, ref input, ref output);
            if (status == GarnetStatus.NOTFOUND)
            {
                while (!RespWriteUtils.TryWriteError("NOGROUP No such consumer group"u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            ProcessOutput(output.SpanByteAndMemory);
            return true;
        }

        // XGROUP DELCONSUMER key group consumer
        private bool StreamGroupDelConsumer()
        {
            if (parseState.Count < 4)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            var key = parseState.GetArgSliceByRef(1);

            var header = new RespInputHeader(GarnetObjectType.Stream) { StreamOp = StreamOperation.XGROUP_DELCONSUMER };
            var input = new ObjectInput(header, ref parseState);
            var output = new ObjectOutput { SpanByteAndMemory = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr)) };

            var status = storageSession.StreamObjectRMW(key, ref input, ref output);
            if (status == GarnetStatus.NOTFOUND)
            {
                while (!RespWriteUtils.TryWriteError("NOGROUP No such consumer group"u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            ProcessOutput(output.SpanByteAndMemory);
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

            var groupSlice = parseState.GetArgSliceByRef(argIdx++);
            var consumerSlice = parseState.GetArgSliceByRef(argIdx++);

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
            var idSlices = new PinnedSpanByte[numStreams];

            for (int i = 0; i < numStreams; i++)
                keys[i] = parseState.GetArgSliceByRef(argIdx + i);
            for (int i = 0; i < numStreams; i++)
                idSlices[i] = parseState.GetArgSliceByRef(argIdx + numStreams + i);

            // XREADGROUP multi-stream format: *N [*2 $key *M entries...]. Like XREAD, ">" (new-message)
            // reads omit streams with no new entries (and reply null when none); history reads (explicit
            // ID) always include the stream so an empty PEL still returns [key, []].
            var _output = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr));
            var writer = new RespMemoryWriter(respProtocolVersion, ref _output);
            var streamResults = new (SpanByteAndMemory output, bool include)[numStreams];
            var groupMissing = false;
            try
            {
                for (int i = 0; i < numStreams; i++)
                {
                    var streamOutput = new ObjectOutput();
                    var status = storageSession.StreamReadGroupOne(keys[i], groupSlice, consumerSlice, idSlices[i], count, noAck, ref streamOutput);
                    // result1 = entries written, or -1 if the group does not exist.
                    if (status == GarnetStatus.NOTFOUND || streamOutput.result1 < 0)
                    {
                        groupMissing = true;
                        streamResults[i] = (streamOutput.SpanByteAndMemory, false);
                        break;
                    }

                    var idSpan = idSlices[i].ReadOnlySpan;
                    var isNewRead = idSpan.Length == 1 && idSpan[0] == (byte)'>';
                    // ">" reads omit empty streams; history reads are always returned.
                    var include = !isNewRead || streamOutput.result1 > 0;
                    streamResults[i] = (streamOutput.SpanByteAndMemory, include);
                }

                if (groupMissing)
                {
                    writer.Dispose();
                    while (!RespWriteUtils.TryWriteError("NOGROUP No such consumer group"u8, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                var includeCount = 0;
                for (int i = 0; i < numStreams; i++)
                {
                    if (streamResults[i].include)
                        includeCount++;
                }

                if (includeCount == 0)
                {
                    writer.WriteNullArray();
                }
                else
                {
                    writer.WriteArrayLength(includeCount);
                    for (int i = 0; i < numStreams; i++)
                    {
                        if (!streamResults[i].include)
                            continue;
                        writer.WriteArrayLength(2);
                        writer.WriteBulkString(keys[i].ReadOnlySpan);

                        var sbm = streamResults[i].output;
                        if (sbm.IsSpanByte)
                            writer.WriteDirect(sbm.SpanByte.ReadOnlySpan);
                        else if (sbm.Memory != null)
                            writer.WriteDirect(sbm.Memory.Memory.Span.Slice(0, sbm.Length));
                        else
                            writer.WriteArrayLength(0);
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
            finally
            {
                for (int i = 0; i < numStreams; i++)
                {
                    if (streamResults[i].output.Memory != null)
                        streamResults[i].output.Memory.Dispose();
                }
            }
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

            var header = new RespInputHeader(GarnetObjectType.Stream) { StreamOp = StreamOperation.XACK };
            var input = new ObjectInput(header, ref parseState);
            var output = new ObjectOutput { SpanByteAndMemory = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr)) };

            var status = storageSession.StreamObjectRMW(key, ref input, ref output);
            if (status == GarnetStatus.NOTFOUND)
            {
                while (!RespWriteUtils.TryWriteError("NOGROUP No such consumer group"u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            ProcessOutput(output.SpanByteAndMemory);
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

            var header = new RespInputHeader(GarnetObjectType.Stream) { StreamOp = StreamOperation.XPENDING };
            var input = new ObjectInput(header, ref parseState);
            var output = new ObjectOutput { SpanByteAndMemory = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr)) };

            var status = storageSession.StreamObjectRead(key, ref input, ref output);
            if (status == GarnetStatus.NOTFOUND)
            {
                while (!RespWriteUtils.TryWriteError("NOGROUP No such consumer group"u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            ProcessOutput(output.SpanByteAndMemory);
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

            var header = new RespInputHeader(GarnetObjectType.Stream) { StreamOp = StreamOperation.XCLAIM };
            var input = new ObjectInput(header, ref parseState);
            var output = new ObjectOutput { SpanByteAndMemory = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr)) };

            var status = storageSession.StreamObjectRMW(key, ref input, ref output);
            if (status == GarnetStatus.NOTFOUND)
            {
                while (!RespWriteUtils.TryWriteError("NOGROUP No such consumer group"u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            ProcessOutput(output.SpanByteAndMemory);
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

            var header = new RespInputHeader(GarnetObjectType.Stream) { StreamOp = StreamOperation.XAUTOCLAIM };
            var input = new ObjectInput(header, ref parseState);
            var output = new ObjectOutput { SpanByteAndMemory = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr)) };

            var status = storageSession.StreamObjectRMW(key, ref input, ref output);
            if (status == GarnetStatus.NOTFOUND)
            {
                while (!RespWriteUtils.TryWriteError("NOGROUP No such consumer group"u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            ProcessOutput(output.SpanByteAndMemory);
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

            StreamOperation op;
            switch (subCommand)
            {
                case "STREAM":
                    op = StreamOperation.XINFO_STREAM;
                    break;
                case "GROUPS":
                    op = StreamOperation.XINFO_GROUPS;
                    break;
                case "CONSUMERS":
                    if (parseState.Count < 3)
                        return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                    op = StreamOperation.XINFO_CONSUMERS;
                    break;
                default:
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
            }

            var header = new RespInputHeader(GarnetObjectType.Stream) { StreamOp = op };
            var input = new ObjectInput(header, ref parseState);
            var output = new ObjectOutput { SpanByteAndMemory = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr)) };

            var status = storageSession.StreamObjectRead(key, ref input, ref output);
            if (status == GarnetStatus.NOTFOUND)
            {
                ReadOnlySpan<byte> err = op == StreamOperation.XINFO_CONSUMERS
                    ? "NOGROUP No such consumer group"u8
                    : "ERR no such key"u8;
                while (!RespWriteUtils.TryWriteError(err, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            ProcessOutput(output.SpanByteAndMemory);
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
            var streamResults = new (PinnedSpanByte key, SpanByteAndMemory output, int count)[numStreams];
            try
            {
                for (int i = 0; i < numStreams; i++)
                {
                    var streamKey = parseState.GetArgSliceByRef(argIdx + i);
                    var idSlice = parseState.GetArgSliceByRef(argIdx + numStreams + i);
                    var idStr = idSlice.ToString();

                    // Validate up front so an invalid ID aborts the whole command (the object re-parses).
                    if (idStr != "$" && !StreamObject.ParseStreamIDFromString(idStr, out _))
                    {
                        writer.Dispose();
                        return AbortWithErrorMessage(CmdStrings.RESP_ERR_XADD_INVALID_STREAM_ID);
                    }

                    var streamOutput = new ObjectOutput();
                    _ = storageSession.StreamReadOne(streamKey, idSlice, count, ref streamOutput);
                    // result1 = number of entries found; 0 means this stream has no new entries.
                    streamResults[i] = (streamKey, streamOutput.SpanByteAndMemory, streamOutput.result1);
                }

                // Redis omits streams with no new entries, and replies null when none have data.
                var nonEmptyCount = 0;
                for (int i = 0; i < numStreams; i++)
                {
                    if (streamResults[i].count > 0)
                        nonEmptyCount++;
                }

                if (nonEmptyCount == 0)
                {
                    writer.WriteNullArray();
                }
                else
                {
                    writer.WriteArrayLength(nonEmptyCount);
                    for (int i = 0; i < numStreams; i++)
                    {
                        if (streamResults[i].count <= 0)
                            continue;
                        writer.WriteArrayLength(2);
                        writer.WriteBulkString(streamResults[i].key.ReadOnlySpan);
                        writer.WriteDirect(streamResults[i].output.Memory.Memory.Span.Slice(0, streamResults[i].output.Length));
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
            finally
            {
                // Release every per-stream pooled buffer, including empty streams that allocated a "*0".
                for (int i = 0; i < numStreams; i++)
                {
                    if (streamResults[i].output.Memory != null)
                        streamResults[i].output.Memory.Dispose();
                }
            }
            return true;
        }
    }
}