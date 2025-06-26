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
        private unsafe bool StreamAdd(byte respProtocolVersion)
        {
            if (parseState.Count < 4)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_XADD_WRONG_NUM_ARGS);
            }

            // Parse the stream key.
            var key = parseState.GetArgSliceByRef(0);

            // Parse the id. We parse as string for easy pattern matching.
            var idGiven = parseState.GetArgSliceByRef(1);

            // get the number of the remaining key-value pairs
            var numPairs = parseState.Count - 2;

            // grab the rest of the input that will mainly be k-v pairs as entry to the stream.
            byte* vPtr = parseState.GetArgSliceByRef(2).ptr - sizeof(int);
            int vsize = (int)(recvBufferPtr + endReadHead - vPtr);
            var streamDataSpan = new ReadOnlySpan<byte>(vPtr, vsize);
            SpanByteAndMemory _output = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));

            var disabledStreams = streamManager == null;
            if (disabledStreams)
            {
                while (!RespWriteUtils.TryWriteError("ERR STREAMS is disabled, enable it with --streams option."u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }


            if (sessionStreamCache.TryGetStreamFromCache(key.Span, out StreamObject cachedStream))
            {
                cachedStream.AddEntry(streamDataSpan, vsize, idGiven, numPairs, ref _output, respProtocolVersion);
            }
            else
            {
                streamManager.StreamAdd(key, idGiven, streamDataSpan, vsize, numPairs, ref _output, out byte[] lastStreamKey, out StreamObject lastStream, respProtocolVersion);
                // since we added to a new stream that was not in the cache, try adding it to the cache
                sessionStreamCache.TryAddStreamToCache(lastStreamKey, lastStream);
            }
            ProcessOutput(_output);
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

            ulong streamLength;

            var disabledStreams = streamManager == null;
            if (disabledStreams)
            {
                while (!RespWriteUtils.TryWriteError("ERR STREAMS is disabled, enable it with --streams option."u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // check if the stream exists in cache 
            if (sessionStreamCache.TryGetStreamFromCache(key.Span, out StreamObject cachedStream))
            {
                streamLength = cachedStream.Length();
            }
            else
            {
                streamLength = streamManager.StreamLength(key);
            }
            // write back result
            while (!RespWriteUtils.TryWriteInt64((long)streamLength, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        ///  Retrieves a range of stream entries.
        /// </summary>
        /// <returns>true if range of stream entries were retrieved successfully; error otherwise</returns>
        public unsafe bool StreamRange(byte respProtocolVersion)
        {
            // command is of format: XRANGE key start end [COUNT count]
            // we expect at least 3 arguments
            if (parseState.Count < 3)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_XRANGE_WRONG_NUM_ARGS);
            }

            // parse the stream key 
            var key = parseState.GetArgSliceByRef(0);

            // parse start and end IDs
            var startId = parseState.GetArgSliceByRef(1).ToString();
            var endId = parseState.GetArgSliceByRef(2).ToString();

            int count = -1;
            if (parseState.Count > 3)
            {
                // parse the count argument
                var countStr = parseState.GetArgSliceByRef(4).ToString();
                if (!int.TryParse(countStr, out count))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                }
            }

            SpanByteAndMemory _output = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));

            var disabledStreams = streamManager == null;
            if (disabledStreams)
            {
                while (!RespWriteUtils.TryWriteError("ERR STREAMS is disabled, enable it with --streams option."u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            bool success = false;

            // check if the stream exists in cache
            if (sessionStreamCache.TryGetStreamFromCache(key.Span, out StreamObject cachedStream))
            {
                cachedStream.ReadRange(startId, endId, count, ref _output, respProtocolVersion);
                success = true;
            }
            else
            {
                success = streamManager.StreamRange(key, startId, endId, count, ref _output, respProtocolVersion);
            }
            if (success)
            {
                // _ = ProcessOutputWithHeader(_output);
                ProcessOutput(_output);
            }
            else
            {
                //return empty array
                while (!RespWriteUtils.TryWriteArrayLength(0, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

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
            int deletedCount = 0;

            var disabledStreams = streamManager == null;
            if (disabledStreams)
            {
                while (!RespWriteUtils.TryWriteError("ERR STREAMS is disabled, enable it with --streams option."u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // for every id, parse and delete the stream entry
            for (int i = 1; i < parseState.Count; i++)
            {
                // parse the id as string
                var idGiven = parseState.GetArgSliceByRef(i);

                bool deleted;
                // check if the stream exists in cache
                if (sessionStreamCache.TryGetStreamFromCache(key.Span, out StreamObject cachedStream))
                {
                    deleted = cachedStream.DeleteEntry(idGiven);
                }
                else
                {
                    // delete the entry in the stream from the streamManager
                    deleted = streamManager.StreamDelete(key, idGiven, out StreamObject lastStream);
                    if (lastStream != null)
                    {
                        // since we deleted from a stream that was not in the cache, try adding it to the cache
                        sessionStreamCache.TryAddStreamToCache(key.ToArray(), lastStream);
                    }
                }

                deletedCount = deleted ? deletedCount + 1 : deletedCount;
            }

            // write back the number of entries deleted
            while (!RespWriteUtils.TryWriteInt64(deletedCount, ref dcurr, dend))
                SendAndReset();
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
            var trimType = parseState.GetArgSliceByRef(1).ToString().ToUpper();
            var trimArg = parseState.GetArgSliceByRef(2);

            ulong validKeysRemoved = 0;
            StreamTrimOpts optType = StreamTrimOpts.NONE;
            switch (trimType)
            {
                case "MAXLEN":
                    optType = StreamTrimOpts.MAXLEN;
                    break;
                case "MINID":
                    optType = StreamTrimOpts.MINID;
                    break;
            }

            var disabledStreams = streamManager == null;
            if (disabledStreams)
            {
                while (!RespWriteUtils.TryWriteError("ERR STREAMS is disabled, enable it with --streams option."u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            bool result;
            if (sessionStreamCache.TryGetStreamFromCache(key.Span, out StreamObject cachedStream))
            {
                result = cachedStream.Trim(trimArg, optType, out validKeysRemoved);
            }
            else
            {
                result = streamManager.StreamTrim(key, trimArg, optType, out validKeysRemoved);
            }
            if (!result)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
            }
            while (!RespWriteUtils.TryWriteInt64((long)validKeysRemoved, ref dcurr, dend))
                SendAndReset();
            return true;
        }

    }
}