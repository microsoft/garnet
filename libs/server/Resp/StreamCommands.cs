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
        /// STREAMADD
        /// </summary>
        /// <param name="ptr"></param>
        /// <returns></returns> 
        private unsafe bool StreamAdd()
        {
            // Parse the stream key.
            var key = parseState.GetArgSliceByRef(0);

            // Parse the id. We parse as string for easy pattern matching.
            var idGiven = parseState.GetArgSliceByRef(1);

            // get the number of the remaining key-value pairs
            var numPairs = parseState.Count - 2;

            // grab the rest of the input that will mainly be k-v pairs as entry to the stream.
            byte* vPtr = parseState.GetArgSliceByRef(2).ptr - sizeof(int);
            //int vsize = (int)(recvBufferPtr + bytesRead - vPtr);
            int vsize = (int)(recvBufferPtr + endReadHead - vPtr);
            SpanByteAndMemory _output = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));


            if (sessionStreamCache.TryGetStreamFromCache(key.Span, out StreamObject cachedStream))
            {
                cachedStream.AddEntry(vPtr, vsize, idGiven, numPairs, ref _output);
            }
            else
            {
                streamManager.StreamAdd(key, idGiven, vPtr, vsize, numPairs, ref _output, out byte[] lastStreamKey, out StreamObject lastStream);
                // since we added to a new stream that was not in the cache, try adding it to the cache
                sessionStreamCache.TryAddStreamToCache(lastStreamKey, lastStream);
            }
            _ = ProcessOutputWithHeader(_output);
            return true;
        }

        /// <summary>
        /// STREAMLENGTH
        /// </summary>
        /// <returns></returns>
        private bool StreamLength()
        {
            // parse the stream key. 
            var key = parseState.GetArgSliceByRef(0);

            ulong streamLength;

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
        ///  STREAMRANGE
        /// </summary>
        /// <param name="ptr"></param>
        /// <returns></returns>
        public unsafe bool StreamRange()
        {
            // command is of format: XRANGE key start end [COUNT count]
            // we expect at least 3 arguments
            if (parseState.Count < 3)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
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

            // check if the stream exists in cache
            if (sessionStreamCache.TryGetStreamFromCache(key.Span, out StreamObject cachedStream))
            {
                cachedStream.ReadRange(startId, endId, count, ref _output);
            }
            else
            {
                streamManager.StreamRange(key, startId, endId, count, ref _output);
            }


            _ = ProcessOutputWithHeader(_output);

            return true;
        }

        public bool StreamDelete()
        {
            // command is of format: XDEL key id [id ...]
            // we expect at least 2 arguments
            if (parseState.Count < 2)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
            }

            // parse the stream key
            var key = parseState.GetArgSliceByRef(0);
            int deletedCount = 0;

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

    }
}