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
    }
}