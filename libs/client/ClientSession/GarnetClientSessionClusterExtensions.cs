// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;

namespace Garnet.client
{
    /// <summary>
    /// Mono-threaded remote client session for Garnet (a session makes a single network connection, and 
    /// expects mono-threaded client access, i.e., no concurrent invocations of API by client)
    /// </summary>
    public sealed unsafe partial class GarnetClientSession : IServerHook, IMessageConsumer
    {
        static ReadOnlySpan<byte> GOSSIP => "GOSSIP"u8;

        /// <summary>
        /// Send gossip message to corresponding node
        /// </summary>
        /// <param name="byteArray"></param>
        /// <returns></returns>
        public Task<string> ExecuteGossip(Memory<byte> byteArray)
        {
            var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcsQueue.Enqueue(tcs);
            byte* curr = offset;
            byte* next = offset;
            int arraySize = 3;

            while (!RespWriteUtils.WriteArrayLength(arraySize, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //1
            while (!RespWriteUtils.WriteDirect(CLUSTER, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //2
            while (!RespWriteUtils.WriteBulkString(GOSSIP, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //3
            while (!RespWriteUtils.WriteBulkString(byteArray.Span, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            Flush();
            Interlocked.Increment(ref numCommands);
            return tcs.Task;
        }
    }
}