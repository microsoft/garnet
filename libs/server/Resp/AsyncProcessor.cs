// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - basic commands are in this file
    /// </summary>
    internal sealed partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Whether async mode is turned on for the session
        /// </summary>
        bool useAsync = false;

        /// <summary>
        /// How many async operations are started and completed
        /// </summary>
        long asyncStarted = 0, asyncCompleted = 0;

        /// <summary>
        /// Async waiter for async operations
        /// </summary>
        SingleWaiterAutoResetEvent asyncWaiter = null;

        /// <summary>
        /// Cancellation token source for async waiter
        /// </summary>
        CancellationTokenSource asyncWaiterCancel = null;

        /// <summary>
        /// Semaphore for barrier command to wait for async operations to complete
        /// </summary>
        SemaphoreSlim asyncDone = null;


        /// <summary>
        /// Handle a async network GET command that goes pending
        /// </summary>
        /// <param name="garnetApi"></param>
        void NetworkGETPending<TKeyLocker, TEpochGuard, TGarnetApi>(ref TGarnetApi garnetApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetApi<TKeyLocker, TEpochGuard>
        {
            unsafe
            {
                while (!RespWriteUtils.WriteError($"ASYNC {asyncStarted}", ref dcurr, dend))
                    SendAndReset();
            }

            if (++asyncStarted == 1) // first async operation on the session, create the IO continuation processor
            {
                asyncWaiterCancel = new();
                asyncWaiter = new()
                {
                    RunContinuationsAsynchronously = true
                };
                var _storageApi = garnetApi;
                _ = Task.Run(async () => await AsyncGetProcessor<TKeyLocker, TEpochGuard, TGarnetApi>(_storageApi));
            }
            else
            {
                Debug.Assert(asyncWaiter != null);
                asyncWaiter.Signal();
            }
        }

        /// <summary>
        /// Background processor for async IO continuations. This is created only when async is turned on for the session.
        /// It handles all the IO completions and takes over the network sender to send async responses when ready.
        /// Note that async responses are not guaranteed to be in the same order that they are issued.
        /// </summary>
        async Task AsyncGetProcessor<TKeyLocker, TEpochGuard, TGarnetApi>(TGarnetApi storageApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetApi<TKeyLocker, TEpochGuard>
        {
            while (!asyncWaiterCancel.Token.IsCancellationRequested)
            {
                while (asyncCompleted < asyncStarted)
                {
                    // First complete all pending ops
                    storageApi.GET_CompletePending(out var completedOutputs, true);

                    try
                    {
                        unsafe
                        {
                            // We are ready to send responses so we take over the network sender from the main ProcessMessage thread
                            // Note that we cannot take over while ProcessMessage is in progress because partial responses may have been
                            // sent at that point. For sync responses that span multiple ProcessMessage calls (e.g., MGET), we need the
                            // main thread to hold the sender lock until the response is done.
                            networkSender.EnterAndGetResponseObject(out dcurr, out dend);

                            // Send async replies with completed outputs
                            while (completedOutputs.Next())
                            {
                                // This is the only thread that updates asyncCompleted so we do not need atomics here
                                asyncCompleted++;
                                var o = completedOutputs.Current.Output;

                                // We write async push response as an array: [ "async", "<token_id>", "<result_string>" ]
                                while (!RespWriteUtils.WritePushLength(3, ref dcurr, dend))
                                    SendAndReset();
                                while (!RespWriteUtils.WriteBulkString(CmdStrings.async, ref dcurr, dend))
                                    SendAndReset();
                                while (!RespWriteUtils.WriteIntegerAsBulkString((int)completedOutputs.Current.Context, ref dcurr, dend))
                                    SendAndReset();
                                if (completedOutputs.Current.Status.Found)
                                {
                                    Debug.Assert(!o.IsSpanByte);
                                    sessionMetrics?.incr_total_found();
                                    SendAndReset(o.Memory, o.Length);
                                }
                                else
                                {
                                    sessionMetrics?.incr_total_notfound();
                                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                                        SendAndReset();
                                }
                            }
                            if (dcurr > networkSender.GetResponseObjectHead())
                                Send(networkSender.GetResponseObjectHead());
                        }
                    }
                    finally
                    {
                        completedOutputs.Dispose();
                        networkSender.ExitAndReturnResponseObject();
                    }
                }

                // Let ongoing barrier command know that all async operations are done
                asyncDone?.Release();

                // Wait for next async operation
                // We do not need to cancel the wait - it should get garbage collected when the session ends
                await asyncWaiter.WaitAsync();
            }
        }
    }
}