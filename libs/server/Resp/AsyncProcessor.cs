// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
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
        /// Set once <see cref="AsyncGetProcessorAsync"/> has stopped, for any reason.
        /// </summary>
        /// <remarks>
        /// BARRIER waits for the processor to tell it the pending operations are done, so the processor
        /// going away is indistinguishable from one that is simply taking a while -- and a processor that
        /// faults takes its fire-and-forget task's exception with it, leaving nothing to say so. The flag is
        /// what the barrier tests to tell "not finished yet" from "never will be".
        /// </remarks>
        int asyncProcessorStopped;


        /// <summary>
        /// Handle a async network GET command that goes pending
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        void NetworkGETPending<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            unsafe
            {
                while (!RespWriteUtils.TryWriteError($"ASYNC {asyncStarted}", ref dcurr, dend))
                    SendAndReset();
            }

            if (++asyncStarted == 1) // first async operation on the session, create the IO continuation processor
            {
                asyncWaiterCancel = new();
                asyncWaiter = new()
                {
                    RunContinuationsAsynchronously = true
                };
                var _storageApi = storageApi;
                _ = AsyncGetProcessorAsync(_storageApi);
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
        async Task AsyncGetProcessorAsync<TGarnetApi>(TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // Force async
            await Task.Yield();

            var faulted = false;
            try
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
                                    while (!RespWriteUtils.TryWritePushLength(3, ref dcurr, dend))
                                        SendAndReset();
                                    while (!RespWriteUtils.TryWriteBulkString(CmdStrings.async, ref dcurr, dend))
                                        SendAndReset();
                                    while (!RespWriteUtils.TryWriteInt32AsBulkString((int)completedOutputs.Current.Context, ref dcurr, dend))
                                        SendAndReset();
                                    if (completedOutputs.Current.Status.Found)
                                    {
                                        Debug.Assert(!o.SpanByteAndMemory.IsSpanByte);
                                        sessionMetrics?.incr_total_found();
                                        SendAndReset(o.SpanByteAndMemory.Memory, o.SpanByteAndMemory.Length);
                                    }
                                    else
                                    {
                                        sessionMetrics?.incr_total_notfound();
                                        WriteNull();
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
                    await asyncWaiter.WaitAsync().ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                // Nothing awaits this task, so without this the fault is lost entirely: the session would go on
                // accepting reads that nothing is left to complete, with no record anywhere of why.
                logger?.LogError(ex, "Async GET processor stopped for session Id={id}", Id);
                faulted = true;
            }
            finally
            {
                try
                {
                    // A stopped processor cannot be replaced -- asyncStarted never returns to 0, and only the
                    // transition to 1 starts one -- so every later pending read on this session would wait on a
                    // reply no-one will send, and every later barrier would report work done that is not. The
                    // stream is mid-push and truncated besides. Closing is the only state the connection can be
                    // left in that does not lie to its client.
                    //
                    // Ordered ahead of both the flag and the release, because either is enough to let a barrier
                    // proceed: the flag is what its loop tests, so a barrier that reads it before entering Wait
                    // never blocks at all. Closing first cannot strand the barrier, because both statements
                    // below are unconditional, and cannot tear the session down underneath it, because closing
                    // only shuts the socket while the barrier's resume lease still holds off reclamation. The
                    // barrier then finds its send failing into the ordinary send-failure path rather than
                    // reporting success it does not have.
                    if (faulted)
                        _ = TryKill();
                }
                catch (Exception ex)
                {
                    logger?.LogWarning(ex, "Error closing session Id={id} after its async GET processor stopped", Id);
                }
                finally
                {
                    // Published before the release and on every exit path -- cancellation, or a fault this
                    // fire-and-forget task would otherwise swallow. Release alone would not do: a fault can stop
                    // the drain with operations still outstanding, and a barrier woken on a count it then
                    // re-tests would simply wait again, so the flag is what tells it to stop waiting rather than
                    // how many are left.
                    Interlocked.Exchange(ref asyncProcessorStopped, 1);

                    // Let an ongoing barrier command know that no further async operations will complete.
                    asyncDone?.Release();
                }
            }
        }
    }
}