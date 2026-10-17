// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
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
        /// Holders of the session's storage sessions: the connection itself, and the async GET processor for as
        /// long as it runs. Whichever releases last disposes them.
        /// </summary>
        int storageHolds = 1;

        /// <summary>
        /// Whether <see cref="StoreWrapper"/> has been told to wait for this session's storage sessions to be
        /// released before it disposes the stores they belong to.
        /// </summary>
        bool storageTeardownCounted;

        /// <summary>
        /// Called when the connection goes away: asks the async GET processor, if the session ever started one, to
        /// finish the reads it is waiting for and then let go of the storage sessions.
        /// </summary>
        /// <remarks>
        /// The processor cannot be waited for here. This runs on the socket callback, the processor resumes on a
        /// thread pool thread, and the pool can be made up entirely of callbacks like this one.
        /// </remarks>
        void StopAsyncGetProcessor()
        {
            if (asyncWaiterCancel == null)
                return;

            // Set before the connection's hold is released, so that whichever thread ends up disposing the storage
            // sessions sees it.
            storageTeardownCounted = true;
            storeWrapper.BeginStorageTeardown();

            asyncWaiterCancel.Cancel();
            asyncWaiter.Signal();
        }

        /// <summary>
        /// Releases one hold on the storage sessions, disposing them if it was the last.
        /// </summary>
        void ReleaseStorageHold()
        {
            if (Interlocked.Decrement(ref storageHolds) == 0)
                DisposeStorage();
        }

        /// <summary>
        /// Waits out the async operations <c>ASYNC BARRIER</c> is barriering on, parking the session rather
        /// than holding the network thread for them.
        /// </summary>
        /// <remarks>
        /// The wait is unbounded, so the batch scope is given back on the way out and retaken on resume --
        /// which is what lets <see cref="AsyncGetProcessorAsync"/> take the response object and write the
        /// completions the barrier is waiting for.
        /// </remarks>
        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask AsyncBarrierBodyAsync()
        {
            asyncDone = new(0);

            while (asyncCompleted < asyncStarted)
                await asyncDone.WaitAsync().ConfigureAwait(false);

            asyncDone.Dispose();
            asyncDone = null;

            WriteDirect(CmdStrings.RESP_OK);
        }


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

                // The processor keeps using the storage sessions after the connection has gone, until its reads are done.
                _ = Interlocked.Increment(ref storageHolds);

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
            try
            {
                // Force async
                await Task.Yield();

                while (!asyncWaiterCancel.Token.IsCancellationRequested)
                {
                    while (asyncCompleted < asyncStarted)
                    {
                        var completedBefore = asyncCompleted;

                        // First complete all pending ops. The task suspends for the device rather than holding a
                        // pool thread for the whole read, which is what lets many sessions have reads in flight at
                        // once without a thread each. Once the connection is gone this still runs until nothing is
                        // in flight, so that the storage sessions are released with no read left to wait for.
                        CompletedOutputIterator<StringInput, StringOutput, long> completedOutputs;
                        storageSession.StartPendingLatency();
                        try
                        {
                            completedOutputs = await storageApi.GET_CompletePendingAsync().ConfigureAwait(false);
                        }
                        finally
                        {
                            storageSession.StopPendingLatency();
                        }

                        var entered = false;
                        try
                        {
                            // We are ready to send responses so we take over the network sender from the main ProcessMessage thread
                            // Note that we cannot take over while ProcessMessage is in progress because partial responses may have been
                            // sent at that point. For sync responses that span multiple ProcessMessage calls (e.g., MGET), we need the
                            // main thread to hold the sender lock until the response is done. The lock is waited for without a
                            // thread: whoever lets go of it next signals the waiter (see ServeSenderWaiters). The wait also ends
                            // on the processor's other signals, after which the lock is tried again.
                            while (!TryEnterSenderForPush())
                            {
                                _ = Interlocked.Or(ref senderWaiters, ProcessorWaitsForSender);
                                if (networkSender.TryEnter())
                                    break;

                                await asyncWaiter.WaitAsync().ConfigureAwait(false);
                            }

                            entered = true;

                            // Taken back, so that exits do not wake the processor for nothing
                            if ((Volatile.Read(ref senderWaiters) & ProcessorWaitsForSender) != 0)
                                _ = Interlocked.And(ref senderWaiters, ~ProcessorWaitsForSender);

                            unsafe
                            {
                                networkSender.GetResponseObject();
                                dcurr = networkSender.GetResponseObjectHead();
                                dend = networkSender.GetResponseObjectTail();
                                writingOutOfBand = true;

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
                                    SendNow(networkSender.GetResponseObjectHead());
                            }
                        }
                        catch (ObjectDisposedException)
                        {
                            // The connection is gone, so there is nobody to send the replies to. The completions are
                            // still consumed below, which is what retires the reads.
                        }
                        finally
                        {
                            // Whatever the writes did not reach
                            while (completedOutputs.Next())
                            {
                                asyncCompleted++;
                                completedOutputs.Current.Output.SpanByteAndMemory.Dispose();
                            }

                            completedOutputs.Dispose();
                            if (entered)
                            {
                                writingOutOfBand = false;
                                networkSender.ExitAndReturnResponseObject();
                                ServeSenderWaiters();
                            }
                        }

                        // A drain that completed nothing found no read of ours on the completion queue, which the
                        // session's synchronous commands also drain. There is nothing to wait for, so go back to
                        // waiting for the next start instead of draining an empty queue in a loop.
                        if (asyncCompleted == completedBefore)
                            break;
                    }

                    // Let ongoing barrier command know that all async operations are done
                    asyncDone?.Release();

                    // The wait for the sender lock above can take the signal that came with the stop
                    if (asyncWaiterCancel.Token.IsCancellationRequested)
                        break;

                    // Wait for next async operation
                    // We do not need to cancel the wait - it should get garbage collected when the session ends
                    await asyncWaiter.WaitAsync().ConfigureAwait(false);
                }
            }
            finally
            {
                ReleaseStorageHold();
            }
        }
    }
}