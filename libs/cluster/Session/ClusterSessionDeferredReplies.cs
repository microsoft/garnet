// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    /// <summary>
    /// Replies that are written after the command that produced them has returned, in command order.
    /// <para>
    /// A disk-based sync chunk (CLUSTER SNAPSHOT_DATA) must not be acknowledged until it is written, but waiting for
    /// the write before returning would stop this connection from receiving the next chunk. Instead the chunk's write
    /// task is queued here, and a background responder writes each reply ("+OK", or the error) once its task completes.
    /// Replies are only ever written from the head of the queue, so they go out in command order.
    /// </para>
    /// <para>
    /// RESP replies must follow command order, so any other reply on this session must wait for the queue to empty.
    /// Before writing one, the session calls <see cref="DrainDeferredReplies(ref byte*, ref byte*)"/>, which waits for the
    /// remaining writes and writes their replies first. A primary sends nothing else while chunk acknowledgements
    /// are outstanding, so in practice the drain finds the queue empty.
    /// </para>
    /// <para>
    /// Concurrency: replies are written only by a thread that holds the network sender. The receiving thread holds it
    /// for each whole ProcessMessages batch, and the responder takes it between batches, as AsyncProcessor does.
    /// Dequeuing only while holding the sender keeps the two in order. <see cref="deferredLock"/> guards the queue and
    /// the flags. The responder also holds it while writing and sending, so once <see cref="DisposeDeferredReplies"/>
    /// has run it can no longer touch a response buffer that the closing connection releases (with TLS, the
    /// connection's own send buffer). Dispose may wait for that send, but not for long: each reply is a few bytes and
    /// the primary bounds how many are outstanding, so a send never waits for the primary to read.
    /// </para>
    /// </summary>
    internal sealed partial class ClusterSession : IClusterSession
    {
        readonly Queue<Task> deferredReplies = new();
        readonly Lock deferredLock = new();
        bool deferredResponderRunning;
        bool deferredRepliesDisposed;

        // Written under deferredLock; read without it as a cheap check by every command on a cluster session.
        volatile bool hasDeferredReplies;

        /// <inheritdoc/>
        public bool HasDeferredReplies => hasDeferredReplies;

        /// <inheritdoc/>
        public unsafe void DrainDeferredReplies(ref byte* dcurr, ref byte* dend)
        {
            this.dcurr = dcurr;
            this.dend = dend;
            try
            {
                DrainDeferredReplies();
            }
            finally
            {
                dcurr = this.dcurr;
                dend = this.dend;
            }
        }

        /// <summary>
        /// Waits for each deferred task and writes its reply, oldest first, until none remain.
        /// The caller must hold the network sender and have set <see cref="dcurr"/> and <see cref="dend"/>.
        /// </summary>
        unsafe void DrainDeferredReplies()
        {
            while (true)
            {
                Task head;
                lock (deferredLock)
                {
                    if (deferredReplies.Count == 0)
                        return;
                    head = deferredReplies.Peek();
                }

                // Each write ends within the replica sync timeout, so this wait is bounded. The write completes on the
                // sink's writer task, which never needs this thread or the network sender it holds.
#pragma warning disable VSTHRD002 // The RESP processing path is synchronous; the next reply cannot be written until this one is.
                head.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing).GetAwaiter().GetResult();
#pragma warning restore VSTHRD002

                lock (deferredLock)
                {
                    // Only a sender holder dequeues, and this thread holds the sender, so the head is unchanged
                    // unless Dispose emptied the queue.
                    if (deferredReplies.Count == 0)
                        return;
                    _ = deferredReplies.Dequeue();
                    hasDeferredReplies = deferredReplies.Count > 0;
                }
                WriteDeferredReply(head);
            }
        }

        /// <summary>
        /// Queues the reply to the current command until <paramref name="pending"/> completes.
        /// The caller must hold the network sender (that is, be processing a command).
        /// </summary>
        void DeferReply(Task pending)
        {
            bool startResponder;
            lock (deferredLock)
            {
                if (deferredRepliesDisposed)
                    return;
                deferredReplies.Enqueue(pending);
                hasDeferredReplies = true;
                startResponder = !deferredResponderRunning;
                deferredResponderRunning = true;
            }

            if (startResponder)
                _ = Task.Run(SendDeferredRepliesAsync);
        }

        /// <summary>
        /// Writes "+OK" for a successful task, or an error reply in the same form the session uses for an exception
        /// thrown by a command.
        /// </summary>
        unsafe void WriteDeferredReply(Task completed)
        {
            if (completed.IsCompletedSuccessfully)
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
                return;
            }

            var error = completed.Exception?.InnerException;
            var message = error switch
            {
                null => "ERR Checkpoint write was canceled",
                GarnetException => $"ERR Garnet Exception: {error.Message}",
                _ => $"ERR {error.Message}",
            };

            // A RESP error reply is a single line.
            message = message.ReplaceLineEndings(" ");
            while (!RespWriteUtils.TryWriteError(message, ref dcurr, dend))
                SendAndReset();
        }

        /// <summary>
        /// Background responder: whenever the oldest deferred task completes, takes over the network sender and writes
        /// the replies of every completed task at the head of the queue. Exits when the queue is empty; the next
        /// deferral starts a new one.
        /// </summary>
        async Task SendDeferredRepliesAsync()
        {
            try
            {
                while (true)
                {
                    Task head;
                    lock (deferredLock)
                    {
                        if (deferredRepliesDisposed || deferredReplies.Count == 0)
                        {
                            deferredResponderRunning = false;
                            return;
                        }
                        head = deferredReplies.Peek();
                    }

                    await head.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                    WriteCompletedDeferredReplies();
                }
            }
            catch (Exception ex)
            {
                // The connection closed or failed while sending; no later reply could reach the primary either.
                logger?.LogDebug(ex, "Stopped sending deferred replies");
                lock (deferredLock)
                {
                    ObserveAndClearDeferredReplies();
                    deferredResponderRunning = false;
                }
            }
        }

        unsafe void WriteCompletedDeferredReplies()
        {
            try
            {
                // Waits for the receiving thread to finish its current batch, whose replies precede these.
                networkSender.EnterAndGetResponseObject(out dcurr, out dend);
                lock (deferredLock)
                {
                    if (deferredRepliesDisposed)
                        return;

                    while (deferredReplies.Count > 0 && deferredReplies.Peek().IsCompleted)
                        WriteDeferredReply(deferredReplies.Dequeue());
                    hasDeferredReplies = deferredReplies.Count > 0;

                    var head = networkSender.GetResponseObjectHead();
                    if (dcurr > head)
                        Send(head);
                }
            }
            finally
            {
                networkSender.ExitAndReturnResponseObject();
            }
        }

        /// <summary>
        /// Stops deferred replies when the session closes. Waits for a responder that is writing to finish.
        /// </summary>
        void DisposeDeferredReplies()
        {
            lock (deferredLock)
            {
                deferredRepliesDisposed = true;
                ObserveAndClearDeferredReplies();
            }
        }

        // Caller holds deferredLock. The writes still complete, but their failures will not be replied to, so they
        // are observed here rather than reported as unobserved task exceptions.
        void ObserveAndClearDeferredReplies()
        {
            foreach (var pending in deferredReplies)
            {
                _ = pending.ContinueWith(static task => _ = task.Exception, CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            }
            deferredReplies.Clear();
            hasDeferredReplies = false;
        }
    }
}