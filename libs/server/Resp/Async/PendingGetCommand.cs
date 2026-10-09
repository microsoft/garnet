// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// The suspending half of <c>GET</c>: what happens when the read misses memory and goes to the device.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A read that goes pending used to be completed with <c>CompletePendingWithOutputs(wait: true)</c>,
    /// which parks the network thread on the device for the whole I/O. Under a working set that does not fit
    /// in memory that is the common case rather than the rare one, so the thread pool ends up holding one
    /// thread per outstanding disk read and the server stops accepting work long before the device is
    /// saturated. Here the session suspends instead: the batch unwinds, the thread goes back to serving other
    /// sessions, and the reply is written when the I/O lands.
    /// </para>
    /// <para>
    /// This is the operation-in-flight kind of suspension, so it keeps the cluster epoch. The record it is
    /// about to read must still belong to this node when the device comes back, which is exactly the
    /// guarantee the epoch gives, and the wait is bounded by the device rather than by a client.
    /// </para>
    /// </remarks>
    internal sealed partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Status and output of the read that completed from pending I/O, handed from the async body to the
        /// non-async method that writes the reply.
        /// </summary>
        /// <remarks>
        /// A field rather than an argument because an <c>async</c> method cannot hold the storage API, and
        /// because it is written and read on the one thread that holds the session scope at the time.
        /// </remarks>
        CompletedOutputIterator<StringInput, StringOutput, long> pendingGetOutputs;

        /// <summary>
        /// Finishes a <c>GET</c> whose read went to disk, by parking the session until the I/O completes.
        /// </summary>
        /// <returns>Always true: the reply is written on resume.</returns>
        bool NetworkGETSuspending()
        {
            ValueTask body;
            using (BeginAsyncCommand(retainClusterEpoch: true))
                body = PendingGetBodyAsync();

            return CompleteAsyncCommand(body);
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask PendingGetBodyAsync()
        {
            storageSession.StartPendingMetrics();
            try
            {
                pendingGetOutputs = await CompletePendingGetAsync().ConfigureAwait(false);
            }
            finally
            {
                storageSession.StopPendingMetrics();
            }

            WriteCompletedGet();
        }

        /// <summary>
        /// Starts the asynchronous completion. Separated from the body because an <c>async</c> method cannot
        /// hold a reference to the storage API struct.
        /// </summary>
        ValueTask<CompletedOutputIterator<StringInput, StringOutput, long>> CompletePendingGetAsync()
            => consistentReadActive
                ? consistentReadGarnetApi.GET_CompletePendingAsync()
                : basicGarnetApi.GET_CompletePendingAsync();

        /// <summary>
        /// Writes the reply for a read that completed from pending I/O, into the response buffer the resume
        /// has just re-entered.
        /// </summary>
        void WriteCompletedGet()
        {
            var completed = pendingGetOutputs;
            pendingGetOutputs = null;

            WriteCompletedGet(completed);
        }

        /// <summary>
        /// Writes the reply for a single completed read and disposes the iterator that carried it.
        /// </summary>
        /// <param name="completed">Outputs of the completion, holding exactly this session's one read.</param>
        void WriteCompletedGet(CompletedOutputIterator<StringInput, StringOutput, long> completed)
        {
            try
            {
                var more = completed.Next();
                Debug.Assert(more, "A pending GET completed without producing an output");

                var status = completed.Current.Status;
                var output = completed.Current.Output;

                Debug.Assert(!completed.Next(), "A pending GET produced more than one output");

                if (status.IsWrongType)
                {
                    WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
                    return;
                }

                storageSession.IncrementPendingReadResult(status.Found);

                if (status.Found)
                    ProcessOutput(output.SpanByteAndMemory);
                else
                    WriteNull();
            }
            finally
            {
                completed.Dispose();
            }
        }

        /// <summary>
        /// Completes a <c>GET</c> whose read went to disk on the calling thread, for the cases that cannot
        /// park: a running transaction holds its key locks and its epoch across the call.
        /// </summary>
        bool NetworkGETCompletePendingInline<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            storageSession.StartPendingMetrics();
            _ = storageApi.GET_CompletePending(out var completed, wait: true);
            storageSession.StopPendingMetrics();

            WriteCompletedGet(completed);
            return true;
        }
    }
}