// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// The suspending half of <c>MGET</c>: what happens when some of its reads miss memory.
    /// </summary>
    /// <remarks>
    /// <c>MGET</c> issues every read up front and drains them together, so one command can have a whole
    /// batch of keys outstanding on the device at once. Waiting for that batch on the network thread holds
    /// the thread for the slowest of the reads, which is why the session suspends for it instead. Like the
    /// single-key <c>GET</c> this is an operation-in-flight suspension and keeps the per-batch scope: the
    /// reads are already writing into this session's response buffer, and the wait is bounded by the device.
    /// </remarks>
    internal sealed partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// The batch whose replies are written when the suspended <c>MGET</c> resumes. Only one command of a
        /// session is ever in flight, so a single slot suffices.
        /// </summary>
        MGetReadArgBatch_SG pendingMGetBatch;

        /// <summary>
        /// Finishes an <c>MGET</c> whose reads went to disk, parking the session for the I/O where that is
        /// allowed and completing inline where it is not.
        /// </summary>
        /// <param name="batch">The batch holding what the reads served from memory already produced.</param>
        /// <param name="storageApi">The storage API the reads were issued through.</param>
        /// <returns>Always true: when the session parks, the replies are written on resume.</returns>
        bool FinishPendingMGet<TGarnetApi>(in MGetReadArgBatch_SG batch, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // A running transaction holds its key locks until it ends. Parking here would hold them for the
            // whole wait, stalling every session that needs one of them, so complete on this thread instead.
            if (txnManager.state == TxnState.Running)
            {
                storageSession.StartPendingLatency();
                _ = storageApi.GET_CompletePending(out var completed, wait: true);
                storageSession.StopPendingLatency();

                batch.WriteCompleted(completed);
                return true;
            }

            pendingMGetBatch = batch;

            ValueTask body;
            using (BeginAsyncCommand(retainBatchScope: true))
                body = PendingMGetBodyAsync(storageApi.GET_CompletePendingAsync());

            return CompleteAsyncCommand(body);
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask PendingMGetBodyAsync(ValueTask<CompletedOutputIterator<StringInput, StringOutput, long>> pending)
        {
            CompletedOutputIterator<StringInput, StringOutput, long> completed;
            storageSession.StartPendingLatency();
            try
            {
                completed = await pending.ConfigureAwait(false);
            }
            finally
            {
                storageSession.StopPendingLatency();
            }

            // Cleared before writing, so a reply that throws cannot leave the batch behind to be written a
            // second time by the next MGET on this session.
            var batch = pendingMGetBatch;
            pendingMGetBatch = default;

            batch.WriteCompleted(completed);
        }
    }
}