// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// What to reply with when a unified-store command finds no record.
    /// </summary>
    /// <remarks>
    /// A tag rather than the bytes themselves, because the reply has to survive a suspension in a session
    /// field and a <c>ReadOnlySpan&lt;byte&gt;</c> cannot be stored in one.
    /// </remarks>
    internal enum UnifiedMissReply : byte
    {
        /// <summary><c>:0</c>, for commands that report how many keys they changed.</summary>
        Zero,

        /// <summary><c>:-2</c>, which <c>TTL</c> and <c>EXPIRETIME</c> use for a key that does not exist.</summary>
        NegativeTwo,

        /// <summary>A null bulk string.</summary>
        Null,

        /// <summary><c>+none</c>, the type of a key that does not exist.</summary>
        NoneType,
    }

    /// <summary>
    /// The suspending half of the unified-store commands - <c>TYPE</c>, <c>TTL</c>, <c>EXPIRETIME</c>,
    /// <c>EXPIRE</c>, <c>PERSIST</c>, <c>OBJECT</c> and <c>MEMORY USAGE</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// These all read a record's header rather than its value, so on a working set larger than memory they
    /// go to the device exactly as often as a <c>GET</c> does, and completing them in place held the network
    /// thread for the whole I/O. They share one suspension because they share one shape: on a hit they write
    /// whatever the operation serialized, and on a miss they write a fixed reply.
    /// </para>
    /// <para>
    /// This is the operation-in-flight kind of suspension, so it keeps the per-batch scope. The wait is
    /// bounded by the device rather than by a client, and the reply is written into the response buffer the
    /// session held across the wait.
    /// </para>
    /// </remarks>
    internal sealed partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// The miss reply of the suspended unified-store command. Only one command of a session is ever in
        /// flight, so a single slot suffices.
        /// </summary>
        UnifiedMissReply pendingUnifiedMiss;

        /// <summary>
        /// Whether the suspended operation was an update, which reports success differently from a read.
        /// </summary>
        bool pendingUnifiedIsRmw;

        /// <summary>
        /// Writes the reply for a unified-store operation, waiting for the device first if the operation did
        /// not find its record in memory.
        /// </summary>
        /// <param name="status">Status of an operation served from memory; meaningless if it went pending.</param>
        /// <param name="output">Output of an operation served from memory.</param>
        /// <param name="pending">Whether the operation went to the device.</param>
        /// <param name="miss">What to write if no record is found.</param>
        /// <param name="storageApi">The storage API the operation was issued through.</param>
        /// <param name="isRmw">True for an update, which counts as having found its record in more ways
        /// than a read does.</param>
        /// <returns>Always true: when the session parks, the reply is written on resume.</returns>
        bool CompleteUnifiedOp<TGarnetApi>(GarnetStatus status, ref UnifiedOutput output, bool pending, UnifiedMissReply miss, ref TGarnetApi storageApi, bool isRmw = false)
            where TGarnetApi : IGarnetApi
        {
            if (!pending)
            {
                WriteUnifiedReply(status, ref output, miss);
                return true;
            }

            // A running transaction holds its key locks until it ends. Parking here would hold them for the
            // whole wait, stalling every session that needs one of them, so complete on this thread instead.
            if (txnManager.state == TxnState.Running)
            {
                storageSession.StartPendingLatency();
                _ = storageApi.Unified_CompletePending(out var completed, wait: true);
                storageSession.StopPendingLatency();

                WriteCompletedUnified(completed, miss, isRmw);
                return true;
            }

            pendingUnifiedMiss = miss;
            pendingUnifiedIsRmw = isRmw;

            ValueTask body;
            using (BeginAsyncCommand(retainBatchScope: true))
                body = PendingUnifiedBodyAsync(storageApi.Unified_CompletePendingAsync());

            return CompleteAsyncCommand(body);
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask PendingUnifiedBodyAsync(ValueTask<CompletedOutputIterator<UnifiedInput, UnifiedOutput, long>> pending)
        {
            CompletedOutputIterator<UnifiedInput, UnifiedOutput, long> completed;
            storageSession.StartPendingLatency();
            try
            {
                completed = await pending.ConfigureAwait(false);
            }
            finally
            {
                storageSession.StopPendingLatency();
            }

            WriteCompletedUnified(completed, pendingUnifiedMiss, pendingUnifiedIsRmw);
        }

        /// <summary>
        /// Writes the reply for the one operation the completion carries, and disposes the iterator.
        /// </summary>
        void WriteCompletedUnified(CompletedOutputIterator<UnifiedInput, UnifiedOutput, long> completed, UnifiedMissReply miss, bool isRmw)
        {
            using (completed)
            {
                var more = completed.Next();
                Debug.Assert(more, "A pending unified-store operation completed without producing an output");

                var status = completed.Current.Status;
                var output = completed.Current.Output;

                WriteUnifiedReply(isRmw ? StorageSession.InterpretRmw(status) : StorageSession.InterpretRead(status), ref output, miss);

                Debug.Assert(!completed.Next(), "A pending unified-store operation produced more than one output");
            }
        }

        /// <summary>
        /// Writes what the operation serialized, or the command's miss reply if it found no record.
        /// </summary>
        unsafe void WriteUnifiedReply(GarnetStatus status, ref UnifiedOutput output, UnifiedMissReply miss)
        {
            if (status == GarnetStatus.OK)
            {
                ProcessOutput(output.SpanByteAndMemory);
                return;
            }

            switch (miss)
            {
                case UnifiedMissReply.Zero:
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                        SendAndReset();
                    break;
                case UnifiedMissReply.NegativeTwo:
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_N2, ref dcurr, dend))
                        SendAndReset();
                    break;
                case UnifiedMissReply.NoneType:
                    while (!RespWriteUtils.TryWriteSimpleString(CmdStrings.none, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    WriteNull();
                    break;
            }
        }
        /// <summary>
        /// Counts carried from the issue loop of a suspended <c>EXISTS</c> to the half that replies.
        /// </summary>
        int pendingExistsFound;
        int pendingExistsCount;

        /// <summary>
        /// Replies to an <c>EXISTS</c>, waiting for the device first if any of its keys were not in memory.
        /// </summary>
        /// <param name="found">How many of the keys were found in memory.</param>
        /// <param name="pendingCount">How many of the keys went to the device.</param>
        /// <param name="storageApi">The storage API the reads were issued through.</param>
        /// <returns>Always true: when the session parks, the reply is written on resume.</returns>
        bool CompleteExists<TGarnetApi>(int found, int pendingCount, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (pendingCount == 0)
            {
                WriteExistsReply(found);
                return true;
            }

            if (txnManager.state == TxnState.Running)
            {
                storageSession.StartPendingLatency();
                _ = storageApi.Unified_CompletePending(out var completed, wait: true);
                storageSession.StopPendingLatency();

                WriteCompletedExists(found, pendingCount, completed);
                return true;
            }

            pendingExistsFound = found;
            pendingExistsCount = pendingCount;

            ValueTask body;
            using (BeginAsyncCommand(retainBatchScope: true))
                body = PendingExistsBodyAsync(storageApi.Unified_CompletePendingAsync());

            return CompleteAsyncCommand(body);
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask PendingExistsBodyAsync(ValueTask<CompletedOutputIterator<UnifiedInput, UnifiedOutput, long>> pending)
        {
            CompletedOutputIterator<UnifiedInput, UnifiedOutput, long> completed;
            storageSession.StartPendingLatency();
            try
            {
                completed = await pending.ConfigureAwait(false);
            }
            finally
            {
                storageSession.StopPendingLatency();
            }

            WriteCompletedExists(pendingExistsFound, pendingExistsCount, completed);
        }

        /// <summary>
        /// Adds the keys the device found to the ones memory found, and writes the count.
        /// </summary>
        void WriteCompletedExists(int found, int pendingCount, CompletedOutputIterator<UnifiedInput, UnifiedOutput, long> completed)
        {
            var drained = 0;
            using (completed)
            {
                while (completed.Next())
                {
                    drained++;
                    if (completed.Current.Status.Found)
                        found++;
                }
            }

            Debug.Assert(drained == pendingCount,
                "A pending EXISTS drained a different number of reads than it issued");

            WriteExistsReply(found);
        }

        unsafe void WriteExistsReply(int found)
        {
            while (!RespWriteUtils.TryWriteInt32(found, ref dcurr, dend))
                SendAndReset();
        }
    }
}