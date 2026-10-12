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
    /// What to reply with when an object-store command finds no record.
    /// </summary>
    /// <remarks>
    /// A tag rather than the bytes themselves, because the reply has to survive a suspension in a session
    /// field and a <c>ReadOnlySpan&lt;byte&gt;</c> cannot be stored in one.
    /// </remarks>
    internal enum ObjectMissReply : byte
    {
        /// <summary><c>:0</c>, for commands that report how many elements they saw or changed.</summary>
        Zero,

        /// <summary>A null bulk string.</summary>
        Null,

        /// <summary>An empty array, for commands that return a collection.</summary>
        EmptyArray,

        /// <summary>
        /// One null per field the command asked about, for the multi-field reads whose miss reply is an
        /// array as long as their argument list. Read from the parse state, which survives a suspension.
        /// </summary>
        NullPerField,
    }

    /// <summary>
    /// The suspending half of the object-store read commands.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A hash, set, list or sorted-set read whose record is not in memory goes to the device exactly as
    /// often as a <c>GET</c> does, and completing it in place held the network thread for the whole I/O.
    /// These commands share one suspension because they share one shape: on a hit they write whatever the
    /// operation serialized, and on a miss they write a fixed reply.
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
        /// The miss reply of the suspended object-store command. Only one command of a session is ever in
        /// flight, so a single slot suffices.
        /// </summary>
        ObjectMissReply pendingObjectMiss;

        /// <summary>
        /// Writes the reply for an object-store read, waiting for the device first if the read did not find
        /// its record in memory.
        /// </summary>
        /// <param name="status">Status of a read served from memory; meaningless if it went pending.</param>
        /// <param name="output">Output of a read served from memory.</param>
        /// <param name="pending">Whether the read went to the device.</param>
        /// <param name="miss">What to write if no record is found.</param>
        /// <param name="storageApi">The storage API the read was issued through.</param>
        /// <returns>Always true: when the session parks, the reply is written on resume.</returns>
        bool CompleteObjectOp<TGarnetApi>(GarnetStatus status, ref ObjectOutput output, bool pending, ObjectMissReply miss, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!pending)
            {
                WriteObjectReply(status, ref output, miss);
                return true;
            }

            // A running transaction holds its key locks until it ends. Parking here would hold them for the
            // whole wait, stalling every session that needs one of them, so complete on this thread instead.
            if (txnManager.state == TxnState.Running)
            {
                storageSession.StartPendingLatency();
                _ = storageApi.Object_CompletePending(out var completed, wait: true);
                storageSession.StopPendingLatency();

                WriteCompletedObject(completed, miss);
                return true;
            }

            pendingObjectMiss = miss;

            ValueTask body;
            using (BeginAsyncCommand(retainBatchScope: true))
                body = PendingObjectBodyAsync(storageApi.Object_CompletePendingAsync());

            return CompleteAsyncCommand(body);
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask PendingObjectBodyAsync(ValueTask<CompletedOutputIterator<ObjectInput, ObjectOutput, long>> pending)
        {
            CompletedOutputIterator<ObjectInput, ObjectOutput, long> completed;
            storageSession.StartPendingLatency();
            try
            {
                completed = await pending.ConfigureAwait(false);
            }
            finally
            {
                storageSession.StopPendingLatency();
            }

            WriteCompletedObject(completed, pendingObjectMiss);
        }

        /// <summary>
        /// Writes the reply for the one operation the completion carries, and disposes the iterator.
        /// </summary>
        void WriteCompletedObject(CompletedOutputIterator<ObjectInput, ObjectOutput, long> completed, ObjectMissReply miss)
        {
            using (completed)
            {
                var more = completed.Next();
                Debug.Assert(more, "A pending object-store operation completed without producing an output");

                var status = completed.Current.Status;
                var output = completed.Current.Output;

                WriteObjectReply(StorageSession.InterpretObjectStoreStatus(status, ref output), ref output, miss);

                Debug.Assert(!completed.Next(), "A pending object-store operation produced more than one output");
            }
        }

        /// <summary>
        /// Writes what the operation serialized, or the command's miss reply if it found no record.
        /// </summary>
        unsafe void WriteObjectReply(GarnetStatus status, ref ObjectOutput output, ObjectMissReply miss)
        {
            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutput(output.SpanByteAndMemory);
                    return;

                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    return;
            }

            switch (miss)
            {
                case ObjectMissReply.Zero:
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                        SendAndReset();
                    break;
                case ObjectMissReply.EmptyArray:
                    while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                        SendAndReset();
                    break;
                case ObjectMissReply.NullPerField:
                    var fields = parseState.Count - 1;
                    while (!RespWriteUtils.TryWriteArrayLength(fields, ref dcurr, dend))
                        SendAndReset();

                    for (var i = 0; i < fields; i++)
                        WriteNull();
                    break;
                default:
                    WriteNull();
                    break;
            }
        }
    }
}