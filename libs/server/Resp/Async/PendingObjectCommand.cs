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
    /// The suspending half of the object-store commands that are one read or one update of one key.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A hash, set, list or sorted-set operation whose record is not in memory goes to the device exactly as
    /// often as a <c>GET</c> does, and completing it in place held the network thread for the whole I/O.
    /// These commands share one suspension because they share one shape: on a hit they write what the
    /// operation produced, and on a miss they write a reply fixed by the command and its arguments.
    /// </para>
    /// <para>
    /// An update that goes to the device is applied when its record arrives, in the completion that is awaited
    /// here, so it is completed rather than issued again.
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
        /// How the suspended object-store command writes its reply. Only one command of a session is ever
        /// in flight, so a single slot suffices.
        /// </summary>
        ObjectReplyShape pendingObjectReply;

        /// <summary>
        /// Writes the reply for an object-store read or update, waiting for the device first if the operation did
        /// not find its record in memory.
        /// </summary>
        /// <param name="status">Status of an operation served from memory; meaningless if it went pending.</param>
        /// <param name="output">Output of an operation served from memory.</param>
        /// <param name="pending">Whether the operation went to the device.</param>
        /// <param name="miss">What to write if no record is found.</param>
        /// <param name="storageApi">The storage API the operation was issued through.</param>
        /// <returns>Always true: when the session parks, the reply is written on resume.</returns>
        bool CompleteObjectOp<TGarnetApi>(GarnetStatus status, ref ObjectOutput output, bool pending, ObjectMissReply miss, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
            => CompleteObjectOp(status, ref output, pending, new ObjectReplyShape(miss), ref storageApi);

        /// <inheritdoc cref="CompleteObjectOp{TGarnetApi}(GarnetStatus, ref ObjectOutput, bool, ObjectMissReply, ref TGarnetApi)"/>
        /// <param name="shape">How this command writes its reply, on a hit and on a miss.</param>
        bool CompleteObjectOp<TGarnetApi>(GarnetStatus status, ref ObjectOutput output, bool pending, ObjectReplyShape shape, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!pending)
            {
                FinishObjectOp(status, ref output, shape);
                return true;
            }

            // A consume that cannot park waits the operation out on this thread instead; see CanSuspend.
            if (!CanSuspend)
            {
                storageSession.StartPendingLatency();
                _ = storageApi.Object_CompletePending(out var completed, wait: true);
                storageSession.StopPendingLatency();

                WriteCompletedObject(completed, shape);
                return true;
            }

            pendingObjectReply = shape;

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

            WriteCompletedObject(completed, pendingObjectReply);
        }

        /// <summary>
        /// Writes the reply for the one operation the completion carries, and disposes the iterator.
        /// </summary>
        void WriteCompletedObject(CompletedOutputIterator<ObjectInput, ObjectOutput, long> completed, ObjectReplyShape shape)
        {
            using (completed)
            {
                var more = completed.Next();
                Debug.Assert(more, "A pending object-store operation completed without producing an output");

                var status = completed.Current.Status;
                var output = completed.Current.Output;

                FinishObjectOp(StorageSession.InterpretObjectStoreStatus(status, ref output), ref output, shape);

                Debug.Assert(!completed.Next(), "A pending object-store operation produced more than one output");
            }
        }

        /// <summary>
        /// Does what follows an applied operation, then writes its reply.
        /// </summary>
        void FinishObjectOp(GarnetStatus status, ref ObjectOutput output, ObjectReplyShape shape)
        {
            if (shape.NotifiesCollectionUpdate)
                storeWrapper.itemBroker?.HandleCollectionUpdate(parseState.GetArgSliceByRef(0).ToArray());

            WriteObjectReply(status, ref output, shape);
        }

        /// <summary>
        /// Writes what the operation produced, or the command's miss reply if it found no record.
        /// </summary>
        unsafe void WriteObjectReply(GarnetStatus status, ref ObjectOutput output, ObjectReplyShape shape)
        {
            if (status == GarnetStatus.WRONGTYPE)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                    SendAndReset();
                return;
            }

            if (status != GarnetStatus.NOTFOUND || shape.Miss == ObjectMissReply.AsHit)
            {
                WriteObjectHit(ref output, shape.Hit);
                return;
            }

            // One element per argument for the replies that mirror their argument list, the key aside.
            var args = parseState.Count - 1;

            switch (shape.Miss)
            {
                case ObjectMissReply.Zero:
                    WriteZero();
                    break;
                case ObjectMissReply.EmptyArray:
                    while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                        SendAndReset();
                    break;
                case ObjectMissReply.EmptySet:
                    WriteEmptySet();
                    break;
                case ObjectMissReply.NullPerArg:
                    WriteArrayLength(args);
                    for (var i = 0; i < args; i++)
                        WriteNull();
                    break;
                case ObjectMissReply.MinusTwoPerCount:
                    WriteArrayLength(shape.Count);
                    for (var i = 0; i < shape.Count; i++)
                    {
                        while (!RespWriteUtils.TryWriteInt32(-2, ref dcurr, dend))
                            SendAndReset();
                    }
                    break;
                case ObjectMissReply.EmptyArrayIfFlagElseNull:
                    if (shape.Flag)
                    {
                        while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                        WriteNull();
                    break;
                case ObjectMissReply.ZeroIfFlagElseZeroPerArg:
                    if (shape.Flag)
                        WriteZero();
                    else
                    {
                        WriteArrayLength(args);
                        for (var i = 0; i < args; i++)
                            WriteZero();
                    }
                    break;
                case ObjectMissReply.NullIfFlagElseNullArrayPerArg:
                    if (shape.Flag)
                        WriteNull();
                    else
                    {
                        WriteArrayLength(args);
                        for (var i = 0; i < args; i++)
                            WriteNullArray();
                    }
                    break;
                case ObjectMissReply.NoSuchKey:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_NOSUCHKEY, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    WriteNull();
                    break;
            }
        }

        /// <summary>Writes what the operation produced for a record it found.</summary>
        unsafe void WriteObjectHit(ref ObjectOutput output, ObjectHitReply hit)
        {
            switch (hit)
            {
                case ObjectHitReply.Int32Result1:
                    while (!RespWriteUtils.TryWriteInt32(output.result1, ref dcurr, dend))
                        SendAndReset();
                    break;

                case ObjectHitReply.Ok:
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                    break;

                case ObjectHitReply.Int32Result1OrLexRangeError:
                    if (output.result1 == int.MaxValue)
                    {
                        while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_STRING, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        while (!RespWriteUtils.TryWriteInt32(output.result1, ref dcurr, dend))
                            SendAndReset();
                    }
                    break;

                case ObjectHitReply.SerializedThenNullIfNegativeOne:
                    ProcessOutput(output.SpanByteAndMemory);
                    if (output.result1 == -1)
                        WriteNull();
                    break;

                default:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
            }
        }
    }
}