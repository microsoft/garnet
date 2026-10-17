// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Which reply a string command writes once its operation completes, and how the store's status maps to it.
    /// </summary>
    /// <remarks>
    /// A tag rather than a delegate, because it has to survive a suspension in a session field without allocating.
    /// </remarks>
    internal enum StringReply : byte
    {
        /// <summary><c>GETEX</c>, <c>GETWITHETAG</c> and <c>GETIFNOTMATCH</c>: the value, or null.</summary>
        Value,

        /// <summary><c>GETDEL</c>: the value, or null, with the outcome not counted.</summary>
        ValueUncounted,

        /// <summary><c>GETRANGE</c> and <c>SUBSTR</c>: the range, or an empty string.</summary>
        Range,

        /// <summary><c>STRLEN</c>: the length of the value.</summary>
        Length,

        /// <summary><c>SETNX</c>: whether the key was set.</summary>
        SetNx,

        /// <summary><c>SET</c> with existence or TTL options and without <c>GET</c>.</summary>
        SetConditional,

        /// <summary><c>SET</c> with <c>GET</c>, and <c>GETSET</c>: the previous value, or null.</summary>
        SetConditionalGet,

        /// <summary><c>SETWITHETAG</c>, <c>SETIFMATCH</c> and <c>SETIFGREATER</c>.</summary>
        ETagSet,

        /// <summary><c>DELIFGREATER</c>: whether the key was deleted.</summary>
        DelIfGreater,

        /// <summary><c>RESTORE</c>: OK, or busy key.</summary>
        Restore,

        /// <summary><c>INCR</c>, <c>DECR</c>, <c>INCRBY</c> and <c>DECRBY</c>: the new value.</summary>
        Increment,

        /// <summary><c>INCRBYFLOAT</c>: the new value.</summary>
        IncrementByFloat,

        /// <summary><c>APPEND</c> and <c>SETRANGE</c>: the new length.</summary>
        NewLength,

        /// <summary><c>SETBIT</c>: the previous bit.</summary>
        SetBit,

        /// <summary><c>GETBIT</c>: the bit.</summary>
        GetBit,

        /// <summary><c>BITCOUNT</c>: the count.</summary>
        BitCount,

        /// <summary><c>BITPOS</c>: the position.</summary>
        BitPos,
    }

    /// <summary>
    /// The suspending half of the single-key string commands other than <c>GET</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Each of these issues one read or one update. When the record is on the device the operation goes pending,
    /// and instead of completing it on the network thread the session parks until the I/O lands, then writes the
    /// same reply the in-memory path would have. A pending update is applied by the completion itself, so it is
    /// never issued again; the reply is written from the completion's status and output.
    /// </para>
    /// <para>
    /// The operation writes its output when it completes, so the output must outlive the handler's frame. An
    /// output in the response buffer does, because the per-batch scope is retained across the park; a small
    /// result such as <c>INCR</c>'s new value goes to <see cref="GetSmallStringOutput"/> instead of the stack.
    /// </para>
    /// </remarks>
    internal sealed partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Large enough for the longest result written by <see cref="GetSmallStringOutput"/>: a formatted double.
        /// </summary>
        const int SmallStringOutputSize = NumUtils.MaximumFormatDoubleLength + 1;

        /// <summary>
        /// The reply of the suspended string command. Only one command of a session is ever in flight, so a single
        /// slot suffices.
        /// </summary>
        StringReply pendingStringReply;

        /// <summary>
        /// Pinned buffer behind <see cref="GetSmallStringOutput"/>, allocated on first use.
        /// </summary>
        byte[] smallStringOutput;

        /// <summary>
        /// An output for a string operation whose result is a few bytes, in memory that stays valid while the
        /// session is parked on the operation.
        /// </summary>
        /// <param name="length">Length of the output, at most <see cref="SmallStringOutputSize"/>.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        unsafe StringOutput GetSmallStringOutput(int length)
        {
            Debug.Assert(length <= SmallStringOutputSize);
            var buffer = smallStringOutput ?? AllocateSmallStringOutput();
            return StringOutput.FromPinnedPointer((byte*)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(buffer)), length);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        byte[] AllocateSmallStringOutput()
            => smallStringOutput = GC.AllocateUninitializedArray<byte>(SmallStringOutputSize, pinned: true);

        /// <summary>
        /// Finishes a string command whose operation went to the device, by parking the session until the I/O
        /// completes, and writes its reply.
        /// </summary>
        /// <param name="reply">Which reply to write.</param>
        /// <param name="storageApi">The storage API the operation was issued through.</param>
        /// <returns>Always true: when the session parks, the reply is written on resume.</returns>
        [MethodImpl(MethodImplOptions.NoInlining)]
        bool CompleteStringOp<TGarnetApi>(StringReply reply, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // A consume that cannot park waits the operation out on this thread instead; see CanSuspend.
            if (!CanSuspend)
            {
                _ = storageApi.GET_CompletePending(out var completed, wait: true);
                WriteCompletedString(completed, reply);
                return true;
            }

            pendingStringReply = reply;

            ValueTask body;
            using (BeginAsyncCommand(retainBatchScope: true))
                body = PendingStringBodyAsync(storageApi.GET_CompletePendingAsync());

            return CompleteAsyncCommand(body);
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask PendingStringBodyAsync(ValueTask<CompletedOutputIterator<StringInput, StringOutput, long>> pending)
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

            WriteCompletedString(completed, pendingStringReply);
        }

        /// <summary>
        /// Writes the reply for the one operation the completion carries, and disposes the iterator.
        /// </summary>
        /// <remarks>
        /// The reply is written after the iterator is released, because a <c>SET</c> that finds another type
        /// retries on the same store session, and completing that retry would reuse the session's iterator.
        /// </remarks>
        void WriteCompletedString(CompletedOutputIterator<StringInput, StringOutput, long> completed, StringReply reply)
        {
            Status status;
            StringInput input;
            StringOutput output;

            using (completed)
            {
                var more = completed.Next();
                Debug.Assert(more, "A pending string operation completed without producing an output");

                ref var current = ref completed.Current;
                status = current.Status;
                input = current.Input;
                output = current.Output;

                Debug.Assert(!completed.Next(), "A pending string operation produced more than one output");
            }

            WriteStringReply(reply, status, ref input, ref output);
        }

        /// <summary>
        /// Maps a completed operation's status the way its command does, and writes the command's reply.
        /// </summary>
        void WriteStringReply(StringReply reply, Status status, ref StringInput input, ref StringOutput output)
        {
            switch (reply)
            {
                case StringReply.Value:
                    WriteStringValueReply(storageSession.CountedStringReadStatus(status), ref output);
                    break;
                case StringReply.ValueUncounted:
                    WriteStringValueReply(StorageSession.StringReadStatus(status), ref output);
                    break;
                case StringReply.Range:
                    WriteGetRangeReply(storageSession.CountedStringReadStatus(status), ref output);
                    break;
                case StringReply.Length:
                    WriteStrlenReply(storageSession.CountedStringReadStatus(status), ref output);
                    break;
                case StringReply.SetNx:
                    WriteSetNxReply(storageSession.ConditionalSetStatus(status));
                    break;
                case StringReply.SetConditional:
                    WriteSetConditionalReply(storageSession.ConditionalSetStatus(status), parseState.GetArgSliceByRef(0), ref input);
                    break;
                case StringReply.SetConditionalGet:
                    WriteSetConditionalGetReply(storageSession.ConditionalSetStatus(status), ref output);
                    break;
                case StringReply.ETagSet:
                    WriteETagSetReply(storageSession.ConditionalSetStatus(status), parseState.GetArgSliceByRef(0), ref input, ref output);
                    break;
                case StringReply.DelIfGreater:
                    WriteDelIfGreaterReply(storageSession.ConditionalDeleteStatus(status));
                    break;
                case StringReply.Restore:
                    WriteRestoreReply(storageSession.ConditionalSetStatus(status));
                    break;
                case StringReply.Increment:
                    WriteIncrementReply(StorageSession.StringTypedStatus(status), ref output);
                    break;
                case StringReply.IncrementByFloat:
                    WriteIncrementByFloatReply(StorageSession.StringTypedStatus(status), ref output);
                    break;
                case StringReply.NewLength:
                    WriteNewLengthReply(StorageSession.StringTypedStatus(status), ref output);
                    break;
                case StringReply.SetBit:
                    WriteSetBitReply(StorageSession.StringUpdateStatus(status), ref output);
                    break;
                case StringReply.GetBit:
                    WriteGetBitReply(StorageSession.StringReadStatus(status), ref output);
                    break;
                case StringReply.BitCount:
                    WriteBitCountReply(StorageSession.StringReadStatus(status), ref output);
                    break;
                case StringReply.BitPos:
                    WriteBitPosReply(StorageSession.StringReadStatus(status), ref output);
                    break;
                default:
                    throw new GarnetException($"Unknown string reply {reply}");
            }
        }

        void WriteStringValueReply(GarnetStatus status, ref StringOutput output)
        {
            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.WRONGTYPE:
                    WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
                    break;
                default:
                    Debug.Assert(output.SpanByteAndMemory.IsSpanByte);
                    WriteNull();
                    break;
            }
        }

        void WriteGetRangeReply(GarnetStatus status, ref StringOutput output)
        {
            if (status == GarnetStatus.OK)
            {
                sessionMetrics?.incr_total_found();
                ProcessOutput(output.SpanByteAndMemory);
            }
            else if (status == GarnetStatus.WRONGTYPE)
            {
                WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
            }
            else
            {
                sessionMetrics?.incr_total_notfound();
                Debug.Assert(output.SpanByteAndMemory.IsSpanByte);
                WriteDirect(CmdStrings.RESP_EMPTY);
            }
        }

        void WriteStrlenReply(GarnetStatus status, ref StringOutput output)
        {
            switch (status)
            {
                case GarnetStatus.OK:
                    var length = output.SpanByteAndMemory.Length;
                    if (!output.SpanByteAndMemory.IsSpanByte)
                        output.SpanByteAndMemory.Dispose();
                    WriteInt32(length);
                    break;
                case GarnetStatus.WRONGTYPE:
                    WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
                    break;
                default:
                    WriteInt32(0);
                    break;
            }
        }

        // The status returned for SETNX as NOTFOUND is the expected status in the happy path
        void WriteSetNxReply(GarnetStatus status)
            => WriteInt32(status == GarnetStatus.NOTFOUND ? 1 : 0);

        void WriteSetConditionalReply(GarnetStatus status, PinnedSpanByte key, ref StringInput input)
        {
            var cmd = input.header.cmd;

            // Type already stored in key which requires an explicit delete. The delete leaves a tombstone at the
            // tail, so the retry finds its record in memory.
            if (status == GarnetStatus.WRONGTYPE)
            {
                using (txnManager.PromoteToTransaction(TransactionStoreTypes.Main | TransactionStoreTypes.Object, key, LockType.Exclusive))
                {
                    _ = storageSession.DELETE(key, ref storageSession.unifiedTransactionalContext);
                    status = storageSession.SET_Conditional(key, ref input, ref storageSession.stringTransactionalContext);
                }
            }

            // KEEPTTL without flags doesn't care whether it was found or not.
            if (cmd == RespCommand.SETKEEPTTL)
            {
                WriteDirect(CmdStrings.RESP_OK);
                return;
            }

            var ok = status != GarnetStatus.NOTFOUND;

            // the status returned for SETEXNX as NOTFOUND is the expected status in the happy path, so flip the ok flag
            if (cmd == RespCommand.SETEXNX)
                ok = !ok;

            if (ok)
                WriteDirect(CmdStrings.RESP_OK);
            else
                WriteNull();
        }

        void WriteSetConditionalGetReply(GarnetStatus status, ref StringOutput output)
        {
            if (status == GarnetStatus.WRONGTYPE)
                WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
            else if (status != GarnetStatus.NOTFOUND)
                ProcessOutput(output.SpanByteAndMemory);
            else
                WriteNull();
        }

        void WriteETagSetReply(GarnetStatus status, PinnedSpanByte key, ref StringInput input, ref StringOutput output)
        {
            // Type already stored in key which requires an explicit delete
            if (status == GarnetStatus.WRONGTYPE)
            {
                using (txnManager.PromoteToTransaction(TransactionStoreTypes.Main | TransactionStoreTypes.Object, key, LockType.Exclusive))
                {
                    _ = storageSession.DELETE(key, ref storageSession.unifiedTransactionalContext);
                    _ = storageSession.SET_Conditional(key, ref input, ref output, ref storageSession.stringTransactionalContext);
                }
            }

            ProcessOutput(output.SpanByteAndMemory);
        }

        void WriteDelIfGreaterReply(GarnetStatus status)
            => WriteInt32(status == GarnetStatus.OK ? 1 : 0);

        void WriteRestoreReply(GarnetStatus status)
        {
            // RESTORE's input carries the payload in a scratch slice, which is released once the update is done
            _ = scratchBufferBuilder.RewindScratchBuffer(parseState.GetArgSliceByRef(0));

            if (status is GarnetStatus.NOTFOUND)
                WriteDirect(CmdStrings.RESP_OK);
            else
                WriteError(CmdStrings.RESP_ERR_BUSSYKEY);
        }

        void WriteIncrementReply(GarnetStatus status, ref StringOutput output)
        {
            if (status == GarnetStatus.WRONGTYPE)
                WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
            else if (!output.HasError)
                WriteIntegerFromBytes(output.SpanByteAndMemory.ReadOnlySpan);
            else
                WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        }

        void WriteIncrementByFloatReply(GarnetStatus status, ref StringOutput output)
        {
            if (status == GarnetStatus.WRONGTYPE)
                WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
            else if (!output.HasError)
                WriteBulkString(output.SpanByteAndMemory.ReadOnlySpan);
            else if ((output.OutputFlags & StringOutputFlags.NaNOrInfinityError) != 0)
                WriteError(CmdStrings.RESP_ERR_GENERIC_NAN_INFINITY_INCR);
            else if ((output.OutputFlags & StringOutputFlags.InvalidTypeError) != 0)
                WriteError(CmdStrings.RESP_ERR_NOT_VALID_FLOAT);
            else
                throw new GarnetException($"Unrecognized return output flags value: {output.OutputFlags}");
        }

        void WriteNewLengthReply(GarnetStatus status, ref StringOutput output)
        {
            if (status == GarnetStatus.WRONGTYPE)
            {
                WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
                return;
            }

            Debug.Assert(output.SpanByteAndMemory.IsSpanByte);
            WriteIntegerFromBytes(output.SpanByteAndMemory.ReadOnlySpan);
        }

        unsafe void WriteSetBitReply(GarnetStatus status, ref StringOutput output)
        {
            if (status == GarnetStatus.OK)
                dcurr += output.SpanByteAndMemory.Length;
            else if (status == GarnetStatus.WRONGTYPE)
                WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
        }

        unsafe void WriteGetBitReply(GarnetStatus status, ref StringOutput output)
        {
            if (status == GarnetStatus.NOTFOUND)
                WriteDirect(CmdStrings.RESP_RETURN_VAL_0);
            else if (status == GarnetStatus.WRONGTYPE)
                WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
            else
                dcurr += output.SpanByteAndMemory.Length;
        }

        void WriteBitCountReply(GarnetStatus status, ref StringOutput output)
        {
            if (status == GarnetStatus.OK)
                ProcessOutput(output.SpanByteAndMemory);
            else if (status == GarnetStatus.NOTFOUND)
                WriteDirect(CmdStrings.RESP_RETURN_VAL_0);
            else if (status == GarnetStatus.WRONGTYPE)
                WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
        }

        void WriteBitPosReply(GarnetStatus status, ref StringOutput output)
        {
            if (status == GarnetStatus.OK)
            {
                ProcessOutput(output.SpanByteAndMemory);
            }
            else if (status == GarnetStatus.WRONGTYPE)
            {
                WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
            }
            else if (status == GarnetStatus.NOTFOUND)
            {
                var bit = parseState.GetArgSliceByRef(1).ReadOnlySpan[0];
                WriteDirect(bit == '0' ? CmdStrings.RESP_RETURN_VAL_0 : CmdStrings.RESP_RETURN_VAL_N1);
            }
        }
    }
}