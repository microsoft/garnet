// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Read batch implementation for <see cref="RespCommand.MGET"/>.
    /// 
    /// Attempts to write directly to output buffer.
    /// Blocks if operation would complete asynchronously.
    /// 
    /// Ref struct on .NET 9+ for efficiency purposes.
    /// </summary>
    internal
#if NET9_0_OR_GREATER
        ref
#endif
        struct MGetReadArgBatch<TGarnetApi>(ref TGarnetApi storageApi, RespServerSession session) : IReadArgBatch<SpanByte, RawStringInput, SpanByteAndMemory>
        where TGarnetApi : IGarnetAdvancedApi
    {
        private Status currentStatus;

        private readonly
#if NET9_0_OR_GREATER
            ref
#endif
            TGarnetApi storageApi =
#if NET9_0_OR_GREATER
            ref
#endif
            storageApi;

        /// <inheritdoc/>
        public readonly int Count
        => session.parseState.Count;

        /// <inheritdoc/>
        public readonly void GetInput(int i, out RawStringInput input)
        => input = new(RespCommand.GET, arg1: -1);

        /// <inheritdoc/>
        public readonly void GetKey(int i, out SpanByte key)
        => key = session.parseState.GetArgSliceByRef(i).SpanByte;

        /// <inheritdoc/>
        public readonly unsafe void GetOutput(int i, out SpanByteAndMemory output)
        => output = SpanByteAndMemory.FromPinnedSpan(MemoryMarshal.CreateSpan(ref Unsafe.AsRef<byte>(session.dcurr), (int)(session.dend - session.dcurr)));

        /// <inheritdoc/>
        public void SetStatus(int i, Status status)
        => currentStatus = status;

        /// <inheritdoc/>
        public readonly unsafe void SetOutput(int i, SpanByteAndMemory output)
        {
            var finalStatus = currentStatus;
            if (finalStatus.IsPending)
            {
                session.storageSession.incr_session_pending();

                // Have to block, unlike with ScatterGather we cannot proceed to next result
                var res = storageApi.GET_CompletePending(out var completedOutputs, wait: true);
                Debug.Assert(res, "Should have completed");

                using (completedOutputs)
                {
                    var more = completedOutputs.Next();
                    Debug.Assert(more, "Expected one result");

                    finalStatus = completedOutputs.Current.Status;
                    output = completedOutputs.Current.Output;
                    more = completedOutputs.Next();

                    Debug.Assert(!more, "Expected only one result");
                }
            }

            if (finalStatus.Found)
            {
                session.storageSession.incr_session_found();

                // Got a result, write it out

                if (output.IsSpanByte)
                {
                    // Place result directly into buffer, just advance session points
                    session.dcurr += output.Length;
                }
                else
                {
                    // Didn't fit inline, copy result over
                    session.SendAndReset(output.Memory, output.Length);
                }
            }
            else
            {
                session.storageSession.incr_session_notfound();

                // Not found, write a null out
                while (!RespWriteUtils.TryWriteNull(ref session.dcurr, session.dend))
                    session.SendAndReset();
            }
        }
    }

    /// <summary>
    /// Read batch implementation for <see cref="RespCommand.MGET"/> with scatter gather.
    /// 
    /// For commands that are served entirely out of memory, writes results directly into the output buffer if possible.
    /// If operation would complete asynchronously, moves onto the next one and buffers results for later writing.
    /// </summary>
    internal struct MGetReadArgBatch_SG(RespServerSession session) : IReadArgBatch<SpanByte, RawStringInput, SpanByteAndMemory>
    {
        private bool pendingNullWrite;
        private Memory<(Status Status, SpanByteAndMemory Output)> runningStatus;

        /// <inheritdoc/>
        public readonly int Count
        => session.parseState.Count;

        private readonly bool HasGoneAsync
        => !runningStatus.IsEmpty;

        /// <inheritdoc/>
        public readonly void GetInput(int i, out RawStringInput input)
        {
            // Save the index so we can order async completions correctly in the response
            // 
            // Use a - so we get "include RESP protocol"-behavior
            input = new(RespCommand.GET, arg1: -(i + 1));
        }

        /// <inheritdoc/>
        public readonly void GetKey(int i, out SpanByte key)
        => key = session.parseState.GetArgSliceByRef(i).SpanByte;

        /// <inheritdoc/>
        public readonly void GetOutput(int i, out SpanByteAndMemory output)
        {
            if (!HasGoneAsync)
            {
                // Attempt to write directly into output buffer
                unsafe
                {
                    output = SpanByteAndMemory.FromPinnedSpan(MemoryMarshal.CreateSpan(ref Unsafe.AsRef<byte>(session.dcurr), (int)(session.dend - session.dcurr)));
                }
            }
            else
            {
                // Otherwise we're gonna allocate
                output = default;
            }
        }

        /// <inheritdoc/>
        public readonly unsafe void SetOutput(int i, SpanByteAndMemory output)
        {
            if (!HasGoneAsync)
            {
                if (pendingNullWrite)
                {
                    while (!RespWriteUtils.TryWriteNull(ref session.dcurr, session.dend))
                        session.SendAndReset();
                }
                else
                {
                    if (output.IsSpanByte)
                    {
                        // We place directly into the output buffer, nothing else needed
                        session.dcurr += output.Length;
                    }
                    else
                    {
                        // Got it synchronously, but it was too big for the buffer
                        session.SendAndReset(output.Memory, output.Length);
                    }
                }
            }
            else
            {
                var asyncOffset = session.parseState.Count - runningStatus.Length;

                var shiftedIndex = i - asyncOffset;
                runningStatus.Span[shiftedIndex] = (runningStatus.Span[shiftedIndex].Status, output);
            }
        }

        /// <inheritdoc/>
        public void SetStatus(int i, Status status)
        {
            if (status.IsPending)
            {
                session.storageSession.incr_session_pending();

                if (!HasGoneAsync)
                {

                    var bufferSize = session.parseState.Count - i;
                    var arr = ArrayPool<(Status, SpanByteAndMemory)>.Shared.Rent(bufferSize);
                    runningStatus = arr.AsMemory()[..bufferSize];

#if DEBUG
                    // Fill with garbage to make easier to debug
                    Status garbage = default;
                    Unsafe.As<Status, byte>(ref garbage) = 255;
                    runningStatus.Span.Fill((garbage, default));
#endif
                }
            }
            else
            {
                // Record synchronous metrics right now
                if (status.Found)
                {
                    session.storageSession.incr_session_found();
                }
                else
                {
                    session.storageSession.incr_session_notfound();
                }
            }

            if (!HasGoneAsync)
            {
                // If we missed, AND we're not pending, we can write a null directly when we get the result
                pendingNullWrite = status.NotFound;
            }
            else
            {
                var asyncOffset = session.parseState.Count - runningStatus.Length;

                var shiftedIndex = i - asyncOffset;
                runningStatus.Span[shiftedIndex] = (status, default);
            }
        }

        /// <summary>
        /// If any operations went async, complete them all and finish writing the results out.
        /// </summary>
        public readonly unsafe void CompletePending<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetAdvancedApi
        {
            if (!HasGoneAsync)
            {
                return;
            }

            try
            {
                var asyncOffset = session.parseState.Count - runningStatus.Length;

                // Force completion
                var res = storageApi.GET_CompletePending(out var iter, wait: true);
                Debug.Assert(res, "Expected all pending operations to complete");

                using (iter)
                {
                    var runningStatusSpan = runningStatus.Span;

                    // Attempt to complete all pending in a single pass
                    for (var i = 0; i < runningStatusSpan.Length; i++)
                    {
                        var (status, output) = runningStatusSpan[i];
                        if (status.IsPending)
                        {
                            // If this status went pending, advance our async completion iterator until we find it
                            //
                            // This may fill in more of the status buffer incidentally

                            while (iter.Next())
                            {
                                var rawIndex = -(int)iter.Current.Input.arg1 - 1;
                                var shiftedIndex = rawIndex - asyncOffset;

                                var asyncStatus = iter.Current.Status;
                                var asyncOutput = iter.Current.Output;

                                // Update metrics for async operations - sync operations were already handle in SetStatus
                                if (asyncStatus.Found)
                                {
                                    session.storageSession.incr_session_found();
                                }
                                else
                                {
                                    session.storageSession.incr_session_notfound();
                                }

                                runningStatusSpan[shiftedIndex] = (asyncStatus, asyncOutput);

                                if (shiftedIndex == i)
                                {
                                    status = asyncStatus;
                                    output = asyncOutput;
                                    break;
                                }
                            }
                        }

                        Debug.Assert(!status.IsPending, "Should have resolved status by now");

                        if (status.Found)
                        {
                            // Found it, either synchronously or async

                            if (output.IsSpanByte)
                            {
                                // We place directly into the output buffer, nothing else needed
                                session.dcurr += output.Length;
                            }
                            else
                            {
                                // Got it synchronously, but it was too big for the buffer
                                session.SendAndReset(output.Memory, output.Length);
                            }
                        }
                        else
                        {
                            // Did not find it, was probably synchronous but we couldn't handle it until now
                            while (!RespWriteUtils.TryWriteNull(ref session.dcurr, session.dend))
                                session.SendAndReset();
                        }
                    }
                }
            }
            finally
            {
                if (MemoryMarshal.TryGetArray<(Status, SpanByteAndMemory)>(runningStatus, out var arrSeg))
                {
                    ArrayPool<(Status, SpanByteAndMemory)>.Shared.Return(arrSeg.Array);
                }
            }
        }
    }
}