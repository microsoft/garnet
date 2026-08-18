// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    public partial class VectorManager
    {
        /// <summary>
        /// Different steps of quantization process.
        /// </summary>
        private enum QuantizationStep
        {
            Invalid = 0,

            /// <summary>
            /// Build the quantization table - only one task can do this per Vector Set Index.
            /// </summary>
            BuildQuantizationTable,

            /// <summary>
            /// Backfill quantized vectors - many tasks can do this concurrently for a Vector Set Index.
            /// </summary>
            BackfillQuantizedVectors,
        }

        private readonly record struct QuantizationState(ReadOnlyMemory<byte> Key, QuantizationStep Step, int StepIndex);

        private readonly Channel<QuantizationState> quantizationChannel;

        /// <summary>
        /// Worker tasks that drain <see cref="quantizationChannel"/>. They run on the .NET thread pool, but a
        /// worker that cannot immediately acquire a vector set's lock yields its pool thread and retries
        /// asynchronously (see <see cref="StartQuantizationTasks"/>) instead of spin-waiting on it. A network VADD
        /// that recreates a disk-tiered index blocks on a pending disk read while holding that lock exclusively;
        /// if the workers spin-waited on the pool they would consume every pool thread and starve the disk-IO
        /// completion that releases the lock, deadlocking concurrent VADD. Cooperative yielding keeps pool threads
        /// available for that completion.
        /// </summary>
        private readonly Task[] quantizationTasks;

        /// <summary>
        /// Number of quantization worker tasks, also used as the backfill shard count.
        /// </summary>
        private readonly int quantizationTaskCount;

        private int quantizationRequestsProcessed;
        private int quantizationBackfillsProcessed;

        /// <summary>
        /// For testing purposes, the number of <see cref="QuantizationStep.BuildQuantizationTable"/> requests processed by <see cref="StartQuantizationTasks"/> tasks.
        /// </summary>
        internal int QuantizationRequestsProcessed => quantizationRequestsProcessed;

        /// <summary>
        /// For testing purposes, the number of <see cref="QuantizationStep.BackfillQuantizedVectors"/> requests processed by <see cref="StartQuantizationTasks"/> tasks.
        /// </summary>
        internal int QuantizationBackfillsProcessed => quantizationBackfillsProcessed;

        /// <summary>
        /// Populate <see cref="quantizationTasks"/> with running tasks for handling any quantization requests.
        /// </summary>
        public void StartQuantizationTasks()
        {
            for (var i = 0; i < quantizationTasks.Length; i++)
            {
                quantizationTasks[i] = QuantizationTaskAsync(this, quantizationChannel.Reader, quantizationChannel.Writer);
            }

            static async Task QuantizationTaskAsync(VectorManager self, ChannelReader<QuantizationState> reader, ChannelWriter<QuantizationState> writer)
            {
                // Force async
                await Task.Yield();

                while (await reader.WaitToReadAsync().ConfigureAwait(false))
                {
                    using var session = (RespServerSession)self.getTempSession();
                    if (session.activeDbId != self.dbId && !session.TrySwitchActiveDatabaseSession(self.dbId))
                    {
                        throw new GarnetException($"Could not switch VectorManager cleanup session to {self.dbId}, initialization failed");
                    }

                    // Pinned once and reused; the synchronous per-request body views it as a Span. Kept as a byte[]
                    // (not a Span) because it must survive the awaits in the cooperative retry loop below.
                    var indexArray = GC.AllocateArray<byte>(IndexSizeBytes, pinned: true);

                    while (reader.TryRead(out var state))
                    {
                        // Cooperative acquisition. TryProcessQuantizationRequest returns false only when the vector
                        // set lock is currently held by another thread - typically a network VADD that is recreating
                        // a disk-tiered index and is blocked on a pending disk read while holding the lock exclusively.
                        // Instead of spin-waiting on a pool thread (which would consume the pool and starve the
                        // disk-IO completion that releases the lock, deadlocking concurrent VADD), yield the pool
                        // thread and retry so a completion can always be scheduled.
                        for (var attempt = 0; !TryProcessQuantizationRequest(self, session, writer, state, indexArray); attempt++)
                        {
                            if (attempt < 16)
                                await Task.Yield();
                            else
                                await Task.Delay(1).ConfigureAwait(false);
                        }
                    }
                }
            }

            // Processes a single request under a non-blocking lock acquisition. Returns true when the request was
            // handled (or is terminal, e.g. the index was dropped), and false when the set lock was contended and
            // the caller should yield its pool thread and retry. All ref struct / Span / native interop stays inside
            // this synchronous method so it never straddles an await.
            static bool TryProcessQuantizationRequest(VectorManager self, RespServerSession session, ChannelWriter<QuantizationState> writer, QuantizationState state, byte[] indexArray)
            {
                try
                {
                    unsafe
                    {
                        fixed (byte* keyPtr = state.Key.Span)
                        {
                            var keySpan = SpanByte.FromPinnedPointer(keyPtr, state.Key.Length);

                            // Dummy command, we just need something Vector Set-y
                            StringInput input = default;
                            input.header.cmd = RespCommand.VSIM;

                            Span<byte> indexSpan = indexArray;

                            using (self.ReadVectorIndexCore(session.storageSession, keySpan, ref input, indexSpan, nonBlocking: true, out var res, out var contended))
                            {
                                // The lock is held by another thread (often a network VADD blocked on a pending disk
                                // read during index recreate). Report back so the caller yields and retries rather
                                // than spinning on a pool thread.
                                if (contended)
                                    return false;

                                if (res != GarnetStatus.OK)
                                {
                                    // Index was dropped before quantization request could be processed, ignore request
                                    return true;
                                }

                                ReadIndex(indexSpan, out var context, out _, out _, out _, out _, out _, out _, out _, out var indexPtr);

                                switch (state.Step)
                                {
                                    case QuantizationStep.BuildQuantizationTable:
                                        if (self.Service.BuildQuantizationTable(context, indexPtr))
                                        {
                                            _ = Interlocked.Increment(ref self.quantizationRequestsProcessed);

                                            // Schedule backfill after quantization table is available
                                            for (var i = 0; i < self.quantizationTaskCount; i++)
                                            {
                                                _ = writer.TryWrite(new(state.Key, QuantizationStep.BackfillQuantizedVectors, i));
                                            }
                                        }

                                        break;

                                    case QuantizationStep.BackfillQuantizedVectors:
                                        self.Service.BackfillQuantizedVectors(context, indexPtr, state.StepIndex, self.quantizationTaskCount);

                                        _ = Interlocked.Increment(ref self.quantizationBackfillsProcessed);
                                        break;
                                    default:
                                        self.logger?.LogError("Unexpected step: {step}", state.Step);
                                        break;
                                }
                            }
                        }
                    }

                    return true;
                }
                catch (Exception ex)
                {
                    self.logger?.LogError(ex, "During Vector Set quantization");
                    return true;
                }
            }
        }
    }
}