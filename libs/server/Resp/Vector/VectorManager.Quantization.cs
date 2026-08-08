// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Channels;
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
        /// Dedicated background threads that drain <see cref="quantizationChannel"/>.
        /// These run off the .NET thread pool on purpose: processing a request spin-waits on a
        /// per-set <see cref="Garnet.common.ReadOptimizedLock"/> (via <see cref="ReadVectorIndex"/>),
        /// and a network thread creating an index can block on a pending disk read while holding that
        /// lock exclusively. Running the workers on pool threads let their spin-waits consume every
        /// pool thread, starving the disk-IO completion that would release the lock and deadlocking
        /// concurrent VADD on a disk-tiered set. Dedicated threads keep that spinning off the pool.
        /// </summary>
        private readonly Thread[] quantizationThreads;

        /// <summary>
        /// Number of quantization worker threads, also used as the backfill shard count.
        /// </summary>
        private readonly int quantizationTaskCount;

        private int quantizationRequestsProcessed;
        private int quantizationBackfillsProcessed;
        private int quantizationRanOnPoolThread;

        /// <summary>
        /// For testing purposes, the number of <see cref="QuantizationStep.BuildQuantizationTable"/> requests processed by <see cref="StartQuantizationTasks"/> tasks.
        /// </summary>
        internal int QuantizationRequestsProcessed => quantizationRequestsProcessed;

        /// <summary>
        /// For testing purposes, the number of <see cref="QuantizationStep.BackfillQuantizedVectors"/> requests processed by <see cref="StartQuantizationTasks"/> tasks.
        /// </summary>
        internal int QuantizationBackfillsProcessed => quantizationBackfillsProcessed;

        /// <summary>
        /// For testing purposes, whether any quantization work has run on a thread-pool thread. It must not:
        /// quantization work spin-waits on the per-set lock and a network thread can block on a disk read while
        /// holding that lock, so running quantization on the pool can starve the pool of the disk-IO completion
        /// that releases the lock and deadlock concurrent VADD. Workers run on dedicated <see cref="quantizationThreads"/>.
        /// </summary>
        internal bool QuantizationRanOnThreadPoolThread => Volatile.Read(ref quantizationRanOnPoolThread) != 0;

        /// <summary>
        /// Populate <see cref="quantizationThreads"/> with running dedicated threads for handling any quantization requests.
        /// </summary>
        public void StartQuantizationTasks()
        {
            for (var i = 0; i < quantizationThreads.Length; i++)
            {
                var thread = new Thread(() => QuantizationWorkerLoop(this, quantizationChannel.Reader, quantizationChannel.Writer))
                {
                    IsBackground = true,
                    Name = $"VectorQuantization-{i}",
                };
                quantizationThreads[i] = thread;
                thread.Start();
            }

            static void QuantizationWorkerLoop(VectorManager self, ChannelReader<QuantizationState> reader, ChannelWriter<QuantizationState> writer)
            {
                while (WaitToRead(reader))
                {
                    using var session = (RespServerSession)self.getTempSession();
                    if (session.activeDbId != self.dbId && !session.TrySwitchActiveDatabaseSession(self.dbId))
                    {
                        throw new GarnetException($"Could not switch VectorManager cleanup session to {self.dbId}, initialization failed");
                    }

                    Span<byte> indexSpan = GC.AllocateArray<byte>(IndexSizeBytes, pinned: true);

                    while (reader.TryRead(out var state))
                    {
                        if (Thread.CurrentThread.IsThreadPoolThread)
                            Volatile.Write(ref self.quantizationRanOnPoolThread, 1);

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

                                    using (self.ReadVectorIndex(session.storageSession, keySpan, ref input, indexSpan, out var res))
                                    {
                                        if (res != GarnetStatus.OK)
                                        {
                                            // Index was dropped before quantization request could be processed, ignore request
                                            continue;
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
                        }
                        catch (Exception ex)
                        {
                            self.logger?.LogError(ex, "During Vector Set quantization");
                        }
                    }
                }

                // Block this dedicated thread until an item is available or the channel completes.
                // WaitToReadAsync returns an IValueTaskSource-backed ValueTask that throws if GetResult is
                // called before completion, so only block on it directly when it is already complete;
                // otherwise materialize a Task and block on that.
                static bool WaitToRead(ChannelReader<QuantizationState> reader)
                {
                    var pending = reader.WaitToReadAsync();
                    return pending.IsCompletedSuccessfully
                        ? AsyncUtils.BlockingWait(pending)
                        : AsyncUtils.BlockingWait(pending.AsTask());
                }
            }
        }
    }
}