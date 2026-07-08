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
        private readonly Task[] quantizationTasks;

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

                    Span<byte> indexSpan = GC.AllocateArray<byte>(IndexSizeBytes, pinned: true);

                    while (reader.TryRead(out var state))
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
                                                    for (var i = 0; i < self.quantizationTasks.Length; i++)
                                                    {
                                                        _ = writer.TryWrite(new(state.Key, QuantizationStep.BackfillQuantizedVectors, i));
                                                    }
                                                }

                                                break;

                                            case QuantizationStep.BackfillQuantizedVectors:
                                                self.Service.BackfillQuantizedVectors(context, indexPtr, state.StepIndex, self.quantizationTasks.Length);

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
            }
        }
    }
}