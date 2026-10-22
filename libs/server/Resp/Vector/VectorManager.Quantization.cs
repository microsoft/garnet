// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        private enum QuantizationOrImportStep
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

            /// <summary>
            /// Import has finished, run finalized steps.
            /// </summary>
            FinalizeImport,
        }

        /// <summary>
        /// One attempt to finalize an import, registered in <see cref="activeImportFinalizations"/> under its key for as
        /// long as it runs. Counts the partitions still to finish, and is signaled once the attempt is over, so a replica
        /// replaying a later write to the set can wait for it without spinning.
        /// </summary>
        private sealed class ImportFinalization(byte[] key, int partitionCount)
        {
            private int remaining = partitionCount;

            // Finalizing an index takes far longer than a spin could usefully cover
            private readonly ManualResetEventSlim ended = new(initialState: false, spinCount: 0);

            /// <summary>
            /// The key this attempt is registered under, which also backs the key of each of its partition steps.
            /// </summary>
            internal byte[] Key { get; } = key;

            /// <summary>
            /// Blocks the calling thread until the attempt has been unregistered.
            /// </summary>
            internal void Wait() => ended.Wait();

            /// <summary>
            /// Counts one partition as finished, and returns true if it was the last.
            /// </summary>
            internal bool TickDown()
            => Interlocked.Decrement(ref remaining) == 0;

            /// <summary>
            /// Unregisters this attempt and releases its waiters. Only this attempt's own entry is removed, so a partition
            /// failing after the attempt has ended cannot unregister a later attempt for the same key. Idempotent.
            /// </summary>
            internal void End(VectorManager owner)
            {
                _ = owner.activeImportFinalizations.TryRemove(KeyValuePair.Create(Key, this));
                ended.Set();
            }
        }

        private readonly record struct QuantizationOrImportState(ReadOnlyMemory<byte> Key, QuantizationOrImportStep Step, int StepIndex, ImportFinalization Finalization);

        private readonly Channel<QuantizationOrImportState> quantizationOrImportChannel;

        /// <summary>
        /// Worker tasks that drain <see cref="quantizationOrImportChannel"/>. They run on the .NET thread pool, but a
        /// worker that cannot immediately acquire a vector set's lock yields its pool thread and retries
        /// asynchronously (see <see cref="StartQuantizationAndImportTasks"/>) instead of spin-waiting on it. A network VADD
        /// that recreates a disk-tiered index blocks on a pending disk read while holding that lock exclusively;
        /// if the workers spin-waited on the pool they would consume every pool thread and starve the disk-IO
        /// completion that releases the lock, deadlocking concurrent VADD. Cooperative yielding keeps pool threads
        /// available for that completion.
        /// </summary>
        private readonly Task[] quantizationAndImportTasks;

        /// <summary>
        /// Number of quantization worker tasks, also used as the backfill shard count.
        /// </summary>
        private readonly int quantizationAndImportTaskCount;

        private int quantizationRequestsProcessed;
        private int quantizationBackfillsProcessed;

        /// <summary>
        /// For testing purposes, the number of <see cref="QuantizationOrImportStep.BuildQuantizationTable"/> requests processed by <see cref="StartQuantizationAndImportTasks"/> tasks.
        /// </summary>
        internal int QuantizationRequestsProcessed => quantizationRequestsProcessed;

        /// <summary>
        /// For testing purposes, the number of <see cref="QuantizationOrImportStep.BackfillQuantizedVectors"/> requests processed by <see cref="StartQuantizationAndImportTasks"/> tasks.
        /// </summary>
        internal int QuantizationBackfillsProcessed => quantizationBackfillsProcessed;

        private void EnqueueQuantization(QuantizationOrImportState state)
        {
            if (!quantizationOrImportChannel.Writer.TryWrite(state))
            {
                logger?.LogCritical("VectorManager quantization channel rejected a request");
                Debug.Assert(false, "VectorManager quantization channel should never drop a request");
            }
        }

        /// <summary>
        /// Populate <see cref="quantizationAndImportTasks"/> with running tasks for handling any quantization requests.
        /// </summary>
        public void StartQuantizationAndImportTasks()
        {
            for (var i = 0; i < quantizationAndImportTasks.Length; i++)
            {
                quantizationAndImportTasks[i] = QuantizationTaskAsync(this, quantizationOrImportChannel.Reader, quantizationOrImportChannel.Writer);
            }

            static async Task QuantizationTaskAsync(VectorManager self, ChannelReader<QuantizationOrImportState> reader, ChannelWriter<QuantizationOrImportState> writer)
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
#if DEBUG
                        if (state.Step == QuantizationOrImportStep.FinalizeImport)
                            await ExceptionInjectionHelper.ResetAndWaitAsync(ExceptionInjectionType.VectorSet_Pause_Before_Import_Finalize_Step).ConfigureAwait(false);
#endif

                        // Cooperative acquisition. TryProcessQuantizationRequest returns false only when the vector
                        // set lock is currently held by another thread - typically a network VADD that is recreating
                        // a disk-tiered index and is blocked on a pending disk read while holding the lock exclusively.
                        // Instead of spin-waiting on a pool thread (which would consume the pool and starve the
                        // disk-IO completion that releases the lock, deadlocking concurrent VADD), yield the pool
                        // thread and retry so a completion can always be scheduled.
                        for (var attempt = 0; !TryProcessQuantizationOrImportRequest(self, session, writer, state, indexArray); attempt++)
                        {
                            if (attempt < 16)
                            {
                                await Task.Yield();
                            }
                            else
                            {
                                // If we're going to delay _but_ are being shutdown, bail on this request
                                if (reader.Completion.IsCompleted)
                                {
                                    break;
                                }

                                await Task.Delay(1).ConfigureAwait(false);
                            }
                        }
                    }
                }
            }

            // Processes a single request under a non-blocking lock acquisition. Returns true when the request was
            // handled (or is terminal, e.g. the index was dropped), and false when the set lock was contended and
            // the caller should yield its pool thread and retry. All ref struct / Span / native interop stays inside
            // this synchronous method so it never straddles an await.
            static bool TryProcessQuantizationOrImportRequest(VectorManager self, RespServerSession session, ChannelWriter<QuantizationOrImportState> writer, QuantizationOrImportState state, byte[] indexArray)
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

                            using (self.ReadVectorIndexCore(session.storageSession, keySpan, ref input, indexSpan, nonBlocking: true, out var res, out var contended, out var importPending))
                            {
                                // The lock is held by another thread (often a network VADD blocked on a pending disk
                                // read during index recreate). Report back so the caller yields and retries rather
                                // than spinning on a pool thread.
                                if (contended)
                                    return false;

                                if (res != GarnetStatus.OK && !importPending)
                                {
                                    // Index was dropped before quantization request could be processed, ignore request
                                    state.Finalization?.End(self);
                                    return true;
                                }

                                ReadIndex(indexSpan, out var context, out _, out _, out _, out _, out _, out _, out _, out var indexPtr);

                                switch (state.Step)
                                {
                                    case QuantizationOrImportStep.BuildQuantizationTable:
                                        if (self.Service.BuildQuantizationTable(context, indexPtr))
                                        {
                                            _ = Interlocked.Increment(ref self.quantizationRequestsProcessed);

                                            // Schedule backfill after quantization table is available
                                            for (var i = 0; i < self.quantizationAndImportTaskCount; i++)
                                            {
                                                self.EnqueueQuantization(new(state.Key, QuantizationOrImportStep.BackfillQuantizedVectors, i, null));
                                            }
                                        }

                                        break;

                                    case QuantizationOrImportStep.BackfillQuantizedVectors:
                                        if (!self.Service.BackfillQuantizedVectors(context, indexPtr, state.StepIndex, self.quantizationAndImportTasks.Length))
                                        {
                                            self.logger?.LogError("Quantization backfill {step}/{total} failed for context {context}", state.StepIndex, self.quantizationAndImportTasks.Length, context);

                                            // Post a retry back on the channel
                                            self.EnqueueQuantization(new(state.Key, QuantizationOrImportStep.BackfillQuantizedVectors, state.StepIndex, null));
                                            break;
                                        }

                                        _ = Interlocked.Increment(ref self.quantizationBackfillsProcessed);
                                        break;

                                    case QuantizationOrImportStep.FinalizeImport:
                                        try
                                        {
                                            if (!self.TryProcessImportPartitionFinalize(keySpan, context, indexPtr, state.StepIndex, self.quantizationAndImportTasks.Length))
                                            {
                                                SetFlags(keySpan, VectorSetFlags.ImportPending | VectorSetFlags.ImportFailed, ref ActiveThreadSession.stringBasicContext, SetImportStateArg);

                                                // On error, we want to unblock immediately
                                                state.Finalization.End(self);
                                            }
                                            else if (state.Finalization.TickDown())
                                            {
                                                // No need to check for failed here, because we only count down on successes
                                                SetFlags(keySpan, VectorSetFlags.ImportCompleted, ref ActiveThreadSession.stringBasicContext, SetImportStateArg);

                                                // On success, if we're the last finalizer, then 
                                                state.Finalization.End(self);
                                            }
                                        }
                                        catch (Exception e)
                                        {
                                            self.logger?.LogError(e, "During VectorManager background FinalizeImport");

                                            state.Finalization.End(self);
                                        }
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