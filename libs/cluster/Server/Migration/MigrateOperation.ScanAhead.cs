// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

#if DEBUG
using Garnet.common;
#endif

namespace Garnet.cluster
{
    internal sealed partial class MigrateSession
    {
        internal sealed partial class MigrateOperation
        {
            /// <summary>
            /// Gather the next key batch while the active batch is transmitted and acknowledged.
            /// </summary>
            /// <param name="beginAddress">Start of this worker's disjoint scan range.</param>
            /// <param name="endAddress">End of this worker's scan range.</param>
            /// <param name="maxAddress">Shared snapshot boundary for liveness checks.</param>
            public async Task<bool> MigrateSlotsAsync(long beginAddress, long endAddress, long maxAddress)
            {
                Interlocked.Increment(ref activeWorkers);
                try
                {
                    ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) != 0, this);
                    using var scanSession = session.GetLocalSession();
                    using var scanCancellation = CancellationTokenSource.CreateLinkedTokenSource(session.cancellationToken);
                    var token = scanCancellation.Token;
                    var pending = new Sketch(keyCount: batchSize << 2, maxBatchSize: batchSize);
                    Task<long> scanTask = null;

                    try
                    {
                        var cursor = ScanBatch(pending, beginAddress);
                        while (!pending.argSliceVector.IsEmpty)
                        {
                            token.ThrowIfCancellationRequested();
                            pending.SetStatus(SketchStatus.TRANSMITTING);
                            var retired = Interlocked.Exchange(ref activeSketch, pending);
                            if (!await session.WaitForConfigPropagationAsync().ConfigureAwait(false))
                            {
                                session.logger?.LogError("Failed to protect migration batch");
                                return false;
                            }

                            // The publication barrier also retires readers of the previous sketch.
                            retired.Clear();
                            var scanStart = cursor;
                            scanTask = cursor < endAddress
#if DEBUG
                                ? Task.Run(async () =>
                                {
                                    var next = ScanBatch(retired, scanStart);
                                    await ExceptionInjectionHelper.ResetAndWaitAsync(ExceptionInjectionType.Migration_Slot_After_Scan_Ahead)
                                        .WaitAsync(token).ConfigureAwait(false);
                                    return next;
                                }, token)
#else
                                ? Task.Run(() => ScanBatch(retired, scanStart), token)
#endif
                                : Task.FromResult(cursor);

                            if (!await TransmitSlotsAsync().ConfigureAwait(false))
                                return false;

                            sketch.SetStatus(SketchStatus.DELETING);
                            if (!await session.WaitForConfigPropagationAsync().ConfigureAwait(false))
                            {
                                session.logger?.LogError("Failed to protect migration batch deletion");
                                return false;
                            }
                            DeleteKeys();
                            sketch.SetStatus(SketchStatus.MIGRATED);

                            cursor = await scanTask.ConfigureAwait(false);
                            scanTask = null;
                            pending = retired;
                        }

                        sketch.Clear();
                        return true;
                    }
                    finally
                    {
                        await scanCancellation.CancelAsync().ConfigureAwait(false);
                        if (scanTask != null)
                        {
                            try
                            {
                                await scanTask.ConfigureAwait(false);
                            }
                            catch (OperationCanceledException) when (token.IsCancellationRequested)
                            {
                                session.logger?.LogTrace("Migration scan-ahead cancelled");
                            }
                        }
                    }

                    long ScanBatch(Sketch destination, long cursor)
                    {
                        token.ThrowIfCancellationRequested();
                        var scanner = new StoreScan(this, destination, token);
                        scanSession.BasicGarnetApi.IterateStore(ref scanner, ref cursor, endAddress, maxAddress, includeTombstones: true);
                        token.ThrowIfCancellationRequested();
                        return cursor;
                    }
                }
                finally
                {
                    if (Interlocked.Decrement(ref activeWorkers) == 0 && Volatile.Read(ref disposed) != 0)
                        DisposeLocalSession();
                }
            }
        }
    }
}