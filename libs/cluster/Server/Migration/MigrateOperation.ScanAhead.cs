// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
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
            /// Migrate slot key batches, optionally gathering the next batch while the active batch is shipped.
            /// </summary>
            /// <param name="beginAddress">Start of this worker's disjoint scan range.</param>
            /// <param name="endAddress">End of this worker's scan range.</param>
            /// <param name="maxAddress">Shared snapshot boundary for liveness checks.</param>
            public async Task<bool> MigrateSlotsAsync(long beginAddress, long endAddress, long maxAddress)
            {
                var useScanAheadMigration = session.clusterProvider.serverOptions.UseScanAheadMigration;
                using var scanSession = session.GetLocalSession();
                using var scanCancellation = useScanAheadMigration
                    ? CancellationTokenSource.CreateLinkedTokenSource(session.migrationCancellationToken)
                    : null;
                var token = scanCancellation?.Token ?? session.migrationCancellationToken;
                var pending = useScanAheadMigration ? new ArgSliceVector(collectionBatchCapacity) : sketch.argSliceVector;
                Task<long> scanTask = null;

                try
                {
                    var cursor = ScanBatch(pending, beginAddress);
                    while (!pending.IsEmpty)
                    {
                        token.ThrowIfCancellationRequested();
                        var nextBatch = sketch.ReplaceBatch(pending);
                        sketch.SetStatus(SketchStatus.TRANSMITTING);
                        if (!await session.WaitForConfigPropagationAsync().ConfigureAwait(false))
                        {
                            session.logger?.LogError("Failed to protect migration batch");
                            return false;
                        }

                        var scanStart = cursor;
                        scanTask = useScanAheadMigration && cursor < endAddress
#if DEBUG
                            ? Task.Run(async () =>
                            {
                                var next = ScanBatch(nextBatch, scanStart);
                                await ExceptionInjectionHelper.ResetAndWaitAsync(ExceptionInjectionType.Migration_Slot_After_Scan_Ahead)
                                    .WaitAsync(token).ConfigureAwait(false);
                                return next;
                            }, token)
#else
                            ? Task.Run(() => ScanBatch(nextBatch, scanStart), token)
#endif
                            : null;

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

                        if (scanTask != null)
                            cursor = await scanTask.ConfigureAwait(false);
                        else
                        {
                            nextBatch.Clear();
                            if (cursor < endAddress)
                                cursor = ScanBatch(nextBatch, scanStart);
                        }
                        scanTask = null;
                        pending = nextBatch;
                    }

                    sketch.Clear();
                    return true;
                }
                finally
                {
                    if (scanCancellation != null)
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

                long ScanBatch(ArgSliceVector destination, long cursor)
                {
                    token.ThrowIfCancellationRequested();
                    var scanner = new StoreScan(this, destination, token);
                    scanSession.BasicGarnetApi.IterateStore(ref scanner, ref cursor, endAddress, maxAddress, includeTombstones: true);
                    token.ThrowIfCancellationRequested();
                    return cursor;
                }
            }
        }
    }
}