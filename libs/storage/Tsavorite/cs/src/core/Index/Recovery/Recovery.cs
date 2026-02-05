// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    internal enum ReadStatus { Pending, Done, Error };
    internal enum FlushStatus { Pending, Done, Error };

    internal sealed class RecoveryStatus
    {
        /// <summary>Main log recovery device, obtained from CheckpointManager.</summary>
        public IDevice recoveryDevice;
        /// <summary>The first page to recover; this is the page index of the snapshotStartAddress.</summary>
        public long recoveryDevicePageOffset;
        /// <summary>Object log recovery device, obtained from CheckpointManager.</summary>
        public IDevice objectLogRecoveryDevice;

        /// <summary>Circular status buffer of 'capacity' size; the indexing wraps per hlog.GetPageIndexForPage().</summary>
        public ReadStatus[] readStatus;
        /// <summary>Circular status buffer of 'capacity' size; the indexing wraps per hlog.GetPageIndexForPage().</summary>
        public FlushStatus[] flushStatus;

        /// <summary>Signals completion of an in-progress page read.</summary>
        private readonly SemaphoreSlim readSemaphore = new(0);
        /// <summary>Signals completion of an in-progress page flush.</summary>
        private readonly SemaphoreSlim flushSemaphore = new(0);

        public RecoveryStatus(int bufferSize)
        {
            readStatus = new ReadStatus[bufferSize];
            flushStatus = new FlushStatus[bufferSize];
            for (int i = 0; i < bufferSize; i++)
            {
                flushStatus[i] = FlushStatus.Done;
                readStatus[i] = ReadStatus.Pending;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SignalRead(int pageIndex)
        {
            readStatus[pageIndex] = ReadStatus.Done;
            _ = readSemaphore.Release();
        }

        internal void SignalReadError(int pageIndex)
        {
            readStatus[pageIndex] = ReadStatus.Error;
            _ = readSemaphore.Release();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WaitRead(int pageIndex)
        {
            while (readStatus[pageIndex] == ReadStatus.Pending)
                readSemaphore.Wait();
            if (readStatus[pageIndex] == ReadStatus.Error)
                throw new TsavoriteException($"Error reading page {pageIndex} from device");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal async ValueTask WaitReadAsync(int pageIndex, CancellationToken cancellationToken)
        {
            while (readStatus[pageIndex] == ReadStatus.Pending)
                await readSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            if (readStatus[pageIndex] == ReadStatus.Error)
                throw new TsavoriteException($"Error reading page {pageIndex} from device");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SignalFlushed(int pageIndex)
        {
            flushStatus[pageIndex] = FlushStatus.Done;
            _ = flushSemaphore.Release();
        }

        internal void SignalFlushedError(int pageIndex)
        {
            flushStatus[pageIndex] = FlushStatus.Error;
            _ = flushSemaphore.Release();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WaitFlush(int pageIndex)
        {
            while (flushStatus[pageIndex] == FlushStatus.Pending)
                flushSemaphore.Wait();
            if (flushStatus[pageIndex] == FlushStatus.Error)
                throw new TsavoriteException($"Error flushing page {pageIndex} to device");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal async ValueTask WaitFlushAsync(int pageIndex, CancellationToken cancellationToken)
        {
            while (flushStatus[pageIndex] == FlushStatus.Pending)
                await flushSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            if (flushStatus[pageIndex] == FlushStatus.Error)
                throw new TsavoriteException($"Error flushing page {pageIndex} to device");
        }

        internal void Dispose()
        {
            recoveryDevice.Dispose();
            objectLogRecoveryDevice.Dispose();
            readSemaphore.Dispose();
            flushSemaphore.Dispose();
        }
    }

    internal struct RecoveryOptions
    {
        internal long headAddress;
        internal long fuzzyRegionStartAddress;
        internal bool undoNextVersion;

        internal RecoveryOptions(long headAddress, long fuzzyRegionStartAddress, bool undoNextVersion)
        {
            this.headAddress = headAddress;
            this.fuzzyRegionStartAddress = fuzzyRegionStartAddress;
            this.undoNextVersion = undoNextVersion;
        }
    }

    /// <summary>
    /// Log File info
    /// </summary>
    public struct LogFileInfo
    {
        /// <summary>Snapshot file end address (start address is always 0).</summary>
        public long snapshotFileEndAddress;

        /// <summary>Hybrid log file start address</summary>
        public long hybridLogFileStartAddress;

        /// <summary>Hybrid log file end address</summary>
        public long hybridLogFileEndAddress;

        /// <summary>Delta log tail address</summary>
        public long deltaLogTailAddress;

        /// <summary>True if this snapshot had object log records</summary>
        public bool hasSnapshotObjects;

        /// <summary>Address of <see cref="HybridLogRecoveryInfo.beginAddressObjectLogSegment"/>; the start of the lowest object log segment
        /// in use by the hybrid log at snapshot PREPARE time</summary>
        public long hybridLogObjectFileStartAddress;
        /// <summary>The objectLogTail taken at the start of WAIT_FLUSH, corresponding to the hlog's FlushedUntilAddress at that point</summary>
        public long hybridLogObjectFileEndAddress;
        /// <summary>The snapshotEndObjectLogTail taken at PERSISTENCE_CALLBACK, which corresponds to the object log position for the final TailAddress
        /// written by the checkpoint. (Start address is always 0.)</summary>
        public long snapshotObjectFileEndAddress;
    }

    public partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        private const long NoPageFreed = -1;

        /// <summary>
        /// GetLatestCheckpointTokens
        /// </summary>
        /// <param name="hlogToken"></param>
        /// <param name="indexToken"></param>
        /// <param name="storeVersion"></param>
        public void GetLatestCheckpointTokens(out Guid hlogToken, out Guid indexToken, out long storeVersion)
        {
            GetClosestHybridLogCheckpointInfo(-1, out hlogToken, out var recoveredHlcInfo, out var _);
            try
            {
                if (hlogToken == default)
                {
                    indexToken = default;
                    storeVersion = -1;
                    return;
                }
                using var current = new HybridLogCheckpointInfo();

                // Make sure we consider delta log in order to compute latest checkpoint version
                current.Recover(hlogToken, checkpointManager, hlogBase.LogPageSizeBits, out var _, true);
                storeVersion = current.info.nextVersion;

                GetClosestIndexCheckpointInfo(ref recoveredHlcInfo, out indexToken, out var recoveredICInfo);
                if (recoveredICInfo.IsDefault)
                    logger?.LogInformation("No index checkpoint found, returning default index token in GetLatestCheckpointTokens");
            }
            finally
            {
                recoveredHlcInfo.Dispose();
            }
        }

        /// <summary>
        /// Get HLog latest version
        /// </summary>
        public long GetLatestCheckpointVersion()
        {
            GetClosestHybridLogCheckpointInfo(-1, out var hlogToken, out var hlcInfo, out var _);
            hlcInfo.Dispose();
            if (hlogToken == default)
                return -1;
            using var current = new HybridLogCheckpointInfo();

            // Make sure we consider delta log in order to compute latest checkpoint version
            current.Recover(hlogToken, checkpointManager, hlogBase.LogPageSizeBits, out var _, true);
            return current.info.nextVersion;
        }

        /// <summary>
        /// Get size of snapshot files for token
        /// </summary>
        public LogFileInfo GetLogFileSize(Guid token, long version = -1)
        {
            using var current = new HybridLogCheckpointInfo();
            // We find the latest checkpoint metadata for the given token, including scanning the delta log for the latest metadata
            current.Recover(token, checkpointManager, hlogBase.LogPageSizeBits, out var _, true, version);
            var hasSnapshotObjects = current.info.snapshotEndObjectLogTail.HasData;
            var snapshotDeviceOffset = hlogBase.GetLogicalAddressOfStartOfPage(hlogBase.GetPage(current.info.snapshotStartFlushedLogicalAddress));
            return new LogFileInfo
            {
                // Hybrid (main log file) info:
                //   - The main log address range is from:
                //     - BeginAddress at PREPARE to...
                //     - FlushedUntilAddress at PERSISTENCE_CALLBACK.
                //   - The snapshot address range starts at 0 in the snapshot files and includes all main-log data until the final TailAddress. In detail, it is from:
                //     - 0, but the start offset is FlushedUntilAddress taken at the start of WAIT_FLUSH (which is used to calculate this.snapshotDeviceOffset) to...
                //     - TailAddress taken at the start of WAIT_FLUSH minus the start offset. This TailAddress is the maximum logical address that will be written to the snapshot.
                // The overlap between the FlushedUntilAddress for the main log being recorded after the flush completes and the FlushedUntilAddress for the snapshot 
                // being recorded before the flush starts ensures there is no gap.
                hybridLogFileStartAddress = hlogBase.GetLogicalAddressOfStartOfPage(hlogBase.GetPage(current.info.beginAddress)),
                hybridLogFileEndAddress = current.info.flushedLogicalAddress,
                snapshotFileEndAddress = current.info.snapshotFinalLogicalAddress - snapshotDeviceOffset,

                // Object log file info:
                //   - The object log address range is from:
                //     - The start of the in-use object segment corresponding to main-log BeginAddress at PREPARE (matching this.hybridLogFileStartAddress) to...
                //     - The hLogEndObjectLogTail taken at PERSISTENCE_CALLBACK (matching this.hybridLogFileEndAddress).
                //   - The snapshot address range starts at 0 in the snapshot file and includes all object-log data until the final TailAddress. In detail, it is from:
                //     - The objectLogTail taken at the start of WAIT_FLUSH as info.snapshotStartObjectLogTail, corresponding to the main log's FlushedUntilAddress at
                //       that point (which is used to calculate this.snapshotDeviceOffset) to...
                //     - The snapshotEndObjectLogTail which is taken at PERSISTENCE_CALLBACK, which corresponds to the main-log TailAddress taken at WAIT_FLUSH.
                //       The snapshotEndObjectLogTail grows during the Flush, so is not final until PERSISTENCE_CALLBACK; but it will only be written for records
                //       up to the TailAddress at the start of WAIT_FLUSH.
                // Note that there are no object-log segments for the mutable region of the hybrid log; they are not written until ReadOnlyAddress growth triggers
                // a main-log Flush. However the snapshot does cause object-log segments for the mutable range to be written.
                hasSnapshotObjects = hasSnapshotObjects,
                hybridLogObjectFileStartAddress = hasSnapshotObjects ? (long)current.info.beginAddressObjectLogSegment << current.info.hlogEndObjectLogTail.SegmentSizeBits : 0,
                hybridLogObjectFileEndAddress = hasSnapshotObjects ? (long)current.info.snapshotStartObjectLogTail.CurrentAddress : 0,
                snapshotObjectFileEndAddress = hasSnapshotObjects ? (long)current.info.snapshotEndObjectLogTail.CurrentAddress : 0,

                // Delta log info
                deltaLogTailAddress = current.info.deltaTailAddress
            };
        }

        /// <summary>
        /// Get size of index file for token
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public long GetIndexFileSize(Guid token)
        {
            var recoveredICInfo = new IndexCheckpointInfo();
            recoveredICInfo.Recover(token, checkpointManager);
            return (long)(recoveredICInfo.info.num_ht_bytes + recoveredICInfo.info.num_ofb_bytes);
        }

        private void GetClosestHybridLogCheckpointInfo(
            long requestedVersion,
            out Guid closestToken,
            out HybridLogCheckpointInfo closest,
            out byte[] cookie)
        {
            HybridLogCheckpointInfo current;
            var closestVersion = long.MaxValue;
            closest = default;
            closestToken = default;
            cookie = default;

            // Traverse through all current tokens to find either the largest version or the version that's closest to
            // but smaller than the requested version. Need to iterate through all unpruned versions because file system
            // is not guaranteed to return tokens in order of freshness.
            foreach (var hybridLogToken in checkpointManager.GetLogCheckpointTokens())
            {
                try
                {
                    current = new HybridLogCheckpointInfo();
                    current.Recover(hybridLogToken, checkpointManager, hlogBase.LogPageSizeBits, out var currCookie, false);
                    var distanceToTarget = (requestedVersion == -1 ? long.MaxValue : requestedVersion) - current.info.version;
                    // This is larger than intended version, cannot recover to this.
                    if (distanceToTarget < 0) continue;
                    // We have found the exact version to recover to --- the above conditional establishes that the
                    // checkpointed version is <= requested version, and if next version is larger than requestedVersion,
                    // there cannot be any closer version. 
                    if (current.info.nextVersion > requestedVersion)
                    {
                        closest = current;
                        closestToken = hybridLogToken;
                        cookie = currCookie;
                        break;
                    }

                    // Otherwise, write it down and wait to see if there's a closer one;
                    if (distanceToTarget < closestVersion)
                    {
                        closestVersion = distanceToTarget;
                        closest.Dispose();
                        closest = current;
                        closestToken = hybridLogToken;
                        cookie = currCookie;
                    }
                    else
                    {
                        current.Dispose();
                    }
                }
                catch
                {
                    continue;
                }

                logger?.LogInformation("HybridLog Checkpoint: {hybridLogToken}", hybridLogToken);
            }
        }

        private void GetClosestIndexCheckpointInfo(ref HybridLogCheckpointInfo recoveredHlcInfo, out Guid closestToken, out IndexCheckpointInfo recoveredICInfo)
        {
            closestToken = default;
            recoveredICInfo = default;
            foreach (var indexToken in checkpointManager.GetIndexCheckpointTokens())
            {
                try
                {
                    // Recovery appropriate context information
                    recoveredICInfo = new IndexCheckpointInfo();
                    recoveredICInfo.Recover(indexToken, checkpointManager);
                }
                catch
                {
                    continue;
                }

                if (!IsCompatible(recoveredICInfo.info, recoveredHlcInfo.info))
                {
                    recoveredICInfo = default;
                    continue;
                }

                logger?.LogInformation("Index Checkpoint: {indexToken}", indexToken);
                recoveredICInfo.info.DebugPrint(logger);
                closestToken = indexToken;
                break;
            }
        }

        private void FindRecoveryInfo(long requestedVersion, out HybridLogCheckpointInfo recoveredHlcInfo, out IndexCheckpointInfo recoveredICInfo)
        {
            logger?.LogInformation("********* Primary Recovery Information ********");

            GetClosestHybridLogCheckpointInfo(requestedVersion, out var closestToken, out recoveredHlcInfo, out recoveredCommitCookie);
            if (recoveredHlcInfo.IsDefault)
                throw new TsavoriteNoHybridLogException("Unable to find valid HybridLog token");

            if (recoveredHlcInfo.deltaLog != null)
            {
                recoveredHlcInfo.Dispose();
                // need to actually scan delta log now
                recoveredHlcInfo.Recover(closestToken, checkpointManager, hlogBase.LogPageSizeBits, out _, true);
            }
            recoveredHlcInfo.info.DebugPrint(logger);

            GetClosestIndexCheckpointInfo(ref recoveredHlcInfo, out _, out recoveredICInfo);
            if (recoveredICInfo.IsDefault)
                logger?.LogInformation("No index checkpoint found, recovering from beginning of log");
        }

        private static bool IsCompatible(in IndexRecoveryInfo indexInfo, in HybridLogRecoveryInfo recoveryInfo)
            => indexInfo.finalLogicalAddress <= recoveryInfo.finalLogicalAddress;

        private void GetRecoveryInfo(Guid indexToken, Guid hybridLogToken, out HybridLogCheckpointInfo recoveredHLCInfo, out IndexCheckpointInfo recoveredICInfo)
        {
            logger?.LogInformation("********* Primary Recovery Information ********");
            logger?.LogInformation("Index Checkpoint: {indexToken}", indexToken);
            logger?.LogInformation("HybridLog Checkpoint: {hybridLogToken}", hybridLogToken);


            // Recovery appropriate context information
            recoveredHLCInfo = new HybridLogCheckpointInfo();
            recoveredHLCInfo.Recover(hybridLogToken, checkpointManager, hlogBase.LogPageSizeBits, out recoveredCommitCookie, true);
            recoveredHLCInfo.info.DebugPrint(logger);
            try
            {
                recoveredICInfo = new IndexCheckpointInfo();
                if (indexToken != default)
                {
                    recoveredICInfo.Recover(indexToken, checkpointManager);
                    recoveredICInfo.info.DebugPrint(logger);
                }
            }
            catch
            {
                recoveredICInfo = default;
            }

            // Verify that the index and log checkpoints are compatible for recovery
            if (recoveredICInfo.IsDefault)
                logger?.LogInformation("Invalid index checkpoint token, recovering from beginning of log");
            else if (!IsCompatible(recoveredICInfo.info, recoveredHLCInfo.info))
                throw new TsavoriteException("Cannot recover from (" + indexToken.ToString() + "," + hybridLogToken.ToString() + ") checkpoint pair!\n");
        }

        /// <inheritdoc />
        public void Reset()
        {
            // Reset the hash index
            Array.Clear(state[resizeInfo.version].tableRaw, 0, state[resizeInfo.version].tableRaw.Length);
            overflowBucketsAllocator.Dispose();
            overflowBucketsAllocator = new MallocFixedPageSize<HashBucket>(logger);

            // Reset the hybrid log
            hlogBase.Reset();

            // Reset system state
            stateMachineDriver.SetSystemState(SystemState.Make(Phase.REST, 1));
            lastVersion = 0;
        }

        /// <summary>Synchronous recovery driver</summary>
        private long InternalRecover(Guid indexToken, Guid hybridLogToken, int numPagesToPreload, bool undoNextVersion, long recoverTo)
        {
            GetRecoveryInfo(indexToken, hybridLogToken, out var recoveredHLCInfo, out var recoveredICInfo);
            return recoverTo != -1 && recoveredHLCInfo.deltaLog == null
                ? throw new TsavoriteException("Recovering to a specific version within a token is only supported for incremental snapshots")
                : InternalRecover(recoveredICInfo, recoveredHLCInfo, numPagesToPreload, undoNextVersion, recoverTo);
        }

        /// <summary>Synchronous recovery driver</summary>
        private long InternalRecover(IndexCheckpointInfo recoveredICInfo, HybridLogCheckpointInfo recoveredHLCInfo, int numPagesToPreload, bool undoNextVersion, long recoverTo)
        {
            hlogBase.VerifyRecoveryInfo(recoveredHLCInfo, false);

            if (hlogBase.GetTailAddress() > hlogBase.GetFirstValidLogicalAddressOnPage(0))
            {
                logger?.LogInformation("Recovery called on non-empty log - resetting to empty state first. Make sure store is quiesced before calling Recover on a running store.");
                Reset();
            }

            if (!GetInitialRecoveryAddress(recoveredICInfo, recoveredHLCInfo, out long recoverFromAddress))
                RecoverFuzzyIndex(recoveredICInfo);

            if (!SetRecoveryPageRanges(recoveredHLCInfo, numPagesToPreload, recoverFromAddress, out long tailAddress, out long headAddress, out long scanFromAddress))
                return -1;
            RecoveryOptions options = new(headAddress, fuzzyRegionStartAddress: recoveredHLCInfo.info.startLogicalAddress, undoNextVersion);

            // Make index consistent for version v
            long readOnlyAddress, lastFreedPage;
            if (recoveredHLCInfo.info.useSnapshotFile == 0)
            {
                lastFreedPage = RecoverHybridLog(scanFromAddress, recoverFromAddress, untilAddress: recoveredHLCInfo.info.finalLogicalAddress,
                        recoveredHLCInfo.info.nextVersion, CheckpointType.FoldOver, options);

                readOnlyAddress = tailAddress;
            }
            else
            {
                if (recoveredHLCInfo.info.flushedLogicalAddress < headAddress)
                    headAddress = recoveredHLCInfo.info.flushedLogicalAddress;

                // First recover from index starting point (fromAddress) to snapshot starting point (flushedLogicalAddress taken at PERSISTENCE_CALLBACK, so it includes
                // any flushes to the hybrid log files due to OnPagesMarkedReadOnly while we were flushing to the snapshot files).
                lastFreedPage = RecoverHybridLog(scanFromAddress, recoverFromAddress, untilAddress: recoveredHLCInfo.info.flushedLogicalAddress,
                        recoveredHLCInfo.info.nextVersion, CheckpointType.Snapshot, options);

                // Then recover snapshot into mutable region. Note that the ObjectAllocator will not write object log records for the mutable region;
                // that only happens during flushes due to OnPagesMarkedReadOnly.
                var snapshotLastFreedPage = RecoverHybridLogFromSnapshotFile(scanFromAddress: recoveredHLCInfo.info.flushedLogicalAddress,
                        recoverFromAddress, untilAddress: recoveredHLCInfo.info.finalLogicalAddress,
                        snapshotStartAddress: recoveredHLCInfo.info.snapshotStartFlushedLogicalAddress, snapshotEndAddress: recoveredHLCInfo.info.snapshotFinalLogicalAddress,
                        recoveredHLCInfo.info.nextVersion, recoveredHLCInfo.info.guid, options, recoveredHLCInfo.deltaLog, recoverTo);

                if (snapshotLastFreedPage != NoPageFreed)
                    lastFreedPage = snapshotLastFreedPage;

                readOnlyAddress = recoveredHLCInfo.info.flushedLogicalAddress;
            }

            DoPostRecovery(recoveredICInfo, recoveredHLCInfo, tailAddress, ref headAddress, ref readOnlyAddress, lastFreedPage);
            return recoveredHLCInfo.info.version;
        }

        /// <summary>Aynchronous recovery driver</summary>
        private ValueTask<long> InternalRecoverAsync(Guid indexToken, Guid hybridLogToken, int numPagesToPreload, bool undoNextVersion, long recoverTo, CancellationToken cancellationToken)
        {
            GetRecoveryInfo(indexToken, hybridLogToken, out var recoveredHLCInfo, out var recoveredICInfo);
            return InternalRecoverAsync(recoveredICInfo, recoveredHLCInfo, numPagesToPreload, undoNextVersion, recoverTo, cancellationToken);
        }

        /// <summary>Asynchronous recovery driver</summary>
        private async ValueTask<long> InternalRecoverAsync(IndexCheckpointInfo recoveredICInfo, HybridLogCheckpointInfo recoveredHLCInfo, int numPagesToPreload, bool undoNextVersion, long recoverTo, CancellationToken cancellationToken)
        {
            hlogBase.VerifyRecoveryInfo(recoveredHLCInfo, false);

            if (hlogBase.GetTailAddress() > hlogBase.GetFirstValidLogicalAddressOnPage(0))
            {
                logger?.LogInformation("Recovery called on non-empty log - resetting to empty state first. Make sure store is quiesced before calling Recover on a running store.");
                Reset();
            }

            if (!GetInitialRecoveryAddress(recoveredICInfo, recoveredHLCInfo, out long recoverFromAddress))
                await RecoverFuzzyIndexAsync(recoveredICInfo, cancellationToken).ConfigureAwait(false);

            if (!SetRecoveryPageRanges(recoveredHLCInfo, numPagesToPreload, recoverFromAddress, out long tailAddress, out long headAddress, out long scanFromAddress))
                return -1;
            RecoveryOptions options = new(headAddress, fuzzyRegionStartAddress: recoveredHLCInfo.info.startLogicalAddress, undoNextVersion);

            // Make index consistent for version v
            long readOnlyAddress, lastFreedPage;
            if (recoveredHLCInfo.info.useSnapshotFile == 0)
            {
                lastFreedPage = await RecoverHybridLogAsync(scanFromAddress, recoverFromAddress, untilAddress: recoveredHLCInfo.info.finalLogicalAddress,
                        recoveredHLCInfo.info.nextVersion, CheckpointType.FoldOver, options, cancellationToken).ConfigureAwait(false);

                readOnlyAddress = tailAddress;
            }
            else
            {
                if (recoveredHLCInfo.info.flushedLogicalAddress < headAddress)
                    headAddress = recoveredHLCInfo.info.flushedLogicalAddress;

                // First recover from index starting point (fromAddress) to snapshot starting point (flushedLogicalAddress taken at PERSISTENCE_CALLBACK, so it includes
                // any flushes to the hybrid log files due to OnPagesMarkedReadOnly while we were flushing to the snapshot files).
                lastFreedPage = await RecoverHybridLogAsync(scanFromAddress, recoverFromAddress, untilAddress: recoveredHLCInfo.info.flushedLogicalAddress,
                        recoveredHLCInfo.info.nextVersion, CheckpointType.Snapshot,
                        new RecoveryOptions(headAddress, fuzzyRegionStartAddress: recoveredHLCInfo.info.startLogicalAddress, undoNextVersion), cancellationToken).ConfigureAwait(false);

                // Then recover snapshot into mutable region. Note that the ObjectAllocator will not write object log records for the mutable region;
                // that only happens during flushes due to OnPagesMarkedReadOnly.
                var snapshotLastFreedPage = await RecoverHybridLogFromSnapshotFileAsync(scanFromAddress: recoveredHLCInfo.info.flushedLogicalAddress,
                        recoverFromAddress, untilAddress: recoveredHLCInfo.info.finalLogicalAddress,
                        snapshotStartAddress: recoveredHLCInfo.info.snapshotStartFlushedLogicalAddress, snapshotEndAddress: recoveredHLCInfo.info.snapshotFinalLogicalAddress,
                        recoveredHLCInfo.info.nextVersion, recoveredHLCInfo.info.guid, options, recoveredHLCInfo.deltaLog, recoverTo, cancellationToken).ConfigureAwait(false);

                if (snapshotLastFreedPage != NoPageFreed)
                    lastFreedPage = snapshotLastFreedPage;

                readOnlyAddress = recoveredHLCInfo.info.flushedLogicalAddress;
            }

            DoPostRecovery(recoveredICInfo, recoveredHLCInfo, tailAddress, ref headAddress, ref readOnlyAddress, lastFreedPage);
            return recoveredHLCInfo.info.version;
        }

        private void DoPostRecovery(IndexCheckpointInfo recoveredICInfo, HybridLogCheckpointInfo recoveredHLCInfo, long tailAddress, ref long headAddress, ref long readOnlyAddress, long lastFreedPage)
        {
            // Adjust head and read-only address post-recovery
            var tailPage = hlogBase.GetPage(tailAddress);
            if (tailAddress > hlogBase.GetFirstValidLogicalAddressOnPage(tailPage))
                tailPage++;
            var headPage = hlogBase.GetPage(tailPage - hlogBase.AllocatedPageCount);
            var _head = hlogBase.GetFirstValidLogicalAddressOnPage(headPage);

            // If additional pages have been freed to accommodate memory constraints, adjust head address accordingly
            if (lastFreedPage != NoPageFreed)
            {
                var nextAddress = hlogBase.GetFirstValidLogicalAddressOnPage(lastFreedPage + 1);
                if (_head < nextAddress)
                    _head = nextAddress;
            }

            if (_head > headAddress)
                headAddress = _head;
            if (readOnlyAddress < headAddress)
                readOnlyAddress = headAddress;

            hlogBase.RecoveryReset(tailAddress, headAddress, recoveredHLCInfo.info.beginAddress, readOnlyAddress);
            hlogBase.SetObjectLogTail(recoveredHLCInfo.info.hlogEndObjectLogTail);
            checkpointManager.OnRecovery(recoveredICInfo.info.token, recoveredHLCInfo.info.guid);
            recoveredHLCInfo.Dispose();
        }

        /// <summary>
        /// Set store version directly. Useful if manually recovering by re-inserting data.
        /// Warning: use only when the system is not taking a checkpoint.
        /// </summary>
        /// <param name="version">Version to set the store to</param>
        public void SetVersion(long version)
        {
            stateMachineDriver.SetSystemState(SystemState.Make(Phase.REST, version));
        }

        /// <summary>
        /// Compute recovery address and determine where to recover from
        /// </summary>
        /// <param name="recoveredICInfo">IndexCheckpointInfo</param>
        /// <param name="recoveredHLCInfo">HybridLogCheckpointInfo</param>
        /// <param name="recoverFromAddress">Address from which to perform recovery (undo v+1 records and append to tag-chain tail)</param>
        /// <returns>Whether we are recovering to the initial page</returns>
        private bool GetInitialRecoveryAddress(IndexCheckpointInfo recoveredICInfo, HybridLogCheckpointInfo recoveredHLCInfo, out long recoverFromAddress)
        {
            // Set new system state after recovery
            stateMachineDriver.SetSystemState(SystemState.Make(Phase.REST, recoveredHLCInfo.info.version + 1));

            if (!recoveredICInfo.IsDefault && recoveryCountdown != null)
            {
                Debug.WriteLine("Ignoring index checkpoint as we have already recovered index previously");
                recoveredICInfo = default;
            }

            // Initialize to recover from beginning of log
            recoverFromAddress = recoveredHLCInfo.info.beginAddress;

            if (recoveredICInfo.IsDefault)
            {
                // No index checkpoint - recover from beginning of log unless we recovered previously until some hlog address
                if (hlogBase.FlushedUntilAddress > recoverFromAddress)
                    recoverFromAddress = hlogBase.FlushedUntilAddress;

                // Start recovery at least from beginning of fuzzy log region. Needed if we are recovering to the same checkpoint
                // a second time, with undo set to true during the second time.
                if (recoveredHLCInfo.info.startLogicalAddress < recoverFromAddress)
                    recoverFromAddress = recoveredHLCInfo.info.startLogicalAddress;
            }
            else
            {
                if (recoveredICInfo.info.startLogicalAddress > recoverFromAddress)
                {
                    // Index checkpoint was given - recover to that
                    recoverFromAddress = recoveredICInfo.info.startLogicalAddress;
                    return false;
                }
            }

            return true;
        }

        private bool SetRecoveryPageRanges(HybridLogCheckpointInfo recoveredHLCInfo, int numPagesToPreload, long recoverFromAddress, out long tailAddress, out long headAddress, out long scanFromAddress)
        {
            if ((recoveredHLCInfo.info.useSnapshotFile == 0) && (recoveredHLCInfo.info.finalLogicalAddress <= hlogBase.GetTailAddress()))
            {
                tailAddress = headAddress = scanFromAddress = default;
                return false;
            }

            tailAddress = recoveredHLCInfo.info.finalLogicalAddress;
            headAddress = recoveredHLCInfo.info.headAddress;
            if (numPagesToPreload != -1)
            {
                var head = hlogBase.GetFirstValidLogicalAddressOnPage(hlogBase.GetPage(tailAddress) - numPagesToPreload);
                if (head > headAddress)
                    headAddress = head;
            }

            scanFromAddress = headAddress;
            if (recoverFromAddress < scanFromAddress)
                scanFromAddress = recoverFromAddress;

            // Adjust head address if we need to anyway preload
            if (scanFromAddress < headAddress)
            {
                headAddress = scanFromAddress;
                if (headAddress < recoveredHLCInfo.info.headAddress)
                    headAddress = recoveredHLCInfo.info.headAddress;
            }

            if (hlogBase.FlushedUntilAddress > scanFromAddress)
                scanFromAddress = hlogBase.FlushedUntilAddress;
            return true;
        }

        private void ReadPagesWithMemoryConstraint(long endAddress, RecoveryStatus recoveryStatus, long page, long endPage, int numPagesToRead)
        {
            // Before reading in additional pages, trim memory if needed to make room for the inline space (we can't know the heap size yet)
            _ = TrimLogMemorySize(recoveryStatus, tailPage: page, numPagesToRead);

            // Set all page read statuses to Pending
            for (var p = page; p < endPage; p++)
                recoveryStatus.readStatus[hlogBase.GetPageIndexForPage(p)] = ReadStatus.Pending;

            // Issue request to read pages as much as possible
            hlogBase.AsyncReadPagesForRecovery(page, numPagesToRead, endAddress, recoveryStatus, recoveryStatus.recoveryDevicePageOffset,
                recoveryStatus.recoveryDevice, recoveryStatus.objectLogRecoveryDevice);
        }

        /// <summary>
        /// Determine how many pages we need to evict, if any.
        /// </summary>
        /// <returns>True if <paramref name="evictPageCount"/> is nonzero, else false</returns>
        private bool GetEvictionPageRange(long tailPage, int numPagesToRead, CancellationToken cancellationToken, out long startPage, out int evictPageCount)
        {
            // Determine how many pages we must evict. TailPage is current page we're recovering; we must start trimming at the earliest possible page.
            startPage = Math.Max(0, tailPage - hlogBase.AllocatedPageCount + 1);

            // If no log size tracker, just ensure MaxPageCount is not exceeded.
            if (hlogBase.logSizeTracker is null)
            {
                var pagesAllowed = hlogBase.MaxAllocatedPageCount - hlogBase.AllocatedPageCount;
                if (numPagesToRead > pagesAllowed)
                {
                    evictPageCount = numPagesToRead - pagesAllowed;
                    return true;
                }
                evictPageCount = 0;
                return false;
            }

            // We have a log size tracker, so we'll stop evictPageCount when we are at the minimum (the caller will also test logSizeTracker.IsBeyondSizeLimit);
            evictPageCount = hlogBase.AllocatedPageCount - 2;
            return hlogBase.logSizeTracker.IsBeyondSizeLimit;
        }

        private long TrimLogMemorySize(RecoveryStatus recoveryStatus, long tailPage, int numPagesToRead = 0)
        {
            var lastFreedPage = NoPageFreed;
            if (GetEvictionPageRange(tailPage, numPagesToRead, cancellationToken: default, out long startPage, out int evictPageCount))
            {
                // Evict pages one at a time
                for (var ii = 0; ii < evictPageCount; ii++)
                {
                    if (hlogBase.logSizeTracker is not null && !hlogBase.logSizeTracker.IsBeyondSizeLimit)
                        break;
                    var page = startPage + ii;
                    var pageIndex = hlogBase.GetPageIndexForPage(page);
                    if (hlogBase.IsAllocated(pageIndex))
                    {
                        recoveryStatus.WaitFlush(pageIndex);
                        hlogBase.EvictPageForRecovery(page);
                        lastFreedPage = page;
                    }
                }
            }

            return lastFreedPage;
        }

        private async Task<long> TrimLogMemorySizeAsync(RecoveryStatus recoveryStatus, long tailPage, int numPagesToRead = 0, CancellationToken cancellationToken = default)
        {
            var lastFreedPage = NoPageFreed;
            if (GetEvictionPageRange(tailPage, numPagesToRead, cancellationToken: default, out long startPage, out int evictPageCount))
            {
                // Evict pages one at a time
                for (var ii = 0; ii < evictPageCount; ii++)
                {
                    if (hlogBase.logSizeTracker is not null && !hlogBase.logSizeTracker.IsBeyondSizeLimit)
                        break;
                    var page = startPage + ii;
                    var pageIndex = hlogBase.GetPageIndexForPage(page);
                    if (hlogBase.IsAllocated(pageIndex))
                    {
                        await recoveryStatus.WaitFlushAsync(pageIndex, cancellationToken);
                        hlogBase.EvictPageForRecovery(page);
                        lastFreedPage = page;
                    }
                }
            }

            return lastFreedPage;
        }

        private long ReadPagesForRecovery(long untilAddress, RecoveryStatus recoveryStatus, long endPage, int numPagesToReadPerIteration, long page)
        {
            var readEndPage = Math.Min(page + numPagesToReadPerIteration, endPage);
            if (page < readEndPage)
            {
                var numPagesToRead = (int)(readEndPage - page);

                // Ensure that page slots that will be read into, have been flushed from previous reads. Due to the use of a single read semaphore,
                // this must be done in batches of "all flushes' followed by "all reads" to ensure proper sequencing of reads when
                // usableCapacity != capacity (and thus the page-read index is not equal to the page-flush index).
                WaitUntilAllPagesHaveBeenFlushed(page, readEndPage, recoveryStatus);
                ReadPagesWithMemoryConstraint(untilAddress, recoveryStatus, page, readEndPage, numPagesToRead);
            }

            return readEndPage;
        }

        private async ValueTask<long> ReadPagesForRecoveryAsync(long untilAddress, RecoveryStatus recoveryStatus, long endPage, int numPagesToReadPerIteration, long page, CancellationToken cancellationToken)
        {
            var readEndPage = Math.Min(page + numPagesToReadPerIteration, endPage);
            if (page < readEndPage)
            {
                var numPagesToRead = (int)(readEndPage - page);

                // Ensure that page slots that will be read into, have been flushed from previous reads. Due to the use of a single read semaphore,
                // this must be done in batches of "all flushes' followed by "all reads" to ensure proper sequencing of reads when
                // usableCapacity != capacity (and thus the page-read index is not equal to the page-flush index).
                await WaitUntilAllPagesHaveBeenFlushedAsync(page, readEndPage, recoveryStatus, cancellationToken).ConfigureAwait(false);
                ReadPagesWithMemoryConstraint(untilAddress, recoveryStatus, page, readEndPage, numPagesToRead);
            }

            return readEndPage;
        }

        /// <summary>
        /// Synchronously recover the hybrid log from hybrid log files (not snapshot files). This also deserializes any objects or overflow and creates
        /// entries for them in the <see cref="ObjectIdMap"/>.
        /// </summary>
        /// <param name="scanFromAddress">The address to start scanning from; either beginAddress if no recovery has yet been done, or FlushedUntilAddress if some recovery has already occurred</param>
        /// <param name="recoverFromAddress">The address from which to perform recovery (undo v+1 records and append to tag-chain tail)</param>
        /// <param name="untilAddress">The last address to scan; this is initially the tailAddress at the time of checkpoint flush, and may be higher due to delta records</param>
        /// <param name="nextVersion">The next version of the database at the time of checkpoint flush</param>
        /// <param name="checkpointType">The type of checkpoint</param>
        /// <param name="options">The recovery options</param>
        /// <returns>The last freed page, if it was necessary to free any to limit heap memory</returns>
        private long RecoverHybridLog(long scanFromAddress, long recoverFromAddress, long untilAddress, long nextVersion, CheckpointType checkpointType, RecoveryOptions options)
        {
            long lastFreedPage = NoPageFreed;
            if (untilAddress <= scanFromAddress)
                return lastFreedPage;

            var recoveryStatus = GetPageRangesToRead(scanFromAddress, untilAddress, checkpointType, out long startPage, out long endPage, out int numPagesToReadPerIteration);

            Debug.Assert(hlogBase.logSizeTracker is null || numPagesToReadPerIteration == 1, "numPagesToReadPerIteration must be 1 when tracking sizes");
            for (var page = startPage; page < endPage; page += numPagesToReadPerIteration)
            {
                var end = ReadPagesForRecovery(untilAddress, recoveryStatus, endPage, numPagesToReadPerIteration, page);

                for (var p = page; p < end; p++)
                {
                    // Ensure page has been read into memory
                    int pageIndex = hlogBase.GetPageIndexForPage(p);
                    recoveryStatus.WaitRead(pageIndex);

                    // Trim the log memory again in case we read large objects on the current page.
                    var freedPage = TrimLogMemorySize(recoveryStatus, p);
                    if (freedPage != NoPageFreed)
                        lastFreedPage = freedPage;

                    // We make an extra pass to clear locks when reading every page back into memory
                    ClearBitsOnPage(p, untilAddress, options);
                    ProcessReadPageAndFlush(recoverFromAddress, untilAddress, nextVersion, options, recoveryStatus, p, pageIndex);
                }
            }

            WaitUntilAllPagesHaveBeenFlushed(startPage, endPage, recoveryStatus);
            return lastFreedPage;
        }

        /// <summary>
        /// Synchronously recover the hybrid log from hybrid log files (not snapshot files). This also deserializes any objects or overflow and creates
        /// entries for them in the <see cref="ObjectIdMap"/>.
        /// </summary>
        /// <param name="scanFromAddress">The address to start scanning from; either beginAddress if no recovery has yet been done, or FlushedUntilAddress if some recovery has already occurred</param>
        /// <param name="recoverFromAddress">The address from which to perform recovery (undo v+1 records and append to tag-chain tail)</param>
        /// <param name="untilAddress">The last address to scan; this is initially the tailAddress at the time of checkpoint flush, and may be higher due to delta records</param>
        /// <param name="nextVersion">The next version of the database at the time of checkpoint flush</param>
        /// <param name="checkpointType">The type of checkpoint</param>
        /// <param name="options">The recovery options</param>
        /// <param name="cancellationToken">The cancellation token</param>
        /// <returns>The last freed page, if it was necessary to free any to limit heap memory</returns>
        private async ValueTask<long> RecoverHybridLogAsync(long scanFromAddress, long recoverFromAddress, long untilAddress, long nextVersion,
            CheckpointType checkpointType, RecoveryOptions options, CancellationToken cancellationToken)
        {
            long lastFreedPage = NoPageFreed;
            if (untilAddress <= scanFromAddress)
                return lastFreedPage;

            var recoveryStatus = GetPageRangesToRead(scanFromAddress, untilAddress, checkpointType, out long startPage, out long endPage, out int numPagesToReadPerIteration);

            Debug.Assert(hlogBase.logSizeTracker is null || numPagesToReadPerIteration == 1, "numPagesToReadPerIteration must be 1 when tracking sizes");
            for (long page = startPage; page < endPage; page += numPagesToReadPerIteration)
            {
                var end = await ReadPagesForRecoveryAsync(untilAddress, recoveryStatus, endPage, numPagesToReadPerIteration, page, cancellationToken).ConfigureAwait(false);

                for (var p = page; p < end; p++)
                {
                    // Ensure page has been read into memory
                    var pageIndex = hlogBase.GetPageIndexForPage(p);
                    await recoveryStatus.WaitReadAsync(pageIndex, cancellationToken).ConfigureAwait(false);

                    // Trim the log memory again in case we read large objects on the current page.
                    var freedPage = await TrimLogMemorySizeAsync(recoveryStatus, p, numPagesToRead: 0, cancellationToken);
                    if (freedPage != NoPageFreed)
                        lastFreedPage = freedPage;

                    // We make an extra pass to clear locks when reading every page back into memory
                    ClearBitsOnPage(p, untilAddress, options);
                    ProcessReadPageAndFlush(recoverFromAddress, untilAddress, nextVersion, options, recoveryStatus, p, pageIndex);
                }
            }

            await WaitUntilAllPagesHaveBeenFlushedAsync(startPage, endPage, recoveryStatus, cancellationToken).ConfigureAwait(false);
            return lastFreedPage;
        }

        /// <summary>
        /// Get the range of pages to read from the hybrid log file(s) for recovery
        /// </summary>
        /// <param name="scanFromAddress">The address to start scanning from; either beginAddress if no recovery has yet been done, or FlushedUntilAddress if some recovery has already occurred</param>
        /// <param name="untilAddress">The last address to scan; this is initially the tailAddress at the time of checkpoint flush, and may be higher due to delta records</param>
        /// <param name="checkpointType">The <see cref="CheckpointType"/></param>
        /// <param name="startPage">The first page to read</param>
        /// <param name="endPage">The last page to read</param>
        /// <param name="numPagesToReadPerIteration">The number of pages to read per iteration</param>
        /// <returns>The allocated <see cref="RecoveryStatus"/> instance.</returns>
        private RecoveryStatus GetPageRangesToRead(long scanFromAddress, long untilAddress, CheckpointType checkpointType,
            out long startPage, out long endPage, out int numPagesToReadPerIteration)
        {
            startPage = hlogBase.GetPage(scanFromAddress);
            endPage = hlogBase.GetPage(untilAddress);
            if (untilAddress > hlogBase.GetFirstValidLogicalAddressOnPage(endPage) && untilAddress > scanFromAddress)
                endPage++;

            // If heap memory is to be tracked, then read one page at a time to control memory usage
            var totalPagesToRead = (int)(endPage - startPage);
            numPagesToReadPerIteration = hlogBase.logSizeTracker is null ? Math.Min(hlogBase.BufferSize, totalPagesToRead) : 1;
            return new RecoveryStatus(hlogBase.BufferSize);
        }

        /// <summary>
        /// Process a page that has been read from the hybrid log file (not snapshot), and flush it if necessary
        /// </summary>
        /// <param name="recoverFromAddress">The address from which to perform recovery (undo v+1 records and append to tag-chain tail)</param>
        /// <param name="untilAddress">The last address to scan; this is initially the tailAddress at the time of checkpoint flush, and may be higher due to delta records</param>
        /// <param name="nextVersion">The next version of the database at the time of checkpoint flush</param>
        /// <param name="options">The recovery options</param>
        /// <param name="recoveryStatus">The <see cref="RecoveryStatus"/> instance</param>
        /// <param name="page">The page number to process</param>
        /// <param name="pageIndex">The index of <paramref name="page"/> in the allocator's circular page buffer</param>
        private void ProcessReadPageAndFlush(long recoverFromAddress, long untilAddress, long nextVersion, RecoveryOptions options,
            RecoveryStatus recoveryStatus, long page, int pageIndex)
        {
            if (ProcessReadPage(recoverFromAddress, untilAddress, nextVersion, options, recoveryStatus, page, pageIndex))
            {
                // Page was modified due to undoFutureVersion. Flush it to disk; the callback issues the after-capacity read request if necessary.
                hlogBase.AsyncFlushPagesForRecovery(page, 1, AsyncFlushPageCallbackForRecovery, recoveryStatus);
                return;
            }

            // We do not need to flush
            recoveryStatus.flushStatus[pageIndex] = FlushStatus.Done;
        }

        /// <summary>
        /// Determine address ranges on a page that has been read from the hybrid log file (not snapshot), then recover from that page.
        /// </summary>
        /// <param name="recoverFromAddress">The address from which to perform recovery (undo v+1 records and append to tag-chain tail)</param>
        /// <param name="untilAddress">The last address to scan; this is initially the tailAddress at the time of checkpoint flush, and may be higher due to delta records</param>
        /// <param name="nextVersion">The next version of the database at the time of checkpoint flush</param>
        /// <param name="options">The recovery options</param>
        /// <param name="recoveryStatus">The <see cref="RecoveryStatus"/> instance</param>
        /// <param name="page">The page number to process</param>
        /// <param name="pageIndex">The index of <paramref name="page"/> in the allocator's circular page buffer</param>
        /// <returns></returns>
        private bool ProcessReadPage(long recoverFromAddress, long untilAddress, long nextVersion, RecoveryOptions options, RecoveryStatus recoveryStatus,
            long page, int pageIndex)
        {
            var startLogicalAddressOfPage = hlogBase.GetLogicalAddressOfStartOfPage(page);    // Do not offset for page header; that's done below and in RecoverFromPage
            var endLogicalAddressOfPage = hlogBase.GetLogicalAddressOfStartOfPage(page + 1);
            var startPhysicalAddressOfPage = hlogBase.GetPhysicalAddress(startLogicalAddressOfPage);

            if (recoverFromAddress >= endLogicalAddressOfPage)
                return false;

            var pageFromAddressOffset = (long)hlogBase.pageHeaderSize;
            var pageUntilAddressOffset = hlogBase.GetPageSize();

            if (recoverFromAddress > startLogicalAddressOfPage)
            {
                pageFromAddressOffset = hlogBase.GetOffsetOnPage(recoverFromAddress);
                Debug.Assert(pageFromAddressOffset >= hlogBase.pageHeaderSize, $"pageFromAddressOffset {pageFromAddressOffset} must be >= hlogBase.pageHeaderSize {hlogBase.pageHeaderSize} (which may be 0)");
            }
            if (untilAddress < endLogicalAddressOfPage)
                pageUntilAddressOffset = hlogBase.GetOffsetOnPage(untilAddress);

            if (RecoverFromPage(recoverFromAddress, pageFromAddressOffset, pageUntilAddressOffset, startLogicalAddressOfPage, startPhysicalAddressOfPage, options))
            {
                // The current page was modified due to undoFutureVersion; caller will flush it to storage and issue a read request if necessary.
                recoveryStatus.readStatus[pageIndex] = ReadStatus.Pending;
                recoveryStatus.flushStatus[pageIndex] = FlushStatus.Pending;
                return true;
            }

            return false;
        }

        private void WaitUntilAllPagesHaveBeenFlushed(long startPage, long endPage, RecoveryStatus recoveryStatus)
        {
            for (long page = startPage; page < endPage; page++)
                recoveryStatus.WaitFlush(hlogBase.GetPageIndexForPage(page));
        }

        private async ValueTask WaitUntilAllPagesHaveBeenFlushedAsync(long startPage, long endPage, RecoveryStatus recoveryStatus, CancellationToken cancellationToken)
        {
            for (long page = startPage; page < endPage; page++)
                await recoveryStatus.WaitFlushAsync(hlogBase.GetPageIndexForPage(page), cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Synchronously recover the hybrid log from snapshot files
        /// </summary>
        /// <param name="scanFromAddress">The start of the mutable region</param>
        /// <param name="recoverFromAddress">The address from which to perform recovery (undo v+1 records and append to tag-chain tail)</param>
        /// <param name="untilAddress">The last address to scan; this is initially the tailAddress at the time of checkpoint flush, and may be higher due to delta records</param>
        /// <param name="snapshotStartAddress">The start of the mutable region; the FlushedUntilAddress at the start of the WAIT_FLUSH phase</param>
        /// <param name="snapshotEndAddress">The end of the snapshot; the tailAddress at the start of the WAIT_FLUSH phase</param>
        /// <param name="nextVersion">The next version of the database at the time of checkpoint flush</param>
        /// <param name="guid">The checkpoint token guid</param>
        /// <param name="options">The recovery options</param>
        /// <param name="deltaLog">The incremental checkpoint log, if any</param>
        /// <param name="recoverTo">Specific version requested to recover to, or -1 for latest version</param>
        /// <returns>The last freed page, if it was necessary to free any to limit heap memory</returns>
        private long RecoverHybridLogFromSnapshotFile(long scanFromAddress, long recoverFromAddress, long untilAddress,
            long snapshotStartAddress, long snapshotEndAddress, long nextVersion, Guid guid, RecoveryOptions options, DeltaLog deltaLog, long recoverTo)
        {
            long lastFreedPage = NoPageFreed;
            GetSnapshotPageRangesToRead(scanFromAddress, untilAddress, snapshotStartAddress, snapshotEndAddress, guid, out long startPage,
                out long endPage, out long snapshotEndPage, out var recoveryStatus, out int numPagesToReadPerIteration);

            for (long page = startPage; page < endPage; page += numPagesToReadPerIteration)
            {
                _ = ReadPagesForRecovery(snapshotEndAddress, recoveryStatus, snapshotEndPage, numPagesToReadPerIteration, page);
                var end = Math.Min(page + numPagesToReadPerIteration, endPage);
                for (long p = page; p < end; p++)
                {
                    int pageIndex = hlogBase.GetPageIndexForPage(p);
                    if (p < snapshotEndPage)
                    {
                        // Ensure the page is read from file
                        recoveryStatus.WaitRead(pageIndex);

                        // Trim the log memory again in case we read large objects on the current page.
                        var freedPage = TrimLogMemorySize(recoveryStatus, p);
                        if (freedPage != NoPageFreed)
                            lastFreedPage = freedPage;

                        // We make an extra pass to clear locks when reading pages back into memory
                        ClearBitsOnPage(p, untilAddress, options);
                    }
                    else
                    {
                        recoveryStatus.WaitFlush(pageIndex);
                        if (!hlogBase.IsAllocated(pageIndex))
                            hlog.AllocatePage(pageIndex);
                        else
                            hlogBase.ClearPage(pageIndex);
                    }
                }

                ApplyDeltaAndRecoverPages(recoverFromAddress, untilAddress, nextVersion, options, deltaLog, recoverTo,
                    endPage, snapshotEndPage, numPagesToReadPerIteration, recoveryStatus, page, end);
            }

            WaitUntilAllPagesHaveBeenFlushed(startPage, endPage, recoveryStatus);
            recoveryStatus.Dispose();
            return lastFreedPage;
        }

        /// <summary>
        /// Asynchronously recover the hybrid log from snapshot files
        /// </summary>
        /// <param name="scanFromAddress">The start of the mutable region</param>
        /// <param name="recoverFromAddress">The address from which to perform recovery (undo v+1 records and append to tag-chain tail)</param>
        /// <param name="untilAddress">The last address to scan; this is initially the tailAddress at the time of checkpoint flush, and may be higher due to delta records</param>
        /// <param name="snapshotStartAddress">The start of the mutable region; the FlushedUntilAddress at the start of the WAIT_FLUSH phase</param>
        /// <param name="snapshotEndAddress">The end of the snapshot; the tailAddress at the start of the WAIT_FLUSH phase</param>
        /// <param name="nextVersion">The next version of the database at the time of checkpoint flush</param>
        /// <param name="guid">The checkpoint token guid</param>
        /// <param name="options">The recovery options</param>
        /// <param name="deltaLog">The incremental checkpoint log, if any</param>
        /// <param name="recoverTo">Specific version requested to recover to, or -1 for latest version</param>
        /// <returns>The last freed page, if it was necessary to free any to limit heap memory</returns>
        private async ValueTask<long> RecoverHybridLogFromSnapshotFileAsync(long scanFromAddress, long recoverFromAddress, long untilAddress,
            long snapshotStartAddress, long snapshotEndAddress, long nextVersion, Guid guid, RecoveryOptions options, DeltaLog deltaLog, long recoverTo,
            CancellationToken cancellationToken)
        {
            long lastFreedPage = NoPageFreed;
            GetSnapshotPageRangesToRead(scanFromAddress, untilAddress, snapshotStartAddress, snapshotEndAddress, guid, out long startPage,
                out long endPage, out long snapshotEndPage, out var recoveryStatus, out int numPagesToReadPerIteration);

            for (long page = startPage; page < endPage; page += numPagesToReadPerIteration)
            {
                _ = await ReadPagesForRecoveryAsync(snapshotEndAddress, recoveryStatus, snapshotEndPage, numPagesToReadPerIteration, page, cancellationToken).ConfigureAwait(false);

                var end = Math.Min(page + numPagesToReadPerIteration, endPage);
                for (long p = page; p < end; p++)
                {
                    int pageIndex = hlogBase.GetPageIndexForPage(p);
                    if (p < snapshotEndPage)
                    {
                        // Ensure the page is read from file
                        await recoveryStatus.WaitReadAsync(pageIndex, cancellationToken).ConfigureAwait(false);

                        // Trim the log memory again in case we read large objects on the current page.
                        var freedPage = await TrimLogMemorySizeAsync(recoveryStatus, p, numPagesToRead: 0, cancellationToken).ConfigureAwait(false);
                        if (freedPage != NoPageFreed)
                            lastFreedPage = freedPage;

                        // We make an extra pass to clear locks when reading pages back into memory
                        ClearBitsOnPage(p, untilAddress, options);
                    }
                    else
                    {
                        await recoveryStatus.WaitFlushAsync(pageIndex, cancellationToken).ConfigureAwait(false);
                        if (!hlogBase.IsAllocated(pageIndex))
                            hlog.AllocatePage(pageIndex);
                        else
                            hlogBase.ClearPage(pageIndex);
                    }
                }

                ApplyDeltaAndRecoverPages(recoverFromAddress, untilAddress, nextVersion, options, deltaLog, recoverTo,
                    endPage, snapshotEndPage, numPagesToReadPerIteration, recoveryStatus, page, end);
            }

            await WaitUntilAllPagesHaveBeenFlushedAsync(startPage, endPage, recoveryStatus, cancellationToken).ConfigureAwait(false);
            recoveryStatus.Dispose();
            return lastFreedPage;
        }

        /// <summary>
        /// Apply the delta log if we have one; then for each page in the snapshot from [page, end), process the page for recovery.
        /// </summary>
        /// <param name="recoverFromAddress">The address from which to perform recovery (undo v+1 records and append to tag-chain tail)</param>
        /// <param name="untilAddress">The last address to scan; this is initially the tailAddress at the time of checkpoint flush, and may be higher due to delta records</param>
        /// <param name="nextVersion">The next version of the database at the time of checkpoint flush</param>
        /// <param name="options">The recovery options</param>
        /// <param name="deltaLog">The incremental checkpoint log, if any</param>
        /// <param name="recoverTo">Specific version requested to recover to, or -1 for latest version</param>
        /// <param name="endPage">The last page to read; the page of <paramref name="untilAddress"/></param>
        /// <param name="snapshotEndPage">The end page of the snapshot; the page of the tailAddress at the start of the WAIT_FLUSH phase</param>
        /// <param name="numPagesToRead">Number of pages to read in this batch; may be greater than <paramref name="end"/> minus <paramref name="page"/> if it's the last partial batch</param>
        /// <param name="recoveryStatus">The allocated <see cref="RecoveryStatus"/> instance</param>
        /// <param name="page">The current page to read</param>
        /// <param name="end">The last page to read</param>
        private void ApplyDeltaAndRecoverPages(long recoverFromAddress, long untilAddress, long nextVersion, RecoveryOptions options, DeltaLog deltaLog,
            long recoverTo, long endPage, long snapshotEndPage, int numPagesToRead, RecoveryStatus recoveryStatus, long page, long end)
        {
            hlogBase.ApplyDelta(deltaLog, page, end, recoverTo);

            for (long p = page; p < end; p++)
            {
                int pageIndex = hlogBase.GetPageIndexForPage(p);

                var endLogicalAddress = hlogBase.GetLogicalAddressOfStartOfPage(p + 1);
                if (recoverFromAddress < endLogicalAddress && recoverFromAddress < untilAddress)
                    ProcessReadSnapshotPage(recoverFromAddress, untilAddress, nextVersion, options, recoveryStatus, p, pageIndex);

                // Issue next read
                if (p + numPagesToRead < endPage)
                {
                    // Flush snapshot page to main log
                    recoveryStatus.flushStatus[pageIndex] = FlushStatus.Pending;
                    hlogBase.AsyncFlushPagesForRecovery(p, 1, AsyncFlushPageCallbackForRecovery, recoveryStatus);
                }
            }
        }

        /// <summary>
        /// Get the range of pages to read from the snapshot file(s) for recovery
        /// </summary>
        /// <param name="scanFromAddress">The address to start scanning from; either beginAddress if no recovery has yet been done, or FlushedUntilAddress if some recovery has already occurred</param>
        /// <param name="untilAddress">The last address to scan; this is initially the tailAddress at the time of checkpoint flush, and may be higher due to delta records</param>
        /// <param name="snapshotStartAddress">The start of the mutable region; the FlushedUntilAddress at the start of the WAIT_FLUSH phase</param>
        /// <param name="snapshotEndAddress">The end of the snapshot; the tailAddress at the start of the WAIT_FLUSH phase</param>
        /// <param name="guid">The checkpoint token guid</param>
        /// <param name="startPage">The first page to read; the page of <paramref name="scanFromAddress"/></param>
        /// <param name="endPage">The last page to read; the page of <paramref name="untilAddress"/></param>
        /// <param name="snapshotEndPage">The page of <paramref name="snapshotEndAddress"/></param>
        /// <param name="recoveryStatus">The allocated <see cref="RecoveryStatus"/> instance</param>
        /// <param name="numPagesToReadPerIteration">The number of pages to read per iteration</param>
        private void GetSnapshotPageRangesToRead(long scanFromAddress, long untilAddress, long snapshotStartAddress, long snapshotEndAddress, Guid guid,
            out long startPage, out long endPage, out long snapshotEndPage, out RecoveryStatus recoveryStatus, out int numPagesToReadPerIteration)
        {
            // Compute startPage and endPage
            startPage = hlogBase.GetPage(scanFromAddress);
            endPage = hlogBase.GetPage(untilAddress);
            if (untilAddress > hlogBase.GetFirstValidLogicalAddressOnPage(endPage) && untilAddress > scanFromAddress)
                endPage++;
            var snapshotStartPage = hlogBase.GetPage(snapshotStartAddress);
            snapshotEndPage = hlogBase.GetPage(snapshotEndAddress);
            if (snapshotEndAddress > hlogBase.GetFirstValidLogicalAddressOnPage(snapshotEndPage) && snapshotEndAddress > snapshotStartAddress)
                snapshotEndPage++;

            // By default first page has one extra record
            var recoveryDevice = checkpointManager.GetSnapshotLogDevice(guid);
            var objectLogRecoveryDevice = checkpointManager.GetSnapshotObjectLogDevice(guid);

            recoveryDevice.Initialize(hlogBase.GetMainLogSegmentSize());
            objectLogRecoveryDevice.Initialize(hlogBase.GetObjectLogSegmentSize());
            recoveryStatus = new RecoveryStatus(hlogBase.BufferSize)
            {
                recoveryDevice = recoveryDevice,
                objectLogRecoveryDevice = objectLogRecoveryDevice,
                recoveryDevicePageOffset = snapshotStartPage
            };

            // Initially issue read request for all pages that can be held in memory
            // If heap memory is to be tracked, then read one page at a time to control memory usage
            var totalPagesToRead = (int)(snapshotEndPage - startPage);
            numPagesToReadPerIteration = hlogBase.logSizeTracker is null ? Math.Min(hlogBase.BufferSize, totalPagesToRead) : 1;
        }

        private void ProcessReadSnapshotPage(long recoverFromAddress, long untilAddress, long nextVersion, RecoveryOptions options, RecoveryStatus recoveryStatus, long page, int pageIndex)
        {
            // Page at hand
            var startLogicalAddressOfPage = hlogBase.GetLogicalAddressOfStartOfPage(page);    // Do not offset for page header; that's done below and in RecoverFromPage
            var endLogicalAddressOfPage = hlogBase.GetLogicalAddressOfStartOfPage(page + 1);

            // Perform recovery if page is part of the re-do portion of log
            if (recoverFromAddress < endLogicalAddressOfPage && recoverFromAddress < untilAddress)
            {
                /*
                 * Handling corner-cases:
                 * ----------------------
                 * When fromAddress is in the middle of the page, then start recovery only from corresponding offset 
                 * in page. Similarly, if untilAddress falls in the middle of the page, perform recovery only until that
                 * offset. Otherwise, scan the entire page [0, PageSize)
                 */

                long pageFromAddressOffset = hlogBase.pageHeaderSize;
                var pageUntilAddressOffset = hlogBase.GetPageSize();
                var startPhysicalAddressOfPage = hlogBase.GetPhysicalAddress(startLogicalAddressOfPage);

                if (recoverFromAddress > startLogicalAddressOfPage && recoverFromAddress < endLogicalAddressOfPage)
                    pageFromAddressOffset = hlogBase.GetOffsetOnPage(recoverFromAddress);
                if (endLogicalAddressOfPage > untilAddress)
                    pageUntilAddressOffset = hlogBase.GetOffsetOnPage(untilAddress);

                _ = RecoverFromPage(recoverFromAddress, pageFromAddressOffset, pageUntilAddressOffset, startLogicalAddressOfPage, startPhysicalAddressOfPage, options);
            }

            recoveryStatus.flushStatus[pageIndex] = FlushStatus.Done;
        }

        private void ClearBitsOnPage(long page, long untilAddress, RecoveryOptions options)
        {
            var startLogicalAddress = hlogBase.GetLogicalAddressOfStartOfPage(page);
            var endLogicalAddress = hlogBase.GetLogicalAddressOfStartOfPage(page + 1);
            var physicalAddress = hlogBase.GetPhysicalAddress(startLogicalAddress);

            // no need to clear locks for records that will not end up in main memory
            if (options.headAddress >= endLogicalAddress)
                return;

            var pageSize = hlogBase.GetPageSize();
            var endOffset = (untilAddress < endLogicalAddress) ? hlogBase.GetOffsetOnPage(untilAddress) : pageSize;

            long recordOffset = hlogBase.pageHeaderSize;
            while (recordOffset < endOffset)
            {
                var logRecord = new LogRecord(physicalAddress + recordOffset);
                logRecord.InfoRef.ClearBitsForDiskImages();

                long recordSize = logRecord.AllocatedSize;
                Debug.Assert(recordSize > 0 && recordSize <= endOffset - recordOffset,
                    $"recordSize {recordSize} must be > 0 and <= remaining page space (possibly limited by untilAddress) {pageSize - endOffset};" +
                    $" recordOffset {recordOffset}, endOffset {endOffset}, pageSize {pageSize}");
                recordOffset += recordSize;
            }
        }

        /// <summary>
        /// Re-do the necessary log records:
        /// <list>
        ///     <item>If the record is in v+1 *and* is in the fuzzy region (which has v and v+1 records) and we are undoing nextVersion records, invalidate it.
        ///         We do this only in the fuzzy region because the earlier records may have a stale InNewVersion status. </item>
        ///     <item>Otherwise, update the tag chain for the record's hash and tag by inserting the record at the tail of the <see cref="HashBucketEntry"/></item>
        /// </list>
        /// </summary>
        /// <param name="recoverFromAddress">The address from which to perform recovery (undo v+1 records and append to tag-chain tail)</param>
        /// <param name="pageFromAddressOffset">The start address offset on the page to recover from (must be &gt;= <see cref="PageHeader.Size"/> if any</param>
        /// <param name="pageUntilAddressOffset">The end address offset on the page to recover from (must be &lt; PageSize)</param>
        /// <param name="pageStartLogicalAddress">The logical address of the start of the page</param>
        /// <param name="pageStartPhysicalAddress">The physical address of the start of the page</param>
        /// <param name="options">Recovery options</param>
        /// <returns>True if we touched the page (and thus it needs to be flushed), else false</returns>
        private unsafe bool RecoverFromPage(long recoverFromAddress, long pageFromAddressOffset, long pageUntilAddressOffset,
                                     long pageStartLogicalAddress, long pageStartPhysicalAddress, RecoveryOptions options)
        {
            Debug.Assert(pageFromAddressOffset >= hlogBase.pageHeaderSize, $"fromLogicalAddressInPage {pageFromAddressOffset} must be >= hlogBase.pageHeaderSize {hlogBase.pageHeaderSize} (which may be 0)");
            Debug.Assert(pageUntilAddressOffset <= hlogBase.GetPageSize(), $"pageSize {pageUntilAddressOffset} must be <= PageSize {hlogBase.GetPageSize()}");
            var touched = false;

            var recordOffset = pageFromAddressOffset;
            while (recordOffset < pageUntilAddressOffset)
            {
                var logRecord = new LogRecord(pageStartPhysicalAddress + recordOffset);
                ref var info = ref logRecord.InfoRef;

                if (info.IsNull)
                {
                    recordOffset += RecordInfo.Size;
                    continue;
                }

                if (!info.Invalid)
                {
                    HashEntryInfo hei = new(storeFunctions.GetKeyHashCode64(logRecord.Key));
                    FindOrCreateTag(ref hei, hlogBase.BeginAddress);

                    if ((pageStartLogicalAddress + recordOffset) < options.fuzzyRegionStartAddress || !info.IsInNewVersion || !options.undoNextVersion)
                    {
                        // Update the hash table with this record
                        hei.entry.Set(pageStartLogicalAddress + recordOffset, hei.tag);
                        hei.bucket->bucket_entries[hei.slot] = hei.entry.word;
                    }
                    else
                    {
                        // Ignore this record
                        touched = true;
                        info.SetInvalid();
                        if (info.PreviousAddress < recoverFromAddress)
                        {
                            hei.entry.Set(info.PreviousAddress, hei.tag);
                            hei.bucket->bucket_entries[hei.slot] = hei.entry.word;
                        }
                    }
                }
                recordOffset += logRecord.AllocatedSize;
            }

            return touched;
        }

        private void AsyncFlushPageCallbackForRecovery(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                logger?.LogError($"{nameof(AsyncFlushPageCallbackForRecovery)} error: {{errorCode}}", errorCode);

            // Set the page status to "flush done"
            var result = (PageAsyncFlushResult<RecoveryStatus>)context;

            if (result.Release() == 0)
            {
                var pageIndex = hlogBase.GetPageIndexForPage(result.page);
                if (errorCode != 0)
                    result.context.SignalFlushedError(pageIndex);
                else
                    result.context.SignalFlushed(pageIndex);
            }
        }
    }

    public abstract partial class AllocatorBase<TStoreFunctions, TAllocator> : IDisposable
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>
        /// Restore log; called from TsavoriteLog
        /// </summary>
        /// <param name="beginAddress"></param>
        /// <param name="headAddress"></param>
        /// <param name="fromAddress"></param>
        /// <param name="untilAddress">The last address to scan; this is initially the tailAddress at the time of checkpoint flush, and may be higher due to delta records</param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        public void RestoreHybridLog(long beginAddress, long headAddress, long fromAddress, long untilAddress, int numPagesToPreload = -1)
        {
            if (RestoreHybridLogInitializePages(beginAddress, headAddress, fromAddress, untilAddress, numPagesToPreload, out var recoveryStatus, out long headPage, out long fromPage))
            {
                for (long page = headPage; page <= fromPage; page++)
                    recoveryStatus.WaitRead(GetPageIndexForPage(page));
            }

            RecoveryReset(untilAddress, headAddress, beginAddress, untilAddress);
        }

        /// <summary>
        /// Restore log; called from TsavoriteLog
        /// </summary>
        /// <param name="beginAddress"></param>
        /// <param name="headAddress"></param>
        /// <param name="fromAddress"></param>
        /// <param name="untilAddress">The last address to scan; this is initially the tailAddress at the time of checkpoint flush, and may be higher due to delta records</param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="cancellationToken"></param>
        public async ValueTask RestoreHybridLogAsync(long beginAddress, long headAddress, long fromAddress, long untilAddress, int numPagesToPreload = -1, CancellationToken cancellationToken = default)
        {
            if (RestoreHybridLogInitializePages(beginAddress, headAddress, fromAddress, untilAddress, numPagesToPreload, out var recoveryStatus, out long headPage, out long fromPage))
            {
                for (long page = headPage; page <= fromPage; page++)
                    await recoveryStatus.WaitReadAsync(GetPageIndexForPage(page), cancellationToken).ConfigureAwait(false);
            }

            RecoveryReset(untilAddress, headAddress, beginAddress, untilAddress);
        }

        private bool RestoreHybridLogInitializePages(long beginAddress, long headAddress, long fromAddress, long untilAddress, int numPagesToPreload,
                                                     out RecoveryStatus recoveryStatus, out long headPage, out long fromPage)
        {
            if (numPagesToPreload != -1)
            {
                var head = GetFirstValidLogicalAddressOnPage(GetPage(untilAddress) - numPagesToPreload);
                if (head > headAddress)
                    headAddress = head;
            }
            Debug.Assert(beginAddress <= headAddress);
            Debug.Assert(headAddress <= untilAddress);

            // Special cases: we do not load any records into memory
            if ((beginAddress == untilAddress) || // Empty log
                ((headAddress == untilAddress) && (GetOffsetOnPage(headAddress) == 0))) // Empty in-memory page
            {
                var pageIndex = GetPageIndexForAddress(headAddress);
                if (!IsAllocated(pageIndex))
                    _wrapper.AllocatePage(pageIndex);
            }
            else if (headAddress < fromAddress)
            {
                fromPage = GetPage(fromAddress);
                headPage = GetPage(headAddress);

                var capacity = logSizeTracker is null ? BufferSize : RoundUp(logSizeTracker.TargetSize, PageSize) / PageSize;

                // Set all ReadStatus to done for the page range we will initially read.
                recoveryStatus = new RecoveryStatus(BufferSize);
                for (int i = 0; i < capacity; i++)
                    recoveryStatus.readStatus[i] = ReadStatus.Done;

                // Set all PendingStatus to Pending for all pages we will read.
                var numPages = 0;
                for (var page = headPage; page <= fromPage; page++)
                {
                    var pageIndex = GetPageIndexForPage(page);
                    recoveryStatus.readStatus[pageIndex] = ReadStatus.Pending;
                    numPages++;
                }

                // Passing no objectLogDevice means we'll use the one in the allocator
                AsyncReadPagesForRecovery(headPage, numPages, untilAddress, recoveryStatus);
                return true;
            }

            // fromAddress <= headAddress, so no pages to read
            recoveryStatus = default;
            headPage = fromPage = 0;
            return false;
        }

        internal void AsyncReadPagesForRecoveryCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                logger?.LogError($"{nameof(AsyncReadPagesForRecoveryCallback)} error: {{errorCode}}", errorCode);

            // Set the page status to "read done"
            var result = (PageAsyncReadResult<RecoveryStatus>)context;

            var pageIndex = GetPageIndexForPage(result.page);
            if (errorCode != 0)
                result.context.SignalReadError(pageIndex);
            else
                result.context.SignalRead(pageIndex);

            result.DisposeHandle();
        }
    }
}