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
        /// <summary>The first page to recover; this is the page index of the snapshotStartAddress and is the page-offset into
        /// the address range of the snapshot file (i.e. the page at the snapshot file's offset 0). This field is populated
        /// from <see cref="HybridLogRecoveryInfo.snapshotStartFlushedLogicalAddress"/>.</summary>
        public long recoveryDevicePageOffset;
        /// <summary>Object log recovery device, obtained from CheckpointManager.</summary>
        public IDevice objectLogRecoveryDevice;

        /// <summary>The current head address; updated as pages are evicted during recovery.</summary>
        public long headAddress;

        /// <summary>For snapshot recovery, the hybrid-log/snapshot boundary (the snapshot phase's scanFromAddress). Pages at or
        /// above its page are snapshot-region pages that were read from the snapshot file and are not yet durable on the main log,
        /// so they must be flushed before eviction. -1 during hybrid-log recovery, where all read pages are already durable.</summary>
        public long snapshotScanFromAddress = -1;

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
                ThrowTsavoriteException($"Error reading page {pageIndex} from device");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal async ValueTask WaitReadAsync(int pageIndex, CancellationToken cancellationToken)
        {
            while (readStatus[pageIndex] == ReadStatus.Pending)
                await readSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            if (readStatus[pageIndex] == ReadStatus.Error)
                ThrowTsavoriteException($"Error reading page {pageIndex} from device");
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
            objectLogRecoveryDevice?.Dispose();
            readSemaphore.Dispose();
            flushSemaphore.Dispose();
        }
    }

    internal readonly struct RecoveryOptions
    {
        internal readonly long fuzzyRegionStartAddress;
        internal readonly bool undoNextVersion;

        internal RecoveryOptions(long fuzzyRegionStartAddress, bool undoNextVersion)
        {
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

                current.Recover(hlogToken, checkpointManager, out var _);
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

            current.Recover(hlogToken, checkpointManager, out var _);
            return current.info.nextVersion;
        }

        /// <summary>
        /// Get size of snapshot files for token
        /// </summary>
        public LogFileInfo GetLogFileSize(Guid token)
        {
            using var current = new HybridLogCheckpointInfo();
            current.Recover(token, checkpointManager, out var _);
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

        private void GetClosestHybridLogCheckpointInfo(long requestedVersion, out Guid closestToken, out HybridLogCheckpointInfo closest, out byte[] cookie)
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
                    current.Recover(hybridLogToken, checkpointManager, out var currCookie);
                    var distanceToTarget = (requestedVersion == -1 ? long.MaxValue : requestedVersion) - current.info.version;

                    // This is larger than intended version, cannot recover to this.
                    if (distanceToTarget < 0)
                        continue;

                    // We have found the exact version to recover to: the above conditional establishes that the checkpointed version is <= requested version,
                    // and if nextVersion is larger than requestedVersion, there cannot be any closer version. 
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
            recoveredHLCInfo.Recover(hybridLogToken, checkpointManager, out recoveredCommitCookie);
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

        /// <summary>Aynchronous recovery driver</summary>
        private ValueTask<long> InternalRecoverAsync(Guid indexToken, Guid hybridLogToken, int numPagesToPreload, bool undoNextVersion, CancellationToken cancellationToken)
        {
            GetRecoveryInfo(indexToken, hybridLogToken, out var recoveredHLCInfo, out var recoveredICInfo);
            return InternalRecoverAsync(recoveredICInfo, recoveredHLCInfo, numPagesToPreload, undoNextVersion, cancellationToken);
        }

        /// <summary>Asynchronous recovery driver</summary>
        private async ValueTask<long> InternalRecoverAsync(IndexCheckpointInfo recoveredICInfo, HybridLogCheckpointInfo recoveredHLCInfo, int numPagesToPreload, bool undoNextVersion, CancellationToken cancellationToken)
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
            RecoveryOptions options = new(fuzzyRegionStartAddress: recoveredHLCInfo.info.startLogicalAddress, undoNextVersion);

            // Make index consistent for version v
            long readOnlyAddress;
            long finalHeadAddress;
            RecoveryStatus recoveryStatus;
            if (recoveredHLCInfo.info.useSnapshotFile == 0)
            {
                recoveryStatus = await RecoverHybridLogAsync(scanFromAddress, recoverFromAddress, untilAddress: recoveredHLCInfo.info.finalLogicalAddress,
                        recoveredHLCInfo.info.nextVersion, CheckpointType.FoldOver, headAddress, options, cancellationToken).ConfigureAwait(false);

                // FoldOver objects are already durable in the main object-log; set the tail to its end so subsequent writes append after it.
                hlogBase.SetObjectLogTail(recoveredHLCInfo.info.hlogEndObjectLogTail);
                finalHeadAddress = recoveryStatus.headAddress;
                readOnlyAddress = tailAddress;
            }
            else
            {
                if (recoveredHLCInfo.info.flushedLogicalAddress < headAddress)
                    headAddress = recoveredHLCInfo.info.flushedLogicalAddress;

                // First recover from index starting point (fromAddress) to snapshot starting point (flushedLogicalAddress taken at PERSISTENCE_CALLBACK, so it includes
                // any flushes to the hybrid log files due to OnPagesMarkedReadOnly while we were flushing to the snapshot files). Object loading is deferred (see below).
                recoveryStatus = await RecoverHybridLogAsync(scanFromAddress, recoverFromAddress, untilAddress: recoveredHLCInfo.info.flushedLogicalAddress,
                        recoveredHLCInfo.info.nextVersion, CheckpointType.Snapshot, headAddress, options, cancellationToken).ConfigureAwait(false);

                // Initialize the main object-log tail to the end of the hybrid-log objects BEFORE recovering the snapshot pages: the snapshot-region flushes copy
                // each record's objects from the snapshot object-log into the main object-log starting here, advancing the tail (via OnPartialFlushComplete) as they go.
                // This must happen after the hybrid-log phase (which runs with the tail unset, like before) and before the snapshot phase.
                hlogBase.SetObjectLogTail(recoveredHLCInfo.info.hlogEndObjectLogTail);

                // Then recover snapshot into mutable region. The snapshot-region pages are read (without their objects), flushed to the main log with their objects
                // copied into the main object-log (so they are durable and can be evicted into a smaller memory budget), and then objects are loaded once over the full
                // recovered range (both the hybrid-log and snapshot regions), honoring the final headAddress.
                finalHeadAddress = await RecoverHybridLogFromSnapshotFileAsync(scanFromAddress: recoveredHLCInfo.info.flushedLogicalAddress,
                        recoverFromAddress, untilAddress: recoveredHLCInfo.info.finalLogicalAddress,
                        snapshotStartAddress: recoveredHLCInfo.info.snapshotStartFlushedLogicalAddress, snapshotEndAddress: recoveredHLCInfo.info.snapshotFinalLogicalAddress,
                        recoveredHLCInfo.info.nextVersion, recoveredHLCInfo.info.guid, headAddress: recoveryStatus.headAddress,
                        options, cancellationToken).ConfigureAwait(false);

                readOnlyAddress = recoveredHLCInfo.info.flushedLogicalAddress;
            }

            DoPostRecovery(recoveredICInfo, recoveredHLCInfo, tailAddress, finalHeadAddress, readOnlyAddress);
            return recoveredHLCInfo.info.version;
        }

        private void DoPostRecovery(IndexCheckpointInfo recoveredICInfo, HybridLogCheckpointInfo recoveredHLCInfo, long tailAddress, long headAddress, long readOnlyAddress)
        {
            // HeadAddress has already been adjusted for any evictions but make sure we are below any existing HeadAddress in the log.
            if (headAddress < hlogBase.HeadAddress)
                headAddress = hlogBase.HeadAddress;
            if (readOnlyAddress < headAddress)
                readOnlyAddress = headAddress;

            // If eviction advanced headAddress (past the checkpoint's flushedLogicalAddress) to the first valid address of its page, that page is fully
            // resident and was NOT flushed to the main log (recovery flushes only pages it evicts), so the flushed/read-only boundary is the page's start
            // (the end of the last flushed page). Leaving ReadOnlyAddress/FlushedUntilAddress at the first-valid address would mark the page's header region
            // as read-only/flushed while the page is not on disk, so a read up to that boundary (e.g. a Scan up to SafeReadOnlyAddress) would issue a disk
            // read of the un-flushed page and fail with EOF. Aligning down keeps the whole resident page in the mutable region. (A partially-evicted head
            // page, whose headAddress is mid-page, was flushed by RecoverHybridLogFromSnapshotFileAsync's object-load pass and needs no adjustment.)
            if (readOnlyAddress == headAddress && headAddress == hlogBase.GetFirstValidLogicalAddressOnPage(hlogBase.GetPage(headAddress)))
            {
                var headPageStart = hlogBase.GetLogicalAddressOfStartOfPage(hlogBase.GetPage(headAddress));
                if (headPageStart >= recoveredHLCInfo.info.flushedLogicalAddress)
                    headAddress = readOnlyAddress = headPageStart;
            }

            hlogBase.RecoveryReset(tailAddress, headAddress, recoveredHLCInfo.info.beginAddress, readOnlyAddress);

            // Bring ReadOnlyAddress to the logMutableFraction boundary as in normal operation (CalculateReadOnlyAddress): recovery leaves the whole
            // resident set mutable, but only the top logMutableFraction should be. Shifting ReadOnlyAddress up flushes the pages that thereby become
            // read-only (they are resident with their objects loaded, so this is a normal heap-object flush) so they are durable on the main log; wait
            // for that flush so FlushedUntilAddress is consistent before recovery returns.
            var targetReadOnlyAddress = hlogBase.CalculateReadOnlyAddress(tailAddress, hlogBase.HeadAddress);
            if (targetReadOnlyAddress > hlogBase.ReadOnlyAddress)
                hlogBase.ShiftReadOnlyAddressWithWait(targetReadOnlyAddress, wait: true);

            checkpointManager.OnRecovery(recoveredICInfo.info.token, recoveredHLCInfo.info.guid);
            recoveredHLCInfo.Dispose();
        }

        /// <summary>
        /// Set store version directly. Useful if manually recovering by re-inserting data.
        /// Warning: use only when the system is not taking a checkpoint.
        /// </summary>
        /// <param name="version">Version to set the store to</param>
        public void SetVersion(long version) => stateMachineDriver.SetSystemState(SystemState.Make(Phase.REST, version));

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
            // Before reading in additional pages, trim memory if needed to make room for the inline page space.
            TrimLogPages(recoveryStatus, tailPage: page, numPagesToRead, untilAddress: endAddress);

            // Set all page read statuses to Pending
            for (var p = page; p < endPage; p++)
                recoveryStatus.readStatus[hlogBase.GetPageIndexForPage(p)] = ReadStatus.Pending;

            // Issue request to read pages as much as possible
            hlogBase.AsyncReadPagesForRecovery(page, numPagesToRead, endAddress, recoveryStatus, recoveryStatus.recoveryDevicePageOffset,
                recoveryStatus.recoveryDevice, recoveryStatus.objectLogRecoveryDevice, RecoveryPhase.Pass1);
        }

        /// <summary>
        /// Evict a page during recovery, first flushing it to the main log if it is a snapshot-region page. Snapshot-region pages
        /// (at or above <see cref="RecoveryStatus.snapshotScanFromAddress"/>) were read from the snapshot file and are not yet durable
        /// on the main log, so they are flushed — which also copies their objects from the snapshot object-log into the main object-log —
        /// before eviction, so an evicted record can be re-read from the main log/object-log on demand. Only pages that are actually
        /// evicted are flushed (replacing the former unconditional flush of every snapshot page). Hybrid-log-region pages, and all pages
        /// during hybrid-log recovery, are already durable, so we only await any in-flight flush before evicting. Recovery eviction is
        /// always bottom-up (ascending page order), which matches the object-log append order this copy relies on. The flush is awaited
        /// before <see cref="AllocatorBase{TStoreFunctions, TAllocator}.EvictPageForRecovery"/> frees the page so the async copy cannot
        /// read the page (or its slot, once the read loop reuses it) after it is freed.
        /// </summary>
        private void FlushIfNeededThenEvictPageForRecovery(RecoveryStatus recoveryStatus, long page)
        {
            var pageIndex = hlogBase.GetPageIndexForPage(page);
            if (recoveryStatus.snapshotScanFromAddress >= 0 && page >= hlogBase.GetPage(recoveryStatus.snapshotScanFromAddress))
                FlushSnapshotPageForRecovery(recoveryStatus, page);
            else
                recoveryStatus.WaitFlush(pageIndex);
            hlogBase.EvictPageForRecovery(page);
        }

        /// <summary>
        /// Flush a snapshot-region page to the main log (copying its objects from the snapshot object-log into the main object-log) and wait for the flush
        /// to complete, WITHOUT evicting it. Used both before evicting the page (see <see cref="FlushIfNeededThenEvictPageForRecovery"/>) and for the head
        /// page that straddles the final headAddress: the object-load pass keeps the page resident above the head cutoff but the records below the cutoff are
        /// disk-resident (below headAddress), so the page must be durable for them to be read back. Must be called before the page's objects are deserialized,
        /// while its records still carry their snapshot object-log positions.
        /// </summary>
        private void FlushSnapshotPageForRecovery(RecoveryStatus recoveryStatus, long page)
        {
            var pageIndex = hlogBase.GetPageIndexForPage(page);
            recoveryStatus.flushStatus[pageIndex] = FlushStatus.Pending;
            hlogBase.AsyncFlushPagesForRecovery(recoveryStatus.snapshotScanFromAddress, page, 1, AsyncFlushPageCallbackForRecovery,
                recoveryStatus, recoveryStatus.objectLogRecoveryDevice, formerFlushedUntilAddress: recoveryStatus.snapshotScanFromAddress);
            recoveryStatus.WaitFlush(pageIndex);
        }

        private void TrimLogPages(RecoveryStatus recoveryStatus, long tailPage, int numPagesToRead, long untilAddress)
        {
            if (hlogBase.logSizeTracker is null)
                return;

            var headPage = hlogBase.GetPage(recoveryStatus.headAddress);
            var loadedPages = tailPage - headPage + 1;
            var totalPagesNeeded = loadedPages + numPagesToRead;

            // Respect the usual MinEvictionHeadAddressLag tail lag. Snapshot pages that must be evicted here are flushed to the main log
            // first (FlushIfNeededThenEvictPageForRecovery), so read-time eviction is free to evict any page to honor the memory budget.
            var maxHeadAddress = untilAddress - LogSizeTracker.MinEvictionHeadAddressLag;

            // Evict already-resident pages (from prior read batches) from headAddress upward to make room for this batch. Two things bound the eviction:
            //  - headPage < tailPage: only evict pages that have actually been read; never advance headAddress past pages not yet read (this batch and
            //    beyond). Advancing past an un-read page would place it below headAddress (disk-resident) without ever flushing it, so it would read back
            //    empty. Trimming the just-read pages to the memory budget is deferred to the object-load pass, which flushes each page it evicts.
            //  - totalPagesNeeded > BufferSize (or over budget): evict enough that the resident pages plus this batch fit the circular buffer. The budget's
            //    highTargetSize can exceed BufferSize * PageSize (e.g. a power-of-two page budget leaves highTargetSize just over BufferSize pages), and
            //    evicting only to the budget would free fewer than numPagesToRead slots, so the read would reuse a still-resident slot and overwrite it.
            // This is during Pass1, so there are no objects to evict; we're evicting a full page each iteration.
            while (totalPagesNeeded > 1
                && headPage < tailPage
                && (totalPagesNeeded > hlogBase.BufferSize
                    || hlogBase.logSizeTracker.RemainingBudget < numPagesToRead * hlogBase.PageSize)
                && recoveryStatus.headAddress < maxHeadAddress)
            {
                var pageIndex = hlogBase.GetPageIndexForPage(headPage);
                if (hlogBase.IsAllocated(pageIndex))
                    FlushIfNeededThenEvictPageForRecovery(recoveryStatus, headPage);
                headPage++;
                recoveryStatus.headAddress = hlogBase.GetFirstValidLogicalAddressOnPage(headPage);
                totalPagesNeeded--;
            }
        }

        /// <summary>
        /// After the recovery read loop and deferred object load, evict object-free resident pages from headAddress upward until AllocatedPageCount is within
        /// <see cref="AllocatorBase{TStoreFunctions, TAllocator}.MaxAllocatedPageCount"/>, respecting <see cref="LogSizeTracker.MinEvictionHeadAddressLag"/>.
        /// The per-batch <see cref="TrimLogPages"/> reserves room for each upcoming read against the delta-padded highTargetSize budget and does not run after
        /// the final batch, so an object-free (inline) store can settle one page above the hard MaxAllocatedPageCount cap. Object-free snapshot pages are made
        /// durable on the main log by <see cref="FlushIfNeededThenEvictPageForRecovery"/> as they are evicted (re-read on demand); the walk stops at the first
        /// page with live objects, whose budget is governed by <see cref="RecoveryLoadObjectsPass2"/>'s heap-aware eviction. Dead pages below startPage (from
        /// store initialization) are freed up front in RecoverHybridLogAsync, so AllocatedPageCount here reflects only resident data and this budget walk is accurate.
        /// </summary>
        private void TrimResidentPagesToBudget(RecoveryStatus recoveryStatus, long untilAddress)
        {
            if (hlogBase.logSizeTracker is null)
                return;

            var maxHeadAddress = untilAddress - LogSizeTracker.MinEvictionHeadAddressLag;
            while (hlogBase.AllocatedPageCount > hlogBase.MaxAllocatedPageCount && recoveryStatus.headAddress < maxHeadAddress)
            {
                var hp = hlogBase.GetPage(recoveryStatus.headAddress);
                if (hlogBase.IsAllocated(hlogBase.GetPageIndexForPage(hp)))
                {
                    var objectIdMap = hlogBase._wrapper.GetPageObjectIdMap(hp);
                    if (objectIdMap is not null && objectIdMap.Count > 0)
                        break;
                    FlushIfNeededThenEvictPageForRecovery(recoveryStatus, hp);
                }
                recoveryStatus.headAddress = hlogBase.GetFirstValidLogicalAddressOnPage(hp + 1);
            }
        }

        private async ValueTask<long> ReadPagesForRecoveryAsync(long untilAddress, RecoveryStatus recoveryStatus, long endPage, int numPagesToReadPerIteration, long page, CancellationToken cancellationToken)
        {
            var readEndPage = Math.Min(page + numPagesToReadPerIteration, endPage);
            if (page < readEndPage)
            {
                var numPagesToRead = (int)(readEndPage - page);

                // Ensure that page slots that will be read into have been flushed from previous reads. Due to the use of a single read semaphore,
                // this must be done in batches of all flushes followed by all reads to ensure proper sequencing of reads when
                // usableCapacity != capacity (and thus the page-read index is not equal to the page-flush index).
                await WaitUntilAllPagesHaveBeenFlushedAsync(page, readEndPage, recoveryStatus, cancellationToken).ConfigureAwait(false);
                ReadPagesWithMemoryConstraint(untilAddress, recoveryStatus, page, readEndPage, numPagesToRead);
            }

            return readEndPage;
        }

        /// <summary>
        /// Asynchronously recover the hybrid log from hybrid log files (not snapshot files).
        /// </summary>
        /// <param name="scanFromAddress">The address to start scanning from; the lowest address at which we will bring pages into the circular buffer (may be in the middle of a page)</param>
        /// <param name="recoverFromAddress">The address from which to perform recovery (undo v+1 records and append to tag-chain tail)</param>
        /// <param name="untilAddress">The last address to scan; this is initially the tailAddress at the time of checkpoint flush, </param>
        /// <param name="nextVersion">The next version of the database at the time of checkpoint flush</param>
        /// <param name="checkpointType">The type of checkpoint</param>
        /// <param name="options">The recovery options</param>
        /// <param name="cancellationToken">The cancellation token</param>
        private async ValueTask<RecoveryStatus> RecoverHybridLogAsync(long scanFromAddress, long recoverFromAddress, long untilAddress, long nextVersion,
            CheckpointType checkpointType, long headAddress, RecoveryOptions options, CancellationToken cancellationToken)
        {
            var recoveryStatus = GetPageRangesToRead(scanFromAddress, untilAddress, checkpointType, out long startPage, out long endPage, out int numPagesToReadPerIteration);
            recoveryStatus.headAddress = headAddress;

            // Free any pages still allocated below startPage before reading. The store is freshly constructed for recovery with the allocator's minimum
            // pages allocated at page 0 (Head=Begin=Tail=0); when the checkpoint's BeginAddress is above page 0 those low pages lie below the first page we
            // read (startPage), and the upward-only read/eviction never reaches them. Freeing them up front keeps them out of AllocatedPageCount for the
            // whole budget-checked recovery, instead of carrying the dead pages through every budget check and reclaiming them at the end.
            for (var deadPage = 0L; deadPage < startPage && deadPage < hlogBase.BufferSize; deadPage++)
            {
                if (hlogBase.IsAllocated(hlogBase.GetPageIndexForPage(deadPage)))
                    hlogBase.EvictPageForRecovery(deadPage);
            }

            if (untilAddress <= scanFromAddress)
                return recoveryStatus;

            for (long page = startPage; page < endPage; page += numPagesToReadPerIteration)
            {
                var end = await ReadPagesForRecoveryAsync(untilAddress, recoveryStatus, endPage, numPagesToReadPerIteration, page, cancellationToken).ConfigureAwait(false);
                for (var p = page; p < end; p++)
                {
                    // Ensure page has been read into memory
                    var pageIndex = hlogBase.GetPageIndexForPage(p);
                    await recoveryStatus.WaitReadAsync(pageIndex, cancellationToken).ConfigureAwait(false);

                    // We make an extra pass to clear locks when reading every page back into memory
                    ClearBitsOnPage(p, untilAddress, in options, recoveryStatus.headAddress);
                    ProcessReadPageAndFlush(scanFromAddress, recoverFromAddress, untilAddress, nextVersion, in options, recoveryStatus, p, pageIndex);
                }
            }

            await WaitUntilAllPagesHaveBeenFlushedAsync(startPage, endPage, recoveryStatus, cancellationToken).ConfigureAwait(false);

            // Defer object loading when this is the hybrid-log phase of a snapshot recovery; RecoverHybridLogFromSnapshotFileAsync
            // loads the objects once after the snapshot pages have also been read (without their objects), so the final headAddress
            // (after eviction over the full recovered range) is honored. For FoldOver there is no following snapshot phase.
            if (checkpointType != CheckpointType.Snapshot)
            {
                RecoveryLoadObjectsPass2(recoveryStatus, recoveryStatus.headAddress, untilAddress, snapshotObjectLogDevice: null, snapshotBoundaryPage: long.MaxValue);
                TrimResidentPagesToBudget(recoveryStatus, untilAddress);
            }
            return recoveryStatus;
        }

        /// <summary>
        /// Get the range of pages to read from the hybrid log file(s) for recovery
        /// </summary>
        /// <param name="scanFromAddress">The address to start scanning from; the lowest address at which we will bring pages into the circular buffer (may be in the middle of a page)</param>
        /// <param name="untilAddress">The last address to scan; this is initially the tailAddress at the time of checkpoint flush, </param>
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

            // Read as many pages as buffer allows, leaving room for at least 1 page for eviction.
            numPagesToReadPerIteration = Math.Min(hlogBase.BufferSize - 1, (int)(endPage - startPage));

            // Never read more pages per batch than the memory budget allows. BufferSize can exceed MaxAllocatedPageCount when the budget is not a
            // power-of-two page count (e.g. a 23k budget => MaxAllocatedPageCount 5, BufferSize 8); reading a full BufferSize-1 batch would fill the
            // circular buffer above MaxAllocatedPageCount, leaving over-budget pages resident that read-time eviction (TrimLogPages) cannot reclaim
            // because they were read below the eviction floor (untilAddress - MinEvictionHeadAddressLag). MaxAllocatedPageCount is the allocator's hard
            // cap on AllocatedPageCount, so honoring it here keeps recovery within budget at every step (modulo the MinEvictionHeadAddressLag tail).
            if (hlogBase.logSizeTracker is not null && hlogBase.MaxAllocatedPageCount < numPagesToReadPerIteration)
                numPagesToReadPerIteration = hlogBase.MaxAllocatedPageCount;
            return new RecoveryStatus(hlogBase.BufferSize);
        }

        /// <summary>
        /// Process a page that has been read from the hybrid log file (not snapshot), and flush it if necessary
        /// </summary>
        /// <param name="scanFromAddress">The address to start scanning from; the lowest address at which we will bring pages into the circular buffer (may be in the middle of a page)</param>
        /// <param name="recoverFromAddress">The address from which to perform recovery (undo v+1 records and append to tag-chain tail)</param>
        /// <param name="untilAddress">The last address to scan; this is initially the tailAddress at the time of checkpoint flush, </param>
        /// <param name="nextVersion">The next version of the database at the time of checkpoint flush</param>
        /// <param name="options">The recovery options</param>
        /// <param name="recoveryStatus">The <see cref="RecoveryStatus"/> instance</param>
        /// <param name="page">The page number to process</param>
        /// <param name="pageIndex">The index of <paramref name="page"/> in the allocator's circular page buffer</param>
        private void ProcessReadPageAndFlush(long scanFromAddress, long recoverFromAddress, long untilAddress, long nextVersion, in RecoveryOptions options,
            RecoveryStatus recoveryStatus, long page, int pageIndex)
        {
            if (ProcessReadPage(recoverFromAddress, untilAddress, nextVersion, options, recoveryStatus, page, pageIndex))
            {
                // Page was modified due to undoFutureVersion. Flush it to disk; the callback issues the after-capacity read request if necessary.
                hlogBase.AsyncFlushPagesForRecovery(scanFromAddress, page, 1, AsyncFlushPageCallbackForRecovery, recoveryStatus);
                return;
            }

            // We do not need to flush
            recoveryStatus.flushStatus[pageIndex] = FlushStatus.Done;
        }

        /// <summary>
        /// Determine address ranges on a page that has been read from the hybrid log file (not snapshot), then recover from that page.
        /// </summary>
        /// <param name="recoverFromAddress">The address from which to perform recovery (undo v+1 records and append to tag-chain tail)</param>
        /// <param name="untilAddress">The last address to scan; this is initially the tailAddress at the time of checkpoint flush, </param>
        /// <param name="nextVersion">The next version of the database at the time of checkpoint flush</param>
        /// <param name="options">The recovery options</param>
        /// <param name="recoveryStatus">The <see cref="RecoveryStatus"/> instance</param>
        /// <param name="page">The page number to process</param>
        /// <param name="pageIndex">The index of <paramref name="page"/> in the allocator's circular page buffer</param>
        /// <returns></returns>
        private bool ProcessReadPage(long recoverFromAddress, long untilAddress, long nextVersion, in RecoveryOptions options, RecoveryStatus recoveryStatus,
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

        private async ValueTask WaitUntilAllPagesHaveBeenFlushedAsync(long startPage, long endPage, RecoveryStatus recoveryStatus, CancellationToken cancellationToken)
        {
            for (long page = startPage; page < endPage; page++)
                await recoveryStatus.WaitFlushAsync(hlogBase.GetPageIndexForPage(page), cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Asynchronously recover the hybrid log from snapshot files.
        /// </summary>
        /// <param name="scanFromAddress">The address to start scanning from; the lowest address at which we will bring pages into the circular buffer (may be in the middle of a page)</param>
        /// <param name="recoverFromAddress">The address from which to perform recovery (undo v+1 records and append to tag-chain tail)</param>
        /// <param name="untilAddress">The last address to scan; this is initially the tailAddress at the time of checkpoint flush, </param>
        /// <param name="snapshotStartAddress">The start of the mutable region; the FlushedUntilAddress at the start of the WAIT_FLUSH phase</param>
        /// <param name="snapshotEndAddress">The end of the snapshot; the tailAddress at the start of the WAIT_FLUSH phase</param>
        /// <param name="nextVersion">The next version of the database at the time of checkpoint flush</param>
        /// <param name="guid">The checkpoint token guid</param>
        /// <param name="headAddress">The headAddress resulting from the preceding hybrid-log recovery phase (the lowest resident address); seeds eviction tracking here</param>
        /// <param name="options">The recovery options</param>
        /// <returns>The final headAddress (lowest resident address) after reading the snapshot pages and loading objects</returns>
        private async ValueTask<long> RecoverHybridLogFromSnapshotFileAsync(long scanFromAddress, long recoverFromAddress, long untilAddress,
            long snapshotStartAddress, long snapshotEndAddress, long nextVersion, Guid guid, long headAddress, RecoveryOptions options,
            CancellationToken cancellationToken)
        {
            GetSnapshotPageRangesToRead(scanFromAddress, untilAddress, snapshotStartAddress, snapshotEndAddress, guid, out long startPage,
                out long endPage, out long snapshotEndPage, out var recoveryStatus, out int numPagesToReadPerIteration);

            // Seed the head from the preceding hybrid-log phase so the snapshot-read loop (TrimLogPages) and the deferred object load
            // evict from, and track, the correct lowest-resident address across the full recovered range.
            recoveryStatus.headAddress = headAddress;

            // Mark this as snapshot recovery: eviction (read-time and load-time) must flush a snapshot-region page to the main log before freeing
            // it (FlushIfNeededThenEvictPageForRecovery), since it was read from the snapshot file and is not otherwise durable. scanFromAddress is
            // the hybrid-log/snapshot boundary; pages at or above its page are the snapshot region (read from the snapshot device without their
            // objects), pages strictly below it are the hybrid-log region (already durable on the main log/object-log).
            recoveryStatus.snapshotScanFromAddress = scanFromAddress;
            var snapshotBoundaryPage = hlogBase.GetPage(scanFromAddress);

            // Notify application of checkpoint token before processing snapshot records
            if (storeFunctions.CallOnDiskRead)
                storeFunctions.OnRecovery(guid);

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

                        // We make an extra pass to clear locks when reading pages back into memory
                        ClearBitsOnPage(p, untilAddress, in options, recoveryStatus.headAddress, snapshotFromAddress: scanFromAddress);
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

                RecoverSnapshotPages(recoverFromAddress, untilAddress, nextVersion, in options, recoveryStatus, page, end);
            }

            await WaitUntilAllPagesHaveBeenFlushedAsync(startPage, endPage, recoveryStatus, cancellationToken).ConfigureAwait(false);

            // Deferred object load: a single top-down pass over the full recovered range [headAddress, untilAddress), evicting bottom-up to honor the
            // memory budget and flushing any evicted snapshot page to the main log first. Records on snapshot-region pages (snapshotBoundaryPage and above)
            // still carry their snapshot object-log positions, so their objects are read from the snapshot object-log device; hybrid-log-region pages (below
            // the boundary) reference the main object-log (null device). The device is selected per page inside the pass. This leaves a clean, unbroken set
            // of resident pages down to headAddress with their objects loaded.
            RecoveryLoadObjectsPass2(recoveryStatus, recoveryStatus.headAddress, untilAddress, recoveryStatus.objectLogRecoveryDevice, snapshotBoundaryPage);

            // Bring AllocatedPageCount within the hard MaxAllocatedPageCount cap for object-free pages (see TrimResidentPagesToBudget): the per-batch
            // read-time trim targets the delta-padded highTargetSize and does not run after the final batch, so an inline store can settle one page over.
            TrimResidentPagesToBudget(recoveryStatus, untilAddress);

            var finalHeadAddress = recoveryStatus.headAddress;
            recoveryStatus.Dispose();
            return finalHeadAddress;
        }

        /// <summary>
        /// Load (deserialize) objects for the recovered pages in the address range [<paramref name="fromAddress"/>, <paramref name="untilAddress"/>),
        /// in a single top-down pass, evicting pages bottom-up (flushing any evicted snapshot page first) to honor the memory budget when a size tracker
        /// is attached. The object-log device is selected per page at <paramref name="snapshotBoundaryPage"/>. The page range is derived from the addresses.
        /// </summary>
        /// <param name="recoveryStatus">The <see cref="RecoveryStatus"/> instance; its headAddress is the eviction floor and is advanced as pages are evicted</param>
        /// <param name="fromAddress">The lowest address whose objects are to be loaded (the load floor; pages below it are not loaded by this call)</param>
        /// <param name="untilAddress">The end of the range whose objects are to be loaded</param>
        /// <param name="snapshotObjectLogDevice">The snapshot object-log device, used for snapshot-region pages (at or above <paramref name="snapshotBoundaryPage"/>)
        ///     whose records still carry snapshot object-log positions; hybrid-log-region pages below the boundary read from the main object-log (null device).</param>
        /// <param name="snapshotBoundaryPage">The first snapshot-region page (the page of the hybrid-log/snapshot boundary); long.MaxValue for FoldOver (no snapshot region).</param>
        private void RecoveryLoadObjectsPass2(RecoveryStatus recoveryStatus, long fromAddress, long untilAddress, IDevice snapshotObjectLogDevice, long snapshotBoundaryPage)
        {
            if (fromAddress >= untilAddress)
                return;

            var startPage = hlogBase.GetPage(fromAddress);
            var endPage = hlogBase.GetPage(untilAddress);
            if (untilAddress > hlogBase.GetFirstValidLogicalAddressOnPage(endPage))
                endPage++;

            // Load all objects from fromAddress to untilAddress with no eviction when there is no size tracker.
            if (hlogBase.logSizeTracker is null)
            {
                for (var page = startPage; page < endPage; page++)
                {
                    var pageIndex = hlogBase.GetPageIndexForPage(page);
                    if (!hlogBase.IsAllocated(pageIndex))
                        continue;

                    var objectLogDevice = page >= snapshotBoundaryPage ? snapshotObjectLogDevice : null;
                    var pageFromAddress = page == startPage ? fromAddress : hlogBase.GetFirstValidLogicalAddressOnPage(page);
                    var pageUntilAddress = page == endPage - 1 ? untilAddress : hlogBase.GetLogicalAddressOfStartOfPage(page + 1);
                    hlogBase.LoadObjectsForRecoveryPass2(page, pageFromAddress, pageUntilAddress, objectLogDevice);
                }
                return;
            }

            // With a size tracker, iterate pages from highest (untilAddress) to lowest (fromAddress) with budget control, evicting pages (and moving headAddress up) as needed.
            var maxHeadAddress = untilAddress - LogSizeTracker.MinEvictionHeadAddressLag;

            for (var page = endPage - 1; page >= startPage; page--)
            {
                var pageIndex = hlogBase.GetPageIndexForPage(page);
                if (!hlogBase.IsAllocated(pageIndex))
                    continue;

                var pageFromAddress = Math.Max(fromAddress, hlogBase.GetFirstValidLogicalAddressOnPage(page));
                var pageUntilAddress = page == endPage - 1 ? untilAddress : hlogBase.GetLogicalAddressOfStartOfPage(page + 1);
                if (pageFromAddress >= pageUntilAddress)
                    continue;

                var objectLogDevice = page >= snapshotBoundaryPage ? snapshotObjectLogDevice : null;

                // Enforce MinEvictionHeadAddressLag: clamp pageFromAddress
                if (pageFromAddress > maxHeadAddress)
                    pageFromAddress = maxHeadAddress;

                var totalPageObjectSize = hlogBase.CalculatePageObjectSizes(page, pageFromAddress, pageUntilAddress);
                if (totalPageObjectSize == 0)
                {
                    hlogBase.LoadObjectsForRecoveryPass2(page, pageFromAddress, pageUntilAddress, objectLogDevice);
                    continue;
                }

                var remainingBudget = hlogBase.logSizeTracker.RemainingBudget;
                var pageCutoff = hlogBase.FindHeadAddressCutoffOnPage(page, pageUntilAddress, totalPageObjectSize, (int)(page - hlogBase.GetPage(recoveryStatus.headAddress)), remainingBudget, out var numPagesBelowToEvict);

                // Evict pages below if needed
                var currentHeadPage = hlogBase.GetPage(recoveryStatus.headAddress);
                while (numPagesBelowToEvict > 0 && currentHeadPage < page)
                {
                    var headPageIndex = hlogBase.GetPageIndexForPage(currentHeadPage);
                    if (hlogBase.IsAllocated(headPageIndex))
                        FlushIfNeededThenEvictPageForRecovery(recoveryStatus, currentHeadPage);

                    currentHeadPage++;
                    recoveryStatus.headAddress = hlogBase.GetFirstValidLogicalAddressOnPage(currentHeadPage);
                    numPagesBelowToEvict--;
                }

                // If the cutoff leaves records below it on this snapshot page, those records become disk-resident (below the final headAddress) while the
                // page stays resident above the cutoff, so flush the page to the main log first — keeping it resident — so they are durable and can be read
                // back. Must be before deserialization, while the records still carry their snapshot object-log positions.
                if (page >= snapshotBoundaryPage && pageCutoff > hlogBase.GetFirstValidLogicalAddressOnPage(page))
                    FlushSnapshotPageForRecovery(recoveryStatus, page);

                // Load objects, using per-record budget checking via DeserializeObjectsOnPage.
                // The method handles all records from pageCutoff to pageUntilAddress.
                hlogBase.LoadObjectsForRecoveryPass2(page, pageCutoff, pageUntilAddress, objectLogDevice);

                // After loading, recheck budget. If over budget, evict from headAddress up to and including loaded records.
                if (hlogBase.logSizeTracker.IsOverBudget && recoveryStatus.headAddress < maxHeadAddress)
                {
                    // Evict from headAddress upward until under budget or at the lag limit
                    while (hlogBase.logSizeTracker.IsOverBudget && recoveryStatus.headAddress < maxHeadAddress)
                    {
                        currentHeadPage = hlogBase.GetPage(recoveryStatus.headAddress);
                        if (currentHeadPage >= page)
                            break;

                        var headPageIndex = hlogBase.GetPageIndexForPage(currentHeadPage);
                        if (hlogBase.IsAllocated(headPageIndex))
                            FlushIfNeededThenEvictPageForRecovery(recoveryStatus, currentHeadPage);

                        recoveryStatus.headAddress = hlogBase.GetFirstValidLogicalAddressOnPage(currentHeadPage + 1);
                    }
                }

                // Advance headAddress to the cutoff ONLY when the current page is the head page (all pages below it were evicted above, so
                // headAddress is already on this page). In that case the cutoff is a sub-page boundary: records below it on this page are
                // evicted (and the page was flushed above, keeping it resident) so headAddress moves up to the cutoff. When pages below this
                // one remain resident (we evicted only enough whole pages to get under budget, and the cutoff is this page's start), headAddress
                // must stay at the lowest resident page — jumping it to this page would leave those still-resident pages below headAddress and
                // un-flushed, reading back empty. The loop then continues down to and loads those pages.
                if (pageCutoff > recoveryStatus.headAddress && hlogBase.GetPage(recoveryStatus.headAddress) == page)
                    recoveryStatus.headAddress = pageCutoff;

                // If headAddress is on or above the current page, we're done
                if (recoveryStatus.headAddress >= hlogBase.GetFirstValidLogicalAddressOnPage(page))
                    break;
            }
        }

        /// <summary>
        /// For each page in the snapshot from [page, end), rebuild the index from the page's records. Pages are read from the
        /// snapshot file without their objects; objects are loaded later by the single deferred <see cref="RecoveryLoadObjectsPass2"/>.
        /// Snapshot pages are made durable on the main log only when (and if) they are evicted, by <see cref="FlushIfNeededThenEvictPageForRecovery"/>.
        /// </summary>
        private void RecoverSnapshotPages(long recoverFromAddress, long untilAddress, long nextVersion, in RecoveryOptions options,
            RecoveryStatus recoveryStatus, long page, long end)
        {
            for (long p = page; p < end; p++)
            {
                int pageIndex = hlogBase.GetPageIndexForPage(p);

                var endLogicalAddress = hlogBase.GetLogicalAddressOfStartOfPage(p + 1);
                if (recoverFromAddress < endLogicalAddress && recoverFromAddress < untilAddress)
                    ProcessReadSnapshotPage(recoverFromAddress, untilAddress, nextVersion, options, recoveryStatus, p, pageIndex);
            }
        }

        /// <summary>
        /// Get the range of pages to read from the snapshot file(s) for recovery
        /// </summary>
        /// <param name="scanFromAddress">The address to start scanning from; the lowest address at which we will bring pages into the circular buffer (may be in the middle of a page)</param>
        /// <param name="untilAddress">The last address to scan; this is initially the tailAddress at the time of checkpoint flush, </param>
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
            var objectLogRecoveryDevice = hlogBase._wrapper.HasObjectLog ? checkpointManager.GetSnapshotObjectLogDevice(guid) : null;

            recoveryDevice.Initialize(hlogBase.GetMainLogSegmentSize());
            objectLogRecoveryDevice?.Initialize(hlogBase.GetObjectLogSegmentSize());
            recoveryStatus = new RecoveryStatus(hlogBase.BufferSize)
            {
                recoveryDevice = recoveryDevice,
                objectLogRecoveryDevice = objectLogRecoveryDevice,
                recoveryDevicePageOffset = snapshotStartPage
            };

            // Read as many pages as buffer allows, leaving room for at least 1 page for eviction.
            numPagesToReadPerIteration = Math.Min(hlogBase.BufferSize - 1, (int)(endPage - startPage));

            // Never read more pages per batch than the memory budget allows (see GetPageRangesToRead for the full rationale): BufferSize can exceed
            // MaxAllocatedPageCount when the budget is not a power-of-two page count, and a full BufferSize-1 batch would fill the circular buffer above
            // MaxAllocatedPageCount with pages read below the eviction floor that TrimLogPages cannot reclaim.
            if (hlogBase.logSizeTracker is not null && hlogBase.MaxAllocatedPageCount < numPagesToReadPerIteration)
                numPagesToReadPerIteration = hlogBase.MaxAllocatedPageCount;
        }

        private void ProcessReadSnapshotPage(long recoverFromAddress, long untilAddress, long nextVersion, in RecoveryOptions options, RecoveryStatus recoveryStatus, long page, int pageIndex)
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

        /// <param name="page">The page number to process</param>
        /// <param name="untilAddress">The last address to process on this page</param>
        /// <param name="options">Recovery options (headAddress determines if page is in-memory)</param>
        /// <param name="snapshotFromAddress">If > 0, records at or above this address will get OnRecoverySnapshotRead.
        /// Records below this address are main-log records that happened to share the boundary page with the snapshot.</param>
        private void ClearBitsOnPage(long page, long untilAddress, in RecoveryOptions options, long headAddress, long snapshotFromAddress = 0)
        {
            var startLogicalAddress = hlogBase.GetLogicalAddressOfStartOfPage(page);
            var endLogicalAddress = hlogBase.GetLogicalAddressOfStartOfPage(page + 1);
            var physicalAddress = hlogBase.GetPhysicalAddress(startLogicalAddress);

            // no need to clear locks for records that will not end up in main memory
            if (headAddress >= endLogicalAddress)
                return;

            var pageSize = hlogBase.GetPageSize();
            var endOffset = (untilAddress < endLogicalAddress) ? hlogBase.GetOffsetOnPage(untilAddress) : pageSize;

            long recordOffset = hlogBase.pageHeaderSize;
            while (recordOffset < endOffset)
            {
                var logRecord = new LogRecord(physicalAddress + recordOffset);
                logRecord.InfoRef.ClearBitsForDiskImages();
                if (storeFunctions.CallOnDiskRead)
                {
                    var recordLogicalAddress = startLogicalAddress + recordOffset;

                    // On the snapshot path, skip records below snapshotFromAddress; they are main-log records on the boundary page
                    // that were already processed (with OnDiskRead) in the main-log recovery pass.
                    if (snapshotFromAddress == 0 || recordLogicalAddress >= snapshotFromAddress)
                    {
                        storeFunctions.OnDiskRead(ref logRecord);

                        // OnRecoverySnapshotRead fires only for snapshot-file records.
                        if (snapshotFromAddress > 0)
                            storeFunctions.OnRecoverySnapshotRead(ref logRecord);
                    }
                }

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
                                     long pageStartLogicalAddress, long pageStartPhysicalAddress, in RecoveryOptions options)
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
                    HashEntryInfo hei = new(storeFunctions.GetKeyHashCode64(logRecord));
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
        /// Restore log; called from TsavoriteLog. TODO: This sync version is invoked via BumpCurrentEpoch, which doesn't have async support.
        /// </summary>
        /// <param name="beginAddress"></param>
        /// <param name="headAddress"></param>
        /// <param name="fromAddress"></param>
        /// <param name="untilAddress">The last address to scan; this is initially the tailAddress at the time of checkpoint flush, </param>
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
        /// <param name="untilAddress">The last address to scan; this is initially the tailAddress at the time of checkpoint flush, </param>
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
                AsyncReadPagesForRecovery(headPage, numPages, untilAddress, recoveryStatus, recoveryPhase: RecoveryPhase.None);
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