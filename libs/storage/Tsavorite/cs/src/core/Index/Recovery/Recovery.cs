// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    internal enum ReadStatus { Pending, Done, Error };
    internal enum FlushStatus { Pending, Done, Error };

    internal sealed class RecoveryStatus
    {
        public long endPage;
        public long snapshotEndPage;
        public long untilAddress;
        public int capacity;
        public int usableCapacity;
        public CheckpointType checkpointType;

        public IDevice recoveryDevice;
        public long recoveryDevicePageOffset;
        public IDevice objectLogRecoveryDevice;

        // These are circular buffers of 'capacity' size; the indexing wraps due to hlog.GetPageIndexForPage().
        public ReadStatus[] readStatus;
        public FlushStatus[] flushStatus;

        private readonly SemaphoreSlim readSemaphore = new(0);
        private readonly SemaphoreSlim flushSemaphore = new(0);

        public RecoveryStatus(int capacity, int emptyPageCount,
                              long endPage, long untilAddress, CheckpointType checkpointType)
        {
            this.capacity = capacity;
            this.usableCapacity = capacity - emptyPageCount;
            this.endPage = endPage;
            this.untilAddress = untilAddress;
            this.checkpointType = checkpointType;

            readStatus = new ReadStatus[capacity];
            flushStatus = new FlushStatus[capacity];
            for (int i = 0; i < capacity; i++)
            {
                flushStatus[i] = FlushStatus.Done;
                readStatus[i] = ReadStatus.Pending;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SignalRead(int pageIndex)
        {
            readStatus[pageIndex] = ReadStatus.Done;
            readSemaphore.Release();
        }

        internal void SignalReadError(int pageIndex)
        {
            readStatus[pageIndex] = ReadStatus.Error;
            readSemaphore.Release();
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
            flushSemaphore.Release();
        }

        internal void SignalFlushedError(int pageIndex)
        {
            flushStatus[pageIndex] = FlushStatus.Error;
            flushSemaphore.Release();
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
        /// <summary>
        /// Snapshot file end address (start address is always 0)
        /// </summary>
        public long snapshotFileEndAddress;
        /// <summary>
        /// Hybrid log file start address
        /// </summary>
        public long hybridLogFileStartAddress;
        /// <summary>
        /// Hybrid log file end address
        /// </summary>
        public long hybridLogFileEndAddress;
        /// <summary>
        /// Delta log tail address
        /// </summary>
        public long deltaLogTailAddress;
    }

    public partial class TsavoriteKV<Key, Value> : TsavoriteBase
    {
        private const long NoPageFreed = -1;

        /// <summary>
        /// GetLatestCheckpointTokens
        /// </summary>
        /// <param name="hlogToken"></param>
        /// <param name="indexToken"></param>
        public void GetLatestCheckpointTokens(out Guid hlogToken, out Guid indexToken)
        {
            GetClosestHybridLogCheckpointInfo(-1, out hlogToken, out var recoveredHlcInfo, out var _);
            GetClosestIndexCheckpointInfo(ref recoveredHlcInfo, out indexToken, out var _);
            recoveredHlcInfo.Dispose();
        }

        /// <summary>
        /// Get HLog latest version
        /// </summary>
        /// <returns></returns>
        public long GetLatestCheckpointVersion()
        {
            GetClosestHybridLogCheckpointInfo(-1, out var hlogToken, out var hlcInfo, out var _);
            hlcInfo.Dispose();
            if (hlogToken == default)
                return -1;
            using var current = new HybridLogCheckpointInfo();

            // Make sure we consider delta log in order to compute latest checkpoint version
            current.Recover(hlogToken, checkpointManager, hlog.LogPageSizeBits,
                out var _, true);
            return current.info.nextVersion;
        }

        /// <summary>
        /// Get size of snapshot files for token
        /// </summary>
        /// <param name="token"></param>
        /// <param name="version"></param>
        /// <returns></returns>
        public LogFileInfo GetLogFileSize(Guid token, long version = -1)
        {
            using var current = new HybridLogCheckpointInfo();
            // We find the latest checkpoint metadata for the given token, including scanning the delta log for the latest metadata
            current.Recover(token, checkpointManager, hlog.LogPageSizeBits,
                out var _, true, version);
            long snapshotDeviceOffset = hlog.GetPage(current.info.snapshotStartFlushedLogicalAddress) << hlog.LogPageSizeBits;
            return new LogFileInfo
            {
                snapshotFileEndAddress = current.info.snapshotFinalLogicalAddress - snapshotDeviceOffset,
                hybridLogFileStartAddress = hlog.GetPage(current.info.beginAddress) << hlog.LogPageSizeBits,
                hybridLogFileEndAddress = current.info.flushedLogicalAddress,
                deltaLogTailAddress = current.info.deltaTailAddress,
            };
        }

        /// <summary>
        /// Get size of index file for token
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public long GetIndexFileSize(Guid token)
        {
            IndexCheckpointInfo recoveredICInfo = new IndexCheckpointInfo();
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
            long closestVersion = long.MaxValue;
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
                    current.Recover(hybridLogToken, checkpointManager, hlog.LogPageSizeBits,
                        out var currCookie, false);
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

        private void FindRecoveryInfo(long requestedVersion, out HybridLogCheckpointInfo recoveredHlcInfo,
            out IndexCheckpointInfo recoveredICInfo)
        {
            logger?.LogInformation("********* Primary Recovery Information ********");

            GetClosestHybridLogCheckpointInfo(requestedVersion, out var closestToken, out recoveredHlcInfo, out recoveredCommitCookie);

            if (recoveredHlcInfo.IsDefault())
                throw new TsavoriteException("Unable to find valid HybridLog token");

            if (recoveredHlcInfo.deltaLog != null)
            {
                recoveredHlcInfo.Dispose();
                // need to actually scan delta log now
                recoveredHlcInfo.Recover(closestToken, checkpointManager, hlog.LogPageSizeBits, out _, true);
            }
            recoveredHlcInfo.info.DebugPrint(logger);

            GetClosestIndexCheckpointInfo(ref recoveredHlcInfo, out _, out recoveredICInfo);

            if (recoveredICInfo.IsDefault())
            {
                logger?.LogInformation("No index checkpoint found, recovering from beginning of log");
            }
        }

        private static bool IsCompatible(in IndexRecoveryInfo indexInfo, in HybridLogRecoveryInfo recoveryInfo)
        {
            var l1 = indexInfo.finalLogicalAddress;
            var l2 = recoveryInfo.finalLogicalAddress;
            return l1 <= l2;
        }

        private long InternalRecover(Guid indexToken, Guid hybridLogToken, int numPagesToPreload, bool undoNextVersion, long recoverTo)
        {
            GetRecoveryInfo(indexToken, hybridLogToken, out HybridLogCheckpointInfo recoveredHLCInfo, out IndexCheckpointInfo recoveredICInfo);
            if (recoverTo != -1 && recoveredHLCInfo.deltaLog == null)
            {
                throw new TsavoriteException("Recovering to a specific version within a token is only supported for incremental snapshots");
            }
            return InternalRecover(recoveredICInfo, recoveredHLCInfo, numPagesToPreload, undoNextVersion, recoverTo);
        }

        private ValueTask<long> InternalRecoverAsync(Guid indexToken, Guid hybridLogToken, int numPagesToPreload, bool undoNextVersion, long recoverTo, CancellationToken cancellationToken)
        {
            GetRecoveryInfo(indexToken, hybridLogToken, out HybridLogCheckpointInfo recoveredHLCInfo, out IndexCheckpointInfo recoveredICInfo);
            return InternalRecoverAsync(recoveredICInfo, recoveredHLCInfo, numPagesToPreload, undoNextVersion, recoverTo, cancellationToken);
        }

        private void GetRecoveryInfo(Guid indexToken, Guid hybridLogToken, out HybridLogCheckpointInfo recoveredHLCInfo, out IndexCheckpointInfo recoveredICInfo)
        {
            logger?.LogInformation("********* Primary Recovery Information ********");
            logger?.LogInformation("Index Checkpoint: {indexToken}", indexToken);
            logger?.LogInformation("HybridLog Checkpoint: {hybridLogToken}", hybridLogToken);


            // Recovery appropriate context information
            recoveredHLCInfo = new HybridLogCheckpointInfo();
            recoveredHLCInfo.Recover(hybridLogToken, checkpointManager, hlog.LogPageSizeBits, out recoveredCommitCookie, true);
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

            if (recoveredICInfo.IsDefault())
            {
                logger?.LogInformation("Invalid index checkpoint token, recovering from beginning of log");
            }
            else
            {
                // Check if the two checkpoints are compatible for recovery
                if (!IsCompatible(recoveredICInfo.info, recoveredHLCInfo.info))
                {
                    throw new TsavoriteException("Cannot recover from (" + indexToken.ToString() + "," + hybridLogToken.ToString() + ") checkpoint pair!\n");
                }
            }
        }

        /// <inheritdoc />
        public void Reset()
        {
            // Reset the hash index
            Array.Clear(state[resizeInfo.version].tableRaw, 0, state[resizeInfo.version].tableRaw.Length);
            overflowBucketsAllocator.Dispose();
            overflowBucketsAllocator = new MallocFixedPageSize<HashBucket>(logger);

            // Reset the hybrid log
            hlog.Reset();
        }


        private long InternalRecover(IndexCheckpointInfo recoveredICInfo, HybridLogCheckpointInfo recoveredHLCInfo, int numPagesToPreload, bool undoNextVersion, long recoverTo)
        {
            hlog.VerifyRecoveryInfo(recoveredHLCInfo, false);

            if (hlog.GetTailAddress() > hlog.GetFirstValidLogicalAddress(0))
            {
                logger?.LogInformation("Recovery called on non-empty log - resetting to empty state first. Make sure store is quiesced before calling Recover on a running store.");
                Reset();
            }

            if (!RecoverToInitialPage(recoveredICInfo, recoveredHLCInfo, out long recoverFromAddress))
                RecoverFuzzyIndex(recoveredICInfo);

            if (!SetRecoveryPageRanges(recoveredHLCInfo, numPagesToPreload, recoverFromAddress, out long tailAddress, out long headAddress, out long scanFromAddress))
                return -1;
            RecoveryOptions options = new(headAddress, recoveredHLCInfo.info.startLogicalAddress, undoNextVersion);

            long readOnlyAddress;
            long lastFreedPage;
            // Make index consistent for version v
            if (recoveredHLCInfo.info.useSnapshotFile == 0)
            {
                lastFreedPage = RecoverHybridLog(scanFromAddress, recoverFromAddress, recoveredHLCInfo.info.finalLogicalAddress, recoveredHLCInfo.info.nextVersion, CheckpointType.FoldOver, options);
                readOnlyAddress = tailAddress;
            }
            else
            {
                if (recoveredHLCInfo.info.flushedLogicalAddress < headAddress)
                    headAddress = recoveredHLCInfo.info.flushedLogicalAddress;

                // First recover from index starting point (fromAddress) to snapshot starting point (flushedLogicalAddress)
                lastFreedPage = RecoverHybridLog(scanFromAddress, recoverFromAddress, recoveredHLCInfo.info.flushedLogicalAddress, recoveredHLCInfo.info.nextVersion, CheckpointType.Snapshot, options);
                // Then recover snapshot into mutable region
                var snapshotLastFreedPage = RecoverHybridLogFromSnapshotFile(recoveredHLCInfo.info.flushedLogicalAddress, recoverFromAddress, recoveredHLCInfo.info.finalLogicalAddress, recoveredHLCInfo.info.snapshotStartFlushedLogicalAddress,
                                recoveredHLCInfo.info.snapshotFinalLogicalAddress, recoveredHLCInfo.info.nextVersion, recoveredHLCInfo.info.guid, options, recoveredHLCInfo.deltaLog, recoverTo);

                if (snapshotLastFreedPage != NoPageFreed)
                    lastFreedPage = snapshotLastFreedPage;

                readOnlyAddress = recoveredHLCInfo.info.flushedLogicalAddress;
            }

            DoPostRecovery(recoveredICInfo, recoveredHLCInfo, tailAddress, ref headAddress, ref readOnlyAddress, lastFreedPage);
            return recoveredHLCInfo.info.version;
        }

        private async ValueTask<long> InternalRecoverAsync(IndexCheckpointInfo recoveredICInfo, HybridLogCheckpointInfo recoveredHLCInfo, int numPagesToPreload, bool undoNextVersion, long recoverTo, CancellationToken cancellationToken)
        {
            hlog.VerifyRecoveryInfo(recoveredHLCInfo, false);

            if (hlog.GetTailAddress() > hlog.GetFirstValidLogicalAddress(0))
            {
                logger?.LogInformation("Recovery called on non-empty log - resetting to empty state first. Make sure store is quiesced before calling Recover on a running store.");
                Reset();
            }

            if (!RecoverToInitialPage(recoveredICInfo, recoveredHLCInfo, out long recoverFromAddress))
                await RecoverFuzzyIndexAsync(recoveredICInfo, cancellationToken).ConfigureAwait(false);

            if (!SetRecoveryPageRanges(recoveredHLCInfo, numPagesToPreload, recoverFromAddress, out long tailAddress, out long headAddress, out long scanFromAddress))
                return -1;
            RecoveryOptions options = new(headAddress, recoveredHLCInfo.info.startLogicalAddress, undoNextVersion);

            long readOnlyAddress;
            long lastFreedPage;
            // Make index consistent for version v
            if (recoveredHLCInfo.info.useSnapshotFile == 0)
            {
                lastFreedPage = await RecoverHybridLogAsync(scanFromAddress, recoverFromAddress, recoveredHLCInfo.info.finalLogicalAddress, recoveredHLCInfo.info.nextVersion, CheckpointType.FoldOver,
                                            options, cancellationToken).ConfigureAwait(false);
                readOnlyAddress = tailAddress;
            }
            else
            {
                if (recoveredHLCInfo.info.flushedLogicalAddress < headAddress)
                    headAddress = recoveredHLCInfo.info.flushedLogicalAddress;

                // First recover from index starting point (fromAddress) to snapshot starting point (flushedLogicalAddress)
                lastFreedPage = await RecoverHybridLogAsync(scanFromAddress, recoverFromAddress, recoveredHLCInfo.info.flushedLogicalAddress, recoveredHLCInfo.info.nextVersion, CheckpointType.Snapshot,
                                           new RecoveryOptions(headAddress, recoveredHLCInfo.info.startLogicalAddress, undoNextVersion), cancellationToken).ConfigureAwait(false);
                // Then recover snapshot into mutable region
                var snapshotLastFreedPage = await RecoverHybridLogFromSnapshotFileAsync(recoveredHLCInfo.info.flushedLogicalAddress, recoverFromAddress, recoveredHLCInfo.info.finalLogicalAddress, recoveredHLCInfo.info.snapshotStartFlushedLogicalAddress,
                                        recoveredHLCInfo.info.snapshotFinalLogicalAddress, recoveredHLCInfo.info.nextVersion, recoveredHLCInfo.info.guid, options, recoveredHLCInfo.deltaLog, recoverTo, cancellationToken).ConfigureAwait(false);

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
            var _head = (1 + (tailAddress >> hlog.LogPageSizeBits) - (hlog.GetCapacityNumPages() - hlog.MinEmptyPageCount)) << hlog.LogPageSizeBits;

            // If additional pages have been freed to accommodate heap memory constraints, adjust head address accordingly
            if (lastFreedPage != NoPageFreed)
            {
                var nextAddress = (lastFreedPage + 1) << hlog.LogPageSizeBits;
                if (_head < nextAddress)
                    _head = nextAddress;
            }

            if (_head > headAddress)
                headAddress = _head;
            if (readOnlyAddress < headAddress)
                readOnlyAddress = headAddress;

            // Recover session information
            hlog.RecoveryReset(tailAddress, headAddress, recoveredHLCInfo.info.beginAddress, readOnlyAddress);
            _recoveredSessions = recoveredHLCInfo.info.continueTokens;
            _recoveredSessionNameMap = recoveredHLCInfo.info.sessionNameMap;
            maxSessionID = Math.Max(recoveredHLCInfo.info.maxSessionID, maxSessionID);
            checkpointManager.OnRecovery(recoveredICInfo.info.token, recoveredHLCInfo.info.guid);
            recoveredHLCInfo.Dispose();
        }

        /// <summary>
        /// Compute recovery address and determine where to recover to
        /// </summary>
        /// <param name="recoveredICInfo">IndexCheckpointInfo</param>
        /// <param name="recoveredHLCInfo">HybridLogCheckpointInfo</param>
        /// <param name="recoverFromAddress">Address from which to perform recovery (undo v+1 records)</param>
        /// <returns>Whether we are recovering to the initial page</returns>
        private bool RecoverToInitialPage(IndexCheckpointInfo recoveredICInfo, HybridLogCheckpointInfo recoveredHLCInfo, out long recoverFromAddress)
        {
            // Ensure active state machine to null
            currentSyncStateMachine = null;

            // Set new system state after recovery
            systemState = SystemState.Make(Phase.REST, recoveredHLCInfo.info.version + 1);

            if (!recoveredICInfo.IsDefault() && recoveryCountdown != null)
            {
                Debug.WriteLine("Ignoring index checkpoint as we have already recovered index previously");
                recoveredICInfo = default;
            }

            if (recoveredICInfo.IsDefault())
            {
                // No index checkpoint - recover from begin of log
                recoverFromAddress = recoveredHLCInfo.info.beginAddress;

                // Unless we recovered previously until some hlog address
                if (hlog.FlushedUntilAddress > recoverFromAddress)
                    recoverFromAddress = hlog.FlushedUntilAddress;

                // Start recovery at least from beginning of fuzzy log region
                // Needed if we are recovering to the same checkpoint a second time, with undo
                // set to true during the second time.
                if (recoveredHLCInfo.info.startLogicalAddress < recoverFromAddress)
                    recoverFromAddress = recoveredHLCInfo.info.startLogicalAddress;
            }
            else
            {
                recoverFromAddress = recoveredHLCInfo.info.beginAddress;

                if (recoveredICInfo.info.startLogicalAddress > recoverFromAddress)
                {
                    // Index checkpoint given - recover to that
                    recoverFromAddress = recoveredICInfo.info.startLogicalAddress;
                    return false;
                }
            }

            return true;
        }

        private bool SetRecoveryPageRanges(HybridLogCheckpointInfo recoveredHLCInfo, int numPagesToPreload, long fromAddress, out long tailAddress, out long headAddress, out long scanFromAddress)
        {
            if ((recoveredHLCInfo.info.useSnapshotFile == 0) && (recoveredHLCInfo.info.finalLogicalAddress <= hlog.GetTailAddress()))
            {
                tailAddress = headAddress = scanFromAddress = default;
                return false;
            }

            // Recover segment offsets for object log
            if (recoveredHLCInfo.info.objectLogSegmentOffsets != null)
                Array.Copy(recoveredHLCInfo.info.objectLogSegmentOffsets,
                    hlog.GetSegmentOffsets(),
                    recoveredHLCInfo.info.objectLogSegmentOffsets.Length);

            tailAddress = recoveredHLCInfo.info.finalLogicalAddress;
            headAddress = recoveredHLCInfo.info.headAddress;
            if (numPagesToPreload != -1)
            {
                var head = (hlog.GetPage(tailAddress) - numPagesToPreload) << hlog.LogPageSizeBits;
                if (head > headAddress)
                    headAddress = head;
            }

            scanFromAddress = headAddress;
            if (fromAddress < scanFromAddress)
                scanFromAddress = fromAddress;

            // Adjust head address if we need to anyway preload
            if (scanFromAddress < headAddress)
            {
                headAddress = scanFromAddress;
                if (headAddress < recoveredHLCInfo.info.headAddress)
                    headAddress = recoveredHLCInfo.info.headAddress;
            }

            if (hlog.FlushedUntilAddress > scanFromAddress)
                scanFromAddress = hlog.FlushedUntilAddress;
            return true;
        }

        /// <summary>
        /// This method ensures that before 'pagesToRead' number of pages are read into memory, any previously allocated pages 
        /// that would cause total number of pages in memory to go beyond usableCapacity are freed. This is to ensure that 
        /// memory size constraint is maintained during recovery.
        /// Illustration with capacity 32, usableCapacity 20, pagesToRead 2:
        ///     beg: startPage - 32
        ///     end: startPage - 18
        /// We free these 14 pages, leaving 18 allocated, and then read 2, which fills up usableCapacity.
        /// The beg, end can only be zero on the first pass through the buffer, as the page number continuously increases 
        /// </summary>
        private void FreePagesBeyondUsableCapacity(long startPage, int capacity, int usableCapacity, int pagesToRead, RecoveryStatus recoveryStatus)
        {
            var beg = Math.Max(0, startPage - capacity);
            var end = Math.Max(0, startPage - (usableCapacity - pagesToRead));

            for (var page = beg; page < end; page++)
            {
                var pageIndex = hlog.GetPageIndexForPage(page);
                if (hlog.IsAllocated(pageIndex))
                {
                    recoveryStatus.WaitFlush(pageIndex);
                    hlog.EvictPage(page);
                }
            }
        }

        private void ReadPagesWithMemoryConstraint(long endAddress, int capacity, RecoveryStatus recoveryStatus, long page, long endPage, int numPagesToRead)
        {
            // Before reading in additional pages, make sure that any previously allocated pages that would violate the memory size
            // constraint are freed.
            FreePagesBeyondUsableCapacity(startPage: page, capacity: capacity, usableCapacity: capacity - hlog.MinEmptyPageCount, pagesToRead: numPagesToRead, recoveryStatus);

            // Issue request to read pages as much as possible
            for (var p = page; p < endPage; p++) recoveryStatus.readStatus[hlog.GetPageIndexForPage(p)] = ReadStatus.Pending;
            hlog.AsyncReadPagesFromDevice(page, numPagesToRead, endAddress,
                                          hlog.AsyncReadPagesCallbackForRecovery,
                                          recoveryStatus, recoveryStatus.recoveryDevicePageOffset,
                                          recoveryStatus.recoveryDevice, recoveryStatus.objectLogRecoveryDevice);
        }

        private long FreePagesToLimitHeapMemory(RecoveryStatus recoveryStatus, long page)
        {
            long lastFreedPage = NoPageFreed;
            if (hlog.IsSizeBeyondLimit == null)
                return lastFreedPage;

            // free up additional pages, one at a time, to bring memory usage under control starting with the earliest possible page
            for (var p = Math.Max(0, page - recoveryStatus.usableCapacity + 1); p < page && hlog.IsSizeBeyondLimit(); p++)
            {
                var pageIndex = hlog.GetPageIndexForPage(p);
                if (hlog.IsAllocated(pageIndex))
                {
                    recoveryStatus.WaitFlush(pageIndex);
                    hlog.EvictPage(p);
                    lastFreedPage = p;
                }
            }

            return lastFreedPage;
        }

        private long ReadPagesForRecovery(long untilAddress, RecoveryStatus recoveryStatus, long endPage, int capacity, int numPagesToReadPerIteration, long page)
        {
            var readEndPage = Math.Min(page + numPagesToReadPerIteration, endPage);
            if (page < readEndPage)
            {
                var numPagesToRead = (int)(readEndPage - page);

                // Ensure that page slots that will be read into, have been flushed from previous reads. Due to the use of a single read semaphore,
                // this must be done in batches of "all flushes' followed by "all reads" to ensure proper sequencing of reads when
                // usableCapacity != capacity (and thus the page-read index is not equal to the page-flush index).
                WaitUntilAllPagesHaveBeenFlushed(page, readEndPage, recoveryStatus);
                ReadPagesWithMemoryConstraint(untilAddress, capacity, recoveryStatus, page, readEndPage, numPagesToRead);
            }

            return readEndPage;
        }

        private async ValueTask<long> ReadPagesForRecoveryAsync(long untilAddress, RecoveryStatus recoveryStatus, long endPage, int capacity, int numPagesToReadPerIteration, long page, CancellationToken cancellationToken)
        {
            var readEndPage = Math.Min(page + numPagesToReadPerIteration, endPage);
            if (page < readEndPage)
            {
                var numPagesToRead = (int)(readEndPage - page);

                // Ensure that page slots that will be read into, have been flushed from previous reads. Due to the use of a single read semaphore,
                // this must be done in batches of "all flushes' followed by "all reads" to ensure proper sequencing of reads when
                // usableCapacity != capacity (and thus the page-read index is not equal to the page-flush index).
                await WaitUntilAllPagesHaveBeenFlushedAsync(page, readEndPage, recoveryStatus, cancellationToken).ConfigureAwait(false);
                ReadPagesWithMemoryConstraint(untilAddress, capacity, recoveryStatus, page, readEndPage, numPagesToRead);
            }

            return readEndPage;
        }

        private async Task<long> FreePagesToLimitHeapMemoryAsync(RecoveryStatus recoveryStatus, long page, CancellationToken cancellationToken)
        {
            long lastFreedPage = NoPageFreed;
            if (hlog.IsSizeBeyondLimit == null)
                return lastFreedPage;

            // free up additional pages, one at a time, to bring memory usage under control starting with the earliest possible page
            for (var p = Math.Max(0, page - recoveryStatus.usableCapacity + 1); p < page && hlog.IsSizeBeyondLimit(); p++)
            {
                var pageIndex = hlog.GetPageIndexForPage(p);
                if (hlog.IsAllocated(pageIndex))
                {
                    await recoveryStatus.WaitFlushAsync(pageIndex, cancellationToken);
                    hlog.EvictPage(p);
                    lastFreedPage = p;
                }
            }

            return lastFreedPage;
        }

        private long RecoverHybridLog(long scanFromAddress, long recoverFromAddress, long untilAddress, long nextVersion, CheckpointType checkpointType, RecoveryOptions options)
        {
            long lastFreedPage = NoPageFreed;
            if (untilAddress <= scanFromAddress)
                return lastFreedPage;
            var recoveryStatus = GetPageRangesToRead(scanFromAddress, untilAddress, checkpointType, out long startPage, out long endPage, out int capacity, out int numPagesToReadPerIteration);

            for (long page = startPage; page < endPage; page += numPagesToReadPerIteration)
            {
                var end = ReadPagesForRecovery(untilAddress, recoveryStatus, endPage, capacity, numPagesToReadPerIteration, page);

                for (var p = page; p < end; p++)
                {
                    // Ensure page has been read into memory
                    int pageIndex = hlog.GetPageIndexForPage(p);
                    recoveryStatus.WaitRead(pageIndex);

                    var freedPage = FreePagesToLimitHeapMemory(recoveryStatus, p);
                    if (freedPage != NoPageFreed)
                        lastFreedPage = freedPage;

                    // We make an extra pass to clear locks when reading every page back into memory
                    ClearLocksOnPage(p, options);

                    ProcessReadPageAndFlush(recoverFromAddress, untilAddress, nextVersion, options, recoveryStatus, p, pageIndex);
                }
            }

            WaitUntilAllPagesHaveBeenFlushed(startPage, endPage, recoveryStatus);
            return lastFreedPage;
        }

        private async ValueTask<long> RecoverHybridLogAsync(long scanFromAddress, long recoverFromAddress, long untilAddress, long nextVersion, CheckpointType checkpointType, RecoveryOptions options, CancellationToken cancellationToken)
        {
            long lastFreedPage = NoPageFreed;
            if (untilAddress <= scanFromAddress)
                return lastFreedPage;

            var recoveryStatus = GetPageRangesToRead(scanFromAddress, untilAddress, checkpointType, out long startPage, out long endPage, out int capacity, out int numPagesToReadPerIteration);

            for (long page = startPage; page < endPage; page += numPagesToReadPerIteration)
            {
                var end = await ReadPagesForRecoveryAsync(untilAddress, recoveryStatus, endPage, capacity, numPagesToReadPerIteration, page, cancellationToken).ConfigureAwait(false);

                for (var p = page; p < end; p++)
                {
                    // Ensure page has been read into memory
                    int pageIndex = hlog.GetPageIndexForPage(p);
                    await recoveryStatus.WaitReadAsync(pageIndex, cancellationToken).ConfigureAwait(false);

                    var freedPage = await FreePagesToLimitHeapMemoryAsync(recoveryStatus, p, cancellationToken).ConfigureAwait(false);
                    if (freedPage != NoPageFreed)
                        lastFreedPage = freedPage;

                    // We make an extra pass to clear locks when reading every page back into memory
                    ClearLocksOnPage(p, options);

                    ProcessReadPageAndFlush(recoverFromAddress, untilAddress, nextVersion, options, recoveryStatus, p, pageIndex);
                }
            }

            await WaitUntilAllPagesHaveBeenFlushedAsync(startPage, endPage, recoveryStatus, cancellationToken).ConfigureAwait(false);
            return lastFreedPage;
        }

        private RecoveryStatus GetPageRangesToRead(long scanFromAddress, long untilAddress, CheckpointType checkpointType, out long startPage, out long endPage, out int capacity, out int numPagesToReadPerIteration)
        {
            startPage = hlog.GetPage(scanFromAddress);
            endPage = hlog.GetPage(untilAddress);
            if (untilAddress > hlog.GetStartLogicalAddress(endPage) && untilAddress > scanFromAddress)
            {
                endPage++;
            }

            capacity = hlog.GetCapacityNumPages();
            int totalPagesToRead = (int)(endPage - startPage);

            // Leave out at least MinEmptyPageCount pages to maintain memory size during recovery
            // If heap memory is to be tracked, then read one page at a time to control memory usage
            numPagesToReadPerIteration = hlog.IsSizeBeyondLimit == null ? Math.Min(capacity - hlog.MinEmptyPageCount, totalPagesToRead) : 1;
            return new RecoveryStatus(capacity, hlog.MinEmptyPageCount, endPage, untilAddress, checkpointType);
        }

        private void ProcessReadPageAndFlush(long recoverFromAddress, long untilAddress, long nextVersion, RecoveryOptions options, RecoveryStatus recoveryStatus, long page, int pageIndex)
        {
            if (ProcessReadPage(recoverFromAddress, untilAddress, nextVersion, options, recoveryStatus, page, pageIndex))
            {
                // Page was modified due to undoFutureVersion. Flush it to disk; the callback issues the after-capacity read request if necessary.
                hlog.AsyncFlushPages(page, 1, AsyncFlushPageCallbackForRecovery, recoveryStatus);
                return;
            }

            // We do not need to flush
            recoveryStatus.flushStatus[pageIndex] = FlushStatus.Done;
        }

        private bool ProcessReadPage(long recoverFromAddress, long untilAddress, long nextVersion, RecoveryOptions options, RecoveryStatus recoveryStatus, long page, int pageIndex)
        {
            var startLogicalAddress = hlog.GetStartLogicalAddress(page);
            var endLogicalAddress = hlog.GetStartLogicalAddress(page + 1);
            var physicalAddress = hlog.GetPhysicalAddress(startLogicalAddress);

            if (recoverFromAddress >= endLogicalAddress)
                return false;

            var pageFromAddress = 0L;
            var pageUntilAddress = hlog.GetPageSize();

            if (recoverFromAddress > startLogicalAddress)
                pageFromAddress = hlog.GetOffsetInPage(recoverFromAddress);

            if (untilAddress < endLogicalAddress)
                pageUntilAddress = hlog.GetOffsetInPage(untilAddress);

            if (RecoverFromPage(recoverFromAddress, pageFromAddress, pageUntilAddress, startLogicalAddress, physicalAddress, nextVersion, options))
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
                recoveryStatus.WaitFlush(hlog.GetPageIndexForPage(page));
        }

        private async ValueTask WaitUntilAllPagesHaveBeenFlushedAsync(long startPage, long endPage, RecoveryStatus recoveryStatus, CancellationToken cancellationToken)
        {
            for (long page = startPage; page < endPage; page++)
                await recoveryStatus.WaitFlushAsync(hlog.GetPageIndexForPage(page), cancellationToken).ConfigureAwait(false);
        }

        private long RecoverHybridLogFromSnapshotFile(long scanFromAddress, long recoverFromAddress, long untilAddress, long snapshotStartAddress, long snapshotEndAddress, long nextVersion, Guid guid, RecoveryOptions options, DeltaLog deltaLog, long recoverTo)
        {
            long lastFreedPage = NoPageFreed;
            GetSnapshotPageRangesToRead(scanFromAddress, untilAddress, snapshotStartAddress, snapshotEndAddress, guid, out long startPage, out long endPage, out long snapshotEndPage, out int capacity, out var recoveryStatus, out int numPagesToReadPerIteration);

            for (long page = startPage; page < endPage; page += numPagesToReadPerIteration)
            {
                ReadPagesForRecovery(snapshotEndAddress, recoveryStatus, snapshotEndPage, capacity, numPagesToReadPerIteration, page);
                var end = Math.Min(page + numPagesToReadPerIteration, endPage);
                for (long p = page; p < end; p++)
                {
                    int pageIndex = hlog.GetPageIndexForPage(p);
                    if (p < snapshotEndPage)
                    {
                        // Ensure the page is read from file
                        recoveryStatus.WaitRead(pageIndex);
                        var freedPage = FreePagesToLimitHeapMemory(recoveryStatus, p);
                        if (freedPage != NoPageFreed)
                            lastFreedPage = freedPage;

                        // We make an extra pass to clear locks when reading pages back into memory
                        ClearLocksOnPage(p, options);
                    }
                    else
                    {
                        recoveryStatus.WaitFlush(pageIndex);
                        if (!hlog.IsAllocated(pageIndex))
                            hlog.AllocatePage(pageIndex);
                        else
                            hlog.ClearPage(pageIndex);
                    }
                }

                ApplyDelta(scanFromAddress, recoverFromAddress, untilAddress, nextVersion, options, deltaLog, recoverTo, endPage, snapshotEndPage, capacity, numPagesToReadPerIteration, recoveryStatus, page, end);
            }

            WaitUntilAllPagesHaveBeenFlushed(startPage, endPage, recoveryStatus);
            recoveryStatus.Dispose();
            return lastFreedPage;
        }

        private async ValueTask<long> RecoverHybridLogFromSnapshotFileAsync(long scanFromAddress, long recoverFromAddress, long untilAddress, long snapshotStartAddress, long snapshotEndAddress, long nextVersion, Guid guid, RecoveryOptions options, DeltaLog deltaLog, long recoverTo, CancellationToken cancellationToken)
        {
            long lastFreedPage = NoPageFreed;
            GetSnapshotPageRangesToRead(scanFromAddress, untilAddress, snapshotStartAddress, snapshotEndAddress, guid, out long startPage, out long endPage, out long snapshotEndPage, out int capacity, out var recoveryStatus, out int numPagesToReadPerIteration);

            for (long page = startPage; page < endPage; page += numPagesToReadPerIteration)
            {
                await ReadPagesForRecoveryAsync(snapshotEndAddress, recoveryStatus, snapshotEndPage, capacity, numPagesToReadPerIteration, page, cancellationToken).ConfigureAwait(false);

                var end = Math.Min(page + numPagesToReadPerIteration, endPage);
                for (long p = page; p < end; p++)
                {
                    int pageIndex = hlog.GetPageIndexForPage(p);
                    if (p < snapshotEndPage)
                    {
                        // Ensure the page is read from file
                        await recoveryStatus.WaitReadAsync(pageIndex, cancellationToken).ConfigureAwait(false);
                        var freedPage = await FreePagesToLimitHeapMemoryAsync(recoveryStatus, p, cancellationToken).ConfigureAwait(false);
                        if (freedPage != NoPageFreed)
                            lastFreedPage = freedPage;

                        // We make an extra pass to clear locks when reading pages back into memory
                        ClearLocksOnPage(p, options);
                    }
                    else
                    {
                        await recoveryStatus.WaitFlushAsync(pageIndex, cancellationToken).ConfigureAwait(false);
                        if (!hlog.IsAllocated(pageIndex))
                            hlog.AllocatePage(pageIndex);
                        else
                            hlog.ClearPage(pageIndex);
                    }
                }

                ApplyDelta(scanFromAddress, recoverFromAddress, untilAddress, nextVersion, options, deltaLog, recoverTo, endPage, snapshotEndPage, capacity, numPagesToReadPerIteration, recoveryStatus, page, end);
            }

            await WaitUntilAllPagesHaveBeenFlushedAsync(startPage, endPage, recoveryStatus, cancellationToken).ConfigureAwait(false);
            recoveryStatus.Dispose();
            return lastFreedPage;
        }

        private void ApplyDelta(long scanFromAddress, long recoverFromAddress, long untilAddress, long nextVersion, RecoveryOptions options, DeltaLog deltaLog, long recoverTo, long endPage, long snapshotEndPage, int capacity, int numPagesToRead, RecoveryStatus recoveryStatus, long page, long end)
        {
            hlog.ApplyDelta(deltaLog, page, end, recoverTo);

            for (long p = page; p < end; p++)
            {
                int pageIndex = hlog.GetPageIndexForPage(p);

                var endLogicalAddress = hlog.GetStartLogicalAddress(p + 1);
                if (recoverFromAddress < endLogicalAddress && recoverFromAddress < untilAddress)
                    ProcessReadSnapshotPage(recoverFromAddress, untilAddress, nextVersion, options, recoveryStatus, p, pageIndex);

                // Issue next read
                if (p + numPagesToRead < endPage)
                {
                    // Flush snapshot page to main log
                    recoveryStatus.flushStatus[pageIndex] = FlushStatus.Pending;
                    hlog.AsyncFlushPages(p, 1, AsyncFlushPageCallbackForRecovery, recoveryStatus);
                }
            }
        }

        private void GetSnapshotPageRangesToRead(long fromAddress, long untilAddress, long snapshotStartAddress, long snapshotEndAddress, Guid guid, out long startPage, out long endPage, out long snapshotEndPage, out int capacity,
                                                 out RecoveryStatus recoveryStatus, out int numPagesToReadPerIteration)
        {
            // Compute startPage and endPage
            startPage = hlog.GetPage(fromAddress);
            endPage = hlog.GetPage(untilAddress);
            if (untilAddress > hlog.GetStartLogicalAddress(endPage) && untilAddress > fromAddress)
                endPage++;
            long snapshotStartPage = hlog.GetPage(snapshotStartAddress);
            snapshotEndPage = hlog.GetPage(snapshotEndAddress);
            if (snapshotEndAddress > hlog.GetStartLogicalAddress(snapshotEndPage) && snapshotEndAddress > snapshotStartAddress)
                snapshotEndPage++;

            // By default first page has one extra record
            capacity = hlog.GetCapacityNumPages();
            var recoveryDevice = checkpointManager.GetSnapshotLogDevice(guid);
            var objectLogRecoveryDevice = checkpointManager.GetSnapshotObjectLogDevice(guid);

            recoveryDevice.Initialize(hlog.GetSegmentSize());
            objectLogRecoveryDevice.Initialize(-1);
            recoveryStatus = new RecoveryStatus(capacity, hlog.MinEmptyPageCount, endPage, untilAddress, CheckpointType.Snapshot)
            {
                recoveryDevice = recoveryDevice,
                objectLogRecoveryDevice = objectLogRecoveryDevice,
                recoveryDevicePageOffset = snapshotStartPage,
                snapshotEndPage = snapshotEndPage
            };

            // Initially issue read request for all pages that can be held in memory
            // If heap memory is to be tracked, then read one page at a time to control memory usage
            int totalPagesToRead = (int)(snapshotEndPage - startPage);
            numPagesToReadPerIteration = hlog.IsSizeBeyondLimit == null ? Math.Min(capacity - hlog.MinEmptyPageCount, totalPagesToRead) : 1;
        }

        private void ProcessReadSnapshotPage(long fromAddress, long untilAddress, long nextVersion, RecoveryOptions options, RecoveryStatus recoveryStatus, long page, int pageIndex)
        {
            // Page at hand
            var startLogicalAddress = hlog.GetStartLogicalAddress(page);
            var endLogicalAddress = hlog.GetStartLogicalAddress(page + 1);

            // Perform recovery if page is part of the re-do portion of log
            if (fromAddress < endLogicalAddress && fromAddress < untilAddress)
            {
                /*
                 * Handling corner-cases:
                 * ----------------------
                 * When fromAddress is in the middle of the page, then start recovery only from corresponding offset 
                 * in page. Similarly, if untilAddress falls in the middle of the page, perform recovery only until that
                 * offset. Otherwise, scan the entire page [0, PageSize)
                 */

                var pageFromAddress = 0L;
                var pageUntilAddress = hlog.GetPageSize();
                var physicalAddress = hlog.GetPhysicalAddress(startLogicalAddress);


                if (fromAddress > startLogicalAddress && fromAddress < endLogicalAddress)
                    pageFromAddress = hlog.GetOffsetInPage(fromAddress);
                if (endLogicalAddress > untilAddress)
                    pageUntilAddress = hlog.GetOffsetInPage(untilAddress);

                RecoverFromPage(fromAddress, pageFromAddress, pageUntilAddress,
                                startLogicalAddress, physicalAddress, nextVersion, options);
            }

            recoveryStatus.flushStatus[pageIndex] = FlushStatus.Done;
        }

        private unsafe void ClearLocksOnPage(long page, RecoveryOptions options)
        {
            var startLogicalAddress = hlog.GetStartLogicalAddress(page);
            var endLogicalAddress = hlog.GetStartLogicalAddress(page + 1);
            var physicalAddress = hlog.GetPhysicalAddress(startLogicalAddress);

            // no need to clear locks for records that will not end up in main memory
            if (options.headAddress >= endLogicalAddress) return;

            long untilLogicalAddressInPage = hlog.GetPageSize();
            long pointer = 0;

            while (pointer < untilLogicalAddressInPage)
            {
                long recordStart = physicalAddress + pointer;
                ref RecordInfo info = ref hlog.GetInfo(recordStart);
                info.ClearBitsForDiskImages();

                if (info.IsNull())
                    pointer += RecordInfo.GetLength();
                else
                {
                    int size = hlog.GetRecordSize(recordStart).Item2;
                    Debug.Assert(size <= hlog.GetPageSize());
                    pointer += size;
                }
            }
        }

        // Re-do the necessary log entries. We ensure that the InNewVersion test (to skip v+1 records)
        // runs ONLY for the fuzzy region (which has v and v+1 records) because the earlier parts may
        // have an incorrect InNewVersion status.
        private unsafe bool RecoverFromPage(long startRecoveryAddress,
                                     long fromLogicalAddressInPage,
                                     long untilLogicalAddressInPage,
                                     long pageLogicalAddress,
                                     long pagePhysicalAddress,
                                     long nextVersion, RecoveryOptions options)
        {
            bool touched = false;

            var pointer = default(long);
            var recordStart = default(long);

            pointer = fromLogicalAddressInPage;
            while (pointer < untilLogicalAddressInPage)
            {
                recordStart = pagePhysicalAddress + pointer;
                ref RecordInfo info = ref hlog.GetInfo(recordStart);

                if (info.IsNull())
                {
                    pointer += RecordInfo.GetLength();
                    continue;
                }

                if (!info.Invalid)
                {
                    HashEntryInfo hei = new(comparer.GetHashCode64(ref hlog.GetKey(recordStart)));
                    FindOrCreateTag(ref hei, hlog.BeginAddress);

                    bool ignoreRecord = ((pageLogicalAddress + pointer) >= options.fuzzyRegionStartAddress) && info.IsInNewVersion;
                    if (!options.undoNextVersion) ignoreRecord = false;

                    if (!ignoreRecord)
                    {
                        hei.entry.Address = pageLogicalAddress + pointer;
                        hei.entry.Tag = hei.tag;
                        hei.entry.Tentative = false;
                        hei.bucket->bucket_entries[hei.slot] = hei.entry.word;
                    }
                    else
                    {
                        touched = true;
                        info.SetInvalid();
                        if (info.PreviousAddress < startRecoveryAddress)
                        {
                            hei.entry.Address = info.PreviousAddress;
                            hei.entry.Tag = hei.tag;
                            hei.entry.Tentative = false;
                            hei.bucket->bucket_entries[hei.slot] = hei.entry.word;
                        }
                    }
                }
                pointer += hlog.GetRecordSize(recordStart).Item2;
            }

            return touched;
        }

        private void AsyncFlushPageCallbackForRecovery(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
            {
                logger?.LogError($"AsyncFlushPageCallbackForRecovery error: {errorCode}");
            }

            // Set the page status to flushed
            var result = (PageAsyncFlushResult<RecoveryStatus>)context;

            if (Interlocked.Decrement(ref result.count) == 0)
            {
                int pageIndex = hlog.GetPageIndexForPage(result.page);

                if (errorCode != 0)
                    result.context.SignalFlushedError(pageIndex);
                else
                    result.context.SignalFlushed(pageIndex);

                result.Free();
            }
        }

        internal static bool AtomicSwitch<Input, Output, Context>(TsavoriteExecutionContext<Input, Output, Context> fromCtx, TsavoriteExecutionContext<Input, Output, Context> toCtx, long version)
        {
            lock (toCtx)
            {
                if (toCtx.version < version)
                {
                    CopyContext(fromCtx, toCtx);
                    return true;
                }
            }
            return false;
        }
    }

    internal abstract partial class AllocatorBase<Key, Value> : IDisposable
    {
        /// <summary>
        /// Restore log
        /// </summary>
        /// <param name="beginAddress"></param>
        /// <param name="headAddress"></param>
        /// <param name="fromAddress"></param>
        /// <param name="untilAddress"></param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        public void RestoreHybridLog(long beginAddress, long headAddress, long fromAddress, long untilAddress, int numPagesToPreload = -1)
        {
            if (RestoreHybridLogInitializePages(beginAddress, headAddress, fromAddress, untilAddress, numPagesToPreload, out var recoveryStatus, out long headPage, out long tailPage))
            {
                for (long page = headPage; page <= tailPage; page++)
                    recoveryStatus.WaitRead(GetPageIndexForPage(page));
            }

            RecoveryReset(untilAddress, headAddress, beginAddress, untilAddress);
        }

        /// <summary>
        /// Restore log
        /// </summary>
        /// <param name="beginAddress"></param>
        /// <param name="headAddress"></param>
        /// <param name="fromAddress"></param>
        /// <param name="untilAddress"></param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="cancellationToken"></param>
        public async ValueTask RestoreHybridLogAsync(long beginAddress, long headAddress, long fromAddress, long untilAddress, int numPagesToPreload = -1, CancellationToken cancellationToken = default)
        {
            if (RestoreHybridLogInitializePages(beginAddress, headAddress, fromAddress, untilAddress, numPagesToPreload, out var recoveryStatus, out long headPage, out long tailPage))
            {
                for (long page = headPage; page <= tailPage; page++)
                    await recoveryStatus.WaitReadAsync(GetPageIndexForPage(page), cancellationToken).ConfigureAwait(false);
            }

            RecoveryReset(untilAddress, headAddress, beginAddress, untilAddress);
        }

        private bool RestoreHybridLogInitializePages(long beginAddress, long headAddress, long fromAddress, long untilAddress, int numPagesToPreload,
                                                     out RecoveryStatus recoveryStatus, out long headPage, out long tailPage)
        {
            if (numPagesToPreload != -1)
            {
                var head = (GetPage(untilAddress) - numPagesToPreload) << LogPageSizeBits;
                if (head > headAddress)
                    headAddress = head;
            }
            Debug.Assert(beginAddress <= headAddress);
            Debug.Assert(headAddress <= untilAddress);

            // Special cases: we do not load any records into memory
            if (
                (beginAddress == untilAddress) || // Empty log
                ((headAddress == untilAddress) && (GetOffsetInPage(headAddress) == 0)) // Empty in-memory page
                )
            {
                if (!IsAllocated(GetPageIndexForAddress(headAddress)))
                    AllocatePage(GetPageIndexForAddress(headAddress));
            }
            else
            {
                if (headAddress < fromAddress)
                {
                    tailPage = GetPage(fromAddress);
                    headPage = GetPage(headAddress);

                    recoveryStatus = new RecoveryStatus(GetCapacityNumPages(), MinEmptyPageCount, tailPage, untilAddress, 0);
                    for (int i = 0; i < recoveryStatus.capacity; i++)
                    {
                        recoveryStatus.readStatus[i] = ReadStatus.Done;
                    }

                    var numPages = 0;
                    for (var page = headPage; page <= tailPage; page++)
                    {
                        var pageIndex = GetPageIndexForPage(page);
                        recoveryStatus.readStatus[pageIndex] = ReadStatus.Pending;
                        numPages++;
                    }

                    AsyncReadPagesFromDevice(headPage, numPages, untilAddress, AsyncReadPagesCallbackForRecovery, recoveryStatus);
                    return true;
                }
            }

            recoveryStatus = default;
            headPage = tailPage = 0;
            return false;
        }

        internal unsafe void AsyncReadPagesCallbackForRecovery(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
            {
                logger?.LogError($"AsyncReadPagesCallbackForRecovery error: {errorCode}");
            }

            // Set the page status to "read done"
            var result = (PageAsyncReadResult<RecoveryStatus>)context;

            if (result.freeBuffer1 != null)
            {
                PopulatePage(result.freeBuffer1.GetValidPointer(), result.freeBuffer1.required_bytes, result.page);
                result.freeBuffer1.Return();
            }
            int pageIndex = GetPageIndexForPage(result.page);
            if (errorCode != 0)
                result.context.SignalReadError(pageIndex);
            else
                result.context.SignalRead(pageIndex);
        }
    }
}