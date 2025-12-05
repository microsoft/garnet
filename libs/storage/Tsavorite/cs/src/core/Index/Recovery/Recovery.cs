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
        /// <summary>Capacity of the circular buffer; AllocatorBase.BufferSize.</summary>
        public int capacity;
        /// <summary>Usable capacity of the circular buffer; AllocatorBase.BufferSize - Allocator.EmptyPageCount.</summary>
        public int usableCapacity;

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

        public RecoveryStatus(int capacity, int emptyPageCount)
        {
            this.capacity = capacity;
            usableCapacity = capacity - emptyPageCount;

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
        /// <summary>Snapshot file end address (start address is always 0) </summary>
        public long snapshotFileEndAddress;

        /// <summary>Hybrid log file start address</summary>
        public long hybridLogFileStartAddress;

        /// <summary>Hybrid log file end address</summary>
        public long hybridLogFileEndAddress;

        /// <summary>Delta log tail address</summary>
        public long deltaLogTailAddress;

        /// <summary>Number of objectLog segments in the hybrid log</summary>
        public int hybridLogObjectSegmentCount;

        /// <summary>Number of objectLog segments in the snapshot</summary>
        public int snapshotObjectSegmentCount;
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
            var hasObjects = current.info.endObjectLogTail.HasData;
            var snapshotDeviceOffset = hlogBase.GetLogicalAddressOfStartOfPage(hlogBase.GetPage(current.info.snapshotStartFlushedLogicalAddress));
            return new LogFileInfo
            {
                // Hybrid (main log file) info:
                //   - The main log address range is from BeginAddress at PREPARE to the FlushedUntilAddress at PERSISTENCE_CALLBACK.
                //   - The snapshot address range is from the FlushedUntilAddress to the TailAddress, both taken at the start of WAIT_FLUSH. TailAddress
                //     here is the maximum logical address that will be written to the snapshot.
                // The overlap between the FlushedUntilAddress for the main log being recorded after the flush completes and the FlushedUntilAddress for the snapshot 
                // being recorded before the flush starts ensures there is no gap.
                hybridLogFileStartAddress = hlogBase.GetLogicalAddressOfStartOfPage(hlogBase.GetPage(current.info.beginAddress)),
                hybridLogFileEndAddress = current.info.flushedLogicalAddress,
                snapshotFileEndAddress = current.info.snapshotFinalLogicalAddress - snapshotDeviceOffset,

                // Object log file info:
                //   - The object log address range is from the start of the in-use object segment corresponding to main-log BeginAddress at PREPARE to the endObjectLogTail
                //     taken at PERSISTENCE_CALLBACK.
                //   - The snapshot address range is from the objectLogTail (taken at the start of WAIT_FLUSH as snapshotStartObjectLogTail, corresponding to the main log's
                //     FlushedUntilAddress at that point) to the snapshotEndObjectLogTail taken at PERSISTENCE_CALLBACK. The snapshotEndObjectLogTail grows during the Flush,
                //     so is not final until PERSISTENCE_CALLBACK; but it will only be written for records up to the TailAddress at the start of WAIT_FLUSH.
                // Note that there are no object-log segments for the mutable region of the hybrid log; they are not written until ReadOnlyAddress growth triggers
                // a main-log Flush. However the snapshot does cause object-log segments for the mutable range to be written.
                hybridLogObjectFileStartAddress = hasObjects ? current.info.lowestObjectLogSegmentInUse << current.info.endObjectLogTail.SegmentSizeBits,
                hybridLogObjectFileEndAddress = hasObjects ? current.info.snapshotStartObjectLogTail.CurrentAddress : 0,
                snapshotObjectFileEndAddress = hasObjects ? current.info.snapshotEndObjectLogTail.CurrentAddress : 0,

                // Delta log info
                deltaLogTailAddress = current.info.deltaTailAddress,



                //hybridLogObjectSegmentCount = current.info.startObjectLogTail.HasData ? current.info.startObjectLogTail.SegmentId + 1 : 0,   // +1 as it's a 0-based ordinal
                //snapshotObjectSegmentCount = current.info.snapshotEndObjectLogTail.HasData ? current.info.snapshotEndObjectLogTail.SegmentId - current.info.startObjectLogTail.SegmentId + 1 : 0
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
            RecoveryOptions options = new(headAddress, recoveredHLCInfo.info.startLogicalAddress, undoNextVersion);

            // Make index consistent for version v
            long readOnlyAddress, lastFreedPage;
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
            RecoveryOptions options = new(headAddress, recoveredHLCInfo.info.startLogicalAddress, undoNextVersion);

            // Make index consistent for version v
            long readOnlyAddress, lastFreedPage;
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
            var _head = hlogBase.GetFirstValidLogicalAddressOnPage(1 + hlogBase.GetPage(tailAddress) - (hlogBase.GetCapacityNumPages() - hlogBase.MinEmptyPageCount));

            // If additional pages have been freed to accommodate heap memory constraints, adjust head address accordingly
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
        /// Compute recovery address and determine where to recover to
        /// </summary>
        /// <param name="recoveredICInfo">IndexCheckpointInfo</param>
        /// <param name="recoveredHLCInfo">HybridLogCheckpointInfo</param>
        /// <param name="recoverFromAddress">Address from which to perform recovery (undo v+1 records)</param>
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

            if (recoveredICInfo.IsDefault)
            {
                // No index checkpoint - recover from begin of log
                recoverFromAddress = recoveredHLCInfo.info.beginAddress;

                // Unless we recovered previously until some hlog address
                if (hlogBase.FlushedUntilAddress > recoverFromAddress)
                    recoverFromAddress = hlogBase.FlushedUntilAddress;

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
            if ((recoveredHLCInfo.info.useSnapshotFile == 0) && (recoveredHLCInfo.info.finalLogicalAddress <= hlogBase.GetTailAddress()))
            {
                tailAddress = headAddress = scanFromAddress = default;
                return false;
            }

            RestoreMetadata(recoveredHLCInfo);

            tailAddress = recoveredHLCInfo.info.finalLogicalAddress;
            headAddress = recoveredHLCInfo.info.headAddress;
            if (numPagesToPreload != -1)
            {
                var head = hlogBase.GetFirstValidLogicalAddressOnPage(hlogBase.GetPage(tailAddress) - numPagesToPreload);
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

            if (hlogBase.FlushedUntilAddress > scanFromAddress)
                scanFromAddress = hlogBase.FlushedUntilAddress;
            return true;
        }

        private void RestoreMetadata(HybridLogCheckpointInfo recoveredHLCInfo)
        {
            // Recover object log tail position
            hlogBase.SetObjectLogTail(recoveredHLCInfo.info.snapshotEndObjectLogTail);
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
                var pageIndex = hlogBase.GetPageIndexForPage(page);
                if (hlogBase.IsAllocated(pageIndex))
                {
                    recoveryStatus.WaitFlush(pageIndex);
                    hlogBase.EvictPage(page);
                }
            }
        }

        private void ReadPagesWithMemoryConstraint(long endAddress, int capacity, RecoveryStatus recoveryStatus, long page, long endPage, int numPagesToRead)
        {
            // Before reading in additional pages, make sure that any previously allocated pages that would violate the memory size
            // constraint are freed.
            FreePagesBeyondUsableCapacity(startPage: page, capacity: capacity, usableCapacity: capacity - hlogBase.MinEmptyPageCount, pagesToRead: numPagesToRead, recoveryStatus);

            // Issue request to read pages as much as possible
            for (var p = page; p < endPage; p++)
                recoveryStatus.readStatus[hlogBase.GetPageIndexForPage(p)] = ReadStatus.Pending;
            hlogBase.AsyncReadPagesForRecovery(page, numPagesToRead, endAddress, hlogBase.AsyncReadPagesCallbackForRecovery, recoveryStatus,
                                          recoveryStatus.recoveryDevicePageOffset, recoveryStatus.recoveryDevice, recoveryStatus.objectLogRecoveryDevice);
        }

        private long FreePagesToLimitHeapMemory(RecoveryStatus recoveryStatus, long page)
        {
            long lastFreedPage = NoPageFreed;
            if (hlogBase.IsSizeBeyondLimit == null)
                return lastFreedPage;

            // free up additional pages, one at a time, to bring memory usage under control starting with the earliest possible page
            for (var p = Math.Max(0, page - recoveryStatus.usableCapacity + 1); p < page && hlogBase.IsSizeBeyondLimit(); p++)
            {
                var pageIndex = hlogBase.GetPageIndexForPage(p);
                if (hlogBase.IsAllocated(pageIndex))
                {
                    recoveryStatus.WaitFlush(pageIndex);
                    hlogBase.EvictPage(p);
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
            if (hlogBase.IsSizeBeyondLimit == null)
                return lastFreedPage;

            // free up additional pages, one at a time, to bring memory usage under control starting with the earliest possible page
            for (var p = Math.Max(0, page - recoveryStatus.usableCapacity + 1); p < page && hlogBase.IsSizeBeyondLimit(); p++)
            {
                var pageIndex = hlogBase.GetPageIndexForPage(p);
                if (hlogBase.IsAllocated(pageIndex))
                {
                    await recoveryStatus.WaitFlushAsync(pageIndex, cancellationToken);
                    hlogBase.EvictPage(p);
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
                    int pageIndex = hlogBase.GetPageIndexForPage(p);
                    recoveryStatus.WaitRead(pageIndex);

                    var freedPage = FreePagesToLimitHeapMemory(recoveryStatus, p);
                    if (freedPage != NoPageFreed)
                        lastFreedPage = freedPage;

                    // We make an extra pass to clear locks when reading every page back into memory
                    ClearBitsOnPage(p, options);
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
                    int pageIndex = hlogBase.GetPageIndexForPage(p);
                    await recoveryStatus.WaitReadAsync(pageIndex, cancellationToken).ConfigureAwait(false);

                    var freedPage = await FreePagesToLimitHeapMemoryAsync(recoveryStatus, p, cancellationToken).ConfigureAwait(false);
                    if (freedPage != NoPageFreed)
                        lastFreedPage = freedPage;

                    // We make an extra pass to clear locks when reading every page back into memory
                    ClearBitsOnPage(p, options);
                    ProcessReadPageAndFlush(recoverFromAddress, untilAddress, nextVersion, options, recoveryStatus, p, pageIndex);
                }
            }

            await WaitUntilAllPagesHaveBeenFlushedAsync(startPage, endPage, recoveryStatus, cancellationToken).ConfigureAwait(false);
            return lastFreedPage;
        }

        private RecoveryStatus GetPageRangesToRead(long scanFromAddress, long untilAddress, CheckpointType checkpointType, out long startPage, out long endPage, out int capacity, out int numPagesToReadPerIteration)
        {
            startPage = hlogBase.GetPage(scanFromAddress);
            endPage = hlogBase.GetPage(untilAddress);
            if (untilAddress > hlogBase.GetFirstValidLogicalAddressOnPage(endPage) && untilAddress > scanFromAddress)
                endPage++;

            capacity = hlogBase.GetCapacityNumPages();
            int totalPagesToRead = (int)(endPage - startPage);

            // Leave out at least MinEmptyPageCount pages to maintain memory size during recovery
            // If heap memory is to be tracked, then read one page at a time to control memory usage
            numPagesToReadPerIteration = hlogBase.IsSizeBeyondLimit == null ? Math.Min(capacity - hlogBase.MinEmptyPageCount, totalPagesToRead) : 1;
            return new RecoveryStatus(capacity, hlogBase.MinEmptyPageCount);
        }

        private void ProcessReadPageAndFlush(long recoverFromAddress, long untilAddress, long nextVersion, RecoveryOptions options, RecoveryStatus recoveryStatus, long page, int pageIndex)
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

        private bool ProcessReadPage(long recoverFromAddress, long untilAddress, long nextVersion, RecoveryOptions options, RecoveryStatus recoveryStatus, long page, int pageIndex)
        {
            var startLogicalAddress = hlogBase.GetFirstValidLogicalAddressOnPage(page);
            var endLogicalAddress = hlogBase.GetLogicalAddressOfStartOfPage(page + 1);
            var physicalAddress = hlogBase.GetPhysicalAddress(startLogicalAddress);

            if (recoverFromAddress >= endLogicalAddress)
                return false;

            var pageFromAddress = 0L;
            var pageUntilAddress = hlogBase.GetPageSize();

            if (recoverFromAddress > startLogicalAddress)
                pageFromAddress = hlogBase.GetOffsetOnPage(recoverFromAddress);

            if (untilAddress < endLogicalAddress)
                pageUntilAddress = hlogBase.GetOffsetOnPage(untilAddress);

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
                recoveryStatus.WaitFlush(hlogBase.GetPageIndexForPage(page));
        }

        private async ValueTask WaitUntilAllPagesHaveBeenFlushedAsync(long startPage, long endPage, RecoveryStatus recoveryStatus, CancellationToken cancellationToken)
        {
            for (long page = startPage; page < endPage; page++)
                await recoveryStatus.WaitFlushAsync(hlogBase.GetPageIndexForPage(page), cancellationToken).ConfigureAwait(false);
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
                    int pageIndex = hlogBase.GetPageIndexForPage(p);
                    if (p < snapshotEndPage)
                    {
                        // Ensure the page is read from file
                        recoveryStatus.WaitRead(pageIndex);
                        var freedPage = FreePagesToLimitHeapMemory(recoveryStatus, p);
                        if (freedPage != NoPageFreed)
                            lastFreedPage = freedPage;

                        // We make an extra pass to clear locks when reading pages back into memory
                        ClearBitsOnPage(p, options);
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
                    int pageIndex = hlogBase.GetPageIndexForPage(p);
                    if (p < snapshotEndPage)
                    {
                        // Ensure the page is read from file
                        await recoveryStatus.WaitReadAsync(pageIndex, cancellationToken).ConfigureAwait(false);
                        var freedPage = await FreePagesToLimitHeapMemoryAsync(recoveryStatus, p, cancellationToken).ConfigureAwait(false);
                        if (freedPage != NoPageFreed)
                            lastFreedPage = freedPage;

                        // We make an extra pass to clear locks when reading pages back into memory
                        ClearBitsOnPage(p, options);
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

                ApplyDelta(scanFromAddress, recoverFromAddress, untilAddress, nextVersion, options, deltaLog, recoverTo, endPage, snapshotEndPage, capacity, numPagesToReadPerIteration, recoveryStatus, page, end);
            }

            await WaitUntilAllPagesHaveBeenFlushedAsync(startPage, endPage, recoveryStatus, cancellationToken).ConfigureAwait(false);
            recoveryStatus.Dispose();
            return lastFreedPage;
        }

        private void ApplyDelta(long scanFromAddress, long recoverFromAddress, long untilAddress, long nextVersion, RecoveryOptions options, DeltaLog deltaLog, long recoverTo, long endPage, long snapshotEndPage, int capacity, int numPagesToRead, RecoveryStatus recoveryStatus, long page, long end)
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

        private void GetSnapshotPageRangesToRead(long fromAddress, long untilAddress, long snapshotStartAddress, long snapshotEndAddress, Guid guid, out long startPage, out long endPage, out long snapshotEndPage, out int capacity,
                                                 out RecoveryStatus recoveryStatus, out int numPagesToReadPerIteration)
        {
            // Compute startPage and endPage
            startPage = hlogBase.GetPage(fromAddress);
            endPage = hlogBase.GetPage(untilAddress);
            if (untilAddress > hlogBase.GetFirstValidLogicalAddressOnPage(endPage) && untilAddress > fromAddress)
                endPage++;
            long snapshotStartPage = hlogBase.GetPage(snapshotStartAddress);
            snapshotEndPage = hlogBase.GetPage(snapshotEndAddress);
            if (snapshotEndAddress > hlogBase.GetFirstValidLogicalAddressOnPage(snapshotEndPage) && snapshotEndAddress > snapshotStartAddress)
                snapshotEndPage++;

            // By default first page has one extra record
            capacity = hlogBase.GetCapacityNumPages();
            var recoveryDevice = checkpointManager.GetSnapshotLogDevice(guid);
            var objectLogRecoveryDevice = checkpointManager.GetSnapshotObjectLogDevice(guid);

            recoveryDevice.Initialize(hlogBase.GetMainLogSegmentSize());
            objectLogRecoveryDevice.Initialize(hlogBase.GetObjectLogSegmentSize());
            recoveryStatus = new RecoveryStatus(capacity, hlogBase.MinEmptyPageCount)
            {
                recoveryDevice = recoveryDevice,
                objectLogRecoveryDevice = objectLogRecoveryDevice,
                recoveryDevicePageOffset = snapshotStartPage
            };

            // Initially issue read request for all pages that can be held in memory
            // If heap memory is to be tracked, then read one page at a time to control memory usage
            int totalPagesToRead = (int)(snapshotEndPage - startPage);
            numPagesToReadPerIteration = hlogBase.IsSizeBeyondLimit == null ? Math.Min(capacity - hlogBase.MinEmptyPageCount, totalPagesToRead) : 1;
        }

        private void ProcessReadSnapshotPage(long fromAddress, long untilAddress, long nextVersion, RecoveryOptions options, RecoveryStatus recoveryStatus, long page, int pageIndex)
        {
            // Page at hand
            var startLogicalAddress = hlogBase.GetFirstValidLogicalAddressOnPage(page);
            var endLogicalAddress = hlogBase.GetLogicalAddressOfStartOfPage(page + 1);

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

                long pageFromAddress = PageHeader.Size;
                var pageUntilAddress = hlogBase.GetPageSize();
                var physicalAddress = hlogBase.GetPhysicalAddress(startLogicalAddress);

                if (fromAddress > startLogicalAddress && fromAddress < endLogicalAddress)
                    pageFromAddress = hlogBase.GetOffsetOnPage(fromAddress);
                if (endLogicalAddress > untilAddress)
                    pageUntilAddress = hlogBase.GetOffsetOnPage(untilAddress);

                _ = RecoverFromPage(fromAddress, pageFromAddress, pageUntilAddress, startLogicalAddress, physicalAddress, nextVersion, options);
            }

            recoveryStatus.flushStatus[pageIndex] = FlushStatus.Done;
        }

        private unsafe void ClearBitsOnPage(long page, RecoveryOptions options)
        {
            var startLogicalAddress = hlogBase.GetLogicalAddressOfStartOfPage(page);
            var endLogicalAddress = hlogBase.GetLogicalAddressOfStartOfPage(page + 1);
            var physicalAddress = hlogBase.GetPhysicalAddress(startLogicalAddress);

            // no need to clear locks for records that will not end up in main memory
            if (options.headAddress >= endLogicalAddress)
                return;

            var untilLogicalAddressInPage = hlogBase.GetPageSize();

            long recordOffset = PageHeader.Size;
            while (recordOffset < untilLogicalAddressInPage)
            {
                var logRecord = new LogRecord(physicalAddress + recordOffset);
                logRecord.InfoRef.ClearBitsForDiskImages();

                long recordSize = logRecord.AllocatedSize;
                Debug.Assert(recordSize > 0 && recordSize <= hlogBase.GetPageSize() - PageHeader.Size, $"recordSize {recordSize} must be > 0 and <= available page space {hlogBase.GetPageSize() - PageHeader.Size}");
                recordOffset += recordSize;
            }
        }

        /// <summary>
        /// Re-do the necessary log entries. We ensure that the InNewVersion test (to skip v+1 records) runs ONLY for the fuzzy region
        /// (which has v and v+1 records) because the earlier parts may have a stale InNewVersion status.
        /// </summary>
        private unsafe bool RecoverFromPage(long startRecoveryAddress, long fromLogicalAddressInPage, long untilLogicalAddressInPage,
                                     long pageLogicalAddress, long pagePhysicalAddress, long nextVersion, RecoveryOptions options)
        {
            var touched = false;

            var recordOffset = 0;
            while (recordOffset + PageHeader.Size < untilLogicalAddressInPage)
            {
                var recordStart = pagePhysicalAddress + recordOffset;
                var logRecord = new LogRecord(recordStart);
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

                    if ((pageLogicalAddress + recordOffset) < options.fuzzyRegionStartAddress || !info.IsInNewVersion || !options.undoNextVersion)
                    {
                        // Update the hash table with this record
                        hei.entry.Address = pageLogicalAddress + recordOffset;
                        hei.entry.Tag = hei.tag;
                        hei.entry.Tentative = false;
                        hei.bucket->bucket_entries[hei.slot] = hei.entry.word;
                    }
                    else
                    {
                        // Ignore this record
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

            if (Interlocked.Decrement(ref result.count) == 0)
            {
                var pageIndex = hlogBase.GetPageIndexForPage(result.page);
                if (errorCode != 0)
                    result.context.SignalFlushedError(pageIndex);
                else
                    result.context.SignalFlushed(pageIndex);
                result.Free();
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
        /// Restore log; called from TsavoriteLog
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
                if (!IsAllocated(GetPageIndexForAddress(headAddress)))
                    _wrapper.AllocatePage(GetPageIndexForAddress(headAddress));
            }
            else if (headAddress < fromAddress)
            {
                tailPage = GetPage(fromAddress);
                headPage = GetPage(headAddress);

                recoveryStatus = new RecoveryStatus(GetCapacityNumPages(), MinEmptyPageCount);
                for (int i = 0; i < recoveryStatus.capacity; i++)
                    recoveryStatus.readStatus[i] = ReadStatus.Done;

                var numPages = 0;
                for (var page = headPage; page <= tailPage; page++)
                {
                    var pageIndex = GetPageIndexForPage(page);
                    recoveryStatus.readStatus[pageIndex] = ReadStatus.Pending;
                    numPages++;
                }

                // Passing no objectLogDevice means we'll use the one in the allocator
                AsyncReadPagesForRecovery(headPage, numPages, untilAddress, AsyncReadPagesCallbackForRecovery, recoveryStatus);
                return true;
            }

            // fromAddress <= headAddress, so no pages to read
            recoveryStatus = default;
            headPage = tailPage = 0;
            return false;
        }

        internal unsafe void AsyncReadPagesCallbackForRecovery(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                logger?.LogError($"{nameof(AsyncReadPagesCallbackForRecovery)} error: {{errorCode}}", errorCode);

            // Set the page status to "read done"
            var result = (PageAsyncReadResult<RecoveryStatus>)context;

            var pageIndex = GetPageIndexForPage(result.page);
            if (errorCode != 0)
                result.context.SignalReadError(pageIndex);
            else
                result.context.SignalRead(pageIndex);
        }
    }
}