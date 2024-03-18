// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    internal enum OperationType
    {
        READ,
        RMW,
        UPSERT,
        DELETE,
        CONDITIONAL_INSERT,
        CONDITIONAL_SCAN_PUSH,
    }

    [Flags]
    internal enum OperationStatus
    {
        // Completed Status codes

        /// <summary>
        /// Operation completed successfully, and a record with the specified key was found.
        /// </summary>
        SUCCESS = StatusCode.Found,

        /// <summary>
        /// Operation completed successfully, and a record with the specified key was not found; the operation may have created a new one.
        /// </summary>
        NOTFOUND = StatusCode.NotFound,

        /// <summary>
        /// Operation was canceled by the client.
        /// </summary>
        CANCELED = StatusCode.Canceled,

        /// <summary>
        /// The maximum range that directly maps to the <see cref="StatusCode"/> enumeration; the operation completed. 
        /// This is an internal code to reserve ranges in the <see cref="OperationStatus"/> enumeration.
        /// </summary>
        MAX_MAP_TO_COMPLETED_STATUSCODE = CANCELED,

        // Not-completed Status codes

        /// <summary>
        /// Retry operation immediately, within the current epoch. This is only used in situations where another thread does not need to do another operation 
        /// to bring things into a consistent state.
        /// </summary>
        RETRY_NOW,

        /// <summary>
        /// Retry operation immediately, after refreshing the epoch. This is used in situations where another thread may have done an operation that requires it
        /// to do a subsequent operation to bring things into a consistent state; that subsequent operation may require <see cref="LightEpoch.BumpCurrentEpoch()"/>.
        /// </summary>
        RETRY_LATER,

        /// <summary>
        /// I/O has been enqueued and the caller must go through <see cref="ITsavoriteContext{Key, Value, Input, Output, Context}.CompletePending(bool, bool)"/> or
        /// <see cref="ITsavoriteContext{Key, Value, Input, Output, Context}.CompletePendingWithOutputs(out CompletedOutputIterator{Key, Value, Input, Output, Context}, bool, bool)"/>,
        /// or one of the Async forms.
        /// </summary>
        RECORD_ON_DISK,

        /// <summary>
        /// A checkpoint is in progress so the operation must be retried internally after refreshing the epoch and updating the session context version.
        /// </summary>
        CPR_SHIFT_DETECTED,

        /// <summary>
        /// Allocation failed, due to a need to flush pages. Clients do not see this status directly; they see <see cref="Status.IsPending"/>.
        /// <list type="bullet">
        ///   <item>For Sync operations we retry this as part of <see cref="TsavoriteKV{Key, Value}.HandleImmediateRetryStatus{Input, Output, Context, TsavoriteSession}(OperationStatus, TsavoriteSession, ref TsavoriteKV{Key, Value}.PendingContext{Input, Output, Context})"/>.</item>
        ///   <item>For Async operations we retry this as part of the ".Complete(...)" or ".CompleteAsync(...)" operation on the appropriate "*AsyncResult{}" object.</item>
        /// </list>
        /// </summary>
        ALLOCATE_FAILED,

        /// <summary>
        /// An internal code to reserve ranges in the <see cref="OperationStatus"/> enumeration.
        /// </summary>
        BASIC_MASK = 0xFF,      // Leave plenty of space for future expansion

        ADVANCED_MASK = 0x700,  // Coordinate any changes with OperationStatusUtils.OpStatusToStatusCodeShif
        CREATED_RECORD = StatusCode.CreatedRecord << OperationStatusUtils.OpStatusToStatusCodeShift,
        INPLACE_UPDATED_RECORD = StatusCode.InPlaceUpdatedRecord << OperationStatusUtils.OpStatusToStatusCodeShift,
        COPY_UPDATED_RECORD = StatusCode.CopyUpdatedRecord << OperationStatusUtils.OpStatusToStatusCodeShift,
        COPIED_RECORD = StatusCode.CopiedRecord << OperationStatusUtils.OpStatusToStatusCodeShift,
        COPIED_RECORD_TO_READ_CACHE = StatusCode.CopiedRecordToReadCache << OperationStatusUtils.OpStatusToStatusCodeShift,
        // unused (StatusCode)0x60,
        // unused (StatusCode)0x70,
        EXPIRED = StatusCode.Expired << OperationStatusUtils.OpStatusToStatusCodeShift
    }

    internal static class OperationStatusUtils
    {
        // StatusCode has this in the high nybble of the first (only) byte; put it in the low nybble of the second byte here).
        // Coordinate any changes with OperationStatus.ADVANCED_MASK.
        internal const int OpStatusToStatusCodeShift = 4;

        internal static OperationStatus BasicOpCode(OperationStatus status) => status & OperationStatus.BASIC_MASK;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static OperationStatus AdvancedOpCode(OperationStatus status, StatusCode advancedStatusCode) => status | (OperationStatus)((int)advancedStatusCode << OpStatusToStatusCodeShift);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool TryConvertToCompletedStatusCode(OperationStatus advInternalStatus, out Status statusCode)
        {
            var internalStatus = BasicOpCode(advInternalStatus);
            if (internalStatus <= OperationStatus.MAX_MAP_TO_COMPLETED_STATUSCODE)
            {
                statusCode = new(advInternalStatus);
                return true;
            }
            statusCode = default;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsAppend(OperationStatus internalStatus)
        {
            var advInternalStatus = internalStatus & OperationStatus.ADVANCED_MASK;
            return advInternalStatus == OperationStatus.CREATED_RECORD || advInternalStatus == OperationStatus.COPY_UPDATED_RECORD;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsRetry(OperationStatus internalStatus) => internalStatus == OperationStatus.RETRY_NOW || internalStatus == OperationStatus.RETRY_LATER;
    }

    public partial class TsavoriteKV<Key, Value> : TsavoriteBase
    {
        internal struct PendingContext<Input, Output, Context>
        {
            // User provided information
            internal OperationType type;
            internal IHeapContainer<Key> key;
            internal IHeapContainer<Value> value;
            internal IHeapContainer<Input> input;
            internal Output output;
            internal Context userContext;
            internal long keyHash;

            // Some additional information about the previous attempt
            internal long id;
            internal long version;  // TODO unused?
            internal long logicalAddress;
            internal long serialNum;
            internal HashBucketEntry entry;

            // operationFlags values
            internal ushort operationFlags;
            internal const ushort kNoOpFlags = 0;
            internal const ushort kNoKey = 0x0001;
            internal const ushort kIsAsync = 0x0002;

            internal ReadCopyOptions readCopyOptions;

            internal RecordInfo recordInfo;
            internal long minAddress;
            internal WriteReason writeReason;   // for ConditionalCopyToTail

            // For flushing head pages on tail allocation.
            internal CompletionEvent flushEvent;

            // For RMW if an allocation caused the source record for a copy to go from readonly to below HeadAddress, or for any operation with CAS failure.
            internal long retryNewLogicalAddress;

            internal ScanCursorState<Key, Value> scanCursorState;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal PendingContext(long keyHash) => this.keyHash = keyHash;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal PendingContext(ReadCopyOptions sessionReadCopyOptions, ref ReadOptions readOptions, bool isAsync = false, bool noKey = false)
            {
                // The async flag is often set when the PendingContext is created, so preserve that.
                operationFlags = (ushort)((noKey ? kNoKey : kNoOpFlags) | (isAsync ? kIsAsync : kNoOpFlags));
                readCopyOptions = ReadCopyOptions.Merge(sessionReadCopyOptions, readOptions.CopyOptions);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal PendingContext(ReadCopyOptions readCopyOptions, bool isAsync = false, bool noKey = false)
            {
                // The async flag is often set when the PendingContext is created, so preserve that.
                operationFlags = (ushort)((noKey ? kNoKey : kNoOpFlags) | (isAsync ? kIsAsync : kNoOpFlags));
                this.readCopyOptions = readCopyOptions;
            }

            internal bool NoKey
            {
                readonly get => (operationFlags & kNoKey) != 0;
                set => operationFlags = value ? (ushort)(operationFlags | kNoKey) : (ushort)(operationFlags & ~kNoKey);
            }

            internal readonly bool HasMinAddress => minAddress != Constants.kInvalidAddress;

            internal bool IsAsync
            {
                readonly get => (operationFlags & kIsAsync) != 0;
                set => operationFlags = value ? (ushort)(operationFlags | kIsAsync) : (ushort)(operationFlags & ~kIsAsync);
            }

            internal long InitialEntryAddress
            {
                readonly get => recordInfo.PreviousAddress;
                set => recordInfo.PreviousAddress = value;
            }

            internal long InitialLatestLogicalAddress
            {
                readonly get => entry.Address;
                set => entry.Address = value;
            }

            public void Dispose()
            {
                key?.Dispose();
                key = default;
                value?.Dispose();
                value = default;
                input?.Dispose();
                input = default;
            }
        }

        internal sealed class TsavoriteExecutionContext<Input, Output, Context>
        {
            internal int sessionID;
            internal string sessionName;

            // Control automatic Read copy operations. These flags override flags specified at the TsavoriteKV level, but may be overridden on the individual Read() operations
            internal ReadCopyOptions ReadCopyOptions;

            internal long version;
            internal long serialNum;
            public Phase phase;

            public bool[] markers;
            public long totalPending;
            public Dictionary<long, PendingContext<Input, Output, Context>> ioPendingRequests;
            public AsyncCountDown pendingReads;
            public AsyncQueue<AsyncIOContext<Key, Value>> readyResponses;
            public List<long> excludedSerialNos;
            public int asyncPendingCount;
            public ISynchronizationStateMachine threadStateMachine;

            internal RevivificationStats RevivificationStats = new();

            public int SyncIoPendingCount => ioPendingRequests.Count - asyncPendingCount;

            public bool IsInV1 => phase switch
            {
                Phase.IN_PROGRESS => true,
                Phase.WAIT_INDEX_CHECKPOINT => true,
                Phase.WAIT_FLUSH => true,
                _ => false,
            };

            internal void MergeReadCopyOptions(ReadCopyOptions storeCopyOptions, ReadCopyOptions copyOptions)
                => ReadCopyOptions = ReadCopyOptions.Merge(storeCopyOptions, copyOptions);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void MergeRevivificationStatsTo(ref RevivificationStats to, bool reset) => RevivificationStats.MergeTo(ref to, reset);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void ResetRevivificationStats() => RevivificationStats.Reset();

            public bool HasNoPendingRequests => SyncIoPendingCount == 0;

            public void WaitPending(LightEpoch epoch)
            {
                if (SyncIoPendingCount > 0)
                {
                    try
                    {
                        epoch.Suspend();
                        readyResponses.WaitForEntry();
                    }
                    finally
                    {
                        epoch.Resume();
                    }
                }
            }

            public async ValueTask WaitPendingAsync(CancellationToken token = default)
            {
                if (SyncIoPendingCount > 0)
                    await readyResponses.WaitForEntryAsync(token).ConfigureAwait(false);
            }

            public bool InNewVersion => phase < Phase.REST;

            public TsavoriteExecutionContext<Input, Output, Context> prevCtx;
        }
    }

    /// <summary>
    /// Descriptor for a CPR commit point
    /// </summary>
    public struct CommitPoint
    {
        /// <summary>
        /// Serial number until which we have committed
        /// </summary>
        public long UntilSerialNo;

        /// <summary>
        /// List of operation serial nos excluded from commit
        /// </summary>
        public List<long> ExcludedSerialNos;
    }

    /// <summary>
    /// Recovery info for hybrid log
    /// </summary>
    public struct HybridLogRecoveryInfo
    {
        const int CheckpointVersion = 5;

        /// <summary>
        /// Guid
        /// </summary>
        public Guid guid;
        /// <summary>
        /// Use snapshot file
        /// </summary>
        public int useSnapshotFile;
        /// <summary>
        /// Version
        /// </summary>
        public long version;
        /// <summary>
        /// Next Version
        /// </summary>
        public long nextVersion;
        /// <summary>
        /// Flushed logical address; indicates the latest immutable address on the main Tsavorite log at checkpoint commit time.
        /// </summary>
        public long flushedLogicalAddress;
        /// <summary>
        /// Flushed logical address at snapshot start; indicates device offset for snapshot file
        /// </summary>
        public long snapshotStartFlushedLogicalAddress;
        /// <summary>
        /// Start logical address
        /// </summary>
        public long startLogicalAddress;
        /// <summary>
        /// Final logical address
        /// </summary>
        public long finalLogicalAddress;
        /// <summary>
        /// Snapshot end logical address: snaphot is [startLogicalAddress, snapshotFinalLogicalAddress)
        /// Note that finalLogicalAddress may be higher due to delta records
        /// </summary>
        public long snapshotFinalLogicalAddress;
        /// <summary>
        /// Head address
        /// </summary>
        public long headAddress;
        /// <summary>
        /// Begin address
        /// </summary>
        public long beginAddress;

        /// <summary>
        /// If true, there was at least one ITsavoriteContext implementation active that did manual locking at some point during the checkpoint;
        /// these pages must be scanned for lock cleanup.
        /// </summary>
        public bool manualLockingActive;

        /// <summary>
        /// Commit tokens per session restored during Restore()
        /// </summary>
        public ConcurrentDictionary<int, (string, CommitPoint)> continueTokens;

        /// <summary>
        /// Map of session name to session ID restored during Restore()
        /// </summary>
        public ConcurrentDictionary<string, int> sessionNameMap;

        /// <summary>
        /// Commit tokens per session created during Checkpoint
        /// </summary>
        public ConcurrentDictionary<int, (string, CommitPoint)> checkpointTokens;

        /// <summary>
        /// Max session ID
        /// </summary>
        public int maxSessionID;

        /// <summary>
        /// Object log segment offsets
        /// </summary>
        public long[] objectLogSegmentOffsets;


        /// <summary>
        /// Tail address of delta file: -1 indicates this is not a delta checkpoint metadata
        /// At recovery, this value denotes the delta tail address excluding the metadata record for the checkpoint
        /// because we create the metadata before writing to the delta file.
        /// </summary>
        public long deltaTailAddress;

        /// <summary>
        /// Initialize
        /// </summary>
        /// <param name="token"></param>
        /// <param name="_version"></param>
        public void Initialize(Guid token, long _version)
        {
            guid = token;
            useSnapshotFile = 0;
            version = _version;
            flushedLogicalAddress = 0;
            snapshotStartFlushedLogicalAddress = 0;
            startLogicalAddress = 0;
            finalLogicalAddress = 0;
            snapshotFinalLogicalAddress = 0;
            deltaTailAddress = -1; // indicates this is not a delta checkpoint metadata
            headAddress = 0;

            checkpointTokens = new();

            objectLogSegmentOffsets = null;
        }

        /// <summary>
        /// Initialize from stream
        /// </summary>
        /// <param name="reader"></param>
        public void Initialize(StreamReader reader)
        {
            continueTokens = new();

            string value = reader.ReadLine();
            var cversion = int.Parse(value);

            if (cversion != CheckpointVersion)
                throw new TsavoriteException($"Invalid checkpoint version {cversion} encountered, current version is {CheckpointVersion}, cannot recover with this checkpoint");

            value = reader.ReadLine();
            var checksum = long.Parse(value);

            value = reader.ReadLine();
            guid = Guid.Parse(value);

            value = reader.ReadLine();
            useSnapshotFile = int.Parse(value);

            value = reader.ReadLine();
            version = long.Parse(value);

            value = reader.ReadLine();
            nextVersion = long.Parse(value);

            value = reader.ReadLine();
            flushedLogicalAddress = long.Parse(value);

            value = reader.ReadLine();
            snapshotStartFlushedLogicalAddress = long.Parse(value);

            value = reader.ReadLine();
            startLogicalAddress = long.Parse(value);

            value = reader.ReadLine();
            finalLogicalAddress = long.Parse(value);

            value = reader.ReadLine();
            snapshotFinalLogicalAddress = long.Parse(value);

            value = reader.ReadLine();
            headAddress = long.Parse(value);

            value = reader.ReadLine();
            beginAddress = long.Parse(value);

            value = reader.ReadLine();
            deltaTailAddress = long.Parse(value);

            value = reader.ReadLine();
            manualLockingActive = bool.Parse(value);

            value = reader.ReadLine();
            var numSessions = int.Parse(value);

            for (int i = 0; i < numSessions; i++)
            {
                var sessionID = int.Parse(reader.ReadLine());
                var sessionName = reader.ReadLine();
                if (sessionName == "") sessionName = null;
                var serialno = long.Parse(reader.ReadLine());

                var exclusions = new List<long>();
                var exclusionCount = int.Parse(reader.ReadLine());
                for (int j = 0; j < exclusionCount; j++)
                    exclusions.Add(long.Parse(reader.ReadLine()));

                continueTokens.TryAdd(sessionID, (sessionName, new CommitPoint
                {
                    UntilSerialNo = serialno,
                    ExcludedSerialNos = exclusions
                }));
                if (sessionName != null)
                {
                    sessionNameMap ??= new();
                    sessionNameMap.TryAdd(sessionName, sessionID);
                }
                if (sessionID > maxSessionID) maxSessionID = sessionID;
            }

            // Read object log segment offsets
            value = reader.ReadLine();
            var numSegments = int.Parse(value);
            if (numSegments > 0)
            {
                objectLogSegmentOffsets = new long[numSegments];
                for (int i = 0; i < numSegments; i++)
                {
                    value = reader.ReadLine();
                    objectLogSegmentOffsets[i] = long.Parse(value);
                }
            }

            if (checksum != Checksum(continueTokens.Count))
                throw new TsavoriteException("Invalid checksum for checkpoint");
        }

        /// <summary>
        ///  Recover info from token
        /// </summary>
        /// <param name="token"></param>
        /// <param name="checkpointManager"></param>
        /// <param name="deltaLog"></param>
        /// <param name = "scanDelta">
        /// whether to scan the delta log to obtain the latest info contained in an incremental snapshot checkpoint.
        /// If false, this will recover the base snapshot info but avoid potentially expensive scans.
        /// </param>
        /// <param name="recoverTo"> specific version to recover to, if using delta log</param>
        internal void Recover(Guid token, ICheckpointManager checkpointManager, DeltaLog deltaLog = null, bool scanDelta = false, long recoverTo = -1)
        {
            var metadata = checkpointManager.GetLogCheckpointMetadata(token, deltaLog, scanDelta, recoverTo);
            if (metadata == null)
                throw new TsavoriteException("Invalid log commit metadata for ID " + token.ToString());
            using StreamReader s = new(new MemoryStream(metadata));
            Initialize(s);
        }

        /// <summary>
        ///  Recover info from token
        /// </summary>
        /// <param name="token"></param>
        /// <param name="checkpointManager"></param>
        /// <param name="deltaLog"></param>
        /// <param name="commitCookie"> Any user-specified commit cookie written as part of the checkpoint </param>
        /// <param name = "scanDelta">
        /// whether to scan the delta log to obtain the latest info contained in an incremental snapshot checkpoint.
        /// If false, this will recover the base snapshot info but avoid potentially expensive scans.
        /// </param>
        /// <param name="recoverTo"> specific version to recover to, if using delta log</param>

        internal void Recover(Guid token, ICheckpointManager checkpointManager, out byte[] commitCookie, DeltaLog deltaLog = null, bool scanDelta = false, long recoverTo = -1)
        {
            var metadata = checkpointManager.GetLogCheckpointMetadata(token, deltaLog, scanDelta, recoverTo);
            if (metadata == null)
                throw new TsavoriteException("Invalid log commit metadata for ID " + token.ToString());
            using StreamReader s = new(new MemoryStream(metadata));
            Initialize(s);
            if (scanDelta && deltaLog != null && deltaTailAddress >= 0)
            {
                // Adjust delta tail address to include the metadata record
                deltaTailAddress = deltaLog.NextAddress;
            }
            var cookie = s.ReadToEnd();
            commitCookie = cookie.Length == 0 ? null : Convert.FromBase64String(cookie);
        }

        /// <summary>
        /// Write info to byte array
        /// </summary>
        public byte[] ToByteArray()
        {
            using (MemoryStream ms = new())
            {
                using (StreamWriter writer = new(ms))
                {
                    writer.WriteLine(CheckpointVersion); // checkpoint version
                    writer.WriteLine(Checksum(checkpointTokens.Count)); // checksum

                    writer.WriteLine(guid);
                    writer.WriteLine(useSnapshotFile);
                    writer.WriteLine(version);
                    writer.WriteLine(nextVersion);
                    writer.WriteLine(flushedLogicalAddress);
                    writer.WriteLine(snapshotStartFlushedLogicalAddress);
                    writer.WriteLine(startLogicalAddress);
                    writer.WriteLine(finalLogicalAddress);
                    writer.WriteLine(snapshotFinalLogicalAddress);
                    writer.WriteLine(headAddress);
                    writer.WriteLine(beginAddress);
                    writer.WriteLine(deltaTailAddress);
                    writer.WriteLine(manualLockingActive);

                    writer.WriteLine(checkpointTokens.Count);
                    foreach (var kvp in checkpointTokens)
                    {
                        writer.WriteLine(kvp.Key);
                        writer.WriteLine(kvp.Value.Item1);
                        writer.WriteLine(kvp.Value.Item2.UntilSerialNo);
                        writer.WriteLine(kvp.Value.Item2.ExcludedSerialNos.Count);
                        foreach (long item in kvp.Value.Item2.ExcludedSerialNos)
                            writer.WriteLine(item);
                    }

                    // Write object log segment offsets
                    writer.WriteLine(objectLogSegmentOffsets == null ? 0 : objectLogSegmentOffsets.Length);
                    if (objectLogSegmentOffsets != null)
                    {
                        for (int i = 0; i < objectLogSegmentOffsets.Length; i++)
                        {
                            writer.WriteLine(objectLogSegmentOffsets[i]);
                        }
                    }
                }
                return ms.ToArray();
            }
        }

        private readonly long Checksum(int checkpointTokensCount)
        {
            var bytes = guid.ToByteArray();
            var long1 = BitConverter.ToInt64(bytes, 0);
            var long2 = BitConverter.ToInt64(bytes, 8);
            return long1 ^ long2 ^ version ^ flushedLogicalAddress ^ snapshotStartFlushedLogicalAddress ^ startLogicalAddress ^ finalLogicalAddress ^ snapshotFinalLogicalAddress ^ headAddress ^ beginAddress
                ^ checkpointTokensCount ^ (objectLogSegmentOffsets == null ? 0 : objectLogSegmentOffsets.Length);
        }

        /// <summary>
        /// Print checkpoint info for debugging purposes
        /// </summary>
        public readonly void DebugPrint(ILogger logger)
        {
            logger?.LogInformation("******** HybridLog Checkpoint Info for {guid} ********", guid);
            logger?.LogInformation("Version: {version}", version);
            logger?.LogInformation("Next Version: {nextVersion}", nextVersion);
            logger?.LogInformation("Is Snapshot?: {useSnapshotFile}", useSnapshotFile == 1);
            logger?.LogInformation("Flushed LogicalAddress: {flushedLogicalAddress}", flushedLogicalAddress);
            logger?.LogInformation("SnapshotStart Flushed LogicalAddress: {snapshotStartFlushedLogicalAddress}", snapshotStartFlushedLogicalAddress);
            logger?.LogInformation("Start Logical Address: {startLogicalAddress}", startLogicalAddress);
            logger?.LogInformation("Final Logical Address: {finalLogicalAddress}", finalLogicalAddress);
            logger?.LogInformation("Snapshot Final Logical Address: {snapshotFinalLogicalAddress}", snapshotFinalLogicalAddress);
            logger?.LogInformation("Head Address: {headAddress}", headAddress);
            logger?.LogInformation("Begin Address: {beginAddress}", beginAddress);
            logger?.LogInformation("Delta Tail Address: {deltaTailAddress}", deltaTailAddress);
            logger?.LogInformation("Manual Locking Active: {manualLockingActive}", manualLockingActive);
            logger?.LogInformation("Num sessions recovered: {continueTokensCount}", continueTokens.Count);
            logger?.LogInformation("Recovered sessions: ");
            foreach (var sessionInfo in continueTokens.Take(10))
            {
                logger?.LogInformation("{sessionInfo.Key}: {sessionInfo.Value}", sessionInfo.Key, sessionInfo.Value);
            }

            if (continueTokens.Count > 10)
                logger?.LogInformation("... {continueTokensSkipped} skipped", continueTokens.Count - 10);
        }
    }

    internal struct HybridLogCheckpointInfo : IDisposable
    {
        public HybridLogRecoveryInfo info;
        public IDevice snapshotFileDevice;
        public IDevice snapshotFileObjectLogDevice;
        public IDevice deltaFileDevice;
        public DeltaLog deltaLog;
        public SemaphoreSlim flushedSemaphore;
        public long prevVersion;

        public void Initialize(Guid token, long _version, ICheckpointManager checkpointManager)
        {
            info.Initialize(token, _version);
            checkpointManager.InitializeLogCheckpoint(token);
        }

        public void Dispose()
        {
            snapshotFileDevice?.Dispose();
            snapshotFileObjectLogDevice?.Dispose();
            deltaLog?.Dispose();
            deltaFileDevice?.Dispose();
            this = default;
        }

        public HybridLogCheckpointInfo Transfer()
        {
            // Ownership transfer of handles across struct copies
            var dest = this;
            dest.snapshotFileDevice = default;
            dest.snapshotFileObjectLogDevice = default;
            deltaLog = default;
            deltaFileDevice = default;
            return dest;
        }

        public void Recover(Guid token, ICheckpointManager checkpointManager, int deltaLogPageSizeBits,
            bool scanDelta = false, long recoverTo = -1)
        {
            deltaFileDevice = checkpointManager.GetDeltaLogDevice(token);
            if (deltaFileDevice is not null)
            {
                deltaFileDevice.Initialize(-1);
                if (deltaFileDevice.GetFileSize(0) > 0)
                {
                    deltaLog = new DeltaLog(deltaFileDevice, deltaLogPageSizeBits, -1);
                    deltaLog.InitializeForReads();
                    info.Recover(token, checkpointManager, deltaLog, scanDelta, recoverTo);
                    return;
                }
            }
            info.Recover(token, checkpointManager, null);
        }

        public void Recover(Guid token, ICheckpointManager checkpointManager, int deltaLogPageSizeBits,
            out byte[] commitCookie, bool scanDelta = false, long recoverTo = -1)
        {
            deltaFileDevice = checkpointManager.GetDeltaLogDevice(token);
            if (deltaFileDevice is not null)
            {
                deltaFileDevice.Initialize(-1);
                if (deltaFileDevice.GetFileSize(0) > 0)
                {
                    deltaLog = new DeltaLog(deltaFileDevice, deltaLogPageSizeBits, -1);
                    deltaLog.InitializeForReads();
                    info.Recover(token, checkpointManager, out commitCookie, deltaLog, scanDelta, recoverTo);
                    return;
                }
            }
            info.Recover(token, checkpointManager, out commitCookie);
        }

        public bool IsDefault()
        {
            return info.guid == default;
        }
    }

    internal struct IndexRecoveryInfo
    {
        const int CheckpointVersion = 1;
        public Guid token;
        public long table_size;
        public ulong num_ht_bytes;
        public ulong num_ofb_bytes;
        public int num_buckets;
        public long startLogicalAddress;
        public long finalLogicalAddress;

        public void Initialize(Guid token, long _size)
        {
            this.token = token;
            table_size = _size;
            num_ht_bytes = 0;
            num_ofb_bytes = 0;
            startLogicalAddress = 0;
            finalLogicalAddress = 0;
            num_buckets = 0;
        }

        public void Initialize(StreamReader reader)
        {
            string value = reader.ReadLine();
            var cversion = int.Parse(value);

            value = reader.ReadLine();
            var checksum = long.Parse(value);

            value = reader.ReadLine();
            token = Guid.Parse(value);

            value = reader.ReadLine();
            table_size = long.Parse(value);

            value = reader.ReadLine();
            num_ht_bytes = ulong.Parse(value);

            value = reader.ReadLine();
            num_ofb_bytes = ulong.Parse(value);

            value = reader.ReadLine();
            num_buckets = int.Parse(value);

            value = reader.ReadLine();
            startLogicalAddress = long.Parse(value);

            value = reader.ReadLine();
            finalLogicalAddress = long.Parse(value);

            if (cversion != CheckpointVersion)
                throw new TsavoriteException("Invalid version");

            if (checksum != Checksum())
                throw new TsavoriteException("Invalid checksum for checkpoint");
        }

        public void Recover(Guid guid, ICheckpointManager checkpointManager)
        {
            token = guid;
            var metadata = checkpointManager.GetIndexCheckpointMetadata(guid);
            if (metadata == null)
                throw new TsavoriteException("Invalid index commit metadata for ID " + guid.ToString());
            using (StreamReader s = new(new MemoryStream(metadata)))
                Initialize(s);
        }

        public readonly byte[] ToByteArray()
        {
            using (MemoryStream ms = new())
            {
                using (StreamWriter writer = new(ms))
                {
                    writer.WriteLine(CheckpointVersion); // checkpoint version
                    writer.WriteLine(Checksum()); // checksum

                    writer.WriteLine(token);
                    writer.WriteLine(table_size);
                    writer.WriteLine(num_ht_bytes);
                    writer.WriteLine(num_ofb_bytes);
                    writer.WriteLine(num_buckets);
                    writer.WriteLine(startLogicalAddress);
                    writer.WriteLine(finalLogicalAddress);
                }
                return ms.ToArray();
            }
        }

        private readonly long Checksum()
        {
            var bytes = token.ToByteArray();
            var long1 = BitConverter.ToInt64(bytes, 0);
            var long2 = BitConverter.ToInt64(bytes, 8);
            return long1 ^ long2 ^ table_size ^ (long)num_ht_bytes ^ (long)num_ofb_bytes
                        ^ num_buckets ^ startLogicalAddress ^ finalLogicalAddress;
        }

        public readonly void DebugPrint(ILogger logger)
        {
            logger?.LogInformation("******** Index Checkpoint Info for {token} ********", token);
            logger?.LogInformation("Table Size: {table_size}", table_size);
            logger?.LogInformation("Main Table Size (in GB): {num_ht_bytes}", ((double)num_ht_bytes) / 1000.0 / 1000.0 / 1000.0);
            logger?.LogInformation("Overflow Table Size (in GB): {num_ofb_bytes}", ((double)num_ofb_bytes) / 1000.0 / 1000.0 / 1000.0);
            logger?.LogInformation("Num Buckets: {num_buckets}", num_buckets);
            logger?.LogInformation("Start Logical Address: {startLogicalAddress}", startLogicalAddress);
            logger?.LogInformation("Final Logical Address: {finalLogicalAddress}", finalLogicalAddress);
        }

        public void Reset()
        {
            token = default;
            table_size = 0;
            num_ht_bytes = 0;
            num_ofb_bytes = 0;
            num_buckets = 0;
            startLogicalAddress = 0;
            finalLogicalAddress = 0;
        }
    }

    internal struct IndexCheckpointInfo
    {
        public IndexRecoveryInfo info;
        public IDevice main_ht_device;

        public void Initialize(Guid token, long _size, ICheckpointManager checkpointManager)
        {
            info.Initialize(token, _size);
            checkpointManager.InitializeIndexCheckpoint(token);
            main_ht_device = checkpointManager.GetIndexDevice(token);
        }

        public void Recover(Guid token, ICheckpointManager checkpointManager)
        {
            info.Recover(token, checkpointManager);
        }

        public void Reset()
        {
            info = default;
            main_ht_device?.Dispose();
            main_ht_device = null;
        }

        public bool IsDefault()
        {
            return info.token == default;
        }
    }
}