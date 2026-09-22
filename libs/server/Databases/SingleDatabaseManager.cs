// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server.Metrics;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Single logical database management
    /// </summary>
    internal class SingleDatabaseManager : DatabaseManagerBase
    {
        /// <inheritdoc/>
        public override GarnetDatabase DefaultDatabase => defaultDatabase;

        /// <inheritdoc/>
        public override int DatabaseCount => 1;

        public override int MaxDatabaseId => 0;

        readonly GarnetDatabase defaultDatabase;

        public SingleDatabaseManager(StoreWrapper.DatabaseCreatorDelegate createDatabaseDelegate, StoreWrapper storeWrapper, bool createDefaultDatabase = true) :
            base(createDatabaseDelegate, storeWrapper)
        {
            Logger = storeWrapper.loggerFactory?.CreateLogger(nameof(SingleDatabaseManager));

            // Create default database of index 0 (unless specified otherwise)
            if (createDefaultDatabase)
            {
                defaultDatabase = createDatabaseDelegate(0);
            }
        }

        public SingleDatabaseManager(SingleDatabaseManager src, bool enableAof) : this(src.CreateDatabaseDelegate, src.StoreWrapper, createDefaultDatabase: false)
        {
            defaultDatabase = new GarnetDatabase(0, src.DefaultDatabase, enableAof);
        }

        /// <inheritdoc/>
        public override GarnetDatabase TryGetOrAddDatabase(int dbId, out bool success, out bool added)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            success = true;
            added = false;
            return defaultDatabase;
        }

        /// <inheritdoc/>
        public override async ValueTask RecoverCheckpointAsync(bool replicaRecover = false, bool recoverFromToken = false, CheckpointMetadata metadata = null)
        {
            long storeVersion = 0;
            try
            {
                if (replicaRecover)
                {
                    ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Replication_Fail_Replica_Checkpoint_Recovery);
#if DEBUG
                    // Stand in for a transferred checkpoint whose metadata cannot be read. That surfaces from the
                    // token scan as a rejected-candidate result rather than a general failure, which is the case the
                    // handler below must not treat as a fresh start.
                    if (ExceptionInjectionHelper.IsEnabled(ExceptionInjectionType.Replication_Fail_Replica_Unreadable_Checkpoint))
                        throw new TsavoriteNoHybridLogException(
                            $"Exception injection triggered {nameof(ExceptionInjectionType.Replication_Fail_Replica_Unreadable_Checkpoint)}",
                            candidateTokenCount: 1, unreadableTokenCount: 1);
#endif

                    // Note: Since replicaRecover only pertains to cluster-mode, we can use the default store pointers (since multi-db mode is disabled in cluster-mode)
                    if (metadata!.storeIndexToken != default && metadata.storeHlogToken != default)
                    {
                        storeVersion = !recoverFromToken
                            ? await Store.RecoverAsync().ConfigureAwait(false)
                            : await Store.RecoverAsync(metadata.storeIndexToken, metadata.storeHlogToken).ConfigureAwait(false);
                    }

                    if (storeVersion > 0)
                        defaultDatabase.LastSaveTime = DateTimeOffset.UtcNow;
                }
                else
                {
                    storeVersion = await RecoverDatabaseCheckpointAsync(defaultDatabase).ConfigureAwait(false);
                }
            }
            catch (TsavoriteNoHybridLogException ex)
            {
                // Finding no hybrid log is not by itself a recovery error: a fresh start and an AOF-only database
                // both land here. Record what the scan saw so VerifyRecoveryIsComplete can tell those apart from a
                // checkpointed prefix that exists on disk but could not be read, once the AOF state is also known.
                defaultDatabase.CheckpointRecovery = new CheckpointRecoveryOutcome
                {
                    CandidateTokenCount = ex.CandidateTokenCount,
                    UnreadableTokenCount = ex.UnreadableTokenCount
                };

                if (ex.CandidateTokenCount == 0)
                {
                    // Nothing was ever written, so the server comes up empty and no disk state contradicts that.
                    // Recovery therefore cannot tell that a checkpoint the client was told had succeeded is missing,
                    // which is why a failed checkpoint must never be reported as a successful save; see
                    // RecordCheckpointOutcome.
                    Logger?.LogInformation(ex, "No Hybrid Log found for recovery; storeVersion = {storeVersion};", storeVersion);
                }
                else
                {
                    Logger?.LogError(ex,
                        "Unable to read any of the {candidateTokenCount} HybridLog checkpoint token(s) found on disk; storeVersion = {storeVersion};",
                        ex.CandidateTokenCount, storeVersion);

                    // A replica that continues here would hold an incomplete store while still advertising the
                    // replication offset the primary sent it, diverging from the primary with nothing to signal it.
                    // Fail the sync instead, independently of FailOnRecoveryError, which governs standalone startup.
                    if (replicaRecover)
                        throw;
                }
            }
            catch (Exception ex)
            {
                // Unless FailOnRecoveryError is set the server continues with whatever was recovered, so this must
                // be visible at the default log level.
                Logger?.LogError(ex, "Error during recovery of store; storeVersion = {storeVersion};", storeVersion);

                // A replica that continues here would hold an incomplete store while still advertising the
                // replication offset the primary sent it, diverging from the primary with nothing to signal it.
                // Fail the sync instead, independently of FailOnRecoveryError, which governs standalone startup.
                if (replicaRecover || StoreWrapper.serverOptions.FailOnRecoveryError)
                    throw;
            }

            // Once everything is setup, initialize the VectorManager
            defaultDatabase.VectorManager.Initialize();
        }

        /// <inheritdoc/>
        public override bool TryPauseCheckpoints(int dbId)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            return TryPauseCheckpoints(defaultDatabase);
        }

        /// <inheritdoc/>
        public override void ResumeCheckpoints(int dbId)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            ResumeCheckpoints(defaultDatabase);
        }

        /// <inheritdoc/>
        public override async Task<CheckpointStatus> TakeCheckpointAsync(bool background, int dbId = -1, CancellationToken token = default, ILogger logger = null)
        {
            if (dbId != -1 && dbId != 0)
                throw new ArgumentOutOfRangeException(nameof(dbId), dbId, "SingleDatabaseManager only supports dbId 0.");

            // Check if checkpoint already in progress
            if (!TryPauseCheckpoints(defaultDatabase.Id))
                return CheckpointStatus.AlreadyInProgress;

            var checkpointTask = TakeCheckpointHelperAsync(defaultDatabase, logger, token);
            if (background)
                return CheckpointStatus.Success;

            return await checkpointTask.ConfigureAwait(false) ? CheckpointStatus.Success : CheckpointStatus.Failed;

            async Task<bool> TakeCheckpointHelperAsync(GarnetDatabase defaultDatabase, ILogger logger, CancellationToken token)
            {
                try
                {
                    var result = await TakeCheckpointAsync(defaultDatabase, logger: logger, token: token).ConfigureAwait(false);
                    RecordCheckpointOutcome(defaultDatabase, result);
                    return result.IsSuccessful;
                }
                finally
                {
                    ResumeCheckpoints(defaultDatabase.Id);
                }
            }
        }

        /// <inheritdoc/>
        public override async Task TakeOnDemandCheckpointAsync(DateTimeOffset entryTime, int dbId = 0)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            // Take lock to ensure no other task will be taking a checkpoint
            while (!TryPauseCheckpoints(dbId))
                await Task.Yield();

            try
            {
                // If an external task has taken a checkpoint beyond the provided entryTime return
                if (defaultDatabase.LastSaveTime > entryTime)
                    return;

                // Necessary to take a checkpoint because the latest checkpoint is before entryTime
                var result = await TakeCheckpointAsync(defaultDatabase, logger: Logger).ConfigureAwait(false);
                RecordCheckpointOutcome(defaultDatabase, result);
            }
            finally
            {
                ResumeCheckpoints(dbId);
            }
        }

        /// <inheritdoc/>
        public override async Task TaskCheckpointBasedOnAofSizeLimitAsync(long aofSizeLimit,
            CancellationToken token = default, ILogger logger = null)
        {
            var aofSize = StoreWrapper.AofSize();
            if (aofSize <= aofSizeLimit) return;

            if (!await TryPauseCheckpointsContinuousAsync(defaultDatabase.Id, token: token).ConfigureAwait(false))
                return;

            try
            {
                // Checkpoint will be triggered from AOF replay
                if (StoreWrapper.serverOptions.EnableCluster && StoreWrapper.clusterProvider.IsReplica())
                {
                    logger?.LogInformation("Replica skipping {method}", nameof(TaskCheckpointBasedOnAofSizeLimitAsync));
                    return;
                }

                logger?.LogInformation("Enforcing AOF size limit currentAofSize: {aofSize} >  AofSizeLimit: {aofSizeLimit}",
                    aofSize, aofSizeLimit);

                var result = await TakeCheckpointAsync(defaultDatabase, logger: logger, token: token).ConfigureAwait(false);
                RecordCheckpointOutcome(defaultDatabase, result);
            }
            finally
            {
                ResumeCheckpoints(defaultDatabase.Id);
            }
        }

        /// <inheritdoc/>
        public override async Task CommitToAofAsync(CancellationToken token = default, ILogger logger = null)
        {
            try
            {
                await AppendOnlyFile.Log.CommitAsync(token: token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex,
                    "Exception raised while committing to AOF. AOF tail address = {tailAddress}; AOF committed until address = {commitAddress}; ",
                    AppendOnlyFile.Log.TailAddress, AppendOnlyFile.Log.CommittedUntilAddress);
                throw;
            }
        }

        /// <inheritdoc/>
        public override async Task CommitToAofAsync(int dbId, CancellationToken token = default, ILogger logger = null)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            await CommitToAofAsync(token, logger).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public override async Task WaitForCommitToAofAsync(CancellationToken token = default, ILogger logger = null)
        {
            await AppendOnlyFile.Log.WaitForCommitAsync(token: token).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public override ValueTask RecoverAOFAsync() => RecoverDatabaseAOFAsync(defaultDatabase);

        /// <inheritdoc/>
        public override void VerifyRecoveryIsComplete(bool canBeRepairedBySync = false)
            => VerifyDatabaseRecoveryIsComplete(defaultDatabase, canBeRepairedBySync);

        /// <inheritdoc/>
        public override AofAddress ReplayAOF(AofAddress untilAddress)
        {
            if (!StoreWrapper.serverOptions.EnableAOF)
                return default;

            // When replaying AOF we do not want to write record again to AOF.
            // So initialize local AofProcessor with recordToAof: false.
            var aofProcessor = new AofProcessor(StoreWrapper, clusterProvider: StoreWrapper.clusterProvider, recordToAof: false, logger: Logger);

            try
            {
                var res = ReplayDatabaseAOF(aofProcessor, defaultDatabase, untilAddress);

                // Wait for Vector Sets to catch up before declaring us "recovered"
                defaultDatabase.VectorManager?.WaitForQuiescence();

                return res;
            }
            finally
            {
                aofProcessor.Dispose();
            }
        }

        /// <inheritdoc/>
        public override ValueTask DoCompactionAsync(CancellationToken token = default, ILogger logger = null) => DoCompactionAsync(defaultDatabase);

        /// <inheritdoc/>
        public override ValueTask<bool> GrowIndexesIfNeededAsync(CancellationToken token = default) =>
            GrowIndexesIfNeededAsync(defaultDatabase);

        /// <inheritdoc/>
        public override void ExecuteObjectCollection() =>
            ExecuteObjectCollection(defaultDatabase, Logger);

        /// <inheritdoc/>
        public override void ExpiredKeyDeletionScan() =>
            ExpiredKeyDeletionScan(defaultDatabase);

        /// <inheritdoc/>
        public override void StartSizeTrackers(CancellationToken token = default) =>
            SizeTracker?.Start(token);

        /// <inheritdoc/>
        public override void Reset(int dbId = 0)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            ResetDatabase(defaultDatabase);
        }

        /// <inheritdoc/>
        public override void ResetRevivificationStats()
            => Store.ResetRevivificationStats();

        /// <inheritdoc/>
        public override void EnqueueCommit(AofEntryType entryType, long version, int dbId = 0)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            EnqueueDatabaseCommit(defaultDatabase, entryType, version);
        }

        public override GarnetDatabase[] GetDatabasesSnapshot() => [defaultDatabase];

        /// <inheritdoc/>
        public override GarnetDatabase TryGetDatabase(int dbId, out bool found)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            found = true;
            return defaultDatabase;
        }

        /// <inheritdoc/>
        public override void FlushDatabase(bool unsafeTruncateLog, int dbId = 0)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            var safeTruncateAof = StoreWrapper.serverOptions.EnableCluster && StoreWrapper.serverOptions.EnableAOF;

            FlushDatabase(defaultDatabase, unsafeTruncateLog, !safeTruncateAof);

            if (safeTruncateAof && StoreWrapper.serverOptions.EnableAOF)
                SafeFlushAOF(AofEntryType.FlushDb, unsafeTruncateLog);
        }

        /// <inheritdoc/>
        public override void FlushAllDatabases(bool unsafeTruncateLog)
        {
            var safeTruncateAof = StoreWrapper.serverOptions.EnableCluster && StoreWrapper.serverOptions.EnableAOF;

            FlushDatabase(defaultDatabase, unsafeTruncateLog, !safeTruncateAof);

            // We truncate AOF safely only in the cluster case.
            // For standalone FlushDatabase will take care of the AOF truncation
            if (safeTruncateAof)
                SafeFlushAOF(AofEntryType.FlushAll, unsafeTruncateLog);
        }

        /// <inheritdoc/>
        public override bool TrySwapDatabases(int dbId1, int dbId2, CancellationToken token = default) => false;

        /// <inheritdoc/>
        public override IDatabaseManager Clone(bool enableAof) => new SingleDatabaseManager(this, enableAof);

        /// <inheritdoc/>
        public override FunctionsState CreateFunctionsState(int dbId = 0, byte respProtocolVersion = ServerOptions.DEFAULT_RESP_VERSION)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            return new(AppendOnlyFile, VersionMap, StoreWrapper, PooledArrayMemoryPool.Shared, SizeTracker, DefaultDatabase.VectorManager, Logger, respProtocolVersion);
        }

        private async Task<bool> TryPauseCheckpointsContinuousAsync(int dbId,
            CancellationToken token = default)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            var checkpointsPaused = TryPauseCheckpoints(defaultDatabase);

            while (!checkpointsPaused && !token.IsCancellationRequested && !Disposed)
            {
                await Task.Yield();
                checkpointsPaused = TryPauseCheckpoints(defaultDatabase);
            }

            return checkpointsPaused;
        }

        public override (long numExpiredKeysFound, long totalRecordsScanned) ExpiredKeyDeletionScan(int dbId)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);
            return StoreExpiredKeyDeletionScan(DefaultDatabase);
        }

        public override (long keyCount, long expireCount) GetKeyspaceStats(int dbId)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);
            return GetDatabaseKeyspaceStats(DefaultDatabase);
        }

        public override (HybridLogScanMetrics mainStore, HybridLogScanMetrics objectStore)[] CollectHybridLogStats() => [CollectHybridLogStatsForDb(defaultDatabase)];

        private unsafe void SafeFlushAOF(AofEntryType entryType, bool unsafeTruncateLog)
        {
            // Safe truncate up to tail for botth primary and replica
            StoreWrapper.clusterProvider.SafeTruncateAOF(AppendOnlyFile.Log.TailAddress);

            // Only enqueue operation if this is a primary
            if (StoreWrapper.clusterProvider.IsPrimary())
            {
                AppendOnlyFile.Log.EnqueueSafeFlushAOF(entryType, unsafeTruncateLog, defaultDatabase.Id);
            }
        }

        /// <inheritdoc/>
        public override void RecoverVectorSets()
        {
            // Guarantee initialize has happened before we attempt to recover
            defaultDatabase.VectorManager?.Initialize();

            defaultDatabase.VectorManager?.ReconcileRecoveredState();
            defaultDatabase.VectorManager?.WaitForQuiescence();
        }

        public override void Dispose()
        {
            if (Disposed) return;

            DefaultDatabase.Dispose();

            Disposed = true;
        }
    }
}