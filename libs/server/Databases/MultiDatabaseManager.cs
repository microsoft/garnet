// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server.Metrics;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Multiple logical database management
    /// </summary>
    internal class MultiDatabaseManager : DatabaseManagerBase
    {
        /// <inheritdoc/>
        public override GarnetDatabase DefaultDatabase => databases.Map[0];

        /// <inheritdoc/>
        public override int DatabaseCount => activeDbIds.ActualSize;

        /// <inheritdoc/>
        public override int MaxDatabaseId => databases.ActualSize - 1;

        // Map of databases by database ID (by default: of size 1, contains only DB 0)
        ExpandableMap<GarnetDatabase> databases;

        // Map containing active database IDs
        ExpandableMap<int> activeDbIds;

        // Reader-Writer lock for thread-safety during a swap-db operation
        // The swap-db operation should take a write lock and any operation that should be swap-db-safe should take a read lock.
        SingleWriterMultiReaderLock databasesContentLock;

        // Lock for synchronizing checkpointing of all active DBs (if more than one)
        SingleWriterMultiReaderLock multiDbCheckpointingLock;

        // True if StartObjectSizeTrackers was previously called
        bool sizeTrackersStarted;

        // Monotonic counter identifying how recent the storage-slot to logical-database mapping is.
        // Recorded with every checkpoint; on recovery it resumes from the highest value read back, so
        // a swap made after a restart still outranks the mapping persisted before it.
        long swapEpoch;

        // Storage slots found under the checkpoint directory when this manager was chosen over the
        // single-database one. Null when the manager was created by other means, in which case recovery
        // scans for itself.
        readonly int[] recoveredStorageSlots;

        public MultiDatabaseManager(StoreWrapper.DatabaseCreatorDelegate createDatabaseDelegate,
            StoreWrapper storeWrapper, bool createDefaultDatabase = true, int[] recoveredStorageSlots = null)
            : base(createDatabaseDelegate, storeWrapper)
        {
            Logger = storeWrapper.loggerFactory?.CreateLogger(nameof(MultiDatabaseManager));

            this.recoveredStorageSlots = recoveredStorageSlots;

            var maxDatabases = storeWrapper.serverOptions.MaxDatabases;

            // Create default databases map of size 1
            databases = new ExpandableMap<GarnetDatabase>(1, 0, maxDatabases - 1);

            // Create default active database ids map of size 1
            activeDbIds = new ExpandableMap<int>(1, 0, maxDatabases - 1);

            // Create default database of index 0 (unless specified otherwise)
            if (createDefaultDatabase)
            {
                var db = createDatabaseDelegate(0);

                // Set new database in map
                if (!TryAddDatabase(0, db))
                    throw new GarnetException("Failed to set initial database in databases map");
            }
        }

        public MultiDatabaseManager(MultiDatabaseManager src, bool enableAof) : this(src.CreateDatabaseDelegate,
            src.StoreWrapper, createDefaultDatabase: false)
        {
            CopyDatabases(src, enableAof);
        }

        public MultiDatabaseManager(SingleDatabaseManager src) :
            this(src.CreateDatabaseDelegate, src.StoreWrapper, false)
        {
            CopyDatabases(src, src.StoreWrapper.serverOptions.EnableAOF);
        }

        /// <inheritdoc/>
        public override async ValueTask RecoverCheckpointAsync(bool replicaRecover = false, bool recoverFromToken = false, CheckpointMetadata metadata = null)
        {
            if (replicaRecover)
                throw new GarnetException(
                    $"Unexpected call to {nameof(MultiDatabaseManager)}.{nameof(RecoverCheckpointAsync)} with {nameof(replicaRecover)} == true.");

            // The factory scanned the checkpoint directories to decide this manager was needed, so reuse what
            // it found; only a manager created by other means has to scan here.
            var storageSlots = recoveredStorageSlots;
            if (storageSlots == null)
            {
                var checkpointParentDir = StoreWrapper.serverOptions.StoreCheckpointBaseDirectory;
                var checkpointDirBaseName = GarnetServerOptions.GetCheckpointDirectoryName(0);

                try
                {
                    if (!TryGetSavedDatabaseIds(checkpointParentDir, checkpointDirBaseName, out storageSlots))
                        return;
                }
                catch (Exception ex)
                {
                    Logger?.LogError(ex,
                        "Error during recovery of database ids; checkpointParentDir = {checkpointParentDir}; checkpointDirBaseName = {checkpointDirBaseName}",
                        checkpointParentDir, checkpointDirBaseName);
                    if (StoreWrapper.serverOptions.FailOnRecoveryError)
                        throw;
                    return;
                }
            }

            long storeVersion = 0, objectStoreVersion = 0;

            // Sample the log segments before any database is created, so a store opening its own log
            // device cannot be mistaken for a log the previous run actually wrote.
            var hybridLogSegments = ListHybridLogSegments();
            var multiDatabaseStore = storageSlots.Any(slot => slot != 0);

            // The directory names give the storage slots; which logical database each slot held is recorded
            // in the checkpoints themselves, because a swap relabels a database without moving its files.
            // Recovery reads that metadata anyway, so the mapping is collected as it goes rather than in a
            // pass of its own. Every database records the whole mapping, so the highest swap epoch seen is
            // authoritative on its own - which matters because databases are checkpointed individually and
            // can therefore disagree.
            int[] persistedMapping = null;

            // Below every epoch a checkpoint can record, so the first current-format checkpoint always
            // wins outright, including one taken at epoch 0.
            var persistedSwapEpoch = -1L;

            // Recover each store under the index of its own storage slot, then relabel once at the end.
            // Recovering directly under the mapped id cannot work: the default database already
            // occupies index 0 bound to slot 0 before any checkpoint can be read, and a permutation
            // can cycle, so an id a store needs may still be held by the store that is about to vacate it.
            foreach (var storageSlot in storageSlots)
            {
                var db = TryGetOrAddDatabase(storageSlot, out var success, out _);
                if (!success)
                    throw new GarnetException($"Failed to retrieve or create database for checkpoint recovery (storage slot = {storageSlot}).");

                try
                {
                    storeVersion = await RecoverDatabaseCheckpointAsync(db).ConfigureAwait(false);

                    // A checkpoint taken while the mapping was the identity records no mapping at all, so
                    // an absent mapping is not an absent opinion: it states "identity, as of this epoch".
                    // Ranking therefore has to be by epoch alone. Gating on a mapping being present would
                    // let a swap that was later undone outrank the very checkpoint that undid it. Only
                    // checkpoints carrying the epoch can be ranked, since below that version both fields
                    // read back as defaults that assert nothing.
                    var outcome = db.CheckpointRecovery;
                    if (outcome.CheckpointVersion >= HybridLogRecoveryInfo.DatabaseMappingCheckpointVersion &&
                        outcome.SwapEpoch > persistedSwapEpoch)
                    {
                        persistedMapping = outcome.DatabaseMapping;
                        persistedSwapEpoch = outcome.SwapEpoch;
                    }
                }
                catch (TsavoriteNoHybridLogException ex)
                {
                    // Finding no hybrid log is not by itself a recovery error: a fresh start and an AOF-only database
                    // both land here. Record what the scan saw so VerifyRecoveryIsComplete can tell those apart from
                    // a checkpointed prefix that exists on disk but could not be read, once the AOF state is known.
                    db.CheckpointRecovery = new CheckpointRecoveryOutcome
                    {
                        CandidateTokenCount = ex.CandidateTokenCount,
                        UnreadableTokenCount = ex.UnreadableTokenCount
                    };

                    if (ex.CandidateTokenCount == 0)
                    {
                        // As in SingleDatabaseManager, nothing was ever written, so recovery cannot tell that a
                        // checkpoint the client was told had succeeded is missing; see RecordCheckpointOutcome.
                        Logger?.LogInformation(ex,
                            "No Hybrid Log found for recovery; storeVersion = {storeVersion}; objectStoreVersion = {objectStoreVersion}",
                            storeVersion, objectStoreVersion);
                    }
                    else
                    {
                        Logger?.LogError(ex,
                            "Unable to read any of the {candidateTokenCount} HybridLog checkpoint token(s) found on disk (DB ID: {id}); storeVersion = {storeVersion}",
                            ex.CandidateTokenCount, storageSlot, storeVersion);
                    }
                }
                catch (Exception ex)
                {
                    // Unless FailOnRecoveryError is set the server continues with whatever was recovered, so this
                    // must be visible at the default log level.
                    Logger?.LogError(ex,
                        "Error during recovery of store; storeVersion = {storeVersion}; objectStoreVersion = {objectStoreVersion}",
                        storeVersion, objectStoreVersion);
                    if (StoreWrapper.serverOptions.FailOnRecoveryError)
                        throw;
                }
                finally
                {
                    // Reported after recovery because it reads the recovered checkpoint's format version, and in a
                    // finally so a database that failed to recover - the case this most needs to explain - still
                    // reports.
                    VerifyMultiDBLogLayout(db, hybridLogSegments, multiDatabaseStore);
                }

                // Once everything is setup, initialize the VectorManager
                db.VectorManager.Initialize();
            }

            ApplyDatabaseMapping(storageSlots, ResolveLogicalDatabaseIds(storageSlots, persistedMapping, persistedSwapEpoch));
        }

        /// <summary>
        /// Relabel recovered databases so each carries the logical id it had when the mapping was
        /// recorded, leaving every store bound to the slot that names its files. This is the same
        /// relabelling a swap performs at runtime, applied once after all stores are in place.
        /// </summary>
        /// <param name="storageSlots">Storage slots recovered, in the order they were recovered</param>
        /// <param name="logicalIds">Logical database id for each entry of <paramref name="storageSlots"/></param>
        private void ApplyDatabaseMapping(int[] storageSlots, int[] logicalIds)
        {
            // ResolveLogicalDatabaseIds returns storageSlots itself when the mapping is the identity.
            if (ReferenceEquals(storageSlots, logicalIds))
                return;

            var enableAof = StoreWrapper.serverOptions.EnableAOF;

            databases.mapLock.WriteLock();
            try
            {
                var databaseMapSnapshot = databases.Map;

                // Detach every store from its slot index before placing any of them: the mapping is a
                // permutation, so an index being vacated may be the one another store moves into.
                // Indexed by position in storageSlots, parallel to logicalIds - not by database id, which is
                // exactly what is about to change.
                var recoveredBySlotIndex = new GarnetDatabase[storageSlots.Length];
                for (var i = 0; i < storageSlots.Length; i++)
                    recoveredBySlotIndex[i] = databaseMapSnapshot[storageSlots[i]];
                for (var i = 0; i < storageSlots.Length; i++)
                    databaseMapSnapshot[storageSlots[i]] = null;

                // A target id can still be occupied by a database that recovered nothing. The default
                // database is created before any checkpoint can be read, so it holds id 0 even when slot 0
                // has no checkpoint directory at all and some other slot is mapped onto that id. It owns a
                // store and that store's devices, so it has to be disposed rather than overwritten:
                // ExpandableMap does not expect an occupied entry and would silently leak it. The
                // recovered stores were detached just above, so nothing disposed here is one of them.
                for (var i = 0; i < logicalIds.Length; i++)
                {
                    // An id past the map's current length cannot be occupied by anything; placement below
                    // expands the map to reach it.
                    if (logicalIds[i] >= databaseMapSnapshot.Length)
                        continue;

                    var displaced = databaseMapSnapshot[logicalIds[i]];
                    if (displaced == null)
                        continue;

                    Logger?.LogInformation(
                        "Discarding the empty database {dbId} created before recovery; storage slot {storageSlot} takes that id per the recorded mapping",
                        displaced.Id, storageSlots[i]);
                    displaced.Dispose();
                    databaseMapSnapshot[logicalIds[i]] = null;
                }

                for (var i = 0; i < storageSlots.Length; i++)
                {
                    var db = recoveredBySlotIndex[i];
                    if (db == null)
                        continue;

                    // GarnetDatabase.Id is get-only and must equal the index the database sits at, so a
                    // database whose id changes needs a new wrapper around the same store. The wrapper is
                    // cheap - it shares the store, AOF and VectorManager - and only the recovered databases
                    // are candidates, so there is nothing already carrying the target id to reuse instead.
                    var relabeled = db.Id == logicalIds[i]
                        ? db
                        : new GarnetDatabase(logicalIds[i], db, enableAof, copyLastSaveData: true);

                    if (!databases.TrySetValueUnsafe(logicalIds[i], ref relabeled, noExpansion: false))
                        throw new GarnetException($"Failed to place recovered storage slot {storageSlots[i]} under database ID {logicalIds[i]}.");

                    AttachDatabaseMappingProvider(relabeled);
                }
            }
            finally
            {
                databases.mapLock.WriteUnlock();
            }

            RebuildActiveDatabaseIds();
        }

        /// <summary>
        /// Rebuild the active database id list from the database map. Relabelling changes which ids are
        /// live and can retire one outright - a slot recovered under another slot's id leaves its own id
        /// vacant, and a database the mapping displaced is gone altogether - so the list is rebuilt rather
        /// than rewritten in place, which would keep a stale entry and report one database twice. Called
        /// only while recovery is still single-threaded.
        /// </summary>
        private void RebuildActiveDatabaseIds()
        {
            var databasesMapSize = databases.ActualSize;
            var databaseMapSnapshot = databases.Map;

            activeDbIds = new ExpandableMap<int>(1, 0, StoreWrapper.serverOptions.MaxDatabases - 1);
            for (var dbId = 0; dbId < databasesMapSize; dbId++)
            {
                if (databaseMapSnapshot[dbId] == null)
                    continue;

                if (!activeDbIds.TryGetNextId(out var nextIdx) || !activeDbIds.TrySetValue(nextIdx, dbId))
                    throw new GarnetException($"Failed to rebuild the active database ID list at database ID {dbId}.");
            }
        }

        /// <summary>
        /// Names of every file under the store directory on the configured log device backend. Goes
        /// through the device factory rather than the filesystem so non-local backends work, and
        /// returns an empty set when storage tiering is off or the listing cannot be read.
        /// </summary>
        private HashSet<string> ListHybridLogSegments()
        {
            var opts = StoreWrapper.serverOptions;
            if (!opts.EnableStorageTier)
                return null;

            try
            {
                var factory = opts.GetInitializedDeviceFactory(opts.LogDir);
                return new HashSet<string>(
                    factory.ListContents(GarnetServerOptions.StoreDirectoryName)
                        .Select(static descriptor => descriptor.fileName)
                        .Where(static name => !string.IsNullOrEmpty(name)),
                    StringComparer.Ordinal);
            }
            catch (Exception ex)
            {
                // Without a listing nothing can be concluded, so report nothing rather than guess.
                Logger?.LogDebug(ex, "Could not enumerate hybrid log segments under {logDir}", opts.LogDir);
                return null;
            }
        }

        /// <summary>
        /// Whether any hybrid log segment exists for a storage slot. Log truncation can remove earlier
        /// segments, so any segment counts, not just segment 0.
        /// </summary>
        /// <param name="segments">File names under the store directory</param>
        /// <param name="storageSlot">Storage slot naming the hybrid log</param>
        private static bool HasHybridLogSegment(HashSet<string> segments, int storageSlot)
        {
            // Segment files are named "<fileName>.<segment>". The '.' keeps "hlog." from matching
            // "hlog_1.0" or "hlog_objs.0".
            var prefix = GarnetServerOptions.GetHybridLogFileName(storageSlot, isObj: false) + ".";
            foreach (var name in segments)
            {
                if (name.StartsWith(prefix, StringComparison.Ordinal))
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Verify the on-disk log layout for a database being recovered, reporting anything that limits
        /// what it can recover. Two conditions are reported, and either may fire for a given database:
        /// <list type="bullet">
        /// <item><description>The checkpoint predates databases having their own log devices, when all of
        /// them shared one log file whose contents cannot be attributed to a single database.</description></item>
        /// <item><description>No hybrid log segment exists for the database's storage slot, so records it
        /// tiered to storage are not on disk to be read back.</description></item>
        /// </list>
        /// Recovery still proceeds in both cases: a database whose records never left memory is recoverable
        /// from its snapshot alone, so these are reported rather than treated as fatal.
        /// </summary>
        /// <param name="db">Database being recovered</param>
        /// <param name="segments">File names under the store directory, or null if unavailable</param>
        /// <param name="multiDatabaseStore">Whether more than one database is being recovered</param>
        private void VerifyMultiDBLogLayout(GarnetDatabase db, HashSet<string> segments, bool multiDatabaseStore)
        {
            if (segments == null)
                return;

            var perDatabaseLayout = WasCheckpointedWithPerDatabaseLogs(db);

            // File names come from the storage slot, never the logical id.
            if (db.StorageSlot == 0)
            {
                // The default slot keeps the unsuffixed file names, so it always finds its log.
                // In a store predating individual logs and holding more than one database, that log is
                // the shared one, and the records in it may belong to any database.
                if (!perDatabaseLayout && multiDatabaseStore)
                {
                    Logger?.LogError(
                        "Database 0 was checkpointed before databases had their own log devices, when every database shared a single log file " +
                        "(see https://github.com/microsoft/garnet/issues/2152). Records this database tiered to storage may have been " +
                        "overwritten by, or may be read back as, another database's records.");
                }
                return;
            }

            if (HasHybridLogSegment(segments, db.StorageSlot))
                return;

            var logName = GarnetServerOptions.GetHybridLogFileName(db.StorageSlot, isObj: false);

            if (perDatabaseLayout)
            {
                Logger?.LogWarning(
                    "No {logName} segment found under {storeDir} for database {dbId}; any records tiered to storage for this database cannot be recovered",
                    logName, GarnetServerOptions.StoreDirectoryName, db.Id);
                return;
            }

            Logger?.LogError(
                "Database {dbId} was checkpointed before databases had their own log devices, when every database shared a single log file " +
                "whose contents cannot be attributed to one database (see https://github.com/microsoft/garnet/issues/2152). " +
                "No {logName} segment exists under {storeDir}, so any records this database tiered to storage cannot be recovered.",
                db.Id, logName, GarnetServerOptions.StoreDirectoryName);
        }

        /// <summary>
        /// Map each storage slot found on disk to the logical database id it last held.
        ///
        /// <para>The slots themselves can only come from the directory names: reading a checkpoint
        /// requires knowing which checkpoint directory to open, so the scan is what discovers the
        /// stores. What the scan cannot know is the <em>label</em> each store carried, because a swap
        /// relabels a database without moving its files. That is what the persisted mapping supplies.</para>
        ///
        /// <para>Falls back to slot == logical id when no mapping is recorded, which covers both a
        /// store written before the mapping existed and one whose databases were never swapped.</para>
        /// </summary>
        /// <param name="storageSlots">Storage slots discovered on disk</param>
        /// <param name="mapping">Highest-epoch mapping of storage slot to logical database id recovered
        /// from the checkpoints, or null if the winning checkpoint recorded the identity</param>
        /// <param name="epoch">Swap epoch <paramref name="mapping"/> belongs to, or -1 if no checkpoint
        /// recorded one</param>
        /// <returns>Logical database id for each entry of <paramref name="storageSlots"/>, or
        /// <paramref name="storageSlots"/> itself when the mapping is the identity or unusable</returns>
        private int[] ResolveLogicalDatabaseIds(int[] storageSlots, int[] mapping, long epoch)
        {
            // Resume from the epoch that was read back, whatever mapping came with it, so a swap made
            // after this recovery outranks it. This matters most when the winning checkpoint recorded the
            // identity: it writes no mapping, but its epoch still has to carry forward, or the next swap
            // would restart numbering below a mapping already on disk. Kept even when the mapping is
            // rejected below, which is strictly safer: a later swap then outranks the rejected one.
            if (epoch >= 0)
                swapEpoch = epoch;

            if (mapping == null)
                return storageSlots;

            if (!TryApplyDatabaseMapping(storageSlots, mapping, out var logicalIds))
            {
                Logger?.LogError(
                    "Ignoring the database mapping recorded at swap epoch {epoch} and recovering each database under the id matching its storage slot. " +
                    "Any database swap performed before the last checkpoint is not restored, but no data is lost or misattributed.",
                    epoch);
                return storageSlots;
            }

            for (var i = 0; i < storageSlots.Length; i++)
            {
                if (storageSlots[i] != logicalIds[i])
                    Logger?.LogInformation("Recovering storage slot {slot} as database {dbId}, per the mapping recorded at swap epoch {epoch}",
                        storageSlots[i], logicalIds[i], epoch);
            }

            return logicalIds;
        }

        /// <summary>
        /// Turn a persisted mapping into a logical database id per storage slot, rejecting it whole if
        /// it cannot be trusted. A mapping is never applied partially: doing so can leave two databases
        /// claiming the same logical id, which is worse than ignoring a stale swap.
        /// </summary>
        /// <param name="storageSlots">Storage slots discovered on disk</param>
        /// <param name="mapping">Mapping of storage slot to logical database id</param>
        /// <param name="logicalIds">Logical database id for each entry of <paramref name="storageSlots"/></param>
        /// <returns>True if the mapping is usable</returns>
        private bool TryApplyDatabaseMapping(int[] storageSlots, int[] mapping, out int[] logicalIds)
        {
            var maxDatabases = StoreWrapper.serverOptions.MaxDatabases;
            logicalIds = new int[storageSlots.Length];
            var assigned = new HashSet<int>();

            for (var i = 0; i < storageSlots.Length; i++)
            {
                var storageSlot = storageSlots[i];

                // A slot the mapping does not cover keeps its own id: it was created after the
                // checkpoint that recorded the mapping.
                var dbId = storageSlot >= 0 && storageSlot < mapping.Length ? mapping[storageSlot] : storageSlot;

                if (dbId < 0 || dbId >= maxDatabases)
                {
                    // Also catches MaxDatabases having been lowered since the mapping was written.
                    Logger?.LogError(
                        "Database mapping assigns storage slot {storageSlot} to database {dbId}, which is outside the configured range of 0 to {maxDatabases}",
                        storageSlot, dbId, maxDatabases - 1);
                    return false;
                }

                if (!assigned.Add(dbId))
                {
                    Logger?.LogError(
                        "Database mapping assigns database {dbId} to more than one storage slot, so it is not a valid permutation", dbId);
                    return false;
                }

                logicalIds[i] = dbId;
            }

            // A mapping entry naming a slot with no directory is not an error: that database's files
            // were removed while others were kept. Its id simply goes unused, and it is worth saying so.
            for (var storageSlot = 0; storageSlot < mapping.Length; storageSlot++)
            {
                if (mapping[storageSlot] != storageSlot && Array.IndexOf(storageSlots, storageSlot) < 0)
                    Logger?.LogWarning(
                        "Database mapping refers to storage slot {storageSlot} as database {dbId}, but no checkpoint directory for that slot exists; it will not be recovered",
                        storageSlot, mapping[storageSlot]);
            }

            return true;
        }

        /// <summary>
        /// Whether the checkpoint this database recovered from was written by a build that gave each
        /// database its own log devices. Checkpoints at
        /// <see cref="HybridLogRecoveryInfo.MinRecoverableCheckpointVersion"/> predate that change.
        /// </summary>
        /// <param name="db">Database being recovered</param>
        private static bool WasCheckpointedWithPerDatabaseLogs(GarnetDatabase db)
        {
            // Recovery already read the metadata and recorded its format version, so nothing is re-read here.
            var checkpointVersion = db.CheckpointRecovery.CheckpointVersion;

            // No checkpoint was recovered, so there is no earlier layout to report; assume the current one
            // rather than say something misleading.
            return checkpointVersion == 0 || checkpointVersion > HybridLogRecoveryInfo.MinRecoverableCheckpointVersion;
        }

        /// <inheritdoc/>
        public override Task<CheckpointStatus> TakeCheckpointAsync(bool background, int dbId = -1, CancellationToken token = default, ILogger logger = null)
        {
            // Acquire databasesContentLock (read) so a concurrent swap-db can't move GarnetDatabase
            // wrappers out from under us mid-checkpoint (which would mis-attribute LASTSAVE to the
            // swapped DB and let a second BGSAVE race against the in-flight checkpoint).
            if (!TryGetDatabasesContentReadLock(token)) return Task.FromResult(CheckpointStatus.AlreadyInProgress);

            var multiDbLockHeld = false;
            int[] pausedDbIds = null;
            var pausedCount = 0;
            var requestedCount = 0;

            try
            {
                if (dbId == -1)
                {
                    // All-active-DBs path: take multiDbCheckpointingLock if multi-db, then synchronously
                    // pause per-DB checkpoints for every active DB so any BGSAVE issued after this method
                    // returns reliably observes the in-progress checkpoint. The buffer is local so a
                    // concurrent HandleDatabaseAdded that resizes shared state cannot strand the IDs.
                    var activeDbIdsMapSize = activeDbIds.ActualSize;

                    if (activeDbIdsMapSize > 1)
                    {
                        if (!multiDbCheckpointingLock.TryWriteLock())
                        {
                            databasesContentLock.ReadUnlock();
                            return Task.FromResult(CheckpointStatus.AlreadyInProgress);
                        }

                        multiDbLockHeld = true;
                    }

                    requestedCount = activeDbIdsMapSize;
                    pausedDbIds = new int[activeDbIdsMapSize];
                    var activeDbIdsMapSnapshot = activeDbIds.Map;
                    for (var i = 0; i < activeDbIdsMapSize; i++)
                    {
                        var id = activeDbIdsMapSnapshot[i];
                        if (TryPauseCheckpoints(id))
                            pausedDbIds[pausedCount++] = id;
                    }
                }
                else
                {
                    // Single-DB path: just pause this one DB. multiDbCheckpointingLock is not taken
                    // because multiple per-DB BGSAVEs on different DBs are legal.
                    Debug.Assert(dbId < databases.ActualSize && databases.Map[dbId] != null);

                    if (!TryPauseCheckpoints(dbId))
                    {
                        databasesContentLock.ReadUnlock();
                        return Task.FromResult(CheckpointStatus.AlreadyInProgress);
                    }

                    pausedDbIds = [dbId];
                    pausedCount = 1;
                    requestedCount = 1;
                }
            }
            catch
            {
                if (pausedDbIds != null)
                {
                    for (var i = 0; i < pausedCount; i++)
                        ResumeCheckpoints(pausedDbIds[i]);
                }

                if (multiDbLockHeld)
                    multiDbCheckpointingLock.WriteUnlock();

                databasesContentLock.ReadUnlock();
                throw;
            }

            var checkpointTask = RunPausedCheckpointsAndReleaseLocksAsync(pausedDbIds, pausedCount, requestedCount, multiDbLockHeld, token, logger);

            if (background)
                return Task.FromResult(CheckpointStatus.Success);

            return checkpointTask;
        }

        /// <inheritdoc/>
        public override async Task TakeOnDemandCheckpointAsync(DateTimeOffset entryTime, int dbId = 0)
        {
            Debug.Assert(dbId < databases.ActualSize && databases.Map[dbId] != null);

            // Acquire databasesContentLock (read) so a concurrent swap-db can't mis-attribute LASTSAVE
            // (UpdateLastSaveData uses databases.Map[dbId] at write time).
            if (!TryGetDatabasesContentReadLock()) return;

            var checkpointsPaused = false;
            try
            {
                checkpointsPaused = TryPauseCheckpoints(dbId);
                var db = databases.Map[dbId];

                // If another checkpoint is in progress or a checkpoint was taken beyond the provided entryTime - return
                if (!checkpointsPaused || db.LastSaveTime > entryTime)
                    return;

                // Necessary to take a checkpoint because the latest checkpoint is before entryTime
                var result = await TakeCheckpointAsync(db, logger: Logger).ConfigureAwait(false);
                UpdateLastSaveData(dbId, result);
            }
            finally
            {
                if (checkpointsPaused)
                    ResumeCheckpoints(dbId);

                databasesContentLock.ReadUnlock();
            }
        }

        /// <inheritdoc/>
        public override async Task TaskCheckpointBasedOnAofSizeLimitAsync(long aofSizeLimit, CancellationToken token = default,
            ILogger logger = null)
        {
            if (!TryGetDatabasesContentReadLock(token)) return;

            var multiDbLockHeld = false;

            try
            {
                var activeDbIdsMapSize = activeDbIds.ActualSize;

                if (activeDbIdsMapSize > 1)
                {
                    if (!multiDbCheckpointingLock.TryWriteLock())
                        return;

                    multiDbLockHeld = true;
                }

                // Find first oversized DB and synchronously pause it.
                var databasesMapSnapshot = databases.Map;
                var activeDbIdsMapSnapshot = activeDbIds.Map;
                var pausedDbId = -1;
                for (var i = 0; i < activeDbIdsMapSize; i++)
                {
                    var dbId = activeDbIdsMapSnapshot[i];
                    var db = databasesMapSnapshot[dbId];
                    Debug.Assert(db != null);

                    var dbAofSize = db.AppendOnlyFile.Log.TailAddress.AggregateDiff(db.AppendOnlyFile.Log.BeginAddress);
                    if (dbAofSize > aofSizeLimit)
                    {
                        logger?.LogInformation("Enforcing AOF size limit currentAofSize: {dbAofSize} > AofSizeLimit: {aofSizeLimit} (Database ID: {dbId})",
                            dbAofSize, aofSizeLimit, dbId);
                        if (TryPauseCheckpoints(dbId))
                            pausedDbId = dbId;
                        break;
                    }
                }

                if (pausedDbId < 0) return;

                try
                {
                    var result = await TakeCheckpointAsync(databasesMapSnapshot[pausedDbId], logger: logger, token: token).ConfigureAwait(false);
                    UpdateLastSaveData(pausedDbId, result);
                }
                finally
                {
                    ResumeCheckpoints(pausedDbId);
                }
            }
            finally
            {
                if (multiDbLockHeld)
                    multiDbCheckpointingLock.WriteUnlock();

                databasesContentLock.ReadUnlock();
            }
        }

        /// <inheritdoc/>
        public override async Task CommitToAofAsync(CancellationToken token = default, ILogger logger = null)
        {
            // Take a read lock to make sure that swap-db operation is not in progress
            var lockAcquired = TryGetDatabasesContentReadLock(token);
            if (!lockAcquired) return;

            try
            {
                var databasesMapSnapshot = databases.Map;
                var activeDbIdsMapSize = activeDbIds.ActualSize;
                var activeDbIdsMapSnapshot = activeDbIds.Map;

                var aofTasks = new Task<(AofAddress, AofAddress)>[activeDbIdsMapSize];

                for (var i = 0; i < activeDbIdsMapSize; i++)
                {
                    var dbId = activeDbIdsMapSnapshot[i];
                    var db = databasesMapSnapshot[dbId];
                    Debug.Assert(db != null);

                    aofTasks[i] = AwaitCommitAsync(db, db.AppendOnlyFile.Log.CommitAsync(token: token));
                }

                var exThrown = false;
                try
                {
                    await Task.WhenAll(aofTasks).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Only first exception is caught here, if any. 
                    // Proper handling of this and consequent exceptions in the next loop. 
                    exThrown = true;
                }

                foreach (var t in aofTasks)
                {
                    if (!t.IsFaulted || t.Exception == null) continue;

                    logger?.LogError(t.Exception, "Exception raised while committing to AOF.");
                }

                if (exThrown)
                    throw new GarnetException($"Error occurred while committing to AOF in {nameof(MultiDatabaseManager)}. Refer to previous log messages for more details.");
            }
            finally
            {
                databasesContentLock.ReadUnlock();
            }

            static async Task<(AofAddress, AofAddress)> AwaitCommitAsync(GarnetDatabase db, ValueTask task)
            {
                await task.ConfigureAwait(false);

                return (db.AppendOnlyFile.Log.TailAddress, db.AppendOnlyFile.Log.CommittedUntilAddress);
            }
        }

        /// <inheritdoc/>
        public override async Task CommitToAofAsync(int dbId, CancellationToken token = default, ILogger logger = null)
        {
            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;
            Debug.Assert(dbId < databasesMapSize && databasesMapSnapshot[dbId] != null);

            await databasesMapSnapshot[dbId].AppendOnlyFile.Log.CommitAsync(token: token).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public override async Task WaitForCommitToAofAsync(CancellationToken token = default, ILogger logger = null)
        {
            // Take a read lock to make sure that swap-db operation is not in progress
            var lockAcquired = TryGetDatabasesContentReadLock(token);
            if (!lockAcquired) return;

            try
            {
                var databasesMapSnapshot = databases.Map;
                var activeDbIdsMapSize = activeDbIds.ActualSize;
                var activeDbIdsMapSnapshot = activeDbIds.Map;

                var aofTasks = new Task[activeDbIdsMapSize];

                for (var i = 0; i < activeDbIdsMapSize; i++)
                {
                    var dbId = activeDbIdsMapSnapshot[i];
                    var db = databasesMapSnapshot[dbId];
                    Debug.Assert(db != null);

                    aofTasks[i] = db.AppendOnlyFile.Log.WaitForCommitAsync(token: token).AsTask();
                }

                await Task.WhenAll(aofTasks).ConfigureAwait(false);
            }
            finally
            {
                databasesContentLock.ReadUnlock();
            }
        }

        /// <inheritdoc/>
        public override async ValueTask RecoverAOFAsync()
        {
            var aofParentDir = StoreWrapper.serverOptions.AppendOnlyFileBaseDirectory;
            var aofDirBaseName = GarnetServerOptions.GetAppendOnlyFileDirectoryName(0);

            int[] dbIdsToRecover;
            try
            {
                if (!TryGetSavedDatabaseIds(aofParentDir, aofDirBaseName, out dbIdsToRecover))
                    return;

            }
            catch (Exception ex)
            {
                // Failing to enumerate the AOF database ids means no database recovers its AOF, so log it at the
                // default level and honor FailOnRecoveryError as the checkpoint path above does.
                Logger?.LogError(ex,
                    "Error during recovery of database ids; aofParentDir = {aofParentDir}; aofDirBaseName = {aofDirBaseName}",
                    aofParentDir, aofDirBaseName);
                if (StoreWrapper.serverOptions.FailOnRecoveryError)
                    throw;
                return;
            }

            foreach (var dbId in dbIdsToRecover)
            {
                var db = TryGetOrAddDatabase(dbId, out var success, out _);
                if (!success)
                    throw new GarnetException($"Failed to retrieve or create database for AOF recovery (DB ID = {dbId}).");

                await RecoverDatabaseAOFAsync(db).ConfigureAwait(false);
            }
        }

        /// <inheritdoc/>
        public override void VerifyRecoveryIsComplete(bool canBeRepairedBySync = false)
        {
            foreach (var db in GetDatabasesSnapshot())
                VerifyDatabaseRecoveryIsComplete(db, canBeRepairedBySync);
        }

        /// <inheritdoc/>
        public override AofAddress ReplayAOF(AofAddress untilAddress)
        {
            if (!StoreWrapper.serverOptions.EnableAOF)
                return default;

            // When replaying AOF we do not want to write record again to AOF.
            // So initialize local AofProcessor with recordToAof: false.
            var aofProcessor = new AofProcessor(StoreWrapper, clusterProvider: StoreWrapper.clusterProvider, recordToAof: false, logger: Logger);

            var replicationOffset = AofAddress.Create(StoreWrapper.serverOptions.AofPhysicalSublogCount, 0);
            try
            {
                var databasesMapSnapshot = databases.Map;

                var activeDbIdsMapSize = activeDbIds.ActualSize;
                var activeDbIdsMapSnapshot = activeDbIds.Map;

                for (var i = 0; i < activeDbIdsMapSize; i++)
                {
                    var dbId = activeDbIdsMapSnapshot[i];
                    var db = databasesMapSnapshot[dbId];

                    var offset = ReplayDatabaseAOF(aofProcessor, db, dbId == 0 ? untilAddress : AppendOnlyFile.InvalidAofAddress);
                    if (dbId == 0) replicationOffset = offset;

                    // Wait for Vector Sets to catch up before declaring us "recovered"
                    db.VectorManager?.WaitForQuiescence();
                }
            }
            finally
            {
                aofProcessor.Dispose();
            }

            return replicationOffset;
        }

        /// <inheritdoc/>
        public override async ValueTask DoCompactionAsync(CancellationToken token = default, ILogger logger = null)
        {
            var lockAcquired = TryGetDatabasesContentReadLock(token);
            if (!lockAcquired) return;

            try
            {
                var databasesMapSnapshot = databases.Map;

                var activeDbIdsMapSize = activeDbIds.ActualSize;
                var activeDbIdsMapSnapshot = activeDbIds.Map;

                var exThrown = false;
                for (var i = 0; i < activeDbIdsMapSize; i++)
                {
                    var dbId = activeDbIdsMapSnapshot[i];
                    var db = databasesMapSnapshot[dbId];
                    Debug.Assert(db != null);

                    try
                    {
                        await DoCompactionAsync(db).ConfigureAwait(false);
                    }
                    catch (Exception)
                    {
                        exThrown = true;
                    }
                }

                if (exThrown)
                    throw new GarnetException($"Error occurred during compaction in {nameof(MultiDatabaseManager)}. Refer to previous log messages for more details.");
            }
            finally
            {
                databasesContentLock.ReadUnlock();
            }
        }

        /// <inheritdoc/>
        public override async ValueTask<bool> GrowIndexesIfNeededAsync(CancellationToken token = default)
        {
            var lockAcquired = TryGetDatabasesContentReadLock(token);
            if (!lockAcquired) return false;

            try
            {
                var activeDbIdsMapSize = activeDbIds.ActualSize;
                var activeDbIdsMapSnapshot = activeDbIds.Map;
                var databasesMapSnapshot = databases.Map;

                var growTasks = new Task<bool>[activeDbIdsMapSize];

                for (var i = 0; i < activeDbIdsMapSize; i++)
                {
                    var dbId = activeDbIdsMapSnapshot[i];

                    growTasks[i] = GrowIndexesIfNeededAsync(databasesMapSnapshot[dbId]).AsTask();
                }

                var indexMaxedOuts = await Task.WhenAll(growTasks).ConfigureAwait(false);

                return indexMaxedOuts.All(static x => x);
            }
            finally
            {
                databasesContentLock.ReadUnlock();
            }
        }

        /// <inheritdoc/>
        public override void ExecuteObjectCollection()
        {
            var databasesMapSnapshot = databases.Map;

            var activeDbIdsMapSize = activeDbIds.ActualSize;
            var activeDbIdsMapSnapshot = activeDbIds.Map;

            for (var i = 0; i < activeDbIdsMapSize; i++)
            {
                var dbId = activeDbIdsMapSnapshot[i];
                ExecuteObjectCollection(databasesMapSnapshot[dbId], Logger);
            }
        }

        /// <inheritdoc/>
        public override void ExpiredKeyDeletionScan()
        {
            var databasesMapSnapshot = databases.Map;

            var activeDbIdsMapSize = activeDbIds.ActualSize;
            var activeDbIdsMapSnapshot = activeDbIds.Map;

            for (var i = 0; i < activeDbIdsMapSize; i++)
            {
                var dbId = activeDbIdsMapSnapshot[i];
                ExpiredKeyDeletionScan(databasesMapSnapshot[dbId]);
            }
        }

        /// <inheritdoc/>
        public override void StartSizeTrackers(CancellationToken token = default)
        {
            sizeTrackersStarted = true;

            var lockAcquired = TryGetDatabasesContentReadLock(token);
            if (!lockAcquired) return;

            try
            {
                var databasesMapSnapshot = databases.Map;

                var activeDbIdsMapSize = activeDbIds.ActualSize;
                var activeDbIdsMapSnapshot = activeDbIds.Map;

                for (var i = 0; i < activeDbIdsMapSize; i++)
                {
                    var dbId = activeDbIdsMapSnapshot[i];
                    var db = databasesMapSnapshot[dbId];
                    Debug.Assert(db != null);

                    db.SizeTracker?.Start(token);
                }
            }
            finally
            {
                databasesContentLock.ReadUnlock();
            }
        }

        /// <inheritdoc/>
        public override void Reset(int dbId = 0)
        {
            var db = TryGetOrAddDatabase(dbId, out var success, out _);
            if (!success)
                throw new GarnetException($"Database with ID {dbId} was not found.");

            ResetDatabase(db);
        }

        /// <inheritdoc/>
        public override void ResetRevivificationStats()
        {
            var activeDbIdsMapSize = activeDbIds.ActualSize;
            var activeDbIdsMapSnapshot = activeDbIds.Map;
            var databaseMapSnapshot = databases.Map;

            for (var i = 0; i < activeDbIdsMapSize; i++)
            {
                var dbId = activeDbIdsMapSnapshot[i];
                databaseMapSnapshot[dbId].Store.ResetRevivificationStats();
            }
        }

        public override void EnqueueCommit(AofEntryType entryType, long version, int dbId = 0)
        {
            var db = TryGetOrAddDatabase(dbId, out var success, out _);
            if (!success)
                throw new GarnetException($"Database with ID {dbId} was not found.");

            EnqueueDatabaseCommit(db, entryType, version);
        }

        /// <inheritdoc/>
        public override GarnetDatabase[] GetDatabasesSnapshot()
        {
            var activeDbIdsMapSize = activeDbIds.ActualSize;
            var activeDbIdsMapSnapshot = activeDbIds.Map;
            var databaseMapSnapshot = databases.Map;
            var databasesSnapshot = new GarnetDatabase[activeDbIdsMapSize];

            for (var i = 0; i < activeDbIdsMapSize; i++)
            {
                var dbId = activeDbIdsMapSnapshot[i];
                databasesSnapshot[i] = databaseMapSnapshot[dbId];
            }

            return databasesSnapshot;
        }

        /// <inheritdoc/>
        public override bool TrySwapDatabases(int dbId1, int dbId2, CancellationToken token = default)
        {
            if (dbId1 == dbId2) return true;

            var db1 = TryGetOrAddDatabase(dbId1, out var success, out _);
            if (!success)
                return false;

            var db2 = TryGetOrAddDatabase(dbId2, out success, out _);
            if (!success)
                return false;

            if (!TryGetDatabasesContentWriteLock(token)) return false;

            try
            {
                var databaseMapSnapshot = databases.Map;
                var enableAof = StoreWrapper.serverOptions.EnableAOF;
                databaseMapSnapshot[dbId1] = new GarnetDatabase(dbId1, db2, enableAof, copyLastSaveData: true);
                databaseMapSnapshot[dbId2] = new GarnetDatabase(dbId2, db1, enableAof, copyLastSaveData: true);

                // The swapped wrappers are new instances, so re-point their checkpoint managers, and
                // advance the epoch so checkpoints taken from here on outrank any persisted earlier.
                swapEpoch++;
                AttachDatabaseMappingProvider(databaseMapSnapshot[dbId1]);
                AttachDatabaseMappingProvider(databaseMapSnapshot[dbId2]);

                var activeSessions = 0;
                foreach (var server in StoreWrapper.Servers)
                {
                    if (server is not GarnetServerBase serverBase) continue;

                    foreach (var session in serverBase.ActiveConsumers())
                    {
                        if (session is not RespServerSession respServerSession) continue;
                        activeSessions++;

                        if (activeSessions > 1) return false;

                        respServerSession.TrySwapDatabaseSessions(dbId1, dbId2);
                    }
                }
            }
            finally
            {
                databasesContentLock.WriteUnlock();
            }

            return true;
        }

        /// <inheritdoc/>
        public override IDatabaseManager Clone(bool enableAof) => new MultiDatabaseManager(this, enableAof);

        /// <inheritdoc/>
        public override FunctionsState CreateFunctionsState(int dbId = 0, byte respProtocolVersion = ServerOptions.DEFAULT_RESP_VERSION)
        {
            var db = TryGetOrAddDatabase(dbId, out var success, out _);
            if (!success)
                throw new GarnetException($"Database with ID {dbId} was not found.");

            return new(db.AppendOnlyFile, db.VersionMap, StoreWrapper, memoryPool: PooledArrayMemoryPool.Shared, db.SizeTracker, db.VectorManager, Logger, respProtocolVersion);
        }

        /// <inheritdoc/>
        public override GarnetDatabase TryGetOrAddDatabase(int dbId, out bool success, out bool added)
        {
            added = false;
            success = false;

            // Get a current snapshot of the databases
            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;

            // If database exists in the map, return it
            if (dbId >= 0 && dbId < databasesMapSize && databasesMapSnapshot[dbId] != null)
            {
                success = true;
                return databasesMapSnapshot[dbId];
            }

            // Take the database map's write lock so that no new databases can be added with the same ID
            // Note that we don't call TrySetValue because that would only guarantee that the map instance does not change,
            // but the underlying values can still change.
            // So here we're calling TrySetValueUnsafe and handling the locks ourselves.
            databases.mapLock.WriteLock();

            try
            {
                // Check again if database exists in the map, if so return it
                if (dbId >= 0 && dbId < databasesMapSize && databasesMapSnapshot[dbId] != null)
                {
                    success = true;
                    return databasesMapSnapshot[dbId];
                }

                // Create the database and use TrySetValueUnsafe to add it to the map. A newly created
                // database binds to the slot matching its id; only recovery relabels that pairing.
                var db = CreateDatabaseDelegate(dbId);
                if (!databases.TrySetValueUnsafe(dbId, ref db, false))
                    return default;
            }
            finally
            {
                // Release the database map's lock
                databases.mapLock.WriteUnlock();
            }

            added = true;
            success = true;

            HandleDatabaseAdded(dbId);

            // Update the databases snapshot and return a reference to the added database
            databasesMapSnapshot = databases.Map;
            return databasesMapSnapshot[dbId];
        }

        /// <summary>
        /// Build the current storage-slot to logical-database mapping, as
        /// <c>mapping[storageSlot] = logicalDatabaseId</c>. Returns null while the mapping is still the
        /// identity, so a server whose databases were never swapped records nothing and its checkpoints
        /// stay byte-identical to those of a build without this feature.
        /// </summary>
        private int[] GetCurrentDatabaseMapping()
        {
            var activeDbIdsMapSize = activeDbIds.ActualSize;
            var activeDbIdsMapSnapshot = activeDbIds.Map;
            var databaseMapSnapshot = databases.Map;

            var maxSlot = -1;
            var isIdentity = true;
            for (var i = 0; i < activeDbIdsMapSize; i++)
            {
                var db = databaseMapSnapshot[activeDbIdsMapSnapshot[i]];
                if (db == null)
                    continue;

                if (db.StorageSlot > maxSlot)
                    maxSlot = db.StorageSlot;
                if (db.StorageSlot != db.Id)
                    isIdentity = false;
            }

            if (isIdentity || maxSlot < 0)
                return null;

            // Slots with no live database keep their own index, so an unmapped slot recovered later
            // still lands on itself rather than on 0.
            var mapping = new int[maxSlot + 1];
            for (var slot = 0; slot < mapping.Length; slot++)
                mapping[slot] = slot;

            for (var i = 0; i < activeDbIdsMapSize; i++)
            {
                var db = databaseMapSnapshot[activeDbIdsMapSnapshot[i]];
                if (db != null)
                    mapping[db.StorageSlot] = db.Id;
            }

            return mapping;
        }

        /// <summary>
        /// Point a database's checkpoint manager at the live mapping, so each checkpoint records the
        /// mapping in force when it ran rather than one pushed at swap time.
        /// </summary>
        /// <param name="db">Database whose checkpoint manager should report the mapping</param>
        private void AttachDatabaseMappingProvider(GarnetDatabase db)
            => db?.Store?.CheckpointManager?.SetDatabaseMappingProvider(() => (GetCurrentDatabaseMapping(), swapEpoch));

        /// <inheritdoc/>
        public override bool TryPauseCheckpoints(int dbId)
        {
            var db = TryGetOrAddDatabase(dbId, out var success, out _);
            if (!success)
                throw new GarnetException($"Database with ID {dbId} was not found.");

            return TryPauseCheckpoints(db);
        }

        /// <inheritdoc/>
        public override void ResumeCheckpoints(int dbId)
        {
            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;
            Debug.Assert(dbId < databasesMapSize && databasesMapSnapshot[dbId] != null);

            ResumeCheckpoints(databasesMapSnapshot[dbId]);
        }

        /// <inheritdoc/>
        public override GarnetDatabase TryGetDatabase(int dbId, out bool found)
        {
            found = false;

            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;

            if (dbId == 0)
            {
                Debug.Assert(databasesMapSnapshot[0] != null);
                found = true;
                return databasesMapSnapshot[0];
            }

            // Check if database already exists
            if (dbId < databasesMapSize)
            {
                if (databasesMapSnapshot[dbId] != null)
                {
                    found = true;
                    return databasesMapSnapshot[dbId];
                }
            }

            found = false;
            return default;
        }

        /// <inheritdoc/>
        public override void FlushDatabase(bool unsafeTruncateLog, int dbId = 0)
        {
            var db = TryGetOrAddDatabase(dbId, out var success, out _);
            if (!success)
                throw new GarnetException($"Database with ID {dbId} was not found.");

            FlushDatabase(db, unsafeTruncateLog);
        }

        /// <inheritdoc/>
        public override void FlushAllDatabases(bool unsafeTruncateLog)
        {
            var activeDbIdsMapSize = activeDbIds.ActualSize;
            var activeDbIdsMapSnapshot = activeDbIds.Map;
            var databaseMapSnapshot = databases.Map;

            for (var i = 0; i < activeDbIdsMapSize; i++)
            {
                var dbId = activeDbIdsMapSnapshot[i];
                FlushDatabase(databaseMapSnapshot[dbId], unsafeTruncateLog);
            }
        }

        /// <summary>
        /// Continuously try to take a databases content read lock
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>True if lock acquired</returns>
        public bool TryGetDatabasesContentReadLock(CancellationToken token = default)
        {
            var lockAcquired = databasesContentLock.TryReadLock();

            while (!lockAcquired && !token.IsCancellationRequested && !Disposed)
            {
                Thread.Yield();
                lockAcquired = databasesContentLock.TryReadLock();
            }

            return lockAcquired;
        }

        /// <summary>
        /// Continuously try to take a databases content write lock
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>True if lock acquired</returns>
        public bool TryGetDatabasesContentWriteLock(CancellationToken token = default)
        {
            var lockAcquired = databasesContentLock.TryWriteLock();

            while (!lockAcquired && !token.IsCancellationRequested && !Disposed)
            {
                Thread.Yield();
                lockAcquired = databasesContentLock.TryWriteLock();
            }

            return lockAcquired;
        }

        /// <summary>
        /// Retrieves saved storage slots from parent checkpoint / AOF path
        /// e.g. if path contains directories: baseName, baseName_1, baseName_2, baseName_10
        /// slots 0,1,2,10 will be returned
        /// </summary>
        /// <remarks>
        /// The directory names identify the storage slots, not necessarily the logical database ids:
        /// a swap relabels a database without moving its files. Callers that need logical ids resolve
        /// them from the mapping persisted in the checkpoints; see
        /// <see cref="ResolveLogicalDatabaseIds"/>.
        /// </remarks>
        /// <param name="path">Parent path</param>
        /// <param name="baseName">Base name of directories containing database-specific checkpoints / AOFs</param>
        /// <param name="storageSlots">Storage slots extracted from parent path</param>
        /// <returns>True if successful</returns>
        internal static bool TryGetSavedDatabaseIds(string path, string baseName, out int[] storageSlots)
        {
            storageSlots = default;
            if (!Directory.Exists(path)) return false;

            var dirs = Directory.GetDirectories(path, $"{baseName}*", SearchOption.TopDirectoryOnly);
            storageSlots = new int[dirs.Length];
            for (var i = 0; i < dirs.Length; i++)
            {
                var dirName = new DirectoryInfo(dirs[i]).Name;
                var sepIdx = dirName.IndexOf('_');
                var storageSlot = 0;

                if (sepIdx != -1 && !int.TryParse(dirName.AsSpan(sepIdx + 1), out storageSlot))
                    continue;

                storageSlots[i] = storageSlot;
            }

            return true;
        }

        /// <summary>
        /// Try to add a new database
        /// </summary>
        /// <param name="dbId">Database ID</param>
        /// <param name="db">Database</param>
        /// <returns></returns>
        private bool TryAddDatabase(int dbId, GarnetDatabase db)
        {
            if (!databases.TrySetValue(dbId, db))
                return false;

            HandleDatabaseAdded(dbId);
            return true;
        }

        /// <summary>
        /// Handle a new database added
        /// </summary>
        /// <param name="dbId">ID of database added</param>
        private void HandleDatabaseAdded(int dbId)
        {
            // If size tracker exists and is stopped, start it (only if DB 0 size tracker is started as well)
            var db = databases.Map[dbId];
            if (sizeTrackersStarted)
                db.SizeTracker?.Start(StoreWrapper.ctsCommit.Token);

            AttachDatabaseMappingProvider(db);

            activeDbIds.TryGetNextId(out var nextIdx);
            activeDbIds.TrySetValue(nextIdx, db.Id);
        }

        /// <summary>
        /// Copy active databases from specified IDatabaseManager instance
        /// </summary>
        /// <param name="src">Source IDatabaseManager</param>
        /// <param name="enableAof">Enable AOF in copied databases</param>
        private void CopyDatabases(IDatabaseManager src, bool enableAof)
        {
            switch (src)
            {
                case SingleDatabaseManager sdbm:
                    var defaultDbCopy = new GarnetDatabase(0, sdbm.DefaultDatabase, enableAof);
                    sizeTrackersStarted = sdbm.SizeTracker?.IsStarted ?? false;
                    TryAddDatabase(0, defaultDbCopy);
                    return;
                case MultiDatabaseManager mdbm:
                    var activeDbIdsMapSize = mdbm.activeDbIds.ActualSize;
                    var activeDbIdsMapSnapshot = mdbm.activeDbIds.Map;
                    var databasesMapSnapshot = mdbm.databases.Map;
                    sizeTrackersStarted = mdbm.sizeTrackersStarted;

                    for (var i = 0; i < activeDbIdsMapSize; i++)
                    {
                        var dbId = activeDbIdsMapSnapshot[i];
                        var dbCopy = new GarnetDatabase(dbId, databasesMapSnapshot[dbId], enableAof);
                        TryAddDatabase(dbId, dbCopy);
                    }

                    return;
                default:
                    throw new NotImplementedException();
            }
        }

        /// <summary>
        /// Run pre-paused per-DB checkpoints in parallel, then resume the per-DB checkpoint locks
        /// and release the outer locks held by the caller.
        /// Caller must hold <see cref="databasesContentLock"/> as a reader and must have synchronously
        /// pause-locked the first <paramref name="pausedCount"/> entries of <paramref name="pausedDbIds"/>.
        /// Per-DB checkpoint locks are held until ALL per-DB checkpoints complete (not just each
        /// individual one) so a per-DB BGSAVE issued mid-flight during a general BGSAVE reliably
        /// observes the in-progress checkpoint and fails with "checkpoint already in progress".
        /// </summary>
        /// <param name="pausedDbIds">Buffer whose first <paramref name="pausedCount"/> entries are pause-locked database IDs.</param>
        /// <param name="pausedCount">Number of databases this request pause-locked and will checkpoint.</param>
        /// <param name="requestedCount">Number of databases this request was asked to checkpoint, which exceeds
        /// <paramref name="pausedCount"/> when a database was skipped because its checkpoint lock was already held.</param>
        /// <param name="multiDbLockHeld">Whether the caller holds <see cref="multiDbCheckpointingLock"/>.</param>
        /// <param name="token">Cancellation token.</param>
        /// <param name="logger">Logger.</param>
        private async Task<CheckpointStatus> RunPausedCheckpointsAndReleaseLocksAsync(int[] pausedDbIds, int pausedCount,
            int requestedCount, bool multiDbLockHeld, CancellationToken token, ILogger logger)
        {
            // Pre-fill with Task.CompletedTask so the catch path can safely await Task.WhenAll
            // even if the synchronous task-creation loop below throws partway through.
            var checkpointTasks = new Task[pausedCount];
            for (var i = 0; i < pausedCount; i++)
                checkpointTasks[i] = Task.CompletedTask;

            // Each checkpoint records its own outcome here rather than through its task's result, so a database
            // whose task was never created or which threw stays counted as a failure.
            var succeeded = new bool[pausedCount];

            try
            {
                // Force async so that the entry point can return synchronously to the caller.
                await Task.Yield();

                var databaseMapSnapshot = databases.Map;

                try
                {
                    for (var i = 0; i < pausedCount; i++)
                        checkpointTasks[i] = TakeOneCheckpointAsync(databaseMapSnapshot[pausedDbIds[i]], pausedDbIds[i], i);

                    await Task.WhenAll(checkpointTasks).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "Checkpointing threw exception");

                    // Make sure any tasks already started are observed before we resume the per-DB
                    // locks in the outer finally (otherwise we could resume a lock while its
                    // checkpoint is still running).
                    try { await Task.WhenAll(checkpointTasks).ConfigureAwait(false); }
                    catch { /* already logged above */ }
                }
            }
            finally
            {
                for (var i = 0; i < pausedCount; i++)
                    ResumeCheckpoints(pausedDbIds[i]);

                if (multiDbLockHeld)
                    multiDbCheckpointingLock.WriteUnlock();

                databasesContentLock.ReadUnlock();
            }

            var allSucceeded = true;
            for (var i = 0; i < pausedCount; i++)
                allSucceeded &= succeeded[i];

            if (!allSucceeded)
                return CheckpointStatus.Failed;

            // A database that could not be pause-locked already had a checkpoint in flight, so this request never
            // attempted it and cannot vouch for it. Reporting success would tell a foreground SAVE that every
            // requested database is on disk when one of them was skipped, and that skipped checkpoint may still
            // fail. The skipped database records its own outcome through its own RecordCheckpointOutcome, so its
            // LASTSAVE and rdb_last_bgsave_status stay truthful either way; this only stops the aggregate reply
            // from claiming more than the request actually did.
            //
            // Background requests reply before this runs, so the BGSAVE contract of "skip the busy databases and
            // report started" - asserted by MultiDatabaseSaveInProgressTest - is unaffected.
            return pausedCount < requestedCount ? CheckpointStatus.AlreadyInProgress : CheckpointStatus.Success;

            // Local function: take one per-DB checkpoint and update LASTSAVE. Does NOT resume the
            // per-DB lock — the outer finally above resumes all paused DBs after WhenAll completes.
            async Task TakeOneCheckpointAsync(GarnetDatabase db, int dbId, int slot)
            {
                var result = await TakeCheckpointAsync(db, logger: logger, token: token).ConfigureAwait(false);
                UpdateLastSaveData(dbId, result);
                succeeded[slot] = result.IsSuccessful;
            }
        }

        /// <summary>
        /// Resolve the database for the given ID and record its checkpoint outcome
        /// </summary>
        /// <param name="dbId">ID of the database that was checkpointed</param>
        /// <param name="result">Outcome of the checkpoint attempt</param>
        /// <remarks>
        /// The database is read from the map here rather than taken from the caller, so that a swap-db that ran while
        /// the checkpoint was in flight cannot attribute the outcome to the database that was swapped away.
        /// </remarks>
        private void UpdateLastSaveData(int dbId, CheckpointResult result)
        {
            var databasesMapSnapshot = databases.Map;

            RecordCheckpointOutcome(databasesMapSnapshot[dbId], result);
        }

        /// <inheritdoc/>
        public override void RecoverVectorSets()
        {
            var databasesMapSnapshot = databases.Map;

            var activeDbIdsMapSize = activeDbIds.ActualSize;
            var activeDbIdsMapSnapshot = activeDbIds.Map;

            for (var i = 0; i < activeDbIdsMapSize; i++)
            {
                var dbId = activeDbIdsMapSnapshot[i];
                var db = databasesMapSnapshot[dbId];

                db.VectorManager?.Initialize();
                db.VectorManager?.ReconcileRecoveredState();
                db.VectorManager?.WaitForQuiescence();
            }
        }

        public override void Dispose()
        {
            if (Disposed) return;

            Disposed = true;

            // Disable changes to databases map and dispose all databases
            while (!databases.mapLock.TryWriteLock())
                _ = Thread.Yield();

            foreach (var db in databases.Map)
                db?.Dispose();

            while (!databasesContentLock.TryWriteLock())
                _ = Thread.Yield();

            while (!activeDbIds.mapLock.TryWriteLock())
                _ = Thread.Yield();
        }

        public override (long numExpiredKeysFound, long totalRecordsScanned) ExpiredKeyDeletionScan(int dbId)
            => StoreExpiredKeyDeletionScan(GetDbById(dbId));

        public override (long keyCount, long expireCount) GetKeyspaceStats(int dbId)
            => GetDatabaseKeyspaceStats(GetDbById(dbId));

        private GarnetDatabase GetDbById(int dbId)
        {
            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;
            Debug.Assert(dbId < databasesMapSize && databasesMapSnapshot[dbId] != null);
            return databasesMapSnapshot[dbId];
        }

        public override (HybridLogScanMetrics mainStore, HybridLogScanMetrics objectStore)[] CollectHybridLogStats()
        {
            var databasesMapSnapshot = databases.Map;
            var result = new (HybridLogScanMetrics mainStore, HybridLogScanMetrics objectStore)[databasesMapSnapshot.Length];
            for (int i = 0; i < databasesMapSnapshot.Length; i++)
            {
                var db = databasesMapSnapshot[i];
                result[i] = CollectHybridLogStatsForDb(db);
            }
            return result;
        }
    }
}