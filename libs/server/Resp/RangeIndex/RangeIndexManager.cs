// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.IO;
using System.IO.Hashing;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Garnet.common;
using Garnet.server.BfTreeInterop;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Manages the lifecycle of RangeIndex (BfTree) instances stored as fixed-size stubs
    /// in Garnet's main store.
    ///
    /// <para>Architecture: Each RangeIndex key in the store holds a <see cref="RangeIndexStub"/>
    /// containing BfTree configuration metadata and a native pointer to the live BfTree instance.
    /// The manager tracks all live BfTree instances (keyed by Guid = XxHash128 of the key bytes —
    /// same scheme as the file-name prefix), coordinates checkpoint/flush/eviction snapshots,
    /// and handles lazy restore from disk.</para>
    ///
    /// <para>File layout (flat under two roots):</para>
    /// <list type="bullet">
    /// <item><b>Log root</b> (<c>{LogDir ?? CheckpointDir ?? cwd}/Store/rangeindex/</c>) — working
    /// files (<c>&lt;hash&gt;.data.bftree</c>) and immutable per-flush snapshots
    /// (<c>&lt;hash&gt;.&lt;addr:x16&gt;.flush.bftree</c>).</item>
    /// <item><b>Checkpoint root</b> (<c>{CheckpointDir}/Store/checkpoints[_dbId]/cpr-checkpoints/&lt;token&gt;/rangeindex/</c>) —
    /// per-checkpoint snapshots (<c>&lt;hash&gt;.bftree</c>); deleted automatically when
    /// Tsavorite removes the parent token directory.</item>
    /// </list>
    ///
    /// <para>Concurrency: Data operations (RI.SET, RI.GET, RI.DEL) acquire a shared lock
    /// via <see cref="ReadRangeIndex"/>; lifecycle operations (DEL key, eviction, checkpoint)
    /// acquire an exclusive lock. See <c>RangeIndexManager.Locking.cs</c> for details.</para>
    ///
    /// <para>liveIndexes access discipline: <see cref="liveIndexes"/> is consulted on the hot
    /// path ONLY for checkpoint coordination (<see cref="WaitForTreeCheckpoint"/>). All other
    /// hot-path code uses the stub's <c>TreeHandle</c> directly.</para>
    /// </summary>
    public sealed partial class RangeIndexManager : IDisposable
    {
        /// <summary>
        /// RecordType discriminator for RangeIndex records in the unified store.
        /// Stored in <c>RecordDataHeader.RecordType</c> to distinguish RI stubs
        /// from normal strings (0) and VectorSet stubs.
        /// </summary>
        internal const byte RangeIndexRecordType = 2;

        /// <summary>Size of the RangeIndex stub in bytes.</summary>
        internal const int IndexSizeBytes = RangeIndexStub.Size;

        /// <summary>Whether range index commands are enabled.</summary>
        public bool IsEnabled { get; }

        /// <summary>Gets the number of live (registered) BfTree indexes (activated + pending).</summary>
        internal int LiveIndexCount => liveIndexes.Count;

        /// <summary>
        /// Tracks BfTree entries (activated + pending), keyed by <see cref="KeyId"/> = a Guid
        /// derived from the key bytes via <c>XxHash128</c>. Same hash scheme used to derive the
        /// on-disk filename prefix; collision risk is the same as for filename collisions
        /// (cryptographically negligible).
        /// </summary>
        private readonly ConcurrentDictionary<Guid, TreeEntry> liveIndexes = new();

        private readonly ILogger logger;

        /// <summary>
        /// Log-tied root directory (without trailing separator). Holds the working file
        /// (<c>&lt;hash&gt;.data.bftree</c>) and immutable per-flush snapshots
        /// (<c>&lt;hash&gt;.&lt;addr:x16&gt;.flush.bftree</c>).
        /// </summary>
        private readonly string riLogRoot;

        /// <summary>
        /// Checkpoint-tied parent directory (the Tsavorite <c>cpr-checkpoints/</c> directory).
        /// Per-checkpoint snapshots live under <c>{cprDir}/&lt;token&gt;/rangeindex/&lt;hash&gt;.bftree</c>.
        /// May be null if checkpointing is not enabled.
        /// </summary>
        private readonly string cprDir;

        /// <summary>
        /// Global checkpoint barrier. When non-zero, a checkpoint is snapshotting trees.
        /// RI operations check this first (one volatile read on hot path); if set, they
        /// look up by <see cref="KeyId"/> and check the per-tree
        /// <see cref="TreeEntry.SnapshotPending"/> flag.
        /// </summary>
        private volatile bool checkpointInProgress;

        /// <summary>
        /// Checkpoint token from the last recovery. Used by
        /// <see cref="RebuildFromSnapshotIfPending"/> to locate the correct checkpoint
        /// snapshot file for above-FUA stubs recovered from snapshot.
        /// </summary>
        private Guid recoveredCheckpointToken;

        /// <summary>
        /// Per-tree entry in <see cref="liveIndexes"/>. Class (not struct) so the
        /// <see cref="SnapshotPending"/> field can be updated in-place via <c>Volatile.Write</c>.
        /// </summary>
        internal sealed class TreeEntry
        {
            /// <summary>The managed BfTree wrapper owning the native tree pointer.
            /// <c>null</c> for a "pending" entry — data.bftree on disk has correct content
            /// but no native BfTree has been opened yet (awaiting <c>RestoreTree</c> activation).</summary>
            public BfTreeService Tree;

            /// <summary>Hash of the Garnet key, used for lock striping.</summary>
            public readonly long KeyHash;

            /// <summary>32-character lowercase-hex prefix derived from the key (XxHash128 → Guid("N")).
            /// Used to construct file paths under both roots.</summary>
            public readonly string HashPrefix;

            /// <summary>
            /// 1 while this tree is being snapshotted for checkpoint, 0 otherwise.
            /// Set at <see cref="CheckpointTrigger.VersionShift"/> time, cleared after snapshot completes.
            /// RI data operations spin-wait on this flag when <see cref="checkpointInProgress"/> is set.
            /// </summary>
            public int SnapshotPending;

            public TreeEntry(BfTreeService tree, long keyHash, string hashPrefix)
            {
                Tree = tree;
                KeyHash = keyHash;
                HashPrefix = hashPrefix;
            }
        }

        /// <summary>
        /// Whether to remove old BfTree per-flush snapshots when their addresses fall below the new
        /// log BeginAddress. Per-checkpoint snapshots are removed by Tsavorite's checkpoint manager
        /// when it deletes the parent token directory; this flag controls only the per-flush cleanup
        /// performed by <see cref="OnTruncateImpl"/>.
        /// </summary>
        private readonly bool removeOutdatedCheckpoints;

        /// <summary>
        /// Creates a new <see cref="RangeIndexManager"/>.
        /// </summary>
        /// <param name="enabled">Whether range index commands are enabled.</param>
        /// <param name="riLogRoot">Log-tied root directory for working/flush files (e.g.
        /// <c>{LogDir ?? CheckpointDir ?? cwd}/Store/rangeindex</c>). May be null/empty for
        /// memory-only test scenarios; disk-backed trees will fail to open in that case.</param>
        /// <param name="cprDir">Tsavorite <c>cpr-checkpoints/</c> directory; per-checkpoint snapshots
        /// live under <c>{cprDir}/&lt;token&gt;/rangeindex/</c>. May be null if no checkpointing.</param>
        /// <param name="removeOutdatedCheckpoints">Whether <see cref="OnTruncateImpl"/> should delete
        /// per-flush snapshots whose address is below the new BeginAddress.</param>
        /// <param name="logger">Optional logger.</param>
        public RangeIndexManager(bool enabled, string riLogRoot = null, string cprDir = null,
            bool removeOutdatedCheckpoints = true, ILogger logger = null)
        {
            IsEnabled = enabled;
            this.riLogRoot = riLogRoot;
            this.cprDir = cprDir;
            this.removeOutdatedCheckpoints = removeOutdatedCheckpoints;
            this.logger = logger;
            rangeIndexLocks = new ReadOptimizedLock(Environment.ProcessorCount);

            if (enabled && !string.IsNullOrEmpty(riLogRoot))
            {
                try { Directory.CreateDirectory(riLogRoot); }
                catch (Exception ex) { logger?.LogWarning(ex, "Failed to create RI log root: {Path}", riLogRoot); }
            }
        }

        /// <summary>
        /// Compute the unambiguous identity of a RangeIndex key as a 128-bit Guid.
        /// Same scheme used to derive the on-disk filename prefix
        /// (<see cref="HashKeyToPrefix"/>). The cryptographically negligible collision risk
        /// is the same risk we accept for filename collisions.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Guid KeyId(ReadOnlySpan<byte> keyBytes) => new(XxHash128.Hash(keyBytes));

        /// <summary>
        /// Hash key bytes to a 32-character lowercase-hex filename prefix using XxHash128.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static string HashKeyToPrefix(ReadOnlySpan<byte> keyBytes)
            => new Guid(XxHash128.Hash(keyBytes)).ToString("N");

        // -- Path helpers --

        /// <summary>{logRoot}/&lt;hash&gt;.data.bftree</summary>
        internal string LogDataPath(string hashPrefix)
            => Path.Combine(riLogRoot ?? string.Empty, hashPrefix + ".data.bftree");

        /// <summary>{logRoot}/&lt;hash&gt;.&lt;addr:x16&gt;.flush.bftree</summary>
        internal string LogFlushPath(string hashPrefix, long logicalAddress)
            => Path.Combine(riLogRoot ?? string.Empty, $"{hashPrefix}.{logicalAddress:x16}.flush.bftree");

        /// <summary>{cprDir}/&lt;token&gt;/rangeindex/&lt;hash&gt;.bftree</summary>
        internal string CheckpointSnapshotPath(string hashPrefix, Guid checkpointToken)
            => Path.Combine(cprDir ?? string.Empty, checkpointToken.ToString(), "rangeindex", hashPrefix + ".bftree");

        /// <summary>The directory holding per-checkpoint RI snapshots for a given token.</summary>
        internal string CheckpointSnapshotDir(Guid checkpointToken)
            => Path.Combine(cprDir ?? string.Empty, checkpointToken.ToString(), "rangeindex");

        // -- Convenience helpers used outside this class (RangeIndexOps via raw key) --
        internal string LogDataPathFor(ReadOnlySpan<byte> keyBytes) => LogDataPath(HashKeyToPrefix(keyBytes));

        /// <summary>
        /// Creates a new BfTree instance via the native interop layer.
        /// For disk-backed trees, derives the file path deterministically from the key bytes.
        /// </summary>
        internal BfTreeService CreateBfTree(
            StorageBackendType storageBackend,
            ReadOnlySpan<byte> keyBytes,
            ulong cacheSize,
            uint minRecordSize,
            uint maxRecordSize,
            uint maxKeyLen,
            uint leafPageSize)
        {
            string filePath = null;
            if (storageBackend == StorageBackendType.Disk)
            {
                filePath = LogDataPathFor(keyBytes);
                Directory.CreateDirectory(Path.GetDirectoryName(filePath)!);
            }

            return new BfTreeService(
                storageBackend: storageBackend,
                filePath: filePath,
                cbSizeByte: cacheSize,
                cbMinRecordSize: minRecordSize,
                cbMaxRecordSize: maxRecordSize,
                cbMaxKeyLen: maxKeyLen,
                leafPageSize: leafPageSize);
        }

        /// <summary>
        /// Compute the leaf page size from the max record size when not explicitly specified.
        /// </summary>
        /// <param name="maxRecordSize">The configured maximum record size in bytes.</param>
        /// <returns>
        /// A power-of-two page size:
        /// <list type="bullet">
        /// <item>For <paramref name="maxRecordSize"/> ≤ 2 KB → 4 KB</item>
        /// <item>For larger values → 2.5× record size rounded to next power of 2, capped at 32 KB</item>
        /// </list>
        /// </returns>
        internal static uint ComputeLeafPageSize(uint maxRecordSize)
        {
            if (maxRecordSize <= 2048)
                return 4096;

            // 2.5x, capped at 32KB
            var target = (uint)(maxRecordSize * 2.5);
            if (target > 32768)
                target = 32768;

            // Round up to next power of 2
            return RoundUpToPowerOf2(target);
        }

        /// <summary>
        /// Rounds up to the next power of 2 using the standard bit-manipulation algorithm.
        /// </summary>
        private static uint RoundUpToPowerOf2(uint v)
        {
            v--;
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            v++;
            return v;
        }

        /// <summary>
        /// Register a BfTreeService in the live index dictionary after successful creation or restore.
        /// Cold path — called once per RI.CREATE or activation (RestoreTree).
        /// </summary>
        internal void RegisterIndex(BfTreeService bfTree, long keyHash, ReadOnlySpan<byte> keyBytes)
        {
            var hashPrefix = HashKeyToPrefix(keyBytes);
            var keyId = KeyId(keyBytes);
            var newEntry = new TreeEntry(bfTree, keyHash, hashPrefix);
            // Activate-or-add: if a pending entry already exists, upgrade it; else add new.
            liveIndexes.AddOrUpdate(keyId, newEntry, (_, existing) =>
            {
                if (existing.Tree is null)
                {
                    existing.Tree = bfTree;
                    return existing;
                }
                // Already activated by a racing thread — caller should dispose the duplicate.
                return existing;
            });
            // If we lost the race (existing.Tree non-null), dispose our duplicate.
            if (liveIndexes.TryGetValue(keyId, out var winner) && !ReferenceEquals(winner.Tree, bfTree))
            {
                try { bfTree.Dispose(); }
                catch (Exception ex) { logger?.LogWarning(ex, "Failed to dispose duplicate BfTree on register race"); }
            }
        }

        /// <summary>
        /// Register a "pending" entry: data.bftree on disk has correct content (just pre-staged) but
        /// no native BfTree has been opened yet. Activated by a later <see cref="RegisterIndex"/> call
        /// from <c>RestoreTree</c>.
        /// </summary>
        internal void RegisterPending(ReadOnlySpan<byte> keyBytes, long keyHash)
        {
            var hashPrefix = HashKeyToPrefix(keyBytes);
            var keyId = KeyId(keyBytes);
            var pending = new TreeEntry(tree: null, keyHash, hashPrefix);
            // Add only if no entry exists; if one exists (activated or pending), leave it alone.
            _ = liveIndexes.TryAdd(keyId, pending);
        }

        /// <summary>
        /// Unregister the entry for a key. Disposes the native tree if present and the entry
        /// is owned by this manager. The caller must already hold the exclusive lock for this key.
        /// </summary>
        /// <returns><c>true</c> if an entry was found and removed.</returns>
        internal bool UnregisterIndex(ReadOnlySpan<byte> keyBytes)
        {
            var keyId = KeyId(keyBytes);
            if (liveIndexes.TryRemove(keyId, out var entry))
            {
                try { entry.Tree?.Dispose(); }
                catch (Exception ex) { logger?.LogWarning(ex, "Failed to dispose BfTree on unregister"); }
                return true;
            }
            return false;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            foreach (var kvp in liveIndexes)
            {
                try { kvp.Value.Tree?.Dispose(); }
                catch (Exception ex) { logger?.LogWarning(ex, "Failed to dispose BfTree {Hash}", kvp.Value.HashPrefix); }
            }
            liveIndexes.Clear();
        }

        /// <summary>
        /// Atomically pre-stage <c>data.bftree</c> from <c>&lt;srcAddr:x16&gt;.flush.bftree</c>
        /// and register a pending entry in <see cref="liveIndexes"/> so a subsequent checkpoint
        /// will capture it via <see cref="SnapshotAllTreesForCheckpoint"/>.
        ///
        /// <para>Called from RIPROMOTE PostCopyUpdater (cold case: src.TreeHandle == 0) and
        /// from <c>PostCopyToTail</c> (compaction with disk source).</para>
        ///
        /// <para>Concurrency: takes the per-key EXCLUSIVE rangeIndex lock for the duration of the
        /// file copy. This is required because <c>CASRecordIntoChain</c> unseals dst immediately
        /// on CAS-success (in <c>Helpers.cs.CASRecordIntoChain</c>), so by the time this trigger
        /// fires, concurrent readers can already observe dst with <c>TreeHandle == 0</c> and
        /// invoke <c>RestoreTree</c>, which opens <c>data.bftree</c> under its own per-key
        /// exclusive lock. Holding the exclusive lock here blocks <c>RestoreTree</c> until the
        /// file is fully written, preventing it from observing a partial <c>data.bftree</c>.</para>
        ///
        /// <para>A direct <c>File.Copy(overwrite: true)</c> is sufficient under this lock — the
        /// exclusive lock serializes against any reader that would open <c>data.bftree</c>, and
        /// against other concurrent <c>PreStageAndRegisterPending</c> calls for the same key.
        /// A crash mid-copy is self-healing: post-recovery either <c>OnRecoverySnapshotRead</c>
        /// (above-FUA stub) or the next RIPROMOTE-cold (FlagFlushed=1 stub) re-pre-stages and
        /// overwrites any partial file before <c>RestoreTree</c> can observe it.</para>
        /// </summary>
        internal void PreStageAndRegisterPending(ReadOnlySpan<byte> keyBytes, long srcFlushAddress)
        {
            if (string.IsNullOrEmpty(riLogRoot))
                return;

            var hashPrefix = HashKeyToPrefix(keyBytes);
            var snapshotPath = LogFlushPath(hashPrefix, srcFlushAddress);
            if (!File.Exists(snapshotPath))
            {
                logger?.LogWarning("PreStageAndRegisterPending: source flush file missing: {Path}", snapshotPath);
                return;
            }

            var dataPath = LogDataPath(hashPrefix);
            var keyHash = GarnetKeyComparer.StaticGetHashCode64((FixedSpanByteKey)PinnedSpanByte.FromPinnedSpan(keyBytes));

            // Acquire the per-key exclusive lock for the duration of the file copy AND the
            // pending-entry registration. This blocks any concurrent RestoreTree (which also
            // takes the exclusive lock) from observing a partial data.bftree.
            rangeIndexLocks.AcquireExclusiveLock(keyHash, out var lockToken);
            try
            {
                Directory.CreateDirectory(riLogRoot);
                File.Copy(snapshotPath, dataPath, overwrite: true);

                var keyId = KeyId(keyBytes);
                _ = liveIndexes.TryAdd(keyId, new TreeEntry(tree: null, keyHash, hashPrefix));
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "PreStageAndRegisterPending: copy/register failed for {Hash}", hashPrefix);
            }
            finally
            {
                rangeIndexLocks.ReleaseExclusiveLock(lockToken);
            }
        }

        /// <summary>
        /// Pre-stage <c>data.bftree</c> from <c>cpr-checkpoints/&lt;recoveredCheckpointToken&gt;/rangeindex/&lt;hash&gt;.bftree</c>
        /// during recovery. Called from <c>OnRecoverySnapshotRead</c> for above-FUA-at-checkpoint stubs
        /// (snapshot file may be deleted post-recovery, so this MUST run during recovery).
        /// Registers a pending entry so any first checkpoint after recovery captures the key correctly.
        ///
        /// <para>Note: a direct <c>File.Copy(overwrite: true)</c> is used instead of the atomic
        /// <c>.tmp</c> + <c>File.Move</c> pattern (cf. <see cref="PreStageAndRegisterPending"/>):
        /// recovery is single-threaded with no concurrent readers, and a crash mid-copy is
        /// self-healing — the next recovery attempt re-fires <c>OnRecoverySnapshotRead</c> for
        /// the same stub and fully overwrites any partial file before any <c>RestoreTree</c>
        /// can observe it.</para>
        /// </summary>
        internal void RebuildFromSnapshotIfPending(ReadOnlySpan<byte> keyBytes)
        {
            if (string.IsNullOrEmpty(riLogRoot) || string.IsNullOrEmpty(cprDir) || recoveredCheckpointToken == Guid.Empty)
                return;

            var hashPrefix = HashKeyToPrefix(keyBytes);
            var snapshotPath = CheckpointSnapshotPath(hashPrefix, recoveredCheckpointToken);
            if (!File.Exists(snapshotPath))
            {
                // Below-FUA stubs have no checkpoint snapshot; RIPROMOTE PostCopyUpdater handles
                // them lazily via the per-flush snapshot file on first access.
                logger?.LogDebug("OnRecoverySnapshotRead: snapshot absent for {Hash} — RIPROMOTE will handle lazily", hashPrefix);
                return;
            }

            var dataPath = LogDataPath(hashPrefix);

            try
            {
                Directory.CreateDirectory(riLogRoot);
                File.Copy(snapshotPath, dataPath, overwrite: true);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "RebuildFromSnapshotIfPending: copy failed for {Hash}", hashPrefix);
                return;
            }

            var keyHash = GarnetKeyComparer.StaticGetHashCode64((FixedSpanByteKey)PinnedSpanByte.FromPinnedSpan(keyBytes));
            var keyId = KeyId(keyBytes);
            _ = liveIndexes.TryAdd(keyId, new TreeEntry(tree: null, keyHash, hashPrefix));
        }

        /// <summary>
        /// Store the recovered checkpoint token for use by
        /// <see cref="RebuildFromSnapshotIfPending"/>.
        /// </summary>
        internal void SetRecoveredCheckpointToken(Guid token) => recoveredCheckpointToken = token;

        /// <summary>
        /// Set the checkpoint barrier. Called at version shift (PREPARE → IN_PROGRESS).
        /// Marks all entries (activated + pending) as snapshot-pending, then sets the global flag.
        /// </summary>
        internal void SetCheckpointBarrier(Guid checkpointToken)
        {
            foreach (var kvp in liveIndexes)
                Volatile.Write(ref kvp.Value.SnapshotPending, 1);
            checkpointInProgress = true;
        }

        /// <summary>
        /// Clear the checkpoint barrier. Called after snapshot completes at WAIT_FLUSH.
        /// </summary>
        internal void ClearCheckpointBarrier()
        {
            checkpointInProgress = false;
            foreach (var kvp in liveIndexes)
                Volatile.Write(ref kvp.Value.SnapshotPending, 0);
        }

        /// <summary>
        /// Snapshot all live BfTrees for a checkpoint. Called at FlushBegin.
        /// Activated entries → <c>Tree.SnapshotToFile</c>. Pending entries (Tree=null) →
        /// <c>File.Copy(data.bftree → snapshot path)</c>.
        /// Failure is fatal — the exception propagates to the state machine driver.
        /// </summary>
        internal void SnapshotAllTreesForCheckpoint(Guid checkpointToken)
        {
            try
            {
                if (string.IsNullOrEmpty(cprDir))
                    return;

                var snapshotDir = CheckpointSnapshotDir(checkpointToken);

                foreach (var kvp in liveIndexes)
                {
                    var entry = kvp.Value;

                    if (Volatile.Read(ref entry.SnapshotPending) == 0)
                        continue;

                    if (entry.Tree?.StorageBackend == StorageBackendType.Memory)
                    {
                        Volatile.Write(ref entry.SnapshotPending, 0);
                        continue;
                    }

                    rangeIndexLocks.AcquireExclusiveLock(entry.KeyHash, out var lockToken);
                    try
                    {
                        Directory.CreateDirectory(snapshotDir);
                        var checkpointPath = CheckpointSnapshotPath(entry.HashPrefix, checkpointToken);

                        if (entry.Tree is not null)
                        {
                            entry.Tree.SnapshotToFile(checkpointPath);
                        }
                        else
                        {
                            // Pending entry: data.bftree is the only correct source — guaranteed
                            // pre-staged by PreStageAndRegisterPending or RebuildFromSnapshotIfPending.
                            var dataPath = LogDataPath(entry.HashPrefix);
                            if (File.Exists(dataPath))
                                File.Copy(dataPath, checkpointPath, overwrite: true);
                            else
                                logger?.LogWarning("SnapshotAllTreesForCheckpoint: data.bftree missing for pending {Hash}", entry.HashPrefix);
                        }
                    }
                    finally
                    {
                        Volatile.Write(ref entry.SnapshotPending, 0);
                        rangeIndexLocks.ReleaseExclusiveLock(lockToken);
                    }
                }
            }
            finally
            {
                ClearCheckpointBarrier();
            }
        }

        /// <summary>
        /// On log truncation, delete per-flush snapshot files in the log root whose address is
        /// strictly less than <paramref name="newBeginAddress"/>. Also defensively cleans any
        /// stale <c>.data.bftree.tmp</c> files (current code paths use direct
        /// <c>System.IO.File.Copy</c> so no <c>.tmp</c> files are produced; this cleanup
        /// catches orphans from any legacy or future atomic-rename code paths).
        /// Per-checkpoint snapshots are NOT touched here — Tsavorite's checkpoint manager
        /// removes them when it deletes the parent token directory.
        /// </summary>
        internal void OnTruncateImpl(long newBeginAddress)
        {
            if (!removeOutdatedCheckpoints || string.IsNullOrEmpty(riLogRoot))
                return;
            if (!Directory.Exists(riLogRoot))
                return;

            try
            {
                foreach (var path in Directory.EnumerateFiles(riLogRoot))
                {
                    var name = Path.GetFileName(path);

                    // Defensive: clean up any stale .tmp orphan from legacy code paths.
                    if (name.EndsWith(".data.bftree.tmp", StringComparison.Ordinal))
                    {
                        TryDelete(path);
                        continue;
                    }

                    if (!name.EndsWith(".flush.bftree", StringComparison.Ordinal))
                        continue;

                    // Pattern: <hash>.<addr:x16>.flush.bftree
                    // hash is 32 hex chars, then '.', then 16 hex chars (addr), then ".flush.bftree".
                    if (name.Length != 32 + 1 + 16 + ".flush.bftree".Length)
                        continue;

                    var addrSegment = name.AsSpan(33, 16);
                    if (!long.TryParse(addrSegment, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var addr))
                        continue;

                    if (addr < newBeginAddress)
                        TryDelete(path);
                }
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "OnTruncate: enumeration failed under {Root}", riLogRoot);
            }

            void TryDelete(string p)
            {
                try { File.Delete(p); }
                catch (Exception ex) { logger?.LogWarning(ex, "OnTruncate: failed to delete {Path}", p); }
            }
        }

        /// <summary>
        /// Log RI.SET to AOF via direct enqueue (no synthetic RMW).
        /// Skipped when <paramref name="storedProcMode"/> is true (stored procedure logs as a unit).
        /// </summary>
        internal void ReplicateRangeIndexSet(PinnedSpanByte key, PinnedSpanByte field, PinnedSpanByte value,
            GarnetAppendOnlyFile appendOnlyFile, long version, int sessionId, bool storedProcMode)
        {
            if (appendOnlyFile == null || storedProcMode) return;

            var replicateParseState = new SessionParseState();
            replicateParseState.InitializeWithArguments(field, value);
            var input = new StringInput(RespCommand.RISET, ref replicateParseState);
            input.header.flags |= RespInputFlags.Deterministic;

            appendOnlyFile.Log.Enqueue(
                AofEntryType.StoreRMW,
                version,
                sessionId,
                key.ReadOnlySpan,
                ref input,
                out _);
        }

        /// <summary>
        /// Log RI.DEL to AOF via direct enqueue (no synthetic RMW).
        /// Skipped when <paramref name="storedProcMode"/> is true (stored procedure logs as a unit).
        /// </summary>
        internal void ReplicateRangeIndexDel(PinnedSpanByte key, PinnedSpanByte field,
            GarnetAppendOnlyFile appendOnlyFile, long version, int sessionId, bool storedProcMode)
        {
            if (appendOnlyFile == null || storedProcMode) return;

            var replicateParseState = new SessionParseState();
            replicateParseState.InitializeWithArgument(field);
            var input = new StringInput(RespCommand.RIDEL, ref replicateParseState);
            input.header.flags |= RespInputFlags.Deterministic;

            appendOnlyFile.Log.Enqueue(
                AofEntryType.StoreRMW,
                version,
                sessionId,
                key.ReadOnlySpan,
                ref input,
                out _);
        }

        /// <summary>
        /// Handle RI.CREATE replay from AOF.
        /// </summary>
        /// <remarks>
        /// The AOF entry contains the serialized stub bytes (including a stale TreeHandle
        /// from the original process). This method:
        /// <list type="number">
        /// <item>Extracts BfTree configuration from the stale stub.</item>
        /// <item>Creates a fresh BfTree instance with a new native pointer.</item>
        /// <item>Replaces the stale TreeHandle in the stub bytes with the new pointer.</item>
        /// <item>Lets the normal RMW path (InitialUpdater) create the store record.</item>
        /// </list>
        /// If the key already exists (e.g., AOF replay of a duplicate RI.CREATE after
        /// checkpoint recovery), the RMW returns <c>InPlaceUpdated</c> and the fresh
        /// BfTree is disposed.
        /// </remarks>
        /// <param name="session">The storage session for issuing the RMW.</param>
        /// <param name="key">The Garnet key being created.</param>
        /// <param name="input">The RMW input containing the stub bytes in parseState.</param>
        internal unsafe void HandleRangeIndexCreateReplay(StorageSession session, ReadOnlySpan<byte> key, ref StringInput input)
        {
            var stubSpan = input.parseState.GetArgSliceByRef(0).Span;
            if (stubSpan.Length != IndexSizeBytes)
                throw new GarnetException($"Corrupt RI.CREATE AOF entry: stub size {stubSpan.Length}, expected {IndexSizeBytes}");

            ref var stub = ref Unsafe.As<byte, RangeIndexStub>(ref MemoryMarshal.GetReference(stubSpan));

            // Create a fresh BfTree from the config in the stub
            BfTreeService bfTree;
            try
            {
                bfTree = CreateBfTree(
                    (StorageBackendType)stub.StorageBackend, key,
                    stub.CacheSize, stub.MinRecordSize, stub.MaxRecordSize,
                    stub.MaxKeyLen, stub.LeafPageSize);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Failed to recreate BfTree during AOF replay");
                return;
            }

            // Replace stale handle with fresh one in the stub bytes
            stub.TreeHandle = bfTree.NativePtr;
            stub.Flags = 0;

            // Let the normal RMW path create the record from the updated stub bytes
            var output = new StringOutput();
            var pinnedKey = PinnedSpanByte.FromPinnedSpan(key);
            var status = session.stringBasicContext.RMW((FixedSpanByteKey)pinnedKey, ref input, ref output);
            if (status.IsPending)
                StorageSession.CompletePendingForSession(ref status, ref output, ref session.stringBasicContext);

            if (status.Record.Created)
            {
                var keyHash = session.stringBasicContext.GetKeyHash((FixedSpanByteKey)pinnedKey);
                RegisterIndex(bfTree, keyHash, key);
            }
            else
            {
                bfTree.Dispose();
            }
        }

        /// <summary>
        /// Handle RI.SET replay from AOF. Acquires a shared lock, reads the stub to get
        /// the live BfTree pointer, then performs the native insert.
        /// </summary>
        /// <param name="session">The storage session for reading the stub.</param>
        /// <param name="key">The Garnet key of the RangeIndex.</param>
        /// <param name="input">The RMW input containing field and value in parseState.</param>
        internal void HandleRangeIndexSetReplay(StorageSession session, ReadOnlySpan<byte> key, ref StringInput input)
        {
            var field = input.parseState.GetArgSliceByRef(0);
            var value = input.parseState.GetArgSliceByRef(1);

            var pinnedKey = PinnedSpanByte.FromPinnedSpan(key);
            var inputCopy = input;
            inputCopy.arg1 = default;
            Span<byte> stubSpan = stackalloc byte[IndexSizeBytes];

            using (ReadRangeIndex(session, pinnedKey, ref inputCopy, stubSpan, out var status))
            {
                if (status != GarnetStatus.OK) return;
                var treePtr = ReadIndex(stubSpan).TreeHandle;
                if (treePtr == nint.Zero) return;
                BfTreeService.InsertByPtr(treePtr, field, value);
            }
        }

        /// <summary>
        /// Handle RI.DEL replay from AOF. Acquires a shared lock, reads the stub to get
        /// the live BfTree pointer, then performs the native delete.
        /// </summary>
        /// <param name="session">The storage session for reading the stub.</param>
        /// <param name="key">The Garnet key of the RangeIndex.</param>
        /// <param name="input">The RMW input containing the field in parseState.</param>
        internal void HandleRangeIndexDelReplay(StorageSession session, ReadOnlySpan<byte> key, ref StringInput input)
        {
            var field = input.parseState.GetArgSliceByRef(0);

            var pinnedKey = PinnedSpanByte.FromPinnedSpan(key);
            var inputCopy = input;
            inputCopy.arg1 = default;
            Span<byte> stubSpan = stackalloc byte[IndexSizeBytes];

            using (ReadRangeIndex(session, pinnedKey, ref inputCopy, stubSpan, out var status))
            {
                if (status != GarnetStatus.OK) return;
                var treePtr = ReadIndex(stubSpan).TreeHandle;
                if (treePtr == nint.Zero) return;
                BfTreeService.DeleteByPtr(treePtr, field);
            }
        }
    }
}