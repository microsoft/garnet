// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.IO;
using System.IO.Hashing;
using System.Runtime.CompilerServices;
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
        /// Tsavorite store epoch, used for deferred BfTree disposal via
        /// <c>storeEpoch.BumpCurrentEpoch(...)</c>. Ensures concurrent readers using a
        /// TreeHandle are not affected by a concurrent DEL — the actual dispose runs
        /// only after all current epoch holders move past.
        /// </summary>
        private readonly LightEpoch storeEpoch;

        /// <summary>
        /// Log-tied root directory (without trailing separator). Holds the working file
        /// (<c>&lt;hash&gt;.data.bftree</c>), CPR scratch files (<c>&lt;hash&gt;.scratch.cpr</c>),
        /// and immutable per-flush snapshots (<c>&lt;hash&gt;.&lt;addr:x16&gt;.flush.bftree</c>).
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

            /// <summary>
            /// Per-tree snapshot serialization atomic. 0 = idle; 1 = a snapshot is in flight.
            /// Both <see cref="GarnetRecordTriggers.OnFlush"/> and
            /// <see cref="SnapshotAllTreesForCheckpoint"/> claim this before calling
            /// <c>cpr_snapshot</c> so the two callers do not race for bftree's internal
            /// <c>snapshot_in_progress</c> flag (which would no-op one of them silently).
            /// </summary>
            public int SnapshotInProgress;

            /// <summary>Try to claim the per-tree snapshot atomic. Returns true if claimed.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool TryClaimSnapshot()
                => Interlocked.CompareExchange(ref SnapshotInProgress, 1, 0) == 0;

            /// <summary>Release the per-tree snapshot atomic.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void ReleaseSnapshot()
                => Volatile.Write(ref SnapshotInProgress, 0);

            /// <summary>Spin-wait until the per-tree snapshot atomic is released.</summary>
            public void WaitForSnapshot()
            {
                while (Volatile.Read(ref SnapshotInProgress) != 0)
                    Thread.Yield();
            }

            public TreeEntry(BfTreeService tree, long keyHash, string hashPrefix)
            {
                Tree = tree;
                KeyHash = keyHash;
                HashPrefix = hashPrefix;
            }
        }

        /// <summary>
        /// Creates a new <see cref="RangeIndexManager"/>. Constructed only when range index
        /// is enabled in server options; <c>GarnetServer</c> passes <c>null</c> in place of
        /// a manager when the feature is disabled, so this constructor never runs in the
        /// disabled case.
        /// </summary>
        /// <param name="riLogRoot">Log-tied root directory for working/flush files (e.g.
        /// <c>{LogDir ?? CheckpointDir ?? cwd}/Store/rangeindex</c>). MUST be a non-empty path
        /// and the directory MUST be creatable — the constructor throws otherwise so
        /// misconfiguration (missing permissions, bad path, etc.) surfaces at server startup
        /// rather than at first use.</param>
        /// <param name="cprDir">Tsavorite <c>cpr-checkpoints/</c> directory; per-checkpoint snapshots
        /// live under <c>{cprDir}/&lt;token&gt;/rangeindex/</c>. May be null if no checkpointing.</param>
        /// <param name="storeEpoch">The store's <see cref="LightEpoch"/>; used to defer native
        /// <c>BfTree.Dispose</c> + file deletion past any in-flight reader observing the
        /// stub's <c>TreeHandle</c>. May be null in unit-test scenarios with no concurrent
        /// readers; in that case disposal is performed synchronously.</param>
        /// <param name="logger">Optional logger.</param>
        /// <exception cref="ArgumentException">Thrown when <paramref name="riLogRoot"/> is
        /// null or empty.</exception>
        /// <exception cref="IOException">Thrown when the riLogRoot directory cannot be
        /// created (e.g., insufficient permissions). Wraps the underlying exception.</exception>
        public RangeIndexManager(string riLogRoot, string cprDir = null,
            LightEpoch storeEpoch = null, ILogger logger = null)
        {
            if (string.IsNullOrEmpty(riLogRoot))
                throw new ArgumentException(
                    "RangeIndexManager: riLogRoot is required.",
                    nameof(riLogRoot));

            this.riLogRoot = riLogRoot;
            this.cprDir = cprDir;
            this.storeEpoch = storeEpoch;
            this.logger = logger;
            rangeIndexLocks = new ReadOptimizedLock(Environment.ProcessorCount);

            try
            {
                Directory.CreateDirectory(riLogRoot);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "RangeIndexManager: failed to create riLogRoot {Path}", riLogRoot);
                throw new IOException(
                    $"RangeIndexManager: failed to create riLogRoot '{riLogRoot}'. " +
                    "Check that the parent directory exists and the process has write permissions.",
                    ex);
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

        /// <summary>{logRoot}/&lt;hash&gt;.scratch.cpr — bftree CPR snapshot scratch file (overwritten each cpr_snapshot).</summary>
        internal string LogScratchPath(string hashPrefix)
            => Path.Combine(riLogRoot ?? string.Empty, hashPrefix + ".scratch.cpr");

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
        internal string LogScratchPathFor(ReadOnlySpan<byte> keyBytes) => LogScratchPath(HashKeyToPrefix(keyBytes));

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
            var hashPrefix = HashKeyToPrefix(keyBytes);
            string filePath = null;
            string snapshotFilePath = null;

            if (storageBackend == StorageBackendType.Disk)
            {
                filePath = LogDataPath(hashPrefix);
            }

            // Configure the bftree's CPR snapshot scratch path. cpr_snapshot writes here;
            // OnFlush / SnapshotAllTreesForCheckpoint File.Move scratch -> final destination.
            // Required for both backends; leave null only if riLogRoot is unset (test scenarios).
            // riLogRoot itself was already created in the constructor.
            if (!string.IsNullOrEmpty(riLogRoot))
            {
                snapshotFilePath = LogScratchPath(hashPrefix);
            }

            return new BfTreeService(
                storageBackend: storageBackend,
                filePath: filePath,
                snapshotFilePath: snapshotFilePath,
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
            if (liveIndexes.TryAdd(keyId, newEntry))
                return; // First registration for this key — done.

            // A prior entry exists. Per caller invariants:
            //   - RICREATE: invoked only when the underlying RMW reports Record.Created=true,
            //     so a prior entry cannot exist for the same key on the create path.
            //   - RestoreTree: holds the per-key rangeIndexLocks X-lock for the duration of
            //     RegisterIndex, so concurrent RegisterIndex on the same key is impossible.
            //     The only legitimate prior entry is a pending one (Tree==null) registered
            //     earlier by PreStageAndRegisterPending; we activate it in place.
            // CompareExchange makes the activation atomic so even a future caller-invariant
            // violation cannot result in two threads both believing they activated the entry
            // (one would observe a non-null prior in CompareExchange and dispose its duplicate).
            if (liveIndexes.TryGetValue(keyId, out var existing))
            {
                var prior = Interlocked.CompareExchange(ref existing.Tree, bfTree, null);
                if (prior is null)
                    return; // We activated the pending entry.
            }

            logger?.LogError(
                "RegisterIndex: liveIndexes entry for {Hash} is unexpectedly already activated. " +
                "Caller invariant violated (RICREATE should fire only on Record.Created=true; " +
                "RestoreTree must hold the per-key rangeIndexLocks X-lock). Disposing duplicate.",
                hashPrefix);
            DisposeBfTreeDeferred(bfTree, "duplicate-register");
        }

        /// <summary>
        /// Defer-dispose a BfTree past any reader that may still be using its TreeHandle.
        /// Falls back to synchronous dispose when no <see cref="storeEpoch"/> is wired
        /// (unit-test scenarios with no concurrent readers).
        /// </summary>
        private void DisposeBfTreeDeferred(BfTreeService bfTree, string reason)
        {
            if (storeEpoch != null)
            {
                var loser = bfTree;
                storeEpoch.BumpCurrentEpoch(() =>
                {
                    try { loser.Dispose(); }
                    catch (Exception ex) { logger?.LogWarning(ex, "Deferred dispose failed: {Reason}", reason); }
                });
            }
            else
            {
                try { bfTree.Dispose(); }
                catch (Exception ex) { logger?.LogWarning(ex, "Synchronous dispose failed: {Reason}", reason); }
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
        /// (above-FUA stub) or the next RIPROMOTE-cold (IsFlushed=true stub) re-pre-stages and
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
                // Per Invariant 2 (Per-flush snapshot invariant), the per-flush file must exist
                // for any IsFlushed=true stub at addr >= BeginAddress. If it's missing here, the
                // invariant has been violated (likely a race with a concurrent OnTruncate, or
                // external file deletion). Log loudly as ERROR — recovering from any other source
                // file would risk restoring the wrong tree version. Leave the destination record
                // with TreeHandle=0 and no pending entry; the next RestoreTree will return NOTFOUND
                // for the affected key, surfacing the data loss explicitly rather than silently
                // restoring incorrect data.
                logger?.LogError("PreStageAndRegisterPending: invariant violation — source flush file missing: {Path}. " +
                    "The destination record at the tail will have no pending entry; subsequent RestoreTree " +
                    "will return NOTFOUND for the affected key. NOT falling back to any other flush file " +
                    "to avoid restoring an incorrect tree version.", snapshotPath);
                return;
            }

            var dataPath = LogDataPath(hashPrefix);
            var keyHash = GarnetKeyComparer.StaticGetHashCode64((FixedSpanByteKey)PinnedSpanByte.FromPinnedSpan(keyBytes));

            // Acquire the per-key exclusive lock for the duration of the file copy AND the
            // pending-entry registration. This blocks any concurrent RestoreTree (which also
            // takes the exclusive lock) from observing a partial data.bftree.
            // riLogRoot was created in the constructor.
            rangeIndexLocks.AcquireExclusiveLock(keyHash, out var lockToken);
            try
            {
                File.Copy(snapshotPath, dataPath, overwrite: true);

                var keyId = KeyId(keyBytes);
                _ = liveIndexes.TryAdd(keyId, new TreeEntry(tree: null, keyHash, hashPrefix));
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "PreStageAndRegisterPending: copy/register failed for {Hash}; " +
                    "destination record will have no pending entry; subsequent RestoreTree will return NOTFOUND",
                    hashPrefix);
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
        /// <para>Recovery is single-threaded with no concurrent readers, and a crash mid-copy is
        /// self-healing — the next recovery attempt re-fires <c>OnRecoverySnapshotRead</c> for
        /// the same stub and fully overwrites any partial file before any <c>RestoreTree</c>
        /// can observe it.</para>
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when this method is called without
        /// the recovery state required to locate checkpoint snapshots
        /// (<see cref="cprDir"/> is empty or <see cref="recoveredCheckpointToken"/> is Guid.Empty).
        /// This indicates that <c>OnRecoverySnapshotRead</c> fired without
        /// <c>OnRecovery(token)</c> having captured the recovered checkpoint token first —
        /// a wiring bug that would otherwise silently lose the recovered tree.</exception>
        internal void RebuildFromSnapshotIfPending(ReadOnlySpan<byte> keyBytes)
        {
            if (string.IsNullOrEmpty(cprDir) || recoveredCheckpointToken == Guid.Empty)
                throw new InvalidOperationException(
                    "RebuildFromSnapshotIfPending: recovery state missing " +
                    $"(cprDir empty: {string.IsNullOrEmpty(cprDir)}, recoveredCheckpointToken empty: {recoveredCheckpointToken == Guid.Empty}). " +
                    "This indicates OnRecoverySnapshotRead fired without OnRecovery(token) " +
                    "having captured the recovered checkpoint token. The recovered RangeIndex tree would " +
                    "otherwise be silently lost.");

            var hashPrefix = HashKeyToPrefix(keyBytes);
            var snapshotPath = CheckpointSnapshotPath(hashPrefix, recoveredCheckpointToken);
            if (!File.Exists(snapshotPath))
            {
                // Below-FUA stubs have no checkpoint snapshot; RIPROMOTE PostCopyUpdater handles
                // them lazily via the per-flush snapshot file on first access. NOT an error.
                logger?.LogDebug("OnRecoverySnapshotRead: snapshot absent for {Hash} — RIPROMOTE will handle lazily", hashPrefix);
                return;
            }

            var dataPath = LogDataPath(hashPrefix);

            try
            {
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
        /// Log an OnFlush invariant violation: <see cref="RangeIndexStub.TreeHandle"/> was zero
        /// (no live tree) but the working file <c>data.bftree</c> was missing. Per Invariant 5
        /// (Pending entry invariant), every above-FUA stub with TreeHandle=0 must have a pending
        /// entry in <see cref="liveIndexes"/> and a pre-staged <c>data.bftree</c> on disk; if
        /// <c>data.bftree</c> is missing, something has corrupted that invariant (e.g. a prior
        /// pre-stage failure, or external file deletion). The caller (<see cref="GarnetRecordTriggers.OnFlush"/>)
        /// must NOT set <see cref="RangeIndexStub.IsFlushed"/> in this case — that would put the
        /// record into an unrestorable state where RIPROMOTE-cold would also fail and the key
        /// would be permanently broken.
        /// </summary>
        internal void LogOnFlushInvariantViolation(string hashPrefix, long logicalAddress)
        {
            logger?.LogError("OnFlush: invariant violation — TreeHandle=0 but data.bftree missing for {Hash} at addr 0x{Addr:x16}; skipping IsFlushed (record will route through RestoreTree which will return NOTFOUND for the affected key)",
                hashPrefix, logicalAddress);
        }

        /// <summary>
        /// Snapshot a BfTree's current contents to its per-flush snapshot file. Called from
        /// <see cref="GarnetRecordTriggers.OnFlush"/> when a page transitions to read-only.
        ///
        /// <para><b>Live case</b> (<c>stub.TreeHandle != 0</c>): take a CPR snapshot via the
        /// native handle. CPR is concurrent-safe with workers (no per-key X-lock needed).
        /// Per-tree atomic <see cref="TreeEntry.SnapshotInProgress"/> serializes against
        /// concurrent <see cref="SnapshotAllTreesForCheckpoint"/> for the same tree (otherwise
        /// bftree's internal <c>snapshot_in_progress</c> would no-op one of them).</para>
        ///
        /// <para><b>Cold case</b> (<c>stub.TreeHandle == 0</c>): the stub was just CAS'd at the
        /// tail by PostCopyToTail-cold or RIPROMOTE-PostCopyUpdater-cold; PreStage already
        /// copied <c>&lt;srcAddr&gt;.flush.bftree → data.bftree</c> but RestoreTree hasn't
        /// activated a live tree yet. ANOTHER stub for the same key may have a live tree in
        /// <see cref="liveIndexes"/> (RestoreTree ran against a different addr's stub) — workers
        /// using that tree would write to <c>data.bftree</c> concurrently, making
        /// <c>File.Copy(data.bftree)</c> unsafe. So:
        /// <list type="bullet">
        /// <item>Acquire per-key SHARED RI lock — blocks RestoreTree's X-lock from registering
        /// a new tree during our copy. Deadlock-free: S-vs-S compatible with hot path; no path
        /// holds an RI X-lock across a Tsavorite op that fires deferred OnFlush.</item>
        /// <item>If <see cref="liveIndexes"/> has a live tree under another stub → use
        /// CPR snapshot (concurrent-safe with workers).</item>
        /// <item>Else → safe to <c>File.Copy(data.bftree → flushPath)</c>.</item>
        /// </list></para>
        ///
        /// <para>Sets <see cref="RangeIndexStub.IsFlushed"/> on the in-memory stub on success so
        /// the next data operation routes through <see cref="GarnetRecordTriggers.PostCopyToTail"/>
        /// or RIPROMOTE PostCopyUpdater to re-anchor the tree at the tail.</para>
        /// </summary>
        /// <param name="key">The raw key bytes (used for hash prefix + lock acquisition).</param>
        /// <param name="valueSpan">The store value span containing the stub.</param>
        /// <param name="logicalAddress">The logical address of the record being flushed.</param>
        internal void SnapshotTreeForFlush(ReadOnlySpan<byte> key, Span<byte> valueSpan, long logicalAddress)
        {
            ref readonly var stub = ref ReadIndex(valueSpan);

            // Stale source whose ownership was transferred to a newer record at the tail: no-op.
            if (stub.IsTransferred)
                return;

            // Need riLogRoot for any disk artifact (both backends use it as staging directory).
            if (string.IsNullOrEmpty(riLogRoot))
                return;

            var hashPrefix = HashKeyToPrefix(key);
            var dataPath = LogDataPath(hashPrefix);
            var scratchPath = LogScratchPath(hashPrefix);
            var flushPath = LogFlushPath(hashPrefix, logicalAddress);
            var keyId = KeyId(key);

            if (stub.TreeHandle != nint.Zero)
            {
                // Live case: stub directly references a live tree. CPR snapshot via the handle.
                if (!liveIndexes.TryGetValue(keyId, out var entry) || entry?.Tree == null)
                {
                    // Edge case: stub.TreeHandle points at a tree no longer in liveIndexes
                    // (DEL deferred-disposed it but stub bytes weren't updated). Treat as cold.
                    SnapshotForFlushCold(key, hashPrefix, dataPath, flushPath, valueSpan, logicalAddress);
                    return;
                }
                SnapshotForFlushViaCpr(entry, scratchPath, flushPath);
                SetFlushedFlag(valueSpan);
            }
            else
            {
                SnapshotForFlushCold(key, hashPrefix, dataPath, flushPath, valueSpan, logicalAddress);
            }
        }

        /// <summary>
        /// Take a CPR snapshot via the live tree's native handle and copy the produced scratch
        /// file to the addr-tagged per-flush destination. We <b>copy</b> rather than move because
        /// bftree's internal VFS keeps a file descriptor for the configured snapshot path; a move
        /// would not invalidate the descriptor and the next cpr_snapshot would write through the
        /// stale FD into the moved-away file (overwriting our finalized flush snapshot).
        /// Per-tree atomic serializes against concurrent
        /// <see cref="SnapshotAllTreesForCheckpoint"/> on the same tree.
        /// </summary>
        private void SnapshotForFlushViaCpr(TreeEntry entry, string scratchPath, string flushPath)
        {
            if (!entry.TryClaimSnapshot())
            {
                // Concurrent SnapshotAllTreesForCheckpoint owns the snapshot. Wait for it,
                // then copy the produced scratch file to our addr-tagged location.
                entry.WaitForSnapshot();
                File.Copy(scratchPath, flushPath, overwrite: false);
                return;
            }
            try
            {
                BfTreeService.CprSnapshotByPtr(entry.Tree.NativePtr);
                File.Copy(scratchPath, flushPath, overwrite: false);
            }
            finally
            {
                entry.ReleaseSnapshot();
            }
        }

        /// <summary>
        /// OnFlush cold-case: stub.TreeHandle == 0. The pre-staged <c>data.bftree</c> is the
        /// only candidate source for capturing this flush. We must serialize against any
        /// concurrent RestoreTree (X-lock) that could activate a tree mid-copy and start
        /// writing to data.bftree from a worker thread. Use SHARED RI lock — deadlock-free
        /// because no firing-thread holds an X-lock across a Tsavorite op that fires deferred
        /// OnFlush (RestoreTree releases X before its RMW; PreStage / DisposeTreeUnderLock
        /// don't issue Tsavorite ops while holding X; OnFlush itself doesn't take X).
        /// </summary>
        private void SnapshotForFlushCold(ReadOnlySpan<byte> key, string hashPrefix,
            string dataPath, string flushPath, Span<byte> valueSpan, long logicalAddress)
        {
            var keyHash = GarnetKeyComparer.StaticGetHashCode64((FixedSpanByteKey)PinnedSpanByte.FromPinnedSpan(key));
            rangeIndexLocks.AcquireSharedLock(keyHash, out var sharedLockToken);
            try
            {
                // Re-check: a tree may have become live under a different stub for this key
                // (RestoreTree completed before we acquired the shared lock).
                if (liveIndexes.TryGetValue(KeyId(key), out var entry) && entry?.Tree != null)
                {
                    var scratchPath = LogScratchPath(hashPrefix);
                    SnapshotForFlushViaCpr(entry, scratchPath, flushPath);
                    SetFlushedFlag(valueSpan);
                    return;
                }

                // No live tree exists for this key (S-lock blocks RestoreTree from activating
                // one mid-copy). data.bftree is stable — no concurrent writer.
                if (!File.Exists(dataPath))
                {
                    LogOnFlushInvariantViolation(hashPrefix, logicalAddress);
                    return; // do NOT set IsFlushed
                }
                File.Copy(dataPath, flushPath, overwrite: false);
                SetFlushedFlag(valueSpan);
            }
            finally
            {
                rangeIndexLocks.ReleaseSharedLock(sharedLockToken);
            }
        }

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
        ///
        /// <para>For each entry with <see cref="TreeEntry.SnapshotPending"/> set:
        /// <list type="bullet">
        /// <item>Activated entries (Tree != null) → take CPR snapshot via the tree handle and
        /// <c>File.Move</c> the produced scratch file to the checkpoint destination.</item>
        /// <item>Pending entries (Tree == null) → <c>File.Copy(data.bftree)</c> (no live tree
        /// to snapshot from; data.bftree was pre-staged by PreStage).</item>
        /// </list></para>
        ///
        /// <para>Uses the per-tree atomic <see cref="TreeEntry.SnapshotInProgress"/> to serialize
        /// against concurrent <see cref="SnapshotTreeForFlush"/> for the same tree. Per-key
        /// X-lock is NOT taken here — that lock would deadlock if any deferred OnFlush fired
        /// on the checkpoint thread while it held S-locks on hot-path readers' shards.</para>
        ///
        /// <para>Memory-backed trees are also captured via CPR snapshot (bftree 0.5.0 supports
        /// CPR for memory-backed trees uniformly with disk-backed).</para>
        ///
        /// <para>Failure is fatal — the exception propagates to the state machine driver.</para>
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

                    try
                    {
                        Directory.CreateDirectory(snapshotDir);
                        var checkpointPath = CheckpointSnapshotPath(entry.HashPrefix, checkpointToken);
                        var scratchPath = LogScratchPath(entry.HashPrefix);

                        if (entry.Tree is not null)
                        {
                            // Per-tree atomic serializes against concurrent OnFlush on the same
                            // tree (which would also call cpr_snapshot via the same handle —
                            // bftree's internal snapshot_in_progress would otherwise no-op one).
                            while (!entry.TryClaimSnapshot()) Thread.Yield();
                            try
                            {
                                BfTreeService.CprSnapshotByPtr(entry.Tree.NativePtr);
                                // Copy scratch -> token-tagged checkpoint destination. Copy rather
                                // than move because bftree's internal VFS keeps a file descriptor
                                // for the scratch path (see SnapshotForFlushViaCpr docs).
                                File.Copy(scratchPath, checkpointPath, overwrite: true);
                            }
                            finally
                            {
                                entry.ReleaseSnapshot();
                            }
                        }
                        else
                        {
                            // Pending entry: data.bftree was pre-staged from the source flush
                            // file by PreStageAndRegisterPending. data.bftree is the only
                            // correct source. No live tree means no concurrent writer.
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
        /// strictly less than <paramref name="newBeginAddress"/>.
        /// Per-checkpoint snapshots are NOT touched here — Tsavorite's checkpoint manager
        /// removes them when it deletes the parent token directory.
        ///
        /// <para>Per-flush files are LOG-tied (their lifetime tracks log addresses), not
        /// checkpoint-tied — they are safe to delete once Tsavorite's BeginAddress passes their
        /// address. This cleanup is unconditional and independent of cluster mode or any
        /// checkpoint-retention policy.</para>
        /// </summary>
        internal void OnTruncateImpl(long newBeginAddress)
        {
            if (string.IsNullOrEmpty(riLogRoot))
                return;
            if (!Directory.Exists(riLogRoot))
                return;

            try
            {
                foreach (var path in Directory.EnumerateFiles(riLogRoot))
                {
                    var name = Path.GetFileName(path);

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
    }
}