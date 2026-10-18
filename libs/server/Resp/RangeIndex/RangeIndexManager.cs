// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
        public const byte RangeIndexRecordType = 2;

        /// <summary>Size of the RangeIndex stub in bytes.</summary>
        internal const int IndexSizeBytes = RangeIndexStub.Size;

        // Flush filename layout: <hash:32>.<addr:16>.flush.bftree
        private const int HashPrefixLength = 32;
        private const int AddrHexLength = 16;
        private const string FlushSuffix = ".flush.bftree";
        private const int FlushFileNameLength = HashPrefixLength + 1 + AddrHexLength + 13; // ".flush.bftree".Length == 13
        private const int AddrStartIndex = HashPrefixLength + 1;

        /// <summary>Gets the number of live (registered) BfTree indexes (activated + pending).</summary>
        internal int LiveIndexCount => liveIndexes.Count;

        /// <summary>Gets the log-tied root directory for RI files. Used by the cluster replication layer
        /// to determine the target directory for received flush files on the replica side.</summary>
        public string RiLogRoot => riLogRoot;

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
        /// (<c>&lt;hash&gt;.data.bftree</c>) and immutable per-flush snapshots
        /// (<c>&lt;hash&gt;.&lt;addr:x16&gt;.flush.bftree</c>).
        /// </summary>
        private readonly string riLogRoot;

        /// <summary>
        /// Temporary directory for in-progress migration files.
        /// Created once in the constructor; cleaned up on startup to remove partial artifacts from prior crashes.
        /// </summary>
        private readonly string migrationTempDir;

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
        /// Last value returned by <see cref="NextSequence"/>.
        /// </summary>
        private static long lastSequence;

        /// <summary>
        /// The sequence of a <see cref="TreeEntry"/> that a retirement has claimed for removal.
        /// </summary>
        private const long ClaimedSequence = long.MaxValue;

        /// <summary>
        /// Next value of a counter that orders entry registrations (<see cref="TreeEntry"/>) and retirement requests
        /// (<see cref="DisposeTreeUnderLock"/>).
        /// </summary>
        private static long NextSequence() => Interlocked.Increment(ref lastSequence);

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

            /// <summary>
            /// When the entry was registered, or re-registered for a newer stub of the key (<see cref="TryReregister"/>), from
            /// <see cref="NextSequence"/>. A retirement removes the entry only if it was registered before the retirement was
            /// requested (<see cref="TryClaimForRemoval"/>). <see cref="ClaimedSequence"/> once a retirement has claimed it.
            /// </summary>
            private long sequence = NextSequence();

            /// <summary>
            /// Claim the entry for a retirement requested at <paramref name="requestedAt"/>, which then removes it and disposes
            /// its tree, if it was registered before then.
            /// </summary>
            public bool TryClaimForRemoval(long requestedAt)
            {
                var current = Volatile.Read(ref sequence);
                while (current <= requestedAt)
                {
                    var observed = Interlocked.CompareExchange(ref sequence, ClaimedSequence, current);
                    if (observed == current)
                        return true;
                    current = observed;
                }

                return false;
            }

            /// <summary>
            /// Mark the entry registered now, so that a retirement requested before now leaves it.
            /// </summary>
            /// <returns>False if a retirement has claimed it.</returns>
            public bool TryReregister()
            {
                var stamp = NextSequence();
                var current = Volatile.Read(ref sequence);
                while (current != ClaimedSequence)
                {
                    if (current >= stamp)
                        return true;

                    var observed = Interlocked.CompareExchange(ref sequence, stamp, current);
                    if (observed == current)
                        return true;
                    current = observed;
                }

                return false;
            }

            /// <summary>
            /// Spin to claim the per-tree snapshot atomic, take a CPR snapshot of the live tree
            /// directly into <paramref name="destinationPath"/>, then release the claim. The
            /// claim serializes against concurrent flush / checkpoint / migration snapshots on
            /// the same tree — bftree silently no-ops a <c>cpr_snapshot</c> that races its
            /// internal <c>snapshot_in_progress</c> flag, so each caller must hold the claim
            /// while it snapshots. bftree takes the destination path as a <c>cpr_snapshot</c>
            /// argument and writes a fresh self-contained file, so we snapshot straight to
            /// <paramref name="destinationPath"/>.
            /// </summary>
            public void SnapshotUnderClaim(string destinationPath)
            {
                while (!TryClaimSnapshot())
                    Thread.Yield();
                try
                {
                    BfTreeService.CprSnapshotByPtr(Tree.NativePtr, destinationPath);
                }
                finally
                {
                    ReleaseSnapshot();
                }
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

            // Clean up partial migration artifacts from prior crashes, then ensure directory exists
            migrationTempDir = Path.Combine(riLogRoot, "migration-tmp");
            if (Directory.Exists(migrationTempDir))
            {
                try
                {
                    Directory.Delete(migrationTempDir, recursive: true);
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "RangeIndexManager: failed to clean up migration temp directory {Path}", migrationTempDir);
                    throw new Exception($"RangeIndexManager: failed to clean up migration temp directory '{migrationTempDir}'. Check for locked files or insufficient permissions.", ex);
                }
            }

            try
            {
                Directory.CreateDirectory(migrationTempDir);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "RangeIndexManager: failed to create migration temp directory {Path}", migrationTempDir);
                throw new Exception($"RangeIndexManager: failed to create migration temp directory '{migrationTempDir}'.", ex);
            }

            retirementTask = RunRetirementTaskAsync();
            flushSnapshotTask = RunFlushSnapshotTaskAsync();
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
        public string LogFlushPath(string hashPrefix, long logicalAddress)
            => Path.Combine(riLogRoot ?? string.Empty, $"{hashPrefix}.{logicalAddress:x16}{FlushSuffix}");

        /// <summary>{cprDir}/&lt;token&gt;/rangeindex/&lt;hash&gt;.bftree</summary>
        public string CheckpointSnapshotPath(string hashPrefix, Guid checkpointToken)
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
            var hashPrefix = HashKeyToPrefix(keyBytes);
            string filePath = null;

            if (storageBackend == StorageBackendType.Disk)
            {
                filePath = LogDataPath(hashPrefix);
            }

            // Enable CPR snapshots whenever a log root is configured. The destination path is
            // supplied per call by OnFlush / SnapshotAllTreesForCheckpoint / migration, so only
            // the on/off toggle is needed here. Leave disabled only if riLogRoot is unset (test
            // scenarios). riLogRoot itself was already created in the constructor.
            bool enableSnapshots = !string.IsNullOrEmpty(riLogRoot);

            return new BfTreeService(
                storageBackend: storageBackend,
                filePath: filePath,
                enableSnapshots: enableSnapshots,
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
            //     earlier by the promote of a stub whose tree was evicted (RegisterPending) or by
            //     recovery; we activate it in place.
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

                // RegisterIndex's callers do not hold the epoch, which BumpCurrentEpoch requires.
                var resumed = storeEpoch.ResumeIfNotProtected();
                try
                {
                    storeEpoch.BumpCurrentEpoch(() =>
                    {
                        try { loser.Dispose(); }
                        catch (Exception ex) { logger?.LogWarning(ex, "Deferred dispose failed: {Reason}", reason); }
                    });
                }
                finally
                {
                    if (resumed)
                        storeEpoch.Suspend();
                }
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
        /// <remarks>
        /// Called by the RIPROMOTE copy of a stub whose tree was evicted, once the copy is in the log, so that an eviction of
        /// the source stub before then cannot remove the entry the copy needs, and by a copy to the tail that relies on the
        /// working file (<see cref="GarnetRecordTriggers.PostCopyToTail"/>). Runs under the bucket latch, so it does not
        /// take the key's lock.
        /// </remarks>
        internal void RegisterPending(ReadOnlySpan<byte> keyBytes, long keyHash)
        {
            var keyId = KeyId(keyBytes);
            TreeEntry pending = null;

            // Each retry follows a concurrent change to the key's entry.
            while (true)
            {
                if (!liveIndexes.TryGetValue(keyId, out var existing))
                {
                    pending ??= new TreeEntry(tree: null, keyHash, HashKeyToPrefix(keyBytes));
                    if (liveIndexes.TryAdd(keyId, pending))
                        return;
                    continue;
                }

                // An active entry's tree is the key's: a stub holds it, or a restore is about to publish it in one.
                if (existing.Tree != null)
                    return;

                // A pending entry is re-registered for this stub, so that a retirement requested before now, which is for an
                // older stub of the key, leaves it.
                if (existing.TryReregister())
                    return;

                // Such a retirement has claimed it; replace it, and the retirement leaves the replacement.
                pending ??= new TreeEntry(tree: null, keyHash, HashKeyToPrefix(keyBytes));
                if (liveIndexes.TryUpdate(keyId, pending, existing))
                    return;
            }
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
            // Drop any in-progress AOF-stream reassembly
            DisposeIncompleteStreamReassembly();

            // Take deferred flush snapshots, which may snapshot live trees, and finish deferred retirements, which remove their
            // trees from liveIndexes before disposing them.
            StopFlushSnapshotTask();
            StopRetirementTask();

            foreach (var kvp in liveIndexes)
            {
                try { kvp.Value.Tree?.Dispose(); }
                catch (Exception ex) { logger?.LogWarning(ex, "Failed to dispose BfTree {Hash}", kvp.Value.HashPrefix); }
            }
            liveIndexes.Clear();
        }

        /// <summary>
        /// Pre-stage <c>data.bftree</c> from <c>&lt;srcAddr:x16&gt;.flush.bftree</c>. The RIPROMOTE copy of the stub then
        /// registers a pending entry in <see cref="liveIndexes"/> (<see cref="RegisterPending"/>), so that a subsequent
        /// checkpoint captures it via <see cref="SnapshotAllTreesForCheckpoint"/>.
        ///
        /// <para>Called by <see cref="StageEvictedTree"/>, before the promote of a flushed stub whose tree
        /// was evicted, with the per-key EXCLUSIVE rangeIndex lock held. The lock serializes this against
        /// <c>RestoreTree</c>, which opens <c>data.bftree</c> under the same lock, so it never observes a
        /// partial file, and against other concurrent staging of the same key. A direct
        /// <c>File.Copy(overwrite: true)</c> is sufficient under it.</para>
        ///
        /// <para>A crash mid-copy is self-healing: post-recovery either <c>OnRecoverySnapshotRead</c>
        /// (above-FUA stub) or the next promote of the still-flushed stub re-stages and overwrites any
        /// partial file before <c>RestoreTree</c> can observe it.</para>
        /// </summary>
        private void StageWorkingFileLocked(ReadOnlySpan<byte> keyBytes, long srcFlushAddress)
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
                // file would risk restoring the wrong tree version. Leave data.bftree as it is; the
                // next RestoreTree will return NOTFOUND for the affected key if it is missing,
                // surfacing the data loss explicitly rather than silently restoring incorrect data.
                logger?.LogError("StageWorkingFileLocked: invariant violation — source flush file missing: {Path}. " +
                    "Subsequent RestoreTree will return NOTFOUND for the affected key if data.bftree is missing. " +
                    "NOT falling back to any other flush file to avoid restoring an incorrect tree version.", snapshotPath);
                return;
            }

            try
            {
                File.Copy(snapshotPath, LogDataPath(hashPrefix), overwrite: true);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "StageWorkingFileLocked: copy failed for {Hash}; " +
                    "subsequent RestoreTree may return NOTFOUND", hashPrefix);
            }
        }

        /// <summary>
        /// Give a stub copied to the tail from a flushed stub whose tree was evicted its own copy of the source's flush
        /// snapshot, so that it stays flushed at its new address and the next data operation promotes it (see
        /// <see cref="GarnetRecordTriggers.PostCopyToTail"/>). Flush snapshots are named by address, and the destination's is
        /// new, so this takes no lock.
        /// </summary>
        /// <returns>True if the destination has its flush snapshot; false if it has none, including when the source's has not
        /// been taken yet (<see cref="DeferFlushSnapshot"/>, which publishes a snapshot only once complete).</returns>
        internal bool CopyFlushSnapshot(ReadOnlySpan<byte> keyBytes, long srcFlushAddress, long dstAddress)
        {
            if (string.IsNullOrEmpty(riLogRoot))
                return false;

            var hashPrefix = HashKeyToPrefix(keyBytes);
            try
            {
                File.Copy(LogFlushPath(hashPrefix, srcFlushAddress), LogFlushPath(hashPrefix, dstAddress), overwrite: true);
                return true;
            }
            catch (Exception ex)
            {
                if (IsFlushSnapshotPending(KeyId(keyBytes)))
                {
                    logger?.LogDebug(ex, "CopyFlushSnapshot: the flush snapshot of {Hash} at 0x{Src:x16} may not be taken yet",
                        hashPrefix, srcFlushAddress);
                }
                else
                {
                    logger?.LogError(ex, "CopyFlushSnapshot: copying the flush snapshot of {Hash} from 0x{Src:x16} to 0x{Dst:x16} failed",
                        hashPrefix, srcFlushAddress, dstAddress);
                }
                return false;
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
                // Below-FUA stubs have no checkpoint snapshot; their promote (RIPROMOTE) stages them
                // lazily from the per-flush snapshot file on first access. NOT an error.
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
        /// <para><b>Cold case</b> (<c>stub.TreeHandle == 0</c>): the stub's tree is not restored, e.g. it was just promoted
        /// to the tail from a flushed stub whose tree was evicted; <see cref="StageEvictedTree"/> already
        /// copied <c>&lt;srcAddr&gt;.flush.bftree → data.bftree</c> but RestoreTree hasn't
        /// activated a live tree yet. ANOTHER stub for the same key may have a live tree in
        /// <see cref="liveIndexes"/> (RestoreTree ran against a different addr's stub) — workers
        /// using that tree would write to <c>data.bftree</c> concurrently, making
        /// <c>File.Copy(data.bftree)</c> unsafe. So:
        /// <list type="bullet">
        /// <item>If <see cref="liveIndexes"/> has a live tree under another stub → use
        /// CPR snapshot (concurrent-safe with workers).</item>
        /// <item>Else → <c>File.Copy(data.bftree → flushPath)</c> holding the per-key SHARED RI lock, which keeps
        /// RestoreTree from opening data.bftree during the copy. This runs in an epoch drain, possibly on a thread
        /// holding a bucket latch, and the lock's holder may be waiting for either, so the lock is never waited for:
        /// if it is held, the copy is deferred until it is free (<see cref="DeferFlushSnapshot"/>).</item>
        /// </list></para>
        ///
        /// <para>A cold stub that is already flushed was copied to the tail by
        /// <see cref="GarnetRecordTriggers.PostCopyToTail"/> together with its flush snapshot, so it
        /// is skipped.</para>
        ///
        /// <para>Sets <see cref="RangeIndexStub.IsFlushed"/> on the in-memory stub on success so
        /// the next data operation promotes it (RIPROMOTE) to re-anchor the tree at the tail.</para>
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

            // Copied to the tail with its tree evicted, and with its own flush snapshot: nothing to snapshot.
            if (stub.IsFlushed && stub.TreeHandle == nint.Zero)
                return;

            // Need riLogRoot for any disk artifact (both backends use it as staging directory).
            if (string.IsNullOrEmpty(riLogRoot))
                return;

            var hashPrefix = HashKeyToPrefix(key);
            var keyId = KeyId(key);

            if (stub.TreeHandle != nint.Zero)
            {
                // Live case: stub directly references a live tree. CPR snapshot via the handle.
                if (!liveIndexes.TryGetValue(keyId, out var entry) || entry?.Tree == null)
                {
                    // Edge case: stub.TreeHandle points at a tree no longer in liveIndexes
                    // (DEL deferred-disposed it but stub bytes weren't updated). Treat as cold.
                    SnapshotForFlushCold(key, keyId, hashPrefix, valueSpan, logicalAddress);
                    return;
                }
                entry.SnapshotUnderClaim(LogFlushPath(hashPrefix, logicalAddress));
                SetFlushedFlag(valueSpan);
            }
            else
            {
                SnapshotForFlushCold(key, keyId, hashPrefix, valueSpan, logicalAddress);
            }
        }

        /// <summary>
        /// OnFlush cold-case: stub.TreeHandle == 0. Snapshots the key's tree if a restore under another stub made it
        /// live, which the epoch this drain holds keeps from being disposed; otherwise copies the pre-staged
        /// <c>data.bftree</c>, the only other source, holding the per-key SHARED RI lock so that no RestoreTree activates
        /// a tree on it mid-copy. That lock's holder, such as a RestoreTree or staging of this key, may be waiting for this
        /// drain or for a bucket latch the thread running it holds, so the lock is not waited for: when it is held, the stub
        /// is marked flushed now, as the page is written with that flag, and its snapshot is deferred until the lock is free.
        /// </summary>
        private void SnapshotForFlushCold(ReadOnlySpan<byte> key, Guid keyId, string hashPrefix, Span<byte> valueSpan, long logicalAddress)
        {
            if (liveIndexes.TryGetValue(keyId, out var entry) && entry?.Tree != null)
            {
                entry.SnapshotUnderClaim(LogFlushPath(hashPrefix, logicalAddress));
                SetFlushedFlag(valueSpan);
                return;
            }

            var keyHash = GarnetKeyComparer.StaticGetHashCode64((FixedSpanByteKey)PinnedSpanByte.FromPinnedSpan(key));
            if (!rangeIndexLocks.TryAcquireSharedLock(keyHash, out var sharedLockToken))
            {
                // Registered before the flag is set, so that whoever sees the stub flushed also sees its snapshot pending.
                DeferFlushSnapshot(key, keyHash, keyId, hashPrefix, logicalAddress);
                SetFlushedFlag(valueSpan);
                return;
            }

            try
            {
                if (TrySnapshotColdTreeLocked(key, hashPrefix, LogFlushPath(hashPrefix, logicalAddress)))
                    SetFlushedFlag(valueSpan);
                else
                    LogOnFlushInvariantViolation(hashPrefix, logicalAddress); // do NOT set IsFlushed
            }
            finally
            {
                rangeIndexLocks.ReleaseLock(sharedLockToken);
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
        /// <item>Activated entries (Tree != null) → take a CPR snapshot via the tree handle
        /// straight to the checkpoint destination (<see cref="TreeEntry.SnapshotUnderClaim"/>).</item>
        /// <item>Pending entries (Tree == null) → <c>File.Copy(data.bftree)</c> (no live tree
        /// to snapshot from; data.bftree was pre-staged by StageWorkingFileLocked).</item>
        /// </list></para>
        ///
        /// <para>Uses the per-tree atomic <see cref="TreeEntry.SnapshotInProgress"/> to serialize
        /// against concurrent <see cref="SnapshotTreeForFlush"/> for the same tree. Per-key
        /// X-lock is NOT taken here — that lock would deadlock if any deferred OnFlush fired
        /// on the checkpoint thread while it held S-locks on hot-path readers' shards.</para>
        ///
        /// <para>Memory-backed trees are also captured via CPR snapshot (bftree supports
        /// CPR for memory-backed trees uniformly with disk-backed).</para>
        ///
        /// <para>First waits for flush snapshots deferred while their key's lock was held (<see cref="DeferFlushSnapshot"/>),
        /// so that every flushed stub the checkpoint covers has its snapshot. This runs on the state machine's thread without
        /// epoch protection, so waiting for those snapshots, which wait for the keys' locks, cannot hold up their holders.</para>
        ///
        /// <para>Failure is fatal — the exception propagates to the state machine driver.</para>
        /// </summary>
        internal void SnapshotAllTreesForCheckpoint(Guid checkpointToken)
        {
            try
            {
                WaitForFlushSnapshots();

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

                        if (entry.Tree is not null)
                        {
                            // Snapshot straight to the token-tagged checkpoint destination under
                            // the per-tree claim (serializes against concurrent OnFlush / migration
                            // on the same tree).
                            entry.SnapshotUnderClaim(checkpointPath);
                        }
                        else
                        {
                            // Pending entry: data.bftree was pre-staged from the source flush
                            // file by StageWorkingFileLocked. data.bftree is the only
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
                foreach (var (path, _, addr) in EnumerateFlushFiles())
                {
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

        // ---------------------------------------------------------------
        // Shared flush-file enumeration helper
        // ---------------------------------------------------------------

        /// <summary>
        /// Enumerates all <c>*.flush.bftree</c> files under <c>riLogRoot</c>, parsing and
        /// yielding the embedded logical address from each valid filename.
        /// </summary>
        /// <returns>Tuples of (fullPath, fileName, logicalAddress). Skips files that don't
        /// match the expected naming pattern. Returns empty if riLogRoot is not configured
        /// or does not exist.</returns>
        private IEnumerable<(string Path, string Name, long Address)> EnumerateFlushFiles()
        {
            if (string.IsNullOrEmpty(riLogRoot) || !Directory.Exists(riLogRoot))
            {
                logger?.LogInformation("Root directory invalid {riLogRoot}", riLogRoot);
                yield break;
            }

            foreach (var path in Directory.EnumerateFiles(riLogRoot, $"*{FlushSuffix}"))
            {
                var name = Path.GetFileName(path);

                // Pattern: <hash:32>.<addr:16>.flush.bftree
                if (name.Length != FlushFileNameLength)
                    continue;

                var addrSegment = name.AsSpan(AddrStartIndex, AddrHexLength);
                if (!long.TryParse(addrSegment, NumberStyles.HexNumber,
                        CultureInfo.InvariantCulture, out var addr))
                    continue;

                yield return (path, name, addr);
            }
        }

        // ---------------------------------------------------------------
        // Replication: file enumeration for checkpoint transfer
        // ---------------------------------------------------------------

        /// <summary>
        /// Represents a single RangeIndex file that must be shipped during replication.
        /// </summary>
        public readonly struct RangeIndexFileEntry
        {
            /// <summary>Full path to the file on the primary.</summary>
            public readonly string Path;

            /// <summary>The 32-character key hash prefix identifying the RangeIndex tree.</summary>
            public readonly string KeyHash;

            /// <summary>The hlog logical address (for flush files); 0 for checkpoint snapshots.</summary>
            public readonly long Address;

            /// <summary>Whether this is a per-flush file (true) or a per-checkpoint snapshot (false).</summary>
            public readonly bool IsFlushFile;

            public RangeIndexFileEntry(string path, string keyHash, long address, bool isFlushFile)
            {
                Path = path;
                KeyHash = keyHash;
                Address = address;
                IsFlushFile = isFlushFile;
            }
        }

        /// <summary>
        /// Enumerates all RangeIndex files that must be sent during disk-based replication
        /// for a given checkpoint token and hlog address range.
        ///
        /// <para>Two categories of files are collected:</para>
        /// <list type="number">
        /// <item><b>Per-flush files</b> in <c>riLogRoot</c> whose embedded address falls within
        /// <c>[hlogStartAddress, hlogEndAddress)</c>. These correspond to RI stubs in the hlog
        /// segments being transferred.</item>
        /// <item><b>Per-checkpoint snapshot files</b> under
        /// <c>cpr-checkpoints/&lt;token&gt;/rangeindex/</c>. These correspond to RI stubs in
        /// the snapshot region at checkpoint time.</item>
        /// </list>
        ///
        /// <para>Multiple flush files per key hash are possible and all within the range are
        /// included — <see cref="StageWorkingFileLocked"/> uses the exact source address
        /// to locate the specific flush file.</para>
        /// </summary>
        /// <param name="checkpointToken">The checkpoint token (storeHlogToken) to locate
        /// per-checkpoint snapshots.</param>
        /// <param name="hlogStartAddress">The <c>hybridLogFileStartAddress</c> from
        /// <c>LogFileInfo</c> — inclusive lower bound for flush file filtering.</param>
        /// <param name="hlogEndAddress">The <c>hybridLogFileEndAddress</c> from
        /// <c>LogFileInfo</c> — exclusive upper bound for flush file filtering.</param>
        /// <returns>An enumerable of <see cref="RangeIndexFileEntry"/> describing files to
        /// transfer. Empty if RangeIndex is not configured or no files match.</returns>
        public IEnumerable<RangeIndexFileEntry> EnumerateFilesForReplication(
            Guid checkpointToken, long hlogStartAddress, long hlogEndAddress)
        {
            // 1. Per-flush files: <hash>.<addr:x16>.flush.bftree where addr ∈ [start, end)
            foreach (var (path, name, addr) in EnumerateFlushFiles())
            {
                if (addr >= hlogStartAddress && addr < hlogEndAddress)
                {
                    var keyHash = name[..HashPrefixLength];
                    yield return new RangeIndexFileEntry(path, keyHash, addr, isFlushFile: true);
                }
            }

            // 2. Per-checkpoint snapshot files: cpr-checkpoints/<token>/rangeindex/<hash>.bftree
            if (!string.IsNullOrEmpty(cprDir))
            {
                var snapshotDir = CheckpointSnapshotDir(checkpointToken);
                if (Directory.Exists(snapshotDir))
                {
                    foreach (var path in Directory.EnumerateFiles(snapshotDir, "*.bftree"))
                    {
                        var name = Path.GetFileName(path);

                        // Skip files that don't match the expected format: <32-hex-hash>.bftree
                        if (name.Length != HashPrefixLength + ".bftree".Length)
                            continue;

                        var keyHash = name[..HashPrefixLength];
                        yield return new RangeIndexFileEntry(path, keyHash, address: 0, isFlushFile: false);
                    }
                }
            }
        }
    }
}