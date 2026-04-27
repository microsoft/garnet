// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
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
    /// The manager tracks all live BfTree instances, coordinates checkpoint/flush/eviction
    /// snapshots, and handles lazy restore from disk.</para>
    ///
    /// <para>Concurrency: Data operations (RI.SET, RI.GET, RI.DEL) acquire a shared lock
    /// via <see cref="ReadRangeIndex"/>; lifecycle operations (DEL key, eviction, checkpoint)
    /// acquire an exclusive lock. See <c>RangeIndexManager.Locking.cs</c> for details.</para>
    ///
    /// <para>Persistence: Stubs survive via Tsavorite's normal log persistence. BfTree data
    /// files are snapshotted independently on flush/checkpoint and lazily restored on access.</para>
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

        /// <summary>Gets the number of live (registered) BfTree indexes.</summary>
        internal int LiveIndexCount => liveIndexes.Count;

        /// <summary>
        /// Tracks live BfTreeService instances keyed by their native tree pointer.
        /// </summary>
        private readonly ConcurrentDictionary<nint, TreeEntry> liveIndexes = new();

        private readonly ILogger logger;

        /// <summary>
        /// Base directory for deterministic BfTree data file paths.
        /// </summary>
        private readonly string dataDir;

        /// <summary>
        /// Global checkpoint barrier. When non-zero, a checkpoint is snapshotting trees.
        /// RI operations check this first (one volatile read on hot path); if set, they
        /// check the per-tree <see cref="TreeEntry.SnapshotPending"/> flag.
        /// </summary>
        private volatile bool checkpointInProgress;

        /// <summary>
        /// Checkpoint token from the last recovery. Used by the restore path to
        /// locate the correct checkpoint snapshot file for recovered stubs.
        /// </summary>
        private Guid recoveredCheckpointToken;

        /// <summary>
        /// Per-tree entry in <see cref="liveIndexes"/>. Class (not struct) so the
        /// <see cref="SnapshotPending"/> field can be updated in-place via <c>Volatile.Write</c>.
        /// </summary>
        internal sealed class TreeEntry
        {
            /// <summary>The managed BfTree wrapper owning the native tree pointer.</summary>
            public readonly BfTreeService Tree;

            /// <summary>Hash of the Garnet key, used for lock striping.</summary>
            public readonly long KeyHash;

            /// <summary>Directory for snapshot files, derived from the key hash via <see cref="HashKeyToDirectoryName"/>.</summary>
            public readonly string KeyDir;

            /// <summary>
            /// 1 while this tree is being snapshotted for checkpoint, 0 otherwise.
            /// Set at <see cref="CheckpointTrigger.VersionShift"/> time, cleared after snapshot completes.
            /// RI data operations spin-wait on this flag when <see cref="checkpointInProgress"/> is set.
            /// </summary>
            public int SnapshotPending;

            /// <summary>
            /// Creates a new tree entry.
            /// </summary>
            /// <param name="tree">The BfTree instance.</param>
            /// <param name="keyHash">Hash of the Garnet key for lock striping.</param>
            /// <param name="keyDir">Base directory for this tree's snapshot files.</param>
            public TreeEntry(BfTreeService tree, long keyHash, string keyDir)
            {
                Tree = tree;
                KeyHash = keyHash;
                KeyDir = keyDir;
            }
        }

        /// <summary>
        /// Whether to remove old BfTree checkpoint snapshots when a new checkpoint completes,
        /// matching Tsavorite's removeOutdated behavior.
        /// </summary>
        private readonly bool removeOutdatedCheckpoints;

        /// <summary>
        /// Creates a new <see cref="RangeIndexManager"/>.
        /// </summary>
        /// <param name="enabled">Whether range index commands are enabled.</param>
        /// <param name="dataDir">Base directory for deterministic BfTree paths. May be null for memory-only usage.</param>
        /// <param name="removeOutdatedCheckpoints">Whether to purge old checkpoint snapshots when a new one completes.</param>
        /// <param name="logger">Optional logger.</param>
        public RangeIndexManager(bool enabled, string dataDir = null, bool removeOutdatedCheckpoints = true, ILogger logger = null)
        {
            IsEnabled = enabled;
            this.dataDir = dataDir;
            this.removeOutdatedCheckpoints = removeOutdatedCheckpoints;
            this.logger = logger;
            rangeIndexLocks = new ReadOptimizedLock(Environment.ProcessorCount);
        }

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
                filePath = DeriveWorkingPath(keyBytes);
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
        /// Cold path — called once per RI.CREATE or lazy restore.
        /// </summary>
        /// <param name="bfTree">The BfTree instance to register.</param>
        /// <param name="keyHash">Hash of the Garnet key, used for lock striping.</param>
        /// <param name="keyBytes">Raw key bytes, used to derive the snapshot directory name.</param>
        internal void RegisterIndex(BfTreeService bfTree, long keyHash, ReadOnlySpan<byte> keyBytes)
        {
            var keyDir = Path.Combine(dataDir ?? string.Empty, "rangeindex", HashKeyToDirectoryName(keyBytes));
            liveIndexes[bfTree.NativePtr] = new TreeEntry(bfTree, keyHash, keyDir);
        }

        /// <summary>
        /// Unregister and dispose a BfTreeService. The caller must already hold
        /// an exclusive lock for the corresponding key hash.
        /// </summary>
        /// <param name="treePtr">Native tree pointer used as the dictionary key.</param>
        /// <returns><c>true</c> if the index was found and disposed; <c>false</c> if not registered.</returns>
        internal bool UnregisterIndex(nint treePtr)
        {
            if (liveIndexes.TryRemove(treePtr, out var entry))
            {
                entry.Tree.Dispose();
                return true;
            }
            return false;
        }

        /// <summary>
        /// Returns <c>true</c> if the given tree handle is registered as a live (in-memory) index.
        /// A tree handle of <see cref="nint.Zero"/> always returns <c>false</c>.
        /// </summary>
        /// <param name="treeHandle">The native pointer stored in the stub's <c>TreeHandle</c> field.</param>
        /// <returns><c>true</c> if the tree is live and registered; <c>false</c> otherwise.</returns>
        internal bool IsTreeLive(nint treeHandle)
            => treeHandle != nint.Zero && liveIndexes.ContainsKey(treeHandle);

        /// <inheritdoc/>
        public void Dispose()
        {
            foreach (var kvp in liveIndexes)
            {
                try
                {
                    kvp.Value.Tree.Dispose();
                }
                catch (Exception ex)
                {
                    logger?.LogWarning(ex, "Failed to dispose BfTree with native pointer {Ptr}", kvp.Key);
                }
            }
            liveIndexes.Clear();
        }

        /// <summary>
        /// Snapshot a BfTree's data to its flush path. Called from <see cref="GarnetRecordTriggers.OnFlush"/>
        /// during page flush so the tree data is persisted before the page is evicted.
        /// Acquires the exclusive lock to serialize with checkpoint snapshots and prevent
        /// concurrent native bftree_snapshot calls on the same tree.
        /// Failure is fatal — the exception propagates to the state machine driver.
        /// </summary>
        internal void SnapshotTreeForFlush(ReadOnlySpan<byte> key, ReadOnlySpan<byte> valueSpan)
        {
            ref readonly var stub = ref ReadIndex(valueSpan);
            if (stub.TreeHandle == nint.Zero)
                return;

            // Memory-only trees cannot be snapshotted (not yet supported by native library).
            // They remain live via TreeHandle but data will be lost on eviction.
            if (stub.StorageBackend == (byte)StorageBackendType.Memory)
                return;

            if (!liveIndexes.TryGetValue(stub.TreeHandle, out var entry))
                return;

            rangeIndexLocks.AcquireExclusiveLock(entry.KeyHash, out var lockToken);
            try
            {
                var flushPath = DeriveFlushPath(key);
                Directory.CreateDirectory(Path.GetDirectoryName(flushPath)!);
                entry.Tree.SnapshotToFile(flushPath);
            }
            finally
            {
                rangeIndexLocks.ReleaseExclusiveLock(lockToken);
            }
        }

        /// <summary>
        /// Derive the deterministic working path for a disk-backed BfTree.
        /// Format: {dataDir}/rangeindex/{key_hash}/data.bftree
        /// </summary>
        internal string DeriveWorkingPath(ReadOnlySpan<byte> keyBytes)
            => Path.Combine(dataDir ?? string.Empty, "rangeindex", HashKeyToDirectoryName(keyBytes), "data.bftree");

        /// <summary>
        /// Derive the deterministic flush snapshot path for a BfTree.
        /// Format: {dataDir}/rangeindex/{key_hash}/flush.bftree
        /// </summary>
        internal string DeriveFlushPath(ReadOnlySpan<byte> keyBytes)
            => Path.Combine(dataDir ?? string.Empty, "rangeindex", HashKeyToDirectoryName(keyBytes), "flush.bftree");

        /// <summary>
        /// Hash key bytes to a directory name using XxHash128, formatted as a 32-char hex string.
        /// </summary>
        internal static string HashKeyToDirectoryName(ReadOnlySpan<byte> keyBytes)
        {
            var hash = XxHash128.Hash(keyBytes);
            return new Guid(hash).ToString("N");
        }

        /// <summary>
        /// Derive the deterministic checkpoint snapshot path for a BfTree.
        /// Format: {dataDir}/rangeindex/{key_hash}/snapshot.{token:N}.bftree
        /// </summary>
        internal string DeriveCheckpointPath(ReadOnlySpan<byte> keyBytes, Guid checkpointToken)
            => Path.Combine(dataDir ?? string.Empty, "rangeindex", HashKeyToDirectoryName(keyBytes),
                $"snapshot.{checkpointToken:N}.bftree");

        /// <summary>
        /// Store the recovered checkpoint token for lazy restore of snapshot-recovered stubs.
        /// </summary>
        internal void SetRecoveredCheckpointToken(Guid token)
        {
            recoveredCheckpointToken = token;
        }

        /// <summary>
        /// Set the checkpoint barrier. Called at version shift (PREPARE → IN_PROGRESS).
        /// Marks all live trees as snapshot-pending, then sets the global flag.
        /// </summary>
        internal void SetCheckpointBarrier(Guid checkpointToken)
        {
            // Mark all current trees as pending snapshot
            foreach (var kvp in liveIndexes)
                Volatile.Write(ref kvp.Value.SnapshotPending, 1);

            // Set global flag (RI operations check this first)
            checkpointInProgress = true;
        }

        /// <summary>
        /// Clear the checkpoint barrier. Called after snapshot completes at WAIT_FLUSH.
        /// </summary>
        internal void ClearCheckpointBarrier()
        {
            // Clear global flag
            checkpointInProgress = false;

            // Clear any remaining per-tree flags (safety: handles trees added during checkpoint)
            foreach (var kvp in liveIndexes)
                Volatile.Write(ref kvp.Value.SnapshotPending, 0);
        }

        /// <summary>
        /// Snapshot all live BfTrees for a checkpoint. Called at WAIT_FLUSH after all v threads
        /// have completed. Takes exclusive lock per tree, snapshots, clears per-tree flag.
        /// The global flag is cleared after all trees are done.
        /// Failure is fatal — the exception propagates to the state machine driver.
        /// </summary>
        internal void SnapshotAllTreesForCheckpoint(Guid checkpointToken)
        {
            try
            {
                foreach (var kvp in liveIndexes)
                {
                    var entry = kvp.Value;

                    // Only snapshot trees that were live at barrier time (SnapshotPending=1).
                    // Trees restored or created in v+1 have SnapshotPending=0 and are skipped —
                    // on recovery they fall back to flush.bftree which has v-state data.
                    if (Volatile.Read(ref entry.SnapshotPending) == 0)
                        continue;

                    // Memory-only trees cannot be snapshotted
                    if (entry.Tree.StorageBackend == StorageBackendType.Memory)
                    {
                        Volatile.Write(ref entry.SnapshotPending, 0);
                        continue;
                    }

                    rangeIndexLocks.AcquireExclusiveLock(entry.KeyHash, out var lockToken);
                    try
                    {
                        var checkpointPath = Path.Combine(entry.KeyDir, $"snapshot.{checkpointToken:N}.bftree");
                        Directory.CreateDirectory(entry.KeyDir);
                        entry.Tree.SnapshotToFile(checkpointPath);
                    }
                    finally
                    {
                        // Clear per-tree flag before releasing lock so waiting RI ops
                        // proceed only after snapshot is complete
                        Volatile.Write(ref entry.SnapshotPending, 0);
                        rangeIndexLocks.ReleaseExclusiveLock(lockToken);
                    }
                }
            }
            finally
            {
                // Always clear global barrier, even if iteration failed
                ClearCheckpointBarrier();
            }
        }

        /// <summary>
        /// Delete BfTree checkpoint snapshot files from prior checkpoints.
        /// Only acts when <see cref="removeOutdatedCheckpoints"/> is true, matching
        /// Tsavorite's removeOutdated behavior. Scans all rangeindex subdirectories
        /// for snapshot.*.bftree files that don't match the current checkpoint token.
        /// </summary>
        internal void PurgeOldCheckpointSnapshots(Guid currentToken)
        {
            if (!removeOutdatedCheckpoints)
                return;

            var currentFileName = $"snapshot.{currentToken:N}.bftree";
            var rangeIndexDir = Path.Combine(dataDir ?? string.Empty, "rangeindex");
            if (!Directory.Exists(rangeIndexDir))
                return;

            try
            {
                foreach (var snapshotFile in Directory.EnumerateFiles(rangeIndexDir, "snapshot.*.bftree", SearchOption.AllDirectories))
                {
                    if (!Path.GetFileName(snapshotFile).Equals(currentFileName, StringComparison.Ordinal))
                    {
                        try
                        {
                            File.Delete(snapshotFile);
                        }
                        catch (Exception ex)
                        {
                            logger?.LogWarning(ex, "Failed to delete old checkpoint snapshot: {Path}", snapshotFile);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "Failed to enumerate old checkpoint snapshots for cleanup");
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