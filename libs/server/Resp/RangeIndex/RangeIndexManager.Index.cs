// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Garnet.common;
using Garnet.server.BfTreeInterop;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Stub struct, serialization, and flag management methods for RangeIndex records.
    /// The stub is the fixed-size value stored inline in Tsavorite's log for each RI key.
    /// </summary>
    public sealed partial class RangeIndexManager
    {
        /// <summary>
        /// Fixed-size struct stored as a raw-byte value in Tsavorite's unified store.
        /// Contains BfTree configuration metadata and a native pointer to the live instance.
        ///
        /// <para>Layout: The struct uses explicit field offsets for deterministic binary layout
        /// (no padding, no reordering). This ensures the stub can be safely reinterpreted
        /// via <c>Unsafe.As</c> from raw store value spans.</para>
        ///
        /// <para>Total size: <see cref="Size"/> (35 bytes).</para>
        ///
        /// <para>Flags byte (offset 33) bit layout (LSB = bit 0 = first allocated):</para>
        /// <code>
        ///   [Unused7][Unused6][Unused5][Pinned*][Modified*][Transferred][Recovered][Flushed]
        /// </code>
        /// <para>* = reserved for future use; bit position allocated, no property exposed yet.</para>
        /// <list type="bullet">
        ///   <item><c>Flushed</c>     — stub has been flushed; needs promotion to tail on next access.</item>
        ///   <item><c>Recovered</c>   — stub was loaded from a checkpoint snapshot file.</item>
        ///   <item><c>Transferred</c> — ownership transferred to a newer record (compaction or RIPROMOTE).</item>
        ///   <item><c>Modified*</c>   — [reserved] for future write-tracking.</item>
        ///   <item><c>Pinned*</c>     — [reserved] for future eviction-pinning.</item>
        ///   <item><c>Unused5..7</c>  — available for future expansion.</item>
        /// </list>
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Size = Size)]
        internal struct RangeIndexStub
        {
            internal const int Size = 35;

            // Flag bit masks (Flags byte at offset 33).
            private const byte kFlushedBitMask = 1 << 0;
            private const byte kRecoveredBitMask = 1 << 1;
            private const byte kTransferredBitMask = 1 << 2;
            // Reserved for future use (bit positions allocated; properties deferred until semantics exist):
            //   bit 3 = Modified
            //   bit 4 = Pinned
            //   bits 5..7 currently unused

            /// <summary>Pointer to the live BfTreeService instance (managed object handle).</summary>
            [FieldOffset(0)]
            public nint TreeHandle;

            /// <summary>BfTree circular buffer size in bytes.</summary>
            [FieldOffset(8)]
            public ulong CacheSize;

            /// <summary>BfTree minimum record size.</summary>
            [FieldOffset(16)]
            public uint MinRecordSize;

            /// <summary>BfTree maximum record size.</summary>
            [FieldOffset(20)]
            public uint MaxRecordSize;

            /// <summary>BfTree maximum key length.</summary>
            [FieldOffset(24)]
            public uint MaxKeyLen;

            /// <summary>BfTree leaf page size.</summary>
            [FieldOffset(28)]
            public uint LeafPageSize;

            /// <summary>Storage backend: 0=Disk, 1=Memory.</summary>
            [FieldOffset(32)]
            public byte StorageBackend;

            /// <summary>Flags byte. Private so all writes must go through typed properties / <see cref="ResetFlags"/>,
            /// which centralizes invariant control as additional bits gain semantics.</summary>
            [FieldOffset(33)]
            private byte flags;

            /// <summary>Serialization phase for checkpoint coordination.</summary>
            [FieldOffset(34)]
            public byte SerializationPhase;

            /// <summary>Whether the stub has been flushed and needs promotion to tail.</summary>
            internal bool IsFlushed
            {
                readonly get => (flags & kFlushedBitMask) != 0;
                set => flags = value ? (byte)(flags | kFlushedBitMask) : (byte)(flags & ~kFlushedBitMask);
            }

            /// <summary>Whether the stub was recovered from a checkpoint snapshot file.</summary>
            internal bool IsRecovered
            {
                readonly get => (flags & kRecoveredBitMask) != 0;
                set => flags = value ? (byte)(flags | kRecoveredBitMask) : (byte)(flags & ~kRecoveredBitMask);
            }

            /// <summary>Whether ownership has been transferred to a newer record at the tail
            /// (compaction <see cref="GarnetRecordTriggers.PostCopyToTail"/> or RIPROMOTE PostCopyUpdater
            /// set this on the source record so a later <see cref="GarnetRecordTriggers.OnEvict"/> on the
            /// stale source does NOT remove the liveIndexes entry that now belongs to the newer
            /// destination, and so a later <see cref="GarnetRecordTriggers.OnFlush"/> on the stale source
            /// does NOT snapshot a stale view of <c>data.bftree</c>).</summary>
            internal bool IsTransferred
            {
                readonly get => (flags & kTransferredBitMask) != 0;
                set => flags = value ? (byte)(flags | kTransferredBitMask) : (byte)(flags & ~kTransferredBitMask);
            }

            /// <summary>Reset all flag bits to 0. Used by <see cref="CreateIndex"/> on a freshly initialized
            /// stub and by AOF replay (<see cref="HandleRangeIndexCreateReplay"/>) when reinitializing an
            /// in-memory stub for an RI.CREATE record.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void ResetFlags() => flags = 0;
        }

        /// <summary>
        /// Write a new stub into the value span of a LogRecord.
        /// Called from InitialUpdater in RMWMethods.cs during RI.CREATE.
        /// </summary>
        /// <param name="cacheSize">BfTree circular buffer size in bytes.</param>
        /// <param name="minRecordSize">BfTree minimum record size.</param>
        /// <param name="maxRecordSize">BfTree maximum record size.</param>
        /// <param name="maxKeyLen">BfTree maximum key length.</param>
        /// <param name="leafPageSize">BfTree leaf page size.</param>
        /// <param name="storageBackend">Storage backend type (0=Disk, 1=Memory).</param>
        /// <param name="treeHandle">Native pointer to the live BfTreeService instance.</param>
        /// <param name="valueSpan">The store value span to write the stub into (must be ≥ <see cref="RangeIndexStub.Size"/>).</param>
        internal void CreateIndex(
            ulong cacheSize,
            uint minRecordSize,
            uint maxRecordSize,
            uint maxKeyLen,
            uint leafPageSize,
            byte storageBackend,
            nint treeHandle,
            Span<byte> valueSpan)
        {
            Debug.Assert(Unsafe.SizeOf<RangeIndexStub>() == RangeIndexStub.Size, "Constant stub size is incorrect");
            Debug.Assert(valueSpan.Length >= RangeIndexStub.Size, $"Value span too small: {valueSpan.Length} < {RangeIndexStub.Size}");

            ref var stub = ref Unsafe.As<byte, RangeIndexStub>(ref MemoryMarshal.GetReference(valueSpan));
            stub.TreeHandle = treeHandle;
            stub.CacheSize = cacheSize;
            stub.MinRecordSize = minRecordSize;
            stub.MaxRecordSize = maxRecordSize;
            stub.MaxKeyLen = maxKeyLen;
            stub.LeafPageSize = leafPageSize;
            stub.StorageBackend = storageBackend;
            stub.ResetFlags();
            stub.SerializationPhase = 0;
        }

        /// <summary>
        /// Get a readonly reference to the stub from a store value span (zero-copy).
        /// Uses <c>Unsafe.As</c> to reinterpret the raw bytes — the caller must ensure the
        /// span is at least <see cref="IndexSizeBytes"/> long.
        /// </summary>
        /// <param name="value">Raw value bytes from the store record.</param>
        /// <returns>A readonly reference to the reinterpreted stub.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ref readonly RangeIndexStub ReadIndex(ReadOnlySpan<byte> value)
            => ref Unsafe.As<byte, RangeIndexStub>(ref MemoryMarshal.GetReference(value));

        /// <summary>
        /// Update TreeHandle after lazy restore from a flush or checkpoint snapshot file.
        /// Called by <see cref="RestoreTree"/> after recovering the native BfTree.
        /// </summary>
        /// <param name="newTreeHandle">The native pointer of the newly restored BfTree.</param>
        /// <param name="valueSpan">The store value span containing the stub.</param>
        internal static void RecreateIndex(nint newTreeHandle, Span<byte> valueSpan)
        {
            ref var stub = ref Unsafe.As<byte, RangeIndexStub>(ref MemoryMarshal.GetReference(valueSpan));
            stub.TreeHandle = newTreeHandle;

            // Clear IsRecovered so that future eviction cycles use the flush snapshot
            // (which reflects post-recovery writes) instead of the stale checkpoint snapshot.
            stub.IsRecovered = false;
        }

        /// <summary>
        /// Zero the TreeHandle in a stub span. Used by CopyUpdater (RIPROMOTE) to prevent
        /// the old record's eviction callback from freeing the BfTree that the new
        /// (promoted) record now owns.
        /// </summary>
        /// <param name="valueSpan">The store value span containing the stub.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void ClearTreeHandle(Span<byte> valueSpan)
        {
            ref var stub = ref Unsafe.As<byte, RangeIndexStub>(
                ref MemoryMarshal.GetReference(valueSpan));
            stub.TreeHandle = nint.Zero;
        }

        /// <summary>
        /// Set the <see cref="RangeIndexStub.IsTransferred"/> flag on a stub span. Called by
        /// PostCopyToTail (compaction) and RIPROMOTE PostCopyUpdater on the SOURCE record after
        /// ownership is transferred to a new tail destination. This guarantees a later
        /// <see cref="GarnetRecordTriggers.OnEvict"/> on the stale source no-ops (does not remove
        /// the liveIndexes entry that now belongs to the destination), and a later
        /// <see cref="GarnetRecordTriggers.OnFlush"/> on the stale source no-ops (does not
        /// snapshot a now-stale view of <c>data.bftree</c>).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void SetTransferredFlag(Span<byte> valueSpan)
        {
            ref var stub = ref Unsafe.As<byte, RangeIndexStub>(
                ref MemoryMarshal.GetReference(valueSpan));
            stub.IsTransferred = true;
        }

        /// <summary>
        /// Dispose the BfTree under an exclusive lock, preventing concurrent RI data
        /// operations and checkpoint snapshots from accessing the tree during disposal.
        /// Used by <see cref="GarnetRecordTriggers.OnDispose"/> (delete) and
        /// <see cref="GarnetRecordTriggers.OnEvict"/> (page eviction).
        /// </summary>
        /// <param name="key">The raw key bytes (used to compute the key hash for lock acquisition).</param>
        /// <param name="valueSpan">The store value span containing the stub.</param>
        /// <param name="deleteFiles">When <c>true</c> (DEL/UNLINK), delete the working
        /// <c>&lt;hash&gt;.data.bftree</c> file and the CPR scratch <c>&lt;hash&gt;.scratch.cpr</c>
        /// in the riLogRoot. Per-flush snapshot files (<c>&lt;hash&gt;.&lt;addr&gt;.flush.bftree</c>)
        /// are deliberately preserved — they are LOG-tied (lifetime tracks BeginAddress) and may
        /// still be needed to recover an OLDER checkpoint that pre-dates the DEL. They are
        /// reclaimed by <see cref="OnTruncateImpl"/> only once Tsavorite's BeginAddress passes
        /// their address. When <c>false</c> (eviction), files are preserved for lazy restore.</param>
        internal void DisposeTreeUnderLock(ReadOnlySpan<byte> key, ReadOnlySpan<byte> valueSpan, bool deleteFiles)
        {
            ref readonly var stub = ref ReadIndex(valueSpan);

            // Stale-source eviction (OnEvict) on a record whose ownership was transferred to a
            // newer tail record: no-op. The liveIndexes entry now belongs to the destination
            // record; removing it here would lose the newer record's tree (live transfer) or
            // pending entry (cold transfer). IsTransferred is set by PostCopyToTail and RIPROMOTE
            // PostCopyUpdater on the source after a successful CAS.
            // For DEL/UNLINK (deleteFiles=true), we still proceed — the user explicitly asked to
            // delete this key, and the tombstone path supersedes any newer record as well.
            if (stub.IsTransferred && !deleteFiles)
                return;

            // All liveIndexes mutations (cold-pending eviction below, plus the activated path
            // further down) MUST hold the per-key exclusive lock to serialize against
            // SetCheckpointBarrier / SnapshotAllTreesForCheckpoint / RegisterIndex /
            // PreStageAndRegisterPending which all touch this entry under the same lock.
            // Without it, a cold eviction concurrent with checkpoint can silently drop a
            // pending entry mid-snapshot iteration, narrowing checkpoint coverage for the key.
            var keyHash = GarnetKeyComparer.StaticGetHashCode64((FixedSpanByteKey)PinnedSpanByte.FromPinnedSpan(key));
            BfTreeService disposedTree = null;
            string hashPrefix = null;
            rangeIndexLocks.AcquireExclusiveLock(keyHash, out var lockToken);
            try
            {
                if (stub.TreeHandle == nint.Zero && !deleteFiles)
                {
                    // Pending entry cleanup on eviction: remove from liveIndexes (no native tree to dispose).
                    _ = liveIndexes.TryRemove(KeyId(key), out _);
                    return;
                }

                // Remove entry from liveIndexes synchronously so concurrent restorers see no
                // tree for this key. Stash the BfTree wrapper (if any) for deferred disposal.
                if (liveIndexes.TryRemove(KeyId(key), out var entry))
                    disposedTree = entry?.Tree;

                if (deleteFiles)
                    hashPrefix = HashKeyToPrefix(key);
            }
            finally
            {
                rangeIndexLocks.ReleaseExclusiveLock(lockToken);
            }

            // Defer the heavyweight cleanup (native dispose + file delete) via the store epoch.
            // The deferred action runs only after all current epoch holders have moved past, so
            // any reader that observed a non-zero TreeHandle from this entry's stub completes
            // its native call before the bfTree is freed.
            if (disposedTree == null && hashPrefix == null)
                return;
            if (storeEpoch != null)
            {
                var capturedTree = disposedTree;
                var capturedPrefix = hashPrefix;
                storeEpoch.BumpCurrentEpoch(() => DisposeAndDeleteFilesDeferred(capturedTree, capturedPrefix));
            }
            else
            {
                // No epoch available (unit-test scenario without a Tsavorite store). Synchronous
                // dispose is OK here because no concurrent readers exist in such tests.
                DisposeAndDeleteFilesDeferred(disposedTree, hashPrefix);
            }
        }

        /// <summary>
        /// Deferred BfTree disposal + file deletion. Invoked from inside
        /// <c>storeEpoch.BumpCurrentEpoch</c> action so it runs only after all readers that
        /// could have observed the tree's TreeHandle have moved past.
        /// </summary>
        /// <param name="tree">The BfTree wrapper to dispose, or null.</param>
        /// <param name="hashPrefix">Hash prefix for file deletion, or null to skip file deletion.</param>
        private void DisposeAndDeleteFilesDeferred(BfTreeService tree, string hashPrefix)
        {
            // Order matters on Windows: dispose first so the native side closes any open
            // file handles, then File.Delete (otherwise the unlink races with the close).
            if (tree != null)
            {
                try { tree.Dispose(); }
                catch (Exception ex) { logger?.LogWarning(ex, "Deferred dispose failed for BfTree"); }
            }
            if (hashPrefix != null && !string.IsNullOrEmpty(riLogRoot) && Directory.Exists(riLogRoot))
            {
                TryDelete(LogDataPath(hashPrefix));
                TryDelete(LogScratchPath(hashPrefix));
            }

            void TryDelete(string p)
            {
                try
                {
                    if (File.Exists(p)) File.Delete(p);
                }
                catch (Exception ex)
                {
                    logger?.LogWarning(ex, "Deferred file delete failed: {Path}", p);
                }
            }
        }

        /// <summary>
        /// Set the Flushed flag on the in-memory stub. Called from <see cref="GarnetRecordTriggers.OnFlush"/>
        /// on the original (not copied) record, so the next data operation detects the flag
        /// and promotes the stub to the mutable tail region via <see cref="PromoteToTail"/>.
        /// </summary>
        /// <param name="valueSpan">The store value span containing the stub (writable even though declared ReadOnlySpan, via Unsafe.As).</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void SetFlushedFlag(Span<byte> valueSpan)
        {
            ref var stub = ref Unsafe.As<byte, RangeIndexStub>(
                ref MemoryMarshal.GetReference(valueSpan));
            stub.IsFlushed = true;
        }

        /// <summary>
        /// Clear the Flushed flag on a stub span. Called by CopyUpdater after promoting
        /// the stub to the mutable tail region, so subsequent operations no longer trigger promotion.
        /// </summary>
        /// <param name="valueSpan">The store value span containing the promoted stub.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void ClearFlushedFlag(Span<byte> valueSpan)
        {
            ref var stub = ref Unsafe.As<byte, RangeIndexStub>(
                ref MemoryMarshal.GetReference(valueSpan));
            stub.IsFlushed = false;
        }

        /// <summary>
        /// Clear TreeHandle on a stub span. Called by <see cref="GarnetRecordTriggers.OnDiskRead"/>
        /// when a record is loaded from disk — the native pointer from the original process is stale,
        /// so we zero it to signal that the BfTree must be lazily restored on next access.
        /// </summary>
        /// <param name="valueSpan">The store value span containing the stub loaded from disk.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void InvalidateStub(Span<byte> valueSpan)
        {
            ref var stub = ref Unsafe.As<byte, RangeIndexStub>(
                ref MemoryMarshal.GetReference(valueSpan));
            stub.TreeHandle = nint.Zero;
        }

        /// <summary>
        /// Set the Recovered flag and zero TreeHandle on a stub loaded from a checkpoint snapshot.
        /// Used by <c>OnRecoverySnapshotRead</c> to mark stubs whose state is the snapshot
        /// rather than any post-recovery flush file.
        /// </summary>
        /// <param name="valueSpan">The store value span containing the stub from the checkpoint snapshot.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void MarkRecoveredFromCheckpoint(Span<byte> valueSpan)
        {
            ref var stub = ref Unsafe.As<byte, RangeIndexStub>(
                ref MemoryMarshal.GetReference(valueSpan));
            stub.TreeHandle = nint.Zero;
            stub.IsRecovered = true;
        }
    }
}