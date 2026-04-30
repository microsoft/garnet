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
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Size = Size)]
        internal struct RangeIndexStub
        {
            internal const int Size = 35;

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

            /// <summary>Reserved flags byte.</summary>
            [FieldOffset(33)]
            public byte Flags;

            /// <summary>Flag bit indicating the stub has been flushed and needs promotion to tail.</summary>
            internal const byte FlagFlushed = 1;

            /// <summary>Flag bit indicating the stub was recovered from a checkpoint snapshot file.</summary>
            internal const byte FlagRecovered = 2;

            /// <summary>Returns true if the stub has been flushed and needs promotion to tail.</summary>
            internal readonly bool IsFlushed => (Flags & FlagFlushed) != 0;

            /// <summary>Returns true if the stub was recovered from a checkpoint snapshot.</summary>
            internal readonly bool IsRecovered => (Flags & FlagRecovered) != 0;

            /// <summary>Serialization phase for checkpoint coordination.</summary>
            [FieldOffset(34)]
            public byte SerializationPhase;
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
            stub.Flags = 0;
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
        /// Called by <see cref="RestoreTreeFromFlush"/> after recovering the native BfTree.
        /// </summary>
        /// <param name="newTreeHandle">The native pointer of the newly restored BfTree.</param>
        /// <param name="valueSpan">The store value span containing the stub.</param>
        internal static void RecreateIndex(nint newTreeHandle, Span<byte> valueSpan)
        {
            ref var stub = ref Unsafe.As<byte, RangeIndexStub>(ref MemoryMarshal.GetReference(valueSpan));
            stub.TreeHandle = newTreeHandle;

            // Clear FlagRecovered so that future eviction cycles use the flush snapshot
            // (which reflects post-recovery writes) instead of the stale checkpoint snapshot.
            stub.Flags &= unchecked((byte)~RangeIndexStub.FlagRecovered);
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
        /// Dispose the BfTree under an exclusive lock, preventing concurrent RI data
        /// operations and checkpoint snapshots from accessing the tree during disposal.
        /// Used by <see cref="GarnetRecordTriggers.OnDispose"/> (delete) and
        /// <see cref="GarnetRecordTriggers.OnEvict"/> (page eviction).
        /// </summary>
        /// <param name="key">The raw key bytes (used to compute the key hash for lock acquisition).</param>
        /// <param name="valueSpan">The store value span containing the stub.</param>
        /// <param name="deleteFiles">When <c>true</c> (DEL/UNLINK), delete the BfTree data files
        /// on disk after disposing the native tree. When <c>false</c> (eviction), files are
        /// preserved for lazy restore on next access.</param>
        internal void DisposeTreeUnderLock(ReadOnlySpan<byte> key, ReadOnlySpan<byte> valueSpan, bool deleteFiles)
        {
            ref readonly var stub = ref ReadIndex(valueSpan);

            if (stub.TreeHandle == nint.Zero && !deleteFiles)
                return;

            var keyHash = GarnetKeyComparer.StaticGetHashCode64((FixedSpanByteKey)PinnedSpanByte.FromPinnedSpan(key));
            rangeIndexLocks.AcquireExclusiveLock(keyHash, out var lockToken);
            try
            {
                if (stub.TreeHandle != nint.Zero)
                    UnregisterIndex(stub.TreeHandle);

                if (deleteFiles && stub.StorageBackend == (byte)StorageBackendType.Disk)
                    DeleteTreeFiles(key);
            }
            finally
            {
                rangeIndexLocks.ReleaseExclusiveLock(lockToken);
            }
        }

        /// <summary>
        /// Delete BfTree data files for a key. Removes the entire key directory
        /// (<c>{dataDir}/rangeindex/{hash}/</c>) containing <c>data.bftree</c>,
        /// <c>flush.bftree</c>, and any <c>snapshot.*.bftree</c> files.
        /// Called after a DEL/UNLINK removes the key from the main store.
        /// </summary>
        /// <param name="key">The raw key bytes used to derive the deterministic directory path.</param>
        private void DeleteTreeFiles(ReadOnlySpan<byte> key)
        {
            try
            {
                var keyDir = Path.Combine(dataDir, "rangeindex", HashKeyToDirectoryName(key));
                if (Directory.Exists(keyDir))
                {
                    Directory.Delete(keyDir, recursive: true);
                }
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "Failed to delete BfTree data files for deleted RangeIndex key");
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
            stub.Flags |= RangeIndexStub.FlagFlushed;
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
            stub.Flags &= unchecked((byte)~RangeIndexStub.FlagFlushed);
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
        /// At restore time, this flag tells <see cref="RestoreTreeFromFlush"/> to use the
        /// checkpoint snapshot file (via <see cref="recoveredCheckpointToken"/>) instead of the
        /// flush snapshot file.
        /// </summary>
        /// <param name="valueSpan">The store value span containing the stub from the checkpoint snapshot.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void MarkRecoveredFromCheckpoint(Span<byte> valueSpan)
        {
            ref var stub = ref Unsafe.As<byte, RangeIndexStub>(
                ref MemoryMarshal.GetReference(valueSpan));
            stub.TreeHandle = nint.Zero;
            stub.Flags |= RangeIndexStub.FlagRecovered;
        }
    }
}