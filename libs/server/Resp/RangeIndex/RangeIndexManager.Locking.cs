// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using Garnet.common;
using Garnet.server.BfTreeInterop;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Locking methods for RangeIndex operations.
    ///
    /// <para>Locking protocol:</para>
    /// <list type="bullet">
    /// <item><b>Shared locks</b> are acquired for data operations (RI.SET, RI.GET, RI.DEL field)
    /// via <see cref="ReadRangeIndex"/>. Multiple sessions can operate on the same BfTree
    /// concurrently — the native BfTree is internally thread-safe for point operations.</item>
    /// <item><b>Exclusive locks</b> are acquired for lifecycle operations (DEL key, eviction,
    /// checkpoint snapshot, lazy restore) via <see cref="AcquireExclusiveForDelete"/>.
    /// These prevent concurrent data operations from accessing a BfTree that is being
    /// freed, snapshotted, or restored.</item>
    /// </list>
    ///
    /// <para>Locks are striped by key hash via <see cref="ReadOptimizedLock"/> for scalability.</para>
    /// </summary>
    public sealed partial class RangeIndexManager
    {
        /// <summary>
        /// RAII holder for a shared lock on a RangeIndex key.
        /// Disposing releases the shared lock.
        /// </summary>
        internal readonly ref struct ReadRangeIndexLock : IDisposable
        {
            private readonly ref readonly ReadOptimizedLock lockRef;
            private readonly int lockToken;

            internal ReadRangeIndexLock(ref readonly ReadOptimizedLock lockRef, int lockToken)
            {
                this.lockToken = lockToken;
                this.lockRef = ref lockRef;
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                if (Unsafe.IsNullRef(in lockRef))
                    return;

                lockRef.ReleaseSharedLock(lockToken);
            }
        }

        /// <summary>
        /// RAII holder for an exclusive lock on a RangeIndex key.
        /// Disposing releases the exclusive lock.
        /// </summary>
        internal readonly ref struct ExclusiveRangeIndexLock : IDisposable
        {
            private readonly ref readonly ReadOptimizedLock lockRef;
            private readonly int lockToken;

            internal ExclusiveRangeIndexLock(ref readonly ReadOptimizedLock lockRef, int lockToken)
            {
                this.lockToken = lockToken;
                this.lockRef = ref lockRef;
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                if (Unsafe.IsNullRef(in lockRef))
                    return;

                lockRef.ReleaseExclusiveLock(lockToken);
            }
        }

        private readonly ReadOptimizedLock rangeIndexLocks;

        /// <summary>
        /// Read the RangeIndex stub under a shared lock.
        /// Used by RI.SET, RI.GET, RI.DEL (field-level operations).
        /// Returns an RAII lock holder; caller operates on the BfTree while the lock is held.
        /// </summary>
        /// <remarks>
        /// This method handles several edge cases transparently:
        /// <list type="bullet">
        /// <item><b>Checkpoint in progress</b>: If the tree is being snapshotted, releases
        /// the lock, waits for snapshot completion, and retries.</item>
        /// <item><b>Flushed stub</b>: If the stub has been flushed to the read-only region
        /// (FlagFlushed set), promotes the stub to the tail via RMW and retries.</item>
        /// <item><b>Null TreeHandle</b>: If the tree was evicted to disk (TreeHandle == 0),
        /// triggers lazy restore from the flush/checkpoint snapshot file and retries.</item>
        /// </list>
        /// </remarks>
        /// <param name="session">The storage session for store reads.</param>
        /// <param name="key">The pinned Garnet key.</param>
        /// <param name="input">The StringInput for the read operation (used for WRONGTYPE checks).</param>
        /// <param name="indexSpan">Caller-provided span to receive the stub bytes (must be ≥ <see cref="IndexSizeBytes"/>).</param>
        /// <param name="status">On return, the read status (OK, NOTFOUND, or WRONGTYPE).</param>
        /// <returns>An RAII lock holder. Dispose to release the shared lock.</returns>
        internal ReadRangeIndexLock ReadRangeIndex(
            StorageSession session,
            PinnedSpanByte key,
            ref StringInput input,
            scoped Span<byte> indexSpan,
            out GarnetStatus status)
        {
            Debug.Assert(indexSpan.Length >= IndexSizeBytes, "Insufficient space for index");

            var keyHash = session.stringBasicContext.GetKeyHash((FixedSpanByteKey)key);

        Retry:
            var output = StringOutput.FromPinnedSpan(indexSpan);
            rangeIndexLocks.AcquireSharedLock(keyHash, out var sharedLockToken);

            GarnetStatus readRes;
            try
            {
                readRes = session.Read_MainStore(key.ReadOnlySpan, ref input, ref output, ref session.stringBasicContext);
            }
            catch
            {
                rangeIndexLocks.ReleaseSharedLock(sharedLockToken);
                throw;
            }

            if (readRes != GarnetStatus.OK)
            {
                status = readRes;
                rangeIndexLocks.ReleaseSharedLock(sharedLockToken);
                return default;
            }

            var outputSpan = output.SpanByteAndMemory.IsSpanByte
                ? output.SpanByteAndMemory.SpanByte.ReadOnlySpan
                : output.SpanByteAndMemory.MemorySpan;

            if (outputSpan.Length != IndexSizeBytes)
            {
                rangeIndexLocks.ReleaseSharedLock(sharedLockToken);
                throw new GarnetException($"Unexpected stub size {outputSpan.Length} for RangeIndex read, expected {IndexSizeBytes}");
            }

            ref readonly var stub = ref ReadIndex(outputSpan);

            // Per-tree checkpoint barrier: one volatile read on hot path (no checkpoint = skipped).
            if (checkpointInProgress
                && WaitForTreeCheckpoint(stub.TreeHandle, ref output, indexSpan, ref sharedLockToken))
            {
                goto Retry;
            }

            if (stub.IsFlushed)
            {
                rangeIndexLocks.ReleaseSharedLock(sharedLockToken);
                PromoteToTail(session, key);
                goto Retry;
            }

            if (stub.TreeHandle == nint.Zero)
            {
                rangeIndexLocks.ReleaseSharedLock(sharedLockToken);

                // Restore under exclusive lock to prevent concurrent restores.
                // No need to wait for checkpoint — restored trees get SnapshotPending=0
                // and are skipped by SnapshotAllTreesForCheckpoint. On recovery they
                // fall back to flush.bftree which has the correct v-state data.
                if (!RestoreTreeFromFlush(session, key, keyHash, ref input, indexSpan))
                {
                    status = GarnetStatus.NOTFOUND;
                    return default;
                }
                goto Retry;
            }

            status = GarnetStatus.OK;
            return new(in rangeIndexLocks, sharedLockToken);
        }

        /// <summary>
        /// Acquire an exclusive lock for the given key hash.
        /// Used by <c>TryDeleteRangeIndex</c> to prevent concurrent data operations
        /// while a BfTree is being freed.
        /// </summary>
        /// <param name="keyHash">The key hash for lock striping.</param>
        /// <returns>An RAII lock holder. Dispose to release the exclusive lock.</returns>
        internal ExclusiveRangeIndexLock AcquireExclusiveForDelete(long keyHash)
        {
            rangeIndexLocks.AcquireExclusiveLock(keyHash, out var exclusiveLockToken);
            return new(in rangeIndexLocks, exclusiveLockToken);
        }

        /// <summary>
        /// Issue an RMW with RIPROMOTE to copy the stub from read-only to the mutable region.
        /// CopyUpdater copies the stub bytes and clears the flushed flag.
        /// Multiple threads may race here — only one wins the CAS; others see the new record.
        /// </summary>
        private static void PromoteToTail(StorageSession session, PinnedSpanByte key)
        {
            session.PromoteRangeIndexToTail(key);
        }

        /// <summary>
        /// Cold path: checks if this tree is currently being snapshotted for a checkpoint.
        /// If so, releases the shared lock, spin-waits for the snapshot to complete, and
        /// signals the caller to retry the entire read operation.
        /// </summary>
        /// <param name="treeHandle">The native tree pointer from the stub.</param>
        /// <param name="output">The StringOutput from the read (passed for lifetime management).</param>
        /// <param name="indexSpan">The stub span (passed for lifetime management).</param>
        /// <param name="sharedLockToken">The shared lock token; released before waiting.</param>
        /// <returns><c>true</c> if caller should retry the read; <c>false</c> if no wait was needed.</returns>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private bool WaitForTreeCheckpoint(nint treeHandle, ref StringOutput output,
            Span<byte> indexSpan, ref int sharedLockToken)
        {
            if (treeHandle == nint.Zero
                || !liveIndexes.TryGetValue(treeHandle, out var treeEntry)
                || Volatile.Read(ref treeEntry.SnapshotPending) == 0)
                return false;

            rangeIndexLocks.ReleaseSharedLock(sharedLockToken);
            while (Volatile.Read(ref treeEntry.SnapshotPending) != 0)
                Thread.Yield();
            return true;
        }

        /// <summary>
        /// Restore a BfTree from its snapshot file under the exclusive lock.
        /// </summary>
        /// <remarks>
        /// Prevents concurrent restores by re-reading the stub under the exclusive lock — if
        /// another thread already set TreeHandle, returns immediately with <c>true</c>.
        ///
        /// <para>Chooses the snapshot source based on the stub's flags:</para>
        /// <list type="bullet">
        /// <item><b>FlagRecovered set</b>: The stub was in a checkpoint snapshot file. Uses
        /// the checkpoint snapshot (<see cref="DeriveCheckpointPath"/>).</item>
        /// <item><b>FlagRecovered not set</b>: The stub was evicted from the log. Uses
        /// the flush snapshot (<see cref="DeriveFlushPath"/>).</item>
        /// </list>
        ///
        /// <para>After recovery, the BfTree data file is copied to the working path so the
        /// native tree uses <c>data.bftree</c> as its live file. This prevents self-copy
        /// corruption when <see cref="GarnetRecordTriggers.OnFlush"/> later snapshots back
        /// to <c>flush.bftree</c>, and keeps checkpoint artifacts immutable.</para>
        /// </remarks>
        /// <param name="session">The storage session for reading the stub and issuing restore RMW.</param>
        /// <param name="key">The pinned Garnet key.</param>
        /// <param name="keyHash">Hash of the key for lock acquisition.</param>
        /// <param name="input">The StringInput for reading the stub.</param>
        /// <param name="indexSpan">Scratch span for the stub bytes.</param>
        /// <returns><c>true</c> if the tree was restored (or already restored by another thread); <c>false</c> on failure.</returns>
        private bool RestoreTreeFromFlush(
            StorageSession session,
            PinnedSpanByte key,
            long keyHash,
            ref StringInput input,
            Span<byte> indexSpan)
        {
            rangeIndexLocks.AcquireExclusiveLock(keyHash, out var exclusiveLockToken);
            try
            {
                // Re-read stub under exclusive lock to check if another thread already restored
                var output = StringOutput.FromPinnedSpan(indexSpan);
                var readRes = session.Read_MainStore(key.ReadOnlySpan, ref input, ref output, ref session.stringBasicContext);
                if (readRes != GarnetStatus.OK)
                    return false;

                var outputSpan = output.SpanByteAndMemory.IsSpanByte
                    ? output.SpanByteAndMemory.SpanByte.ReadOnlySpan
                    : output.SpanByteAndMemory.MemorySpan;
                if (outputSpan.Length < IndexSizeBytes)
                    return false;

                ref readonly var stub = ref ReadIndex(outputSpan);
                if (stub.TreeHandle != nint.Zero)
                    return true; // Another thread already restored — retry will find it

                var keySpan = key.ReadOnlySpan;
                string restorePath;

                if (stub.IsRecovered && recoveredCheckpointToken != Guid.Empty)
                {
                    // Stub was in the checkpoint snapshot file — use checkpoint snapshot.
                    restorePath = DeriveCheckpointPath(keySpan, recoveredCheckpointToken);
                    Debug.Assert(File.Exists(restorePath),
                        $"Checkpoint snapshot file missing for recovered stub: {restorePath}. " +
                        "FlagRecovered should only be set for stubs above flushedLogicalAddress, " +
                        "which means the tree was in liveIndexes at checkpoint time.");
                }
                else
                {
                    // Eviction path (or main log recovery) — use the flush snapshot
                    restorePath = DeriveFlushPath(keySpan);
                }

                if (!File.Exists(restorePath))
                {
                    logger?.LogWarning("Snapshot file not found for lazy restore: {Path}", restorePath);
                    return false;
                }

                // Copy snapshot to the deterministic working path so the native tree
                // uses data.bftree as its live file. This prevents self-copy corruption
                // when OnFlush later snapshots back to flush.bftree, and keeps checkpoint
                // artifacts immutable.
                var workingPath = DeriveWorkingPath(keySpan);
                Directory.CreateDirectory(Path.GetDirectoryName(workingPath)!);
                if (restorePath != workingPath)
                    File.Copy(restorePath, workingPath, overwrite: true);

                var bfTree = BfTreeService.RecoverFromSnapshot(
                    workingPath,
                    (StorageBackendType)stub.StorageBackend,
                    stub.CacheSize,
                    stub.MinRecordSize,
                    stub.MaxRecordSize,
                    stub.MaxKeyLen,
                    stub.LeafPageSize);

                RegisterIndex(bfTree, keyHash, keySpan);
                session.RestoreRangeIndexStub(key, bfTree.NativePtr);
                return true;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Failed to restore BfTree from snapshot");
                return false;
            }
            finally
            {
                rangeIndexLocks.ReleaseExclusiveLock(exclusiveLockToken);
            }
        }
    }
}