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
    /// <item>DEL key and eviction run in record triggers, under the record's bucket latch or in an epoch drain,
    /// which a shared lock holder may be waiting on. They never wait for the lock; when it is held, the retirement
    /// is deferred until it is free (see <see cref="DisposeTreeUnderLock"/>).</item>
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
            private readonly ReadOptimizedLock.LockToken lockToken;

            internal ReadRangeIndexLock(ref readonly ReadOptimizedLock lockRef, ReadOptimizedLock.LockToken lockToken)
            {
                this.lockToken = lockToken;
                this.lockRef = ref lockRef;
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                if (Unsafe.IsNullRef(in lockRef))
                    return;

                lockRef.ReleaseLock(lockToken);
            }
        }

        /// <summary>
        /// RAII holder for an exclusive lock on a RangeIndex key.
        /// Disposing releases the exclusive lock.
        /// </summary>
        internal readonly ref struct ExclusiveRangeIndexLock : IDisposable
        {
            private readonly ref readonly ReadOptimizedLock lockRef;
            private readonly ReadOptimizedLock.LockToken lockToken;

            internal ExclusiveRangeIndexLock(ref readonly ReadOptimizedLock lockRef, ReadOptimizedLock.LockToken lockToken)
            {
                this.lockToken = lockToken;
                this.lockRef = ref lockRef;
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                if (Unsafe.IsNullRef(in lockRef))
                    return;

                lockRef.ReleaseLock(lockToken);
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
        /// (IsFlushed set), promotes the stub to the tail via RMW and retries.</item>
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
                OnSharedLockAcquired();

                readRes = session.Read_RangeIndex(key.ReadOnlySpan, ref input, ref output, ref session.stringBasicContext);
            }
            catch
            {
                rangeIndexLocks.ReleaseLock(sharedLockToken);
                throw;
            }

            if (readRes != GarnetStatus.OK)
            {
                status = readRes;
                rangeIndexLocks.ReleaseLock(sharedLockToken);
                return default;
            }

            var outputSpan = output.SpanByteAndMemory.IsSpanByte
                ? output.SpanByteAndMemory.SpanByte.ReadOnlySpan
                : output.SpanByteAndMemory.MemorySpan;

            if (outputSpan.Length != IndexSizeBytes)
            {
                rangeIndexLocks.ReleaseLock(sharedLockToken);
                throw new GarnetException($"Unexpected stub size {outputSpan.Length} for RangeIndex read, expected {IndexSizeBytes}");
            }

            ref readonly var stub = ref ReadIndex(outputSpan);

            // Per-tree checkpoint barrier: one volatile read on hot path (no checkpoint = skipped).
            if (checkpointInProgress
                && WaitForTreeCheckpoint(key.ReadOnlySpan, ref output, indexSpan, ref sharedLockToken))
            {
                goto Retry;
            }

            if (stub.IsFlushed)
            {
                var treeEvicted = stub.TreeHandle == nint.Zero;
                rangeIndexLocks.ReleaseLock(sharedLockToken);

                // An evicted predecessor's tree may still be retiring; promotion stages the working file it uses.
                WaitForRetirement(key.ReadOnlySpan);

                // Promotion stages the working file from the stub's flush snapshot, which may not be taken yet.
                if (treeEvicted)
                    WaitForFlushSnapshot(key.ReadOnlySpan);

                PromoteToTail(session, key, keyHash, ref input, indexSpan, treeEvicted);
                goto Retry;
            }

            if (stub.TreeHandle == nint.Zero)
            {
                rangeIndexLocks.ReleaseLock(sharedLockToken);

                // A retiring tree for this key must be gone before restore, which would otherwise reuse it.
                WaitForRetirement(key.ReadOnlySpan);

                // Restore under exclusive lock to prevent concurrent restores.
                // Pre-staging of data.bftree always happened earlier (by the promote of a flushed
                // stub whose tree was evicted, or OnRecoverySnapshotRead) so RestoreTree just opens
                // data.bftree directly.
                if (!RestoreTree(session, key, keyHash, ref input, indexSpan))
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
        /// Test hook: pauses, or fails, a data operation that holds the shared lock, before it reads the stub.
        /// The caller releases the lock if this fails the operation.
        /// </summary>
        [Conditional("DEBUG")]
        private static void OnSharedLockAcquired()
        {
            ExceptionInjectionHelper.ResetAndWait(ExceptionInjectionType.RangeIndex_Pause_After_Shared_Lock);

            if (ExceptionInjectionHelper.TriggerCondition(ExceptionInjectionType.RangeIndex_Fail_After_Shared_Lock))
                throw new GarnetException($"Exception injection triggered {ExceptionInjectionType.RangeIndex_Fail_After_Shared_Lock}");
        }

        /// <summary>
        /// Test hook: signals that an operation is about to take the exclusive lock to stage a key's working file.
        /// </summary>
        [Conditional("DEBUG")]
        private static void OnBeforeStagingLock()
            => _ = ExceptionInjectionHelper.TriggerCondition(ExceptionInjectionType.RangeIndex_Before_Staging_Lock);

        /// <summary>
        /// Test hook: pauses a promote of a flushed stub after it stages the key's working file, before the RMW that copies the
        /// stub to the tail. The promote holds no lock here.
        /// </summary>
        [Conditional("DEBUG")]
        private static void OnBeforePromote()
            => ExceptionInjectionHelper.ResetAndWait(ExceptionInjectionType.RangeIndex_Pause_Before_Promote);

        /// <summary>
        /// Test hook: pauses a restore of a key's tree that holds the key's exclusive lock, before it reads the stub.
        /// </summary>
        [Conditional("DEBUG")]
        private static void OnRestoreLockAcquired()
            => ExceptionInjectionHelper.ResetAndWait(ExceptionInjectionType.RangeIndex_Pause_Restoring_Tree);

        /// <summary>
        /// Issue an RMW with RIPROMOTE to copy a flushed stub from the read-only region to the mutable region, so that data
        /// operations can use its tree again. Multiple threads may race here — only one wins the CAS; others see the new
        /// record.
        /// </summary>
        /// <remarks>
        /// A live tree moves with the stub. The tree of a stub whose tree was evicted is only in the stub's flush snapshot,
        /// and is staged into the key's working file first (<see cref="StageEvictedTree"/>). That staging takes the exclusive
        /// lock, so it is done here rather than in the RMW's callbacks, which run under the stub's bucket latch while a data
        /// operation holding the lock shared may be waiting for that latch.
        /// </remarks>
        private void PromoteToTail(StorageSession session, PinnedSpanByte key, long keyHash, ref StringInput input, Span<byte> indexSpan, bool treeEvicted)
        {
            var stagedFrom = LogAddress.kInvalidAddress;
            if (treeEvicted && !StageEvictedTree(session, key, keyHash, ref input, indexSpan, out stagedFrom))
                return;

            OnBeforePromote();
            session.PromoteRangeIndexToTail(key, stagedFrom);
        }

        /// <summary>
        /// Stage the key's working file from the flush snapshot of its stub, if that stub is still flushed with its tree
        /// evicted. RIPROMOTE then promotes such a stub only from the record staged from (<see cref="CanPromote"/>), so the
        /// working file holds the tree of the stub it promotes, and registers the key's pending entry once the copy is in
        /// the log (<see cref="RegisterPending"/>).
        /// </summary>
        /// <param name="session">The storage session for store reads.</param>
        /// <param name="key">The RangeIndex key.</param>
        /// <param name="keyHash">The key hash for lock striping.</param>
        /// <param name="input">The StringInput for the read.</param>
        /// <param name="indexSpan">Span to receive the stub bytes.</param>
        /// <param name="stagedFrom">Address of the record staged from, or <see cref="LogAddress.kInvalidAddress"/>.</param>
        /// <returns>False if there is nothing to promote now: the stub is no longer flushed, or its flush snapshot, which a
        /// flush deferred while the lock was held, is not taken yet. The caller retries.</returns>
        private bool StageEvictedTree(StorageSession session, PinnedSpanByte key, long keyHash, ref StringInput input, Span<byte> indexSpan,
            out long stagedFrom)
        {
            stagedFrom = LogAddress.kInvalidAddress;

            OnBeforeStagingLock();
            rangeIndexLocks.AcquireExclusiveLock(keyHash, out var exclusiveLockToken);
            try
            {
                // Re-read under the lock: another promote may have staged and promoted the stub meanwhile.
                var output = StringOutput.FromPinnedSpan(indexSpan);
                if (session.Read_RangeIndex(key.ReadOnlySpan, ref input, ref output, ref session.stringBasicContext, out var address) != GarnetStatus.OK)
                    return false;

                var outputSpan = output.SpanByteAndMemory.IsSpanByte
                    ? output.SpanByteAndMemory.SpanByte.ReadOnlySpan
                    : output.SpanByteAndMemory.MemorySpan;
                if (outputSpan.Length != IndexSizeBytes)
                    return false;

                ref readonly var stub = ref ReadIndex(outputSpan);
                if (!stub.IsFlushed)
                    return false;

                if (stub.TreeHandle == nint.Zero)
                {
                    // The snapshot's task waits for this lock; the caller waits for the task without it.
                    if (IsFlushSnapshotPending(KeyId(key.ReadOnlySpan)))
                        return false;

                    // A stub read from the read cache has no address to name a flush snapshot by; it is promoted unstaged.
                    if (address != LogAddress.kInvalidAddress)
                    {
                        StageWorkingFileLocked(key.ReadOnlySpan, address);
                        stagedFrom = address;
                    }
                }

                return true;
            }
            finally
            {
                rangeIndexLocks.ReleaseLock(exclusiveLockToken);
            }
        }

        /// <summary>
        /// True if RIPROMOTE may copy the stub in <paramref name="valueSpan"/>, at <paramref name="address"/>, to the tail:
        /// it is not flushed, or its tree is live and moves with it, or its tree was evicted and the key's working file was
        /// staged from this record's flush snapshot (<paramref name="stagedFrom"/>).
        /// </summary>
        internal static bool CanPromote(ReadOnlySpan<byte> valueSpan, long address, long stagedFrom)
        {
            ref readonly var stub = ref ReadIndex(valueSpan);
            return !stub.IsFlushed || stub.TreeHandle != nint.Zero || address == stagedFrom;
        }

        /// <summary>
        /// Cold path: checks if this key's tree (activated or pending) is currently being snapshotted
        /// for a checkpoint. If so, releases the shared lock, spin-waits for the snapshot to complete,
        /// and signals the caller to retry the entire read operation.
        ///
        /// <para>Lookup is by <see cref="KeyId"/> (XxHash128 → Guid) so it works uniformly for activated
        /// (TreeHandle != 0) and pending (TreeHandle == 0) entries. Hot-path Guid derivation is gated
        /// by the <see cref="checkpointInProgress"/> short-circuit so steady-state cost is one volatile
        /// bool read.</para>
        /// </summary>
        /// <param name="key">The raw key bytes.</param>
        /// <param name="output">The StringOutput from the read (passed for lifetime management).</param>
        /// <param name="indexSpan">The stub span (passed for lifetime management).</param>
        /// <param name="sharedLockToken">The shared lock token; released before waiting.</param>
        /// <returns><c>true</c> if caller should retry the read; <c>false</c> if no wait was needed.</returns>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private bool WaitForTreeCheckpoint(ReadOnlySpan<byte> key, ref StringOutput output,
            Span<byte> indexSpan, ref ReadOptimizedLock.LockToken sharedLockToken)
        {
            var keyId = KeyId(key);
            if (!liveIndexes.TryGetValue(keyId, out var treeEntry)
                || Volatile.Read(ref treeEntry.SnapshotPending) == 0)
                return false;

            rangeIndexLocks.ReleaseLock(sharedLockToken);
            while (Volatile.Read(ref treeEntry.SnapshotPending) != 0)
                Thread.Yield();
            return true;
        }

        /// <summary>
        /// Restore a BfTree from its pre-staged CPR snapshot (<c>data.bftree</c>) and publish
        /// the resulting native pointer into the stub via a RIRESTORE RMW.
        /// </summary>
        /// <remarks>
        /// <para><b>Split design</b>: the per-key X-lock is held ONLY for the recovery + register step, then RELEASED
        /// before issuing the RMW, so that it is not held across a store operation, whose record callbacks and epoch
        /// drains never wait for it (see <see cref="SnapshotTreeForFlush"/> and <see cref="DisposeTreeUnderLock"/>) and
        /// so would defer their work until the RMW completes.</para>
        ///
        /// <para>Race between releasing X and issuing RIRESTORE RMW: a concurrent DEL could
        /// fire, taking the X-lock, removing our entry from <see cref="liveIndexes"/>, and
        /// queuing the bfTree for deferred disposal via <c>storeEpoch.BumpCurrentEpoch</c>. The
        /// deferred dispose CANNOT execute until our thread's epoch advances — i.e., until the
        /// RMW's Tsavorite session-suspend point. By that time the RMW has either:
        /// <list type="bullet">
        /// <item>Succeeded — our nativePtr is now in the stub bytes; subsequent readers see
        /// it and route through ReadRangeIndex's hot path which acquires the shared RI lock
        /// before calling the native handle. The DEL's deferred dispose fires only after all
        /// such readers complete (epoch barrier).</item>
        /// <item>Returned NOTFOUND on a tombstoned key (RIRESTORE.NeedInitialUpdate=false).
        /// Caller treats this as "key was deleted concurrently" → no re-restore loop.</item>
        /// </list></para>
        ///
        /// <para>Pre-staging of <c>data.bftree</c> always happened earlier:
        /// <list type="bullet">
        /// <item><see cref="StageEvictedTree"/>, before the promote of a flushed stub whose tree was evicted
        /// (post-eviction, post-recovery, or copied to the tail by compaction or CopyReadsToTail).</item>
        /// <item><c>OnRecoverySnapshotRead</c> (above-FUA-at-checkpoint stubs DURING recovery, since
        /// the checkpoint snapshot file may be deleted post-recovery).</item>
        /// </list></para>
        /// </remarks>
        private bool RestoreTree(
            StorageSession session,
            PinnedSpanByte key,
            long keyHash,
            ref StringInput input,
            Span<byte> indexSpan)
        {
            nint nativePtr;
            rangeIndexLocks.AcquireExclusiveLock(keyHash, out var exclusiveLockToken);
            try
            {
                OnRestoreLockAcquired();

                // Re-read stub under exclusive lock to check if another thread already restored.
                var output = StringOutput.FromPinnedSpan(indexSpan);
                var readRes = session.Read_RangeIndex(key.ReadOnlySpan, ref input, ref output, ref session.stringBasicContext);
                if (readRes != GarnetStatus.OK)
                    return false;

                var outputSpan = output.SpanByteAndMemory.IsSpanByte
                    ? output.SpanByteAndMemory.SpanByte.ReadOnlySpan
                    : output.SpanByteAndMemory.MemorySpan;
                if (outputSpan.Length < IndexSizeBytes)
                    return false;

                ref readonly var stub = ref ReadIndex(outputSpan);
                if (stub.TreeHandle != nint.Zero)
                    return true; // Another thread already restored

                var keySpan = key.ReadOnlySpan;
                var keyId = KeyId(keySpan);

                // A retirement of this key's tree deferred while its lock was held may remove or dispose any tree this
                // would reuse or register. The caller waits for it, without the lock, and retries.
                if (IsRetirementPending(keyId))
                    return true;

                // Race-resolved path: another stub for this key may already have a live tree
                // (RestoreTree ran via a different addr). Reuse it instead of opening another.
                if (liveIndexes.TryGetValue(keyId, out var existing) && existing?.Tree != null)
                {
                    nativePtr = existing.Tree.NativePtr;
                }
                else
                {
                    var hashPrefix = HashKeyToPrefix(keySpan);
                    var workingPath = LogDataPath(hashPrefix);

                    if (!File.Exists(workingPath))
                    {
                        // Should not happen in normal flow — pre-staging guarantees data.bftree
                        // exists for any stub above FUA whose TreeHandle is 0. Assert in Debug;
                        // LogWarning + return false in Release so the affected key surfaces
                        // NOTFOUND rather than crashing the process.
                        Debug.Assert(false, $"RestoreTree: data.bftree missing for {hashPrefix} — pre-stage invariant violated");
                        logger?.LogWarning("RestoreTree: data.bftree missing for {Hash} — pre-stage invariant violated", hashPrefix);
                        return false;
                    }

                    var bfTree = BfTreeService.RecoverFromCprSnapshot(
                        workingPath,
                        enableSnapshots: true,
                        (StorageBackendType)stub.StorageBackend);

                    RegisterIndex(bfTree, keyHash, keySpan);

                    // A retirement deferred meanwhile owns whatever is registered now; retry once it completes.
                    if (IsRetirementPending(keyId))
                        return true;

                    // Re-look-up to use the WINNER's pointer (handles concurrent restorer race).
                    if (!liveIndexes.TryGetValue(keyId, out var winner) || winner?.Tree == null)
                    {
                        // We disposed our duplicate via RegisterIndex's loser-disposal path AND
                        // someone removed the winner before we observed it — extremely rare race
                        // (concurrent DEL between RegisterIndex and TryGetValue). Bail out; the
                        // next reader will retry.
                        return false;
                    }
                    nativePtr = winner.Tree.NativePtr;
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Failed to restore BfTree from data.bftree");
                return false;
            }
            finally
            {
                rangeIndexLocks.ReleaseLock(exclusiveLockToken);
            }

            // RIRESTORE RMW WITHOUT the X-lock. Safe because OnFlush no longer takes any RI
            // X-lock from any code path that may fire as a deferred epoch action.
            // RIRESTORE.NeedInitialUpdate=false → if the key was concurrently DEL'd (tombstone),
            // the RMW returns NOTFOUND and the caller does not loop.
            try
            {
                session.RestoreRangeIndexStub(key, nativePtr);
                return true;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "RestoreRangeIndexStub RMW failed");
                return false;
            }
        }
    }
}