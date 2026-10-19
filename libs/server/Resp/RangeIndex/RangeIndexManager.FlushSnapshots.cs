// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Deferred flush snapshots of stubs whose tree is not restored.
    ///
    /// <para>Such a stub's tree is in the key's working file, which a restore opens and data operations then write, so its
    /// flush snapshot is copied from that file holding the key's lock shared, which excludes a restore.
    /// <see cref="GarnetRecordTriggers.OnFlush"/> runs in an epoch drain, possibly on a thread holding a bucket latch, and the
    /// lock's holder may be waiting for either, so the flush never waits for the lock: when the lock is held, the stub is
    /// marked flushed and its snapshot is handed to a task that takes it once the lock is free, and publishes it under its
    /// name only once complete. Until then, a promote of the stub, which stages the working file from that snapshot, waits
    /// for it (<see cref="WaitForFlushSnapshot"/>), a copy of the stub to the tail goes without it
    /// (<see cref="CopyFlushSnapshot"/>), and a checkpoint waits for all of them before it writes its metadata
    /// (<see cref="WaitForFlushSnapshots"/>), so that every flushed stub it covers has its snapshot, including the stubs a
    /// fold-over checkpoint flushes itself.</para>
    ///
    /// <para>Waiters block on each pending snapshot rather than poll the count: a snapshot waits for a RangeIndex lock, which
    /// a restore or staging holds for as long as it reads or copies a tree's file.</para>
    /// </summary>
    public sealed partial class RangeIndexManager
    {
        /// <summary>
        /// A flush snapshot handed to <see cref="RunFlushSnapshotTaskAsync"/>.
        /// </summary>
        private sealed class FlushSnapshot(byte[] key, long keyHash, Guid keyId, string hashPrefix, long logicalAddress)
        {
            internal byte[] Key { get; } = key;
            internal long KeyHash { get; } = keyHash;
            internal Guid KeyId { get; } = keyId;
            internal string HashPrefix { get; } = hashPrefix;
            internal long LogicalAddress { get; } = logicalAddress;

            private readonly TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);

            /// <summary>
            /// Releases the waiters of a snapshot that is taken, or abandoned.
            /// </summary>
            internal void MarkDone() => done.TrySetResult();

            /// <summary>
            /// Blocks until the snapshot is taken or abandoned. A blocking task wait spins briefly before it blocks, which
            /// suits a wait that is usually short but lasts as long as the key's lock is held.
            /// </summary>
            internal void WaitUntilDone() => AsyncUtils.BlockingWait(done.Task);
        }

        private readonly Channel<FlushSnapshot> flushSnapshots =
            Channel.CreateUnbounded<FlushSnapshot>(new() { SingleReader = true, SingleWriter = false, AllowSynchronousContinuations = false });

        /// <summary>
        /// Flush snapshots handed to <see cref="RunFlushSnapshotTaskAsync"/> and not yet taken.
        /// </summary>
        private readonly ConcurrentDictionary<FlushSnapshot, byte> pendingFlushSnapshots = new();

        /// <summary>
        /// Number of <see cref="pendingFlushSnapshots"/>, so that the usual case of none skips looking through them.
        /// </summary>
        private int pendingFlushSnapshotCount;

        private readonly Task flushSnapshotTask;

        /// <summary>
        /// Number of flush snapshots handed to <see cref="RunFlushSnapshotTaskAsync"/> and not yet taken.
        /// </summary>
        internal int PendingFlushSnapshotCount => Volatile.Read(ref pendingFlushSnapshotCount);

        /// <summary>
        /// Hand the flush snapshot of a stub whose tree is not restored to <see cref="RunFlushSnapshotTaskAsync"/>, because
        /// the key's lock is held. Called before the stub is marked flushed, so that whoever sees it flushed also sees its
        /// snapshot pending.
        /// </summary>
        private void DeferFlushSnapshot(ReadOnlySpan<byte> key, long keyHash, Guid keyId, string hashPrefix, long logicalAddress)
        {
            var snapshot = new FlushSnapshot(key.ToArray(), keyHash, keyId, hashPrefix, logicalAddress);
            _ = pendingFlushSnapshots.TryAdd(snapshot, 0);
            _ = Interlocked.Increment(ref pendingFlushSnapshotCount);

            if (!flushSnapshots.Writer.TryWrite(snapshot))
            {
                // Only once disposed
                CompleteFlushSnapshot(snapshot);
            }
        }

        /// <summary>
        /// Takes flush snapshots handed over by <see cref="DeferFlushSnapshot"/>, each once its key's lock is free.
        /// </summary>
        private async Task RunFlushSnapshotTaskAsync()
        {
            var reader = flushSnapshots.Reader;
            while (await reader.WaitToReadAsync().ConfigureAwait(false))
            {
                while (reader.TryRead(out var snapshot))
                {
                    try
                    {
                        TakeFlushSnapshot(snapshot);
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "Deferred flush snapshot of RangeIndex {Hash} at 0x{Addr:x16} failed",
                            snapshot.HashPrefix, snapshot.LogicalAddress);
                    }
                    finally
                    {
                        CompleteFlushSnapshot(snapshot);
                    }
                }
            }
        }

        /// <summary>
        /// Stop counting a flush snapshot as pending, then release its waiters, which may check that count again.
        /// </summary>
        private void CompleteFlushSnapshot(FlushSnapshot snapshot)
        {
            if (pendingFlushSnapshots.TryRemove(snapshot, out _))
                _ = Interlocked.Decrement(ref pendingFlushSnapshotCount);

            snapshot.MarkDone();
        }

        private void TakeFlushSnapshot(FlushSnapshot snapshot)
        {
            // Taken to a temporary file and renamed once complete, because a copy of the flushed stub to the tail copies its
            // snapshot without the lock (CopyFlushSnapshot).
            var flushPath = LogFlushPath(snapshot.HashPrefix, snapshot.LogicalAddress);
            var tempPath = flushPath + ".tmp";

            try
            {
                File.Delete(tempPath);

                bool taken;
                rangeIndexLocks.AcquireSharedLock(snapshot.KeyHash, out var lockToken);
                try
                {
                    taken = TrySnapshotColdTreeLocked(snapshot.Key, snapshot.HashPrefix, tempPath);
                }
                finally
                {
                    rangeIndexLocks.ReleaseLock(lockToken);
                }

                if (!taken)
                {
                    logger?.LogError("Deferred flush snapshot: TreeHandle=0 but data.bftree missing for {Hash} at addr 0x{Addr:x16}; " +
                        "the stub is flushed without a snapshot, so a promote of it will find none and the key will return NOTFOUND",
                        snapshot.HashPrefix, snapshot.LogicalAddress);
                    return;
                }

                File.Move(tempPath, flushPath, overwrite: true);
            }
            catch
            {
                try { File.Delete(tempPath); }
                catch (Exception ex) { logger?.LogWarning(ex, "Failed to delete {Path}", tempPath); }
                throw;
            }
        }

        /// <summary>
        /// Take the flush snapshot of a stub whose tree is not restored, holding the key's lock shared: of the key's tree if a
        /// restore under another stub of the key has made it live, and otherwise a copy of the key's working file, which holds
        /// the stub's tree while no restore has opened it.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="hashPrefix">The key's hash prefix.</param>
        /// <param name="destinationPath">Where to write the snapshot; it must not exist.</param>
        /// <returns>False if there is neither a live tree nor a working file to snapshot.</returns>
        private bool TrySnapshotColdTreeLocked(ReadOnlySpan<byte> key, string hashPrefix, string destinationPath)
        {
            if (liveIndexes.TryGetValue(KeyId(key), out var entry) && entry?.Tree != null)
            {
                entry.SnapshotUnderClaim(destinationPath);
                return true;
            }

            var dataPath = LogDataPath(hashPrefix);
            if (!File.Exists(dataPath))
                return false;

            File.Copy(dataPath, destinationPath, overwrite: false);
            return true;
        }

        /// <summary>
        /// True if a flush snapshot of one of the key's stubs is not taken yet.
        /// </summary>
        private bool IsFlushSnapshotPending(Guid keyId)
        {
            if (Volatile.Read(ref pendingFlushSnapshotCount) == 0)
                return false;

            foreach (var pending in pendingFlushSnapshots)
            {
                if (pending.Key.KeyId == keyId)
                    return true;
            }

            return false;
        }

        /// <summary>
        /// Wait until the flush snapshots of the key's stubs that are pending now are taken. Their task waits for the key's
        /// lock, so this must not be called while holding a RangeIndex lock or epoch protection.
        /// </summary>
        internal void WaitForFlushSnapshot(ReadOnlySpan<byte> key)
        {
            if (Volatile.Read(ref pendingFlushSnapshotCount) == 0)
                return;

            var keyId = KeyId(key);
            foreach (var pending in pendingFlushSnapshots)
            {
                if (pending.Key.KeyId == keyId)
                    pending.Key.WaitUntilDone();
            }
        }

        /// <summary>
        /// Wait until every flush snapshot that is pending now is taken. Called by a checkpoint before it writes its metadata
        /// (<see cref="Tsavorite.core.CheckpointTrigger.FlushCompleted"/>), when the flushes of every page it covers have
        /// handed over their snapshots. It looks through the pending snapshots once, so it may also wait for some deferred
        /// while it looks, which belong to later flushes, but a steady stream of them cannot hold up the checkpoint. Their task
        /// waits for the keys' locks, so this must not be called while holding a RangeIndex lock or epoch protection; the
        /// checkpoint state machine holds neither.
        /// </summary>
        internal void WaitForFlushSnapshots()
        {
            if (Volatile.Read(ref pendingFlushSnapshotCount) == 0)
                return;

            OnCheckpointWaitsForFlushSnapshots();

            foreach (var pending in pendingFlushSnapshots)
                pending.Key.WaitUntilDone();
        }

        /// <summary>
        /// Test hook: signals that a checkpoint is about to wait for pending flush snapshots.
        /// </summary>
        [Conditional("DEBUG")]
        private static void OnCheckpointWaitsForFlushSnapshots()
            => _ = ExceptionInjectionHelper.TriggerCondition(ExceptionInjectionType.RangeIndex_Checkpoint_Waits_For_Flush_Snapshots);

        /// <summary>
        /// Stop accepting deferred flush snapshots, and take the ones already handed over.
        /// </summary>
        private void StopFlushSnapshotTask()
        {
            _ = flushSnapshots.Writer.TryComplete();

            if (flushSnapshotTask != null)
                AsyncUtils.BlockingWait(flushSnapshotTask);
        }
    }
}