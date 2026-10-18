// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
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
    /// (<see cref="CopyFlushSnapshot"/>), and a checkpoint waits for all of them, so that every flushed stub it covers has
    /// its snapshot.</para>
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
        }

        private readonly Channel<FlushSnapshot> flushSnapshots =
            Channel.CreateUnbounded<FlushSnapshot>(new() { SingleReader = true, SingleWriter = false, AllowSynchronousContinuations = false });

        /// <summary>
        /// Number of flush snapshots not yet taken for each key, by <see cref="KeyId"/>.
        /// </summary>
        private readonly ConcurrentDictionary<Guid, int> pendingFlushSnapshots = new();

        /// <summary>
        /// Total of <see cref="pendingFlushSnapshots"/>, so that the usual case of none skips hashing the key.
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
            _ = Interlocked.Increment(ref pendingFlushSnapshotCount);
            _ = pendingFlushSnapshots.AddOrUpdate(keyId, 1, static (_, count) => count + 1);

            if (!flushSnapshots.Writer.TryWrite(new FlushSnapshot(key.ToArray(), keyHash, keyId, hashPrefix, logicalAddress)))
            {
                // Only once disposed
                DecrementPending(pendingFlushSnapshots, keyId, ref pendingFlushSnapshotCount);
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
                        DecrementPending(pendingFlushSnapshots, snapshot.KeyId, ref pendingFlushSnapshotCount);
                    }
                }
            }
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
            => Volatile.Read(ref pendingFlushSnapshotCount) != 0 && pendingFlushSnapshots.ContainsKey(keyId);

        /// <summary>
        /// Wait until the flush snapshots of the key's stubs are taken. Their task waits for the key's lock, so this must not be
        /// called while holding a RangeIndex lock or epoch protection.
        /// </summary>
        internal void WaitForFlushSnapshot(ReadOnlySpan<byte> key)
        {
            if (Volatile.Read(ref pendingFlushSnapshotCount) == 0)
                return;

            var keyId = KeyId(key);
            while (pendingFlushSnapshots.ContainsKey(keyId))
                _ = Thread.Yield();
        }

        /// <summary>
        /// Wait until no flush snapshot is pending. Their task waits for the keys' locks, so this must not be called while
        /// holding a RangeIndex lock or epoch protection.
        /// </summary>
        private void WaitForFlushSnapshots()
        {
            while (Volatile.Read(ref pendingFlushSnapshotCount) != 0)
                _ = Thread.Yield();
        }

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