// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server.BfTreeInterop;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Deferred retirement of BfTrees whose stub is deleted or evicted.
    ///
    /// <para>The record triggers that retire a tree run under the record's bucket latch (delete) or in an epoch drain
    /// (eviction), while a data operation holding the per-key lock shared may be waiting for that latch or be the thread
    /// running the drain. So when the lock is held, <see cref="DisposeTreeUnderLock"/> hands the retirement to a task
    /// that takes the lock once those operations release it. A retirement, deferred or not, is pending until its tree is
    /// disposed and the key's working file deleted, which waits for the store epoch. Meanwhile operations that would
    /// register, restore or recreate a tree for the key wait for it (<see cref="WaitForRetirement"/>), so they cannot
    /// reuse the retiring tree, have their registration removed by the retirement, or have the key's working file
    /// deleted under them.</para>
    /// </summary>
    public sealed partial class RangeIndexManager
    {
        /// <summary>
        /// A retirement handed to <see cref="RunRetirementTaskAsync"/>, requested at <paramref name="requestedAt"/> (see
        /// <see cref="RetireLocked"/>).
        /// </summary>
        private sealed class Retirement(byte[] key, long keyHash, Guid keyId, nint treeHandle, bool deleteFiles, long requestedAt)
        {
            internal byte[] Key { get; } = key;
            internal long KeyHash { get; } = keyHash;
            internal Guid KeyId { get; } = keyId;
            internal nint TreeHandle { get; } = treeHandle;
            internal bool DeleteFiles { get; } = deleteFiles;
            internal long RequestedAt { get; } = requestedAt;

            private int completed;

            /// <summary>
            /// True the first time it is called, so that the retirement is counted complete once.
            /// </summary>
            internal bool MarkCompleted() => Interlocked.Exchange(ref completed, 1) == 0;
        }

        private readonly Channel<Retirement> retirements =
            Channel.CreateUnbounded<Retirement>(new() { SingleReader = true, SingleWriter = false, AllowSynchronousContinuations = false });

        /// <summary>
        /// Number of incomplete retirements for each key, by <see cref="KeyId"/>.
        /// </summary>
        private readonly ConcurrentDictionary<Guid, int> pendingRetirements = new();

        /// <summary>
        /// Total of <see cref="pendingRetirements"/>, so that the usual case of none skips hashing the key.
        /// </summary>
        private int pendingRetirementCount;

        private readonly Task retirementTask;

        /// <summary>
        /// Hand the retirement of the key's tree to <see cref="RunRetirementTaskAsync"/>, because the key's lock is held.
        /// </summary>
        private void DeferRetirement(ReadOnlySpan<byte> key, long keyHash, nint treeHandle, bool deleteFiles, long requestedAt)
        {
            var keyId = KeyId(key);

            AddPendingRetirement(keyId);

            if (!retirements.Writer.TryWrite(new Retirement(key.ToArray(), keyHash, keyId, treeHandle, deleteFiles, requestedAt)))
            {
                // Only once disposed, which disposes every remaining tree itself.
                CompleteRetirement(keyId);
            }
        }

        /// <summary>
        /// Count a retirement of the key's tree pending until <see cref="CompleteRetirement(Guid)"/>.
        /// </summary>
        private void AddPendingRetirement(Guid keyId)
        {
            _ = Interlocked.Increment(ref pendingRetirementCount);
            _ = pendingRetirements.AddOrUpdate(keyId, 1, static (_, count) => count + 1);
        }

        /// <summary>
        /// Retires trees handed over by <see cref="DeferRetirement"/>, each once its key's lock is free.
        /// </summary>
        private async Task RunRetirementTaskAsync()
        {
            var reader = retirements.Reader;
            while (await reader.WaitToReadAsync().ConfigureAwait(false))
            {
                while (reader.TryRead(out var retirement))
                {
                    try
                    {
                        Retire(retirement);
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "Deferred RangeIndex tree retirement failed");
                        CompleteRetirement(retirement);
                        Debug.Fail($"Deferred RangeIndex tree retirement failed: {ex}");
                    }
                }
            }
        }

        private void Retire(Retirement retirement)
        {
            BfTreeService disposedTree;
            string hashPrefix;

            rangeIndexLocks.AcquireExclusiveLock(retirement.KeyHash, out var lockToken);
            try
            {
                RetireLocked(retirement.Key, retirement.KeyId, retirement.TreeHandle, retirement.DeleteFiles, retirement.RequestedAt,
                    out disposedTree, out hashPrefix);
            }
            finally
            {
                rangeIndexLocks.ReleaseLock(lockToken);
            }

            // Complete only once the tree is disposed and the working file deleted, so that a recreated key's tree
            // cannot open the working file before it is deleted.
            DisposeAndDeleteFilesAfterReaders(disposedTree, hashPrefix, () => CompleteRetirement(retirement));
        }

        /// <summary>
        /// Count a retirement handed to <see cref="RunRetirementTaskAsync"/> complete. Both its epoch action and the
        /// failure path may report it, and only the first counts.
        /// </summary>
        private void CompleteRetirement(Retirement retirement)
        {
            if (retirement.MarkCompleted())
                CompleteRetirement(retirement.KeyId);
        }

        private void CompleteRetirement(Guid keyId)
        {
            while (true)
            {
                if (!pendingRetirements.TryGetValue(keyId, out var count))
                {
                    Debug.Fail("Completed a RangeIndex retirement that was not pending");
                    return;
                }

                var updated = count == 1
                    ? pendingRetirements.TryRemove(KeyValuePair.Create(keyId, count))
                    : pendingRetirements.TryUpdate(keyId, count - 1, count);

                if (updated)
                    break;
            }

            _ = Interlocked.Decrement(ref pendingRetirementCount);
        }

        /// <summary>
        /// True if a retirement of the key's tree is pending.
        /// </summary>
        private bool IsRetirementPending(Guid keyId)
            => Volatile.Read(ref pendingRetirementCount) != 0 && pendingRetirements.ContainsKey(keyId);

        /// <summary>
        /// Wait until no retirement of the key's tree is pending. The retirement waits for the key's lock and for the
        /// store epoch to advance, so this must not be called while holding a RangeIndex lock or epoch protection.
        /// </summary>
        internal void WaitForRetirement(ReadOnlySpan<byte> key)
        {
            if (Volatile.Read(ref pendingRetirementCount) == 0)
                return;

            var keyId = KeyId(key);
            if (!pendingRetirements.ContainsKey(keyId))
                return;

            OnWaitForRetirement();

            while (pendingRetirements.ContainsKey(keyId))
                _ = Thread.Yield();
        }

        /// <summary>
        /// Test hook: signals that an operation waits for a pending retirement of its key's tree.
        /// </summary>
        [Conditional("DEBUG")]
        private static void OnWaitForRetirement()
            => _ = ExceptionInjectionHelper.TriggerCondition(ExceptionInjectionType.RangeIndex_Waits_For_Retirement);

        /// <summary>
        /// Stop accepting deferred retirements, and retire the ones already handed over.
        /// </summary>
        private void StopRetirementTask()
        {
            _ = retirements.Writer.TryComplete();

            if (retirementTask != null)
                AsyncUtils.BlockingWait(retirementTask);
        }
    }
}