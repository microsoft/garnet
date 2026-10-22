// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;

namespace Garnet.server
{
    /// <summary>
    /// Sessions parked until another session lets go of the key locks they wait on, woken as a transaction unlocks its keys.
    /// </summary>
    /// <remarks>
    /// <para>
    /// One per store, shared by every session that locks keys transactionally, so that a transaction, script, stored
    /// procedure or replayed transaction unlocking its keys wakes the sessions parked on any of them. A latch taken and
    /// released inside a single operation is not announced here; the wait polls for those (see <see cref="LockReleaseWait"/>).
    /// </para>
    /// <para>
    /// A waiter is listed before it probes the latch it waits on, and an unlocker reads <see cref="HasWaiters"/> after it
    /// has released its latches. Listing increments the count with a full fence, and releasing a latch is interlocked, so
    /// either the waiter's probe sees the latch released, or the unlocker sees the waiter listed.
    /// </para>
    /// </remarks>
    internal sealed class LockReleaseSignal
    {
        readonly Lock gate = new();
        readonly List<LockReleaseWait> waiters = [];
        int waiterCount;

        /// <summary>
        /// Whether any session is parked on a lock. Read by an unlocker after it has released its latches.
        /// </summary>
        internal bool HasWaiters => Volatile.Read(ref waiterCount) != 0;

        /// <summary>
        /// Lists a waiter. Called before the waiter probes the latch it waits on.
        /// </summary>
        /// <param name="wait">The waiter, not listed.</param>
        internal void Add(LockReleaseWait wait)
        {
            lock (gate)
            {
                wait.listIndex = waiters.Count;
                waiters.Add(wait);
                _ = Interlocked.Increment(ref waiterCount);
            }
        }

        /// <summary>
        /// Takes a waiter off the list, if it is on it. Whoever takes a waiter off the list completes its wait.
        /// </summary>
        /// <param name="wait">The waiter.</param>
        /// <returns>True if the waiter was listed.</returns>
        internal bool TryRemove(LockReleaseWait wait)
        {
            lock (gate)
            {
                if (wait.listIndex < 0)
                    return false;

                RemoveAt(wait.listIndex);
                return true;
            }
        }

        /// <summary>
        /// Wakes the sessions waiting on a bucket that one of <paramref name="keys"/> falls in. Called after the keys are unlocked.
        /// </summary>
        /// <param name="keys">The keys just unlocked.</param>
        internal void OnKeysUnlocked(ReadOnlySpan<TxnKeyEntry> keys)
        {
            lock (gate)
            {
                // Backwards, so that the swap in RemoveAt moves only waiters already visited
                for (var i = waiters.Count - 1; i >= 0; i--)
                {
                    var wait = waiters[i];
                    foreach (ref readonly var key in keys)
                    {
                        if (wait.SharesBucketWith(key.keyHash))
                        {
                            RemoveAt(i);
                            wait.Wake();
                            break;
                        }
                    }
                }
            }
        }

        void RemoveAt(int index)
        {
            var last = waiters.Count - 1;
            var removed = waiters[index];
            if (index != last)
            {
                var moved = waiters[last];
                waiters[index] = moved;
                moved.listIndex = index;
            }
            waiters.RemoveAt(last);
            removed.listIndex = -1;
            _ = Interlocked.Decrement(ref waiterCount);
        }
    }
}