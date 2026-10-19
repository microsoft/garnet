// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Threading;

namespace Garnet.server
{
    /// <summary>
    /// Sessions parked until the DiskANN index behind a Vector Set has been dropped, woken by the task that drops it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// One per <see cref="VectorManager"/>. A command that has to recreate an index whose drop is still pending cannot run
    /// until the drop is done; a session that can park lists itself here instead of waiting on its thread (see
    /// <see cref="RespServerSession.ParkUntilIndexDropped"/>).
    /// </para>
    /// <para>
    /// A waiter is listed before it checks whether the drop is still pending, and the drop task wakes waiters after it
    /// has taken the drop off the pending set. Both are ordered by this signal's lock and the pending set's own
    /// synchronization, so either the waiter sees the drop done, or the drop task sees the waiter listed.
    /// </para>
    /// </remarks>
    internal sealed class IndexDropSignal
    {
        readonly Lock gate = new();
        readonly List<IndexDropWait> waiters = [];

        /// <summary>
        /// Lists a waiter. Called before the waiter checks whether the drop it waits on is still pending.
        /// </summary>
        /// <param name="wait">The waiter, not listed.</param>
        internal void Add(IndexDropWait wait)
        {
            lock (gate)
            {
                wait.listIndex = waiters.Count;
                waiters.Add(wait);
            }
        }

        /// <summary>
        /// Takes a waiter off the list, if it is on it. Whoever takes a waiter off the list completes its wait.
        /// </summary>
        /// <param name="wait">The waiter.</param>
        /// <returns>True if the waiter was listed.</returns>
        internal bool TryRemove(IndexDropWait wait)
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
        /// Wakes the sessions waiting on the drop of the index of a key with hash <paramref name="keyHash"/>. Called once
        /// the drop is no longer pending.
        /// </summary>
        /// <param name="keyHash">Hash of the key whose index was dropped.</param>
        internal void OnDropped(long keyHash)
        {
            lock (gate)
            {
                // Backwards, so that the swap in RemoveAt moves only waiters already visited
                for (var i = waiters.Count - 1; i >= 0; i--)
                {
                    var wait = waiters[i];
                    if (wait.KeyHash == keyHash)
                    {
                        RemoveAt(i);
                        wait.Wake();
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
        }
    }
}