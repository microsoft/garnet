// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Garnet.server
{
    /// <summary>
    /// Server-wide rendezvous between sessions parked on a name and sessions that release them. Stands in for
    /// the per-key waiter lists a real blocking command keeps, and exists so <c>DEBUG BLOCKON</c> /
    /// <c>DEBUG SIGNAL</c> can exercise cross-session wakeup without a data structure getting in the way.
    /// </summary>
    internal sealed class SessionSignalRegistry
    {
        readonly ConcurrentDictionary<string, List<SessionSignal>> waiters = new();

        /// <summary>
        /// Arms a session's signal and parks it under a name.
        /// </summary>
        /// <remarks>
        /// Arming happens here, before the signal is reachable by any signaller, so the order cannot be got
        /// wrong at a call site. A signal published before it is armed is dropped by the first signaller that
        /// finds it: the waiter is taken off the list and never woken.
        /// </remarks>
        /// <param name="name">Name to wait on.</param>
        /// <param name="signal">The waiting session's signal.</param>
        internal void Register(string name, SessionSignal signal)
        {
            signal.Arm();

            var list = waiters.GetOrAdd(name, static _ => new List<SessionSignal>());
            lock (list)
                list.Add(signal);
        }

        /// <summary>
        /// Removes a signal from a name, for a session that stopped waiting on its own.
        /// </summary>
        /// <param name="name">Name it was waiting on.</param>
        /// <param name="signal">The signal to remove.</param>
        internal void Unregister(string name, SessionSignal signal)
        {
            if (!waiters.TryGetValue(name, out var list))
                return;

            lock (list)
                _ = list.Remove(signal);
        }

        /// <summary>
        /// Wakes every session parked on a name, inline on the calling thread.
        /// </summary>
        /// <param name="name">Name to release.</param>
        /// <returns>Number of sessions woken.</returns>
        internal int SignalAll(string name)
        {
            if (!waiters.TryGetValue(name, out var list))
                return 0;

            SessionSignal[] pending;
            lock (list)
            {
                if (list.Count == 0)
                    return 0;

                pending = [.. list];
                list.Clear();
            }

            // Signalled outside the lock: a woken session may come straight back to wait on this same name.
            var woken = 0;
            foreach (var signal in pending)
            {
                if (signal.Signal())
                    woken++;
            }

            return woken;
        }
    }
}