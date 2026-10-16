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
    /// <remarks>
    /// <para>
    /// A name's list is kept once created, rather than retired when its last waiter leaves: retiring it would
    /// have to exclude a registration that already holds the list it is about to add to, and this is
    /// <c>DEBUG</c> scaffolding whose names come from a test rather than from production traffic.
    /// </para>
    /// <para>
    /// A signal leaves its list when a signaller wakes it, or when its session is torn down. The second is not
    /// left to the parked command: a command whose session is torn down is not reliably resumed, so nothing
    /// after its await, a <c>finally</c> included, can be counted on to run. The signal takes itself off
    /// instead, from the teardown path.
    /// </para>
    /// </remarks>
    internal sealed class SessionSignalRegistry
    {
        readonly ConcurrentDictionary<string, Waiters> names = new();

        /// <summary>
        /// Arms a session's signal and parks it under a name.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Arming happens here, before the signal is reachable by any signaller, so the order cannot be got
        /// wrong at a call site. A signal published before it is armed is dropped by the first signaller that
        /// finds it: the waiter is taken off the list and never woken.
        /// </para>
        /// <para>
        /// The signal joins the list before it is told which list it is on, and a teardown is checked for after
        /// that. A teardown that ran earlier found no list to take the signal off and is seen by the check; one
        /// that runs later finds the list, and the signal is already on it.
        /// </para>
        /// </remarks>
        /// <param name="name">Name to wait on.</param>
        /// <param name="signal">The waiting session's signal.</param>
        internal void Register(string name, SessionSignal signal)
        {
            signal.Arm();

            var waiters = names.GetOrAdd(name, static _ => new Waiters());
            waiters.Add(signal);

            signal.ListedOn(waiters);
            if (signal.TornDown)
                waiters.Remove(signal);
        }

        /// <summary>
        /// The number of sessions listed under a name and not yet woken.
        /// </summary>
        /// <param name="name">Name to count.</param>
        /// <returns>The number of signals on the name.</returns>
        internal int WaiterCount(string name)
            => names.TryGetValue(name, out var waiters) ? waiters.Count : 0;

        /// <summary>
        /// Wakes every session parked on a name, inline on the calling thread.
        /// </summary>
        /// <param name="name">Name to release.</param>
        /// <returns>Number of sessions woken.</returns>
        internal int SignalAll(string name)
        {
            if (!names.TryGetValue(name, out var waiters))
                return 0;

            // Signalled outside the lock: a woken session may come straight back to wait on this same name.
            var woken = 0;
            foreach (var signal in waiters.TakeAll())
            {
                if (signal.Signal())
                    woken++;
            }

            return woken;
        }

        /// <summary>
        /// The signals listed under one name.
        /// </summary>
        internal sealed class Waiters
        {
            readonly List<SessionSignal> signals = [];

            internal int Count
            {
                get
                {
                    lock (signals)
                        return signals.Count;
                }
            }

            internal void Add(SessionSignal signal)
            {
                lock (signals)
                    signals.Add(signal);
            }

            /// <summary>
            /// Takes a signal off the list. Does nothing if it is not on it.
            /// </summary>
            internal void Remove(SessionSignal signal)
            {
                lock (signals)
                    _ = signals.Remove(signal);
            }

            /// <summary>
            /// Takes every signal off the list.
            /// </summary>
            internal SessionSignal[] TakeAll()
            {
                lock (signals)
                {
                    if (signals.Count == 0)
                        return [];

                    var taken = signals.ToArray();
                    signals.Clear();
                    return taken;
                }
            }
        }
    }
}