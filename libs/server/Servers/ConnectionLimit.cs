// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace Garnet.server
{
    /// <summary>
    /// Process-wide admission control for client connections, backing the <c>maxclients</c>
    /// configuration parameter.
    ///
    /// One instance is shared by every listener, so the ceiling applies to the server as a whole
    /// rather than to each bound endpoint. Binding several endpoints therefore does not multiply
    /// the limit, which is what a Redis operator setting <c>maxclients</c> expects.
    ///
    /// The current population is summed from the listeners' own live-handler counts rather than
    /// tracked in a counter of its own. That is deliberate: a second counter would have to be
    /// maintained at every accept, reject, setup-failure and dispose path, and a counter that
    /// ratchets because one of those paths was missed fails closed -- it would refuse every
    /// connection forever, with no way to recover short of a restart.
    /// </summary>
    public sealed class ConnectionLimit
    {
        /// <summary>Sentinel meaning no limit is enforced.</summary>
        public const int Unlimited = -1;

        volatile int limit;

        // Copy-on-write: listeners register during startup, before any accept can run, while
        // IsWithinLimit reads on IOCP threads. Publishing a fresh array under a lock keeps readers
        // lock-free without exposing a partially-built list.
        readonly object registrationLock = new();
        volatile IConnectionSource[] sources = [];

        /// <summary>
        /// Creates a limit.
        /// </summary>
        /// <param name="limit">Maximum simultaneous connections, or -1 for unlimited.</param>
        public ConnectionLimit(int limit) => this.limit = limit;

        /// <summary>
        /// Maximum simultaneous connections across all listeners, or -1 for unlimited.
        /// Settable at runtime through <c>CONFIG SET maxclients</c>.
        ///
        /// Lowering it below the current population does not disconnect anyone; existing
        /// connections are kept and new ones are refused until the population falls back under the
        /// limit. This matches Redis, which also applies a lowered <c>maxclients</c> only to
        /// subsequent connections.
        /// </summary>
        public int Limit
        {
            get => limit;
            set => limit = value;
        }

        /// <summary>
        /// Registers a listener whose live connections count toward the limit.
        /// </summary>
        /// <param name="source">The listener to include in the population.</param>
        public void Register(IConnectionSource source)
        {
            ArgumentNullException.ThrowIfNull(source);

            lock (registrationLock)
            {
                var updated = new List<IConnectionSource>(sources) { source };
                sources = [.. updated];
            }
        }

        /// <summary>
        /// Current number of live connections across every registered listener.
        /// </summary>
        public int CurrentConnections()
        {
            var total = 0;
            foreach (var source in sources)
                total += source.LiveConnectionCount;
            return total;
        }

        /// <summary>
        /// Whether the server is within its connection limit, called by a listener that has
        /// already counted the connection being admitted.
        ///
        /// Racy by construction, exactly as the per-listener check it replaces: two listeners can
        /// both observe room and both admit. The overshoot is bounded by the number of listeners
        /// and is self-correcting, which is the right trade for admission control -- the
        /// alternative is serializing every accept in the process behind one lock.
        /// </summary>
        public bool IsWithinLimit()
        {
            var current = limit;
            return current < 0 || CurrentConnections() <= current;
        }
    }

    /// <summary>
    /// A listener that contributes live connections to the process-wide connection limit.
    /// </summary>
    public interface IConnectionSource
    {
        /// <summary>
        /// Live connections held by this listener. Never negative, so a listener that has been
        /// disposed contributes nothing rather than subtracting from its peers.
        /// </summary>
        int LiveConnectionCount { get; }
    }
}