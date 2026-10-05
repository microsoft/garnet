// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;

namespace Garnet.cluster
{
    /// <summary>
    /// Supplies the set of epoch observers (active cluster sessions) that a bump must wait on.
    /// Decoupling the scan behind this interface lets the wake barrier be exercised in isolation,
    /// independently of a live <see cref="StoreWrapper"/> and its network servers.
    /// </summary>
    internal interface IEpochObserver
    {
        /// <summary>
        /// Returns true when every active observer has quiesced past <paramref name="targetEpoch"/>:
        /// each session is either idle (<c>LocalCurrentEpoch == 0</c>) or has already observed an
        /// epoch at or beyond the target. This is the barrier's grace-period predicate.
        /// </summary>
        /// <param name="targetEpoch">The epoch value published by the in-flight bump.</param>
        bool AllSessionsQuiesced(long targetEpoch);
    }

    /// <summary>
    /// Production <see cref="IEpochObserver"/> that scans the active cluster sessions of every
    /// server owned by the <see cref="StoreWrapper"/>. Mirrors the scan performed by the legacy
    /// busy-spin barrier.
    ///
    /// Declared as a <c>readonly struct</c> so that, when used as the type argument of
    /// <see cref="GarnetEpoch{TEpochObserver}"/>, the JIT specializes the generic and the
    /// quiescence call devirtualizes and inlines with no boxing or virtual dispatch.
    /// </summary>
    internal readonly struct ServerEpochObserverSource : IEpochObserver
    {
        readonly StoreWrapper storeWrapper;

        internal ServerEpochObserverSource(StoreWrapper storeWrapper)
        {
            this.storeWrapper = storeWrapper;
        }

        /// <inheritdoc/>
        public bool AllSessionsQuiesced(long targetEpoch)
        {
            foreach (var server in storeWrapper.Servers)
            {
                foreach (var session in ((GarnetServerTcp)server).ActiveClusterSessions())
                {
                    var entryEpoch = session.LocalCurrentEpoch;
                    if (entryEpoch != 0 && entryEpoch < targetEpoch)
                        return false;
                }
            }
            return true;
        }
    }
}