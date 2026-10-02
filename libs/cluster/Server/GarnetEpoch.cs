// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;
using Garnet.server;

namespace Garnet.cluster
{
    /// <summary>
    /// Tracks cluster configuration epochs shared across sessions.
    /// </summary>
    internal sealed class GarnetEpoch
    {
        readonly StoreWrapper storeWrapper;
        long CurrentEpoch = 1;

        /// <summary>
        /// Creates a new Garnet epoch tracker.
        /// </summary>
        /// <param name="storeWrapper">Store wrapper containing the active servers.</param>
        internal GarnetEpoch(StoreWrapper storeWrapper)
        {
            this.storeWrapper = storeWrapper;
        }

        /// <summary>
        /// Gets the current epoch.
        /// </summary>
        internal long GetCurrentEpoch() => Volatile.Read(ref CurrentEpoch);

        /// <summary>
        /// Bumps the current epoch and waits for active cluster sessions to observe the transition.
        /// </summary>
        /// <returns>True when all active cluster sessions have transitioned.</returns>
        internal async Task<bool> BumpAndWaitForEpochTransitionAsync()
        {
            var currentEpoch = Interlocked.Increment(ref CurrentEpoch);
            foreach (var server in storeWrapper.Servers)
            {
                while (true)
                {
                retry:
                    await Task.Yield();
                    var sessions = ((GarnetServerTcp)server).ActiveClusterSessions();
                    foreach (var session in sessions)
                    {
                        var entryEpoch = session.LocalCurrentEpoch;
                        if (entryEpoch != 0 && entryEpoch < currentEpoch)
                            goto retry;
                    }
                    break;
                }
            }
            return true;
        }
    }
}