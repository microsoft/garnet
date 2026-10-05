// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace BDN.benchmark.Cluster
{
    /// <summary>
    /// Parameters for <see cref="EpochWakeBarrier"/>. A single composite param renders one results
    /// column (e.g. <c>Bump,S7</c>), which the perf-gate matches unambiguously.
    /// </summary>
    public struct EpochBenchParams
    {
        /// <summary>Whether a background thread periodically bumps the epoch during measurement.</summary>
        public bool backgroundBumps;

        /// <summary>Number of background session threads churning the epoch alongside the measured one.</summary>
        public int backgroundSessions;

        /// <summary>
        /// Constructor
        /// </summary>
        public EpochBenchParams(bool backgroundBumps, int backgroundSessions)
        {
            this.backgroundBumps = backgroundBumps;
            this.backgroundSessions = backgroundSessions;
        }

        /// <summary>
        /// String representation
        /// </summary>
        public override string ToString()
            => $"{(backgroundBumps ? "Bump" : "NoBump")},S{backgroundSessions}";
    }
}