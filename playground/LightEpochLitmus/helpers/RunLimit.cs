// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;

namespace Tsavorite.epoch.litmus
{
    /// <summary>When a run stops: after a fixed duration or after a fixed number of rounds.</summary>
    internal readonly struct RunLimit
    {
        private readonly TimeSpan duration;
        private readonly long rounds;

        private RunLimit(TimeSpan duration, long rounds)
        {
            this.duration = duration;
            this.rounds = rounds;
        }

        internal static RunLimit ForDuration(TimeSpan duration) => new(duration, 0);

        internal static RunLimit ForRounds(long rounds) => new(TimeSpan.Zero, rounds);

        /// <summary>Deadline to pass to <see cref="IsReached"/>; computed when the run starts.</summary>
        internal long StartDeadline()
            => rounds > 0 ? long.MaxValue : Environment.TickCount64 + (long)duration.TotalMilliseconds;

        internal bool IsReached(long completedRounds, long deadline)
            => rounds > 0 ? completedRounds >= rounds : Environment.TickCount64 >= deadline;

        public override string ToString()
            => rounds > 0 ? $"{rounds:N0} rounds" : $"{duration.TotalSeconds:F0}s";

        /// <summary>Parses a positive integer, allowing '_' digit separators as in C# literals (10_000).</summary>
        internal static bool TryParseCount(string text, out long value)
        {
            value = 0;
            if (string.IsNullOrEmpty(text) || text[0] == '_' || text[^1] == '_' || text.Contains("__", StringComparison.Ordinal))
                return false;
            return long.TryParse(text.Replace("_", "", StringComparison.Ordinal), NumberStyles.None, CultureInfo.InvariantCulture, out value) && value > 0;
        }
    }
}