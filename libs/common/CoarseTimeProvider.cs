// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// A <see cref="TimeProvider"/> whose <see cref="GetUtcNow"/> returns a coarse,
    /// cached value (refreshed every <see cref="RefreshPeriod"/>) so hot paths read
    /// the wall clock as a pure memory load. All other members inherit the base
    /// defaults (equivalent to <see cref="TimeProvider.System"/>); this type exists
    /// purely as a <see cref="TimeProvider"/> injection point.
    /// </summary>
    public sealed class CoarseTimeProvider : TimeProvider
    {
        /// <summary>
        /// Refresh cadence. Cached value can lag the wall clock by ~this duration.
        /// </summary>
        public static readonly TimeSpan RefreshPeriod = TimeSpan.FromSeconds(1);

        /// <summary>
        /// Shared instance for callers that want to avoid an allocation.
        /// </summary>
        public static readonly CoarseTimeProvider Instance = new();

        private static volatile DateTimeOffsetSnapshot snapshot = new(TimeProvider.System.GetUtcNow());

        // Held in a static field to keep the refresh timer rooted for process lifetime.
        private static readonly ITimer timer = TimeProvider.System.CreateTimer(
            static _ => snapshot = new DateTimeOffsetSnapshot(TimeProvider.System.GetUtcNow()),
            null,
            RefreshPeriod,
            RefreshPeriod);

        /// <summary>
        /// Constructs a <see cref="CoarseTimeProvider"/>. Instances hold no state — all
        /// reads route through the shared static cache.
        /// </summary>
        public CoarseTimeProvider() { }

        /// <summary>
        /// Coarse, cached UTC time. May lag the wall clock by ~<see cref="RefreshPeriod"/>.
        /// </summary>
        public override DateTimeOffset GetUtcNow() => snapshot.Value;

        /// <summary>
        /// Coarse, cached UTC time as a <see cref="DateTime"/>.
        /// </summary>
        public DateTime UtcNow => snapshot.Value.UtcDateTime;

        // Heap-allocated wrapper: DateTimeOffset reads/writes aren't atomic, but a
        // reference load is — so readers atomically load the reference and copy the
        // immutable struct out, with no tearing.
        private sealed class DateTimeOffsetSnapshot
        {
            public readonly DateTimeOffset Value;
            public DateTimeOffsetSnapshot(DateTimeOffset value) => Value = value;
        }
    }
}