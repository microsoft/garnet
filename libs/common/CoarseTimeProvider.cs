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
        public static readonly TimeSpan RefreshPeriod = TimeSpan.FromSeconds(1);

        /// <summary>
        /// Shared instance.
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
        /// Private to enforce singleton usage via <see cref="Instance"/>.
        /// </summary>
        private CoarseTimeProvider() { }

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