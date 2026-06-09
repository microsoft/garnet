// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// A <see cref="TimeProvider"/> whose <see cref="GetUtcNow"/> returns a coarse,
    /// cached value (refreshed every <see cref="RefreshPeriod"/>) so hot paths can
    /// read the wall clock as a pure memory load. All other <see cref="TimeProvider"/>
    /// members delegate to <see cref="TimeProvider.System"/>. The cache and its refresh
    /// timer are static (process-wide), so this type is not customizable — its sole
    /// purpose is to act as an injection point for code that wants to depend on the
    /// <see cref="TimeProvider"/> abstraction while still benefiting from the cached
    /// hot-path read.
    /// </summary>
    public sealed class CoarseTimeProvider : TimeProvider
    {
        /// <summary>
        /// Refresh cadence. The cached value can lag the wall clock by approximately
        /// this duration (best-effort: actual lag depends on Timer cadence, which can
        /// slip under GC pauses or ThreadPool starvation).
        /// </summary>
        public static readonly TimeSpan RefreshPeriod = TimeSpan.FromSeconds(1);

        /// <summary>
        /// Shared instance for callers that want to avoid an allocation. All
        /// <see cref="CoarseTimeProvider"/> instances are functionally identical
        /// — they all read the same static cache — so this is purely a convenience.
        /// </summary>
        public static readonly CoarseTimeProvider Instance = new();

        // Heap-allocated wrapper around the cached DateTimeOffset. A reference load
        // is always atomic on .NET (and `volatile` gives it acquire semantics); a
        // multi-word DateTimeOffset struct copy is not. Wrapping in a class lets
        // readers do a single atomic reference load and copy the immutable struct
        // out — no tearing, no DateTimeOffset constructor on the read path. The
        // Timer allocates one DateTimeOffsetSnapshot per refresh tick.
        private sealed class DateTimeOffsetSnapshot
        {
            public readonly DateTimeOffset Value;
            public DateTimeOffsetSnapshot(DateTimeOffset value) => Value = value;
        }

        private static volatile DateTimeOffsetSnapshot snapshot = new(TimeProvider.System.GetUtcNow());

        // Process-wide background refresh. Held in a static field to keep it rooted
        // for process lifetime — the cache is shared by every CoarseTimeProvider
        // instance, so there is exactly one refresh Timer regardless of how many
        // instances are constructed.
        private static readonly ITimer timer = TimeProvider.System.CreateTimer(
            static _ => snapshot = new DateTimeOffsetSnapshot(TimeProvider.System.GetUtcNow()),
            null,
            RefreshPeriod,
            RefreshPeriod);

        /// <summary>
        /// Constructs a <see cref="CoarseTimeProvider"/>. Instances hold no state —
        /// all reads route through the shared static cache. Use this constructor (or
        /// the <see cref="Instance"/> singleton) when you need a <see cref="TimeProvider"/>
        /// to inject and want production code to benefit from the cached hot-path read.
        /// </summary>
        public CoarseTimeProvider() { }

        /// <summary>
        /// Coarse, cached UTC time. May lag the wall clock by ~<see cref="RefreshPeriod"/>.
        /// </summary>
        public override DateTimeOffset GetUtcNow() => snapshot.Value;

        /// <summary>
        /// Coarse, cached UTC time as a <see cref="DateTime"/>. Convenience wrapper
        /// around <see cref="GetUtcNow"/> for callers that want a <see cref="DateTime"/>
        /// directly.
        /// </summary>
        public DateTime UtcNow => snapshot.Value.UtcDateTime;

        // Only GetUtcNow is coarse. The remaining TimeProvider members (GetTimestamp,
        // TimestampFrequency, LocalTimeZone, CreateTimer) inherit the base class's
        // default implementation, which is equivalent to TimeProvider.System.
    }
}