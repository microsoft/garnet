// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.common
{
    /// <summary>
    /// Static convenience wrapper around <see cref="CoarseTimeProvider.System"/> —
    /// the process-wide coarse UTC cache backed by <see cref="TimeProvider.System"/>.
    /// Prefer injecting a <see cref="CoarseTimeProvider"/> directly when the
    /// callsite needs to substitute a different clock source (e.g. tests).
    /// </summary>
    public static class CoarseDateTime
    {
        /// <summary>
        /// Refresh period in milliseconds — mirrors <see cref="CoarseTimeProvider.RefreshPeriodMs"/>.
        /// </summary>
        public const int RefreshPeriodMs = CoarseTimeProvider.RefreshPeriodMs;

        /// <inheritdoc cref="CoarseTimeProvider.UtcNow"/>
        public static DateTime UtcNow => CoarseTimeProvider.System.UtcNow;

        /// <inheritdoc cref="CoarseTimeProvider.UtcNowTicks"/>
        public static long UtcNowTicks => CoarseTimeProvider.System.UtcNowTicks;
    }
}

