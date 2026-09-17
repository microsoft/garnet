// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.test
{
    /// <summary>
    /// Absolute expiration deadlines for tests that exercise member-level expiration of hash fields
    /// and sorted set members. Deadlines are absolute rather than relative so that no assertion
    /// depends on an upper bound on the wall-clock time that elapses between commands.
    /// </summary>
    internal static class MemberExpiry
    {
        /// <summary>
        /// Milliseconds by which <see cref="Imminent"/> leads the current time. The deadline has to be
        /// in the future when the server processes the command: a deadline that has already passed
        /// makes the server drop the member outright instead of letting it expire lazily.
        /// </summary>
        private const int ImminentLeadMs = 200;

        /// <summary>
        /// Time to live, in milliseconds, of a deadline returned by <see cref="Pending"/>.
        /// </summary>
        public const long PendingTtlMs = 60 * 60 * 1000;

        /// <summary>
        /// An absolute deadline, in unix milliseconds, that is not reached while a test runs. A member
        /// carrying it has a pending expiration but always reads back as live.
        /// </summary>
        public static long Pending() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + PendingTtlMs;

        /// <summary>
        /// An absolute deadline, in unix milliseconds, shortly in the future. Pair it with
        /// <see cref="WaitUntilPast"/> or <see cref="WaitUntilPastAsync"/> to drive a member into the
        /// expired state through the lazy expiration path.
        /// </summary>
        public static long Imminent() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + ImminentLeadMs;

        /// <summary>
        /// Blocks until the wall clock is strictly past <paramref name="deadlineMs"/>, which is the
        /// condition under which the server reports a member as expired.
        /// </summary>
        /// <param name="deadlineMs">Absolute deadline in unix milliseconds.</param>
        public static void WaitUntilPast(long deadlineMs)
        {
            for (var remaining = RemainingMs(deadlineMs); remaining >= 0; remaining = RemainingMs(deadlineMs))
                Thread.Sleep((int)remaining + 1);
        }

        /// <summary>
        /// Awaits until the wall clock is strictly past <paramref name="deadlineMs"/>, which is the
        /// condition under which the server reports a member as expired.
        /// </summary>
        /// <param name="deadlineMs">Absolute deadline in unix milliseconds.</param>
        public static async Task WaitUntilPastAsync(long deadlineMs)
        {
            for (var remaining = RemainingMs(deadlineMs); remaining >= 0; remaining = RemainingMs(deadlineMs))
                await Task.Delay((int)remaining + 1).ConfigureAwait(false);
        }

        private static long RemainingMs(long deadlineMs) => deadlineMs - DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }
}