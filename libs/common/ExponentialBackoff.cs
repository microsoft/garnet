// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.common
{
    /// <summary>
    /// Maintains capped exponential-backoff state for a retryable operation.
    /// </summary>
    /// <remarks>This type is not thread-safe.</remarks>
    public struct ExponentialBackoff
    {
        /// <summary>
        /// Default delay envelope after the first failure.
        /// </summary>
        public static readonly TimeSpan DefaultBaseDelay = TimeSpan.FromSeconds(1);

        /// <summary>
        /// Default maximum delay envelope.
        /// </summary>
        public static readonly TimeSpan DefaultMaxDelay = TimeSpan.FromMinutes(1);

        /// <summary>
        /// Gets the number of consecutive failures.
        /// </summary>
        public readonly int FailureCount => failureCount;

        readonly long baseDelayMilliseconds;
        readonly long maxDelayMilliseconds;
        readonly Func<long> timestampProvider;
        readonly Func<double> jitterProvider;
        private int failureCount;
        long retryAtMilliseconds;

        /// <summary>
        /// Creates an exponential-backoff policy with the default delays.
        /// </summary>
        public ExponentialBackoff()
            : this(null, null)
        {
        }

        /// <summary>
        /// Creates an exponential-backoff policy.
        /// </summary>
        /// <param name="baseDelay">Delay envelope after the first failure, or <see cref="DefaultBaseDelay"/>.</param>
        /// <param name="maxDelay">Maximum delay envelope, or <see cref="DefaultMaxDelay"/>.</param>
        /// <param name="timestampProvider">Optional monotonic millisecond timestamp provider.</param>
        /// <param name="jitterProvider">Optional provider returning a value between zero and one.</param>
        public ExponentialBackoff(
            TimeSpan? baseDelay = null,
            TimeSpan? maxDelay = null,
            Func<long> timestampProvider = null,
            Func<double> jitterProvider = null)
        {
            baseDelayMilliseconds = Math.Max(1, (long)(baseDelay ?? DefaultBaseDelay).TotalMilliseconds);
            maxDelayMilliseconds = Math.Max(baseDelayMilliseconds, (long)(maxDelay ?? DefaultMaxDelay).TotalMilliseconds);
            this.timestampProvider = timestampProvider ?? (static () => Environment.TickCount64);
            this.jitterProvider = jitterProvider ?? (static () => Random.Shared.NextDouble());
            failureCount = 0;
            retryAtMilliseconds = 0;
        }

        /// <summary>
        /// Whether a new attempt may start.
        /// </summary>
        public bool CanAttempt()
            => failureCount == 0 || timestampProvider() >= retryAtMilliseconds;

        /// <summary>
        /// Records a failed attempt and returns the delay before the next attempt.
        /// </summary>
        public TimeSpan RecordFailure()
        {
            failureCount++;

            var shift = Math.Min(failureCount - 1, 62);
            var exponentialDelay = baseDelayMilliseconds > (maxDelayMilliseconds >> shift)
                ? maxDelayMilliseconds
                : baseDelayMilliseconds << shift;

            // Equal jitter in the upper 20% prevents retries from occurring in lockstep while
            // preserving the configured exponential envelope.
            var jitterWindow = Math.Max(1, exponentialDelay / 5);
            var jitter = Math.Clamp(jitterProvider(), 0, 1);
            var delay = exponentialDelay - jitterWindow + (long)(jitterWindow * jitter);
            delay = Math.Clamp(delay, 1, maxDelayMilliseconds);

            retryAtMilliseconds = SaturatingAdd(timestampProvider(), delay);
            return TimeSpan.FromMilliseconds(delay);

            static long SaturatingAdd(long left, long right) => left > long.MaxValue - right ? long.MaxValue : left + right;
        }

        /// <summary>
        /// Clears failure history after a successful attempt.
        /// </summary>
        public void Reset()
        {
            failureCount = 0;
            retryAtMilliseconds = 0;
        }
    }
}