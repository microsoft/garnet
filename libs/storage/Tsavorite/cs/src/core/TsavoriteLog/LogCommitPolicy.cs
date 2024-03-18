// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// LogCommitPolicy defines the way TsavoriteLog behaves on Commit(). In addition
    /// to choosing from a set of pre-defined ones, users can implement their own for custom behavior
    /// </summary>
    public abstract class LogCommitPolicy
    {
        /// <summary>
        /// Invoked when policy object is attached to a TsavoriteLog instance.
        /// </summary>
        /// <param name="log">The log this log commit policy is attached to</param>
        public abstract void OnAttached(TsavoriteLog log);

        /// <summary>
        /// Admission control to decide whether a call to Commit() should successfully start or not.
        /// If false, commit logic will not execute. If true, a commit will be created to cover at least the tail given,
        /// although the underlying implementation may choose to compact multiple admitted Commit() invocations into
        /// one commit operation. It is the implementer's responsibility to log and retry any filtered Commit() when
        /// necessary (e.g., when there will not be any future Commit() invocations, but the last Commit() was filtered)
        /// </summary>
        /// <param name="currentTail">If successful, this request will commit at least up to this tail</param>
        /// <param name="commitRequired">Whether commit may be required, i.e., tail address is higher than last covered commit request or metadata (e.g., iterators) changed </param>
        /// <returns></returns>
        public abstract bool AdmitCommit(long currentTail, bool commitRequired);

        /// <summary>
        /// Invoked when a commit is successfully created
        /// </summary>
        /// <param name="info"> commit content </param>
        public abstract void OnCommitCreated(TsavoriteLogRecoveryInfo info);

        /// <summary>
        /// Invoked after a commit is complete
        /// </summary>
        /// <param name="info"> commit content </param>
        public abstract void OnCommitFinished(TsavoriteLogRecoveryInfo info);

        /// <summary>
        /// The default log commit policy ensures that each record is covered by at most one commit request (except when
        /// the metadata has changed). Redundant commit calls are dropped and corresponding commit invocation will
        /// return false.
        /// </summary>
        /// <returns> policy object </returns>
        public static LogCommitPolicy Default() => new DefaultLogCommitPolicy();

        /// <summary>
        /// MaxParallel log commit policy allows k (non-strong) commit requests to be in progress at any giving time. The k commits are guaranteed
        /// to be non-overlapping unless there are metadata changes. Additional commit requests will fail and
        /// automatically retried.
        /// </summary>
        /// <param name="k"> maximum number of commits that can be outstanding at a time </param>
        /// <returns> policy object </returns>
        public static LogCommitPolicy MaxParallel(int k) => new MaxParallelLogCommitPolicy(k);


        /// <summary>
        /// RateLimit log commit policy will only issue a request if it covers at least m bytes or if there has not been a
        /// commit request in n milliseconds. Additional commit requests will fail and automatically retried
        /// </summary>
        /// <param name="thresholdMilli">
        /// minimum time, in milliseconds, to be allowed between two commits, unless thresholdRange bytes will be committed
        /// </param>
        /// <param name="thresholdBytes">
        /// minimum range, in bytes, to be allowed between two commits, unless it has been thresholdMilli milliseconds
        /// </param>
        /// <returns> policy object </returns>
        public static LogCommitPolicy RateLimit(long thresholdMilli, long thresholdBytes) => new RateLimitLogCommitPolicy(thresholdMilli, thresholdBytes);
    }

    internal sealed class DefaultLogCommitPolicy : LogCommitPolicy
    {
        /// <inheritdoc/>
        public override void OnAttached(TsavoriteLog log) { }

        /// <inheritdoc/>
        public override bool AdmitCommit(long currentTail, bool commitRequired) => commitRequired;

        /// <inheritdoc/>
        public override void OnCommitCreated(TsavoriteLogRecoveryInfo info) { }

        /// <inheritdoc/>
        public override void OnCommitFinished(TsavoriteLogRecoveryInfo info) { }
    }

    internal sealed class MaxParallelLogCommitPolicy : LogCommitPolicy
    {
        readonly int maxCommitInProgress;
        TsavoriteLog log;
        int commitInProgress;
        // If we filtered out some commit, make sure to remember to retry later 
        bool shouldRetry;

        internal MaxParallelLogCommitPolicy(int maxCommitInProgress)
        {
            this.maxCommitInProgress = maxCommitInProgress;
        }

        /// <inheritdoc/>
        public override void OnAttached(TsavoriteLog log) => this.log = log;

        /// <inheritdoc/>
        public override bool AdmitCommit(long currentTail, bool commitRequired)
        {
            while (true)
            {
                var cip = commitInProgress;
                if (cip == maxCommitInProgress)
                {
                    shouldRetry = commitRequired;
                    return false;
                }

                if (Interlocked.CompareExchange(ref commitInProgress, cip + 1, cip) == cip) return true;
            }
        }

        /// <inheritdoc/>
        public override void OnCommitCreated(TsavoriteLogRecoveryInfo info) { }

        /// <inheritdoc/>
        public override void OnCommitFinished(TsavoriteLogRecoveryInfo info)
        {
            Interlocked.Decrement(ref commitInProgress);
            if (shouldRetry)
            {
                shouldRetry = false;
                log.Commit();
            }
        }
    }

    internal sealed class RateLimitLogCommitPolicy : LogCommitPolicy
    {
        readonly Stopwatch stopwatch;
        readonly long thresholdMilli;
        readonly long thresholdRange;
        TsavoriteLog log;
        long lastAdmittedMilli;
        long lastAdmittedAddress;
        int shouldRetry = 0;

        internal RateLimitLogCommitPolicy(long thresholdMilli, long thresholdRange)
        {
            this.thresholdMilli = thresholdMilli;
            this.thresholdRange = thresholdRange;
            stopwatch = Stopwatch.StartNew();
            lastAdmittedMilli = -thresholdMilli;
            lastAdmittedAddress = -thresholdRange;
        }

        /// <inheritdoc/>
        public override void OnAttached(TsavoriteLog log) => this.log = log;

        /// <inheritdoc/>
        public override bool AdmitCommit(long currentTail, bool commitRequired)
        {
            var now = stopwatch.ElapsedMilliseconds;
            while (true)
            {
                var lastSeenMilli = lastAdmittedMilli;
                var lastSeenAddress = lastAdmittedAddress;
                if (now - lastSeenMilli < thresholdMilli && currentTail - lastSeenAddress < thresholdRange)
                {
                    // Only allow spawning of task if no other task is already underway
                    if (Interlocked.CompareExchange(ref shouldRetry, 1, 0) == 0)
                    {
                        Task.Run(async () =>
                        {
                            await Task.Delay(TimeSpan.FromMilliseconds(thresholdMilli));
                            shouldRetry = 0;
                            log.Commit();
                        });
                    }
                    return false;
                }

                if (Interlocked.CompareExchange(ref lastAdmittedMilli, now, lastSeenMilli) == lastSeenMilli
                    && Interlocked.CompareExchange(ref lastAdmittedAddress, currentTail, lastSeenAddress) == lastSeenAddress)
                    return true;
            }
        }

        /// <inheritdoc/>
        public override void OnCommitCreated(TsavoriteLogRecoveryInfo info) { }

        /// <inheritdoc/>
        public override void OnCommitFinished(TsavoriteLogRecoveryInfo info) { }
    }
}