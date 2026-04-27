// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Scan iterator for TsavoriteLog's hybrid log - only a single scan is supported per instance
    /// This modification allows us to use a SingleWaiterAutoResetEvent per iterator
    /// so we can avoid TCS allocations per tail bump.
    /// </summary>
    public sealed class TsavoriteLogScanSingleIterator : TsavoriteLogScanIterator
    {
        readonly SingleWaiterAutoResetEvent onEnqueue;

        /// <summary>Maximum backoff interval (in Stopwatch ticks) before re-scanning the epoch table.
        /// Caps at 10ms to match the old baseline refresh frequency.</summary>
        static readonly long MaxBackoffTicks = Stopwatch.Frequency / 100; // 10ms

        /// <summary>Current backoff interval in Stopwatch ticks. Doubles on each scan that fails to
        /// advance SafeTailAddress past NextAddress; resets to
        /// zero when a scan succeeds or when new records are observed via the cached SafeTailAddress.
        /// </summary>
        long scanBackoffTicks;

        /// <summary>Timestamp (via <see cref="Stopwatch.GetTimestamp"/>) at which the backoff expires
        /// and the next scan is permitted.</summary>
        long scanBackoffUntil;

        internal TsavoriteLogScanSingleIterator(TsavoriteLog TsavoriteLog, TsavoriteLogAllocatorImpl hlog, long beginAddress, long endAddress,
                GetMemory getMemory, DiskScanBufferingMode scanBufferingMode, LightEpoch epoch, int headerSize, bool scanUncommitted = false, ILogger logger = null)
            : base(TsavoriteLog, hlog, beginAddress, endAddress, getMemory, scanBufferingMode, epoch, headerSize, scanUncommitted, logger)
        {
            onEnqueue = new()
            {
                RunContinuationsAsynchronously = true
            };
        }

        public override void Dispose()
        {
            tsavoriteLog.RemoveIterator(this);
            base.Dispose();
            // Any awaiting iterator should be woken up during dispose
            onEnqueue.Signal();
        }

        public void Signal()
            => onEnqueue.Signal();

        protected override async ValueTask<bool> SlowWaitUncommittedAsync(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                if (this.disposed)
                    return false;
                if (this.Ended) return false;

                // Fast check: cached SafeTailAddress may already be ahead (e.g., another iterator
                // or the multi-iterator pre-refresh in NotifyParkedWaiters advanced it).
                if (this.NextAddress < this.tsavoriteLog.SafeTailAddress)
                {
                    scanBackoffTicks = 0; // data available — reset backoff
                    return true;
                }

                // Scan the epoch table only if the backoff interval has elapsed. This limits scan
                // frequency when the consumer is caught up and the producer is slow, capping at one
                // scan per 10ms in the worst case (matching the old baseline refresh frequency).
                if (Stopwatch.GetTimestamp() >= scanBackoffUntil)
                {
                    if (this.NextAddress < this.tsavoriteLog.RefreshSafeTailAddress())
                    {
                        scanBackoffTicks = 0; // scan advanced past NextAddress — reset backoff
                        return true;
                    }

                    // Scan didn't help — double the backoff interval (starting from ~100μs).
                    scanBackoffTicks = scanBackoffTicks == 0
                        ? Stopwatch.Frequency / 10_000  // initial: ~100μs
                        : Math.Min(scanBackoffTicks * 2, MaxBackoffTicks);
                    scanBackoffUntil = Stopwatch.GetTimestamp() + scanBackoffTicks;
                }

                // Ignore refresh-uncommitted exceptions, except when the token is signaled
                await onEnqueue.WaitAsync().ConfigureAwait(false);
            }
            return false;
        }
    }
}