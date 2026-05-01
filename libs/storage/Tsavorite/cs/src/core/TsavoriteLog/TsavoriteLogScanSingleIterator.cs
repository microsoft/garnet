// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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

                if (this.NextAddress < this.tsavoriteLog.SafeTailAddress
                    || this.NextAddress < this.tsavoriteLog.RefreshSafeTailAddress())
                    return true;

                // Arm the notifier so the next producer completion schedules a wake.
                Volatile.Write(ref this.tsavoriteLog.notifierState, 1); // NotifyArmed

                // Race closure: a producer may have completed between our last refresh and arming.
                if (this.NextAddress < this.tsavoriteLog.RefreshSafeTailAddress())
                    return true;

                // Park until the notifier signals us.
                await onEnqueue.WaitAsync().ConfigureAwait(false);
            }
            return false;
        }
    }
}