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

                if (this.NextAddress < this.tsavoriteLog.SafeTailAddress)
                    return true;

                // Ignore refresh-uncommitted exceptions, except when the token is signaled
                await onEnqueue.WaitAsync().ConfigureAwait(false);
            }
            return false;
        }
    }
}