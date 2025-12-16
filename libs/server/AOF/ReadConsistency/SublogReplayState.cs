// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Tsavorite.core;

namespace Garnet.server
{
    internal class SublogReplayState(GarnetServerOptions serverOptions)
    {
        readonly VirtualSublogSketch[] vsrs = [.. Enumerable.Range(0, serverOptions.AofReplaySubtaskCount).Select(_ => new VirtualSublogSketch())];

        public long Max => vsrs.Select(s => s.Max).Max();

        /// <summary>
        /// Add waiter to the virtual sublog, identified by the provided key hash
        /// </summary>
        /// <param name="keyHash"></param>
        /// <param name="waiter"></param>
        public void AddWaiter(long keyHash, ReadSessionWaiter waiter)
        {
            vsrs[keyHash % vsrs.Length].AddWaiter(waiter);
        }

        /// <summary>
        /// Get maximum sequence number for all virtual sublogs
        /// </summary>
        /// <returns></returns>
        public long GetSublogMaxSequenceNumber()
            => vsrs.Select(x => x.MaxSequenceNumber).Max();

        /// <summary>
        /// Gets the current frontier (<see cref="VirtualSublogSketch.GetFrontierSequenceNumber(long)"/>) sequence number for the virtual sublog identified by the provided keyHash.
        /// </summary>
        /// <param name="keyHash"></param>
        /// <returns></returns>
        public long GetSublogFrontierSequenceNumber(long keyHash)
        {
            return vsrs[keyHash % vsrs.Length].GetFrontierSequenceNumber(keyHash);
        }

        /// <summary>
        /// Gets the current frontier (<see cref="VirtualSublogSketch.GetFrontierSequenceNumber(long)"/>) sequence number for the virtual sublog identified by the provided keyHash.
        /// </summary>
        /// <param name="keyHash"></param>
        /// <returns></returns>
        public long GetKeySequenceNumber(long keyHash)
        {
            return vsrs[keyHash % vsrs.Length].GetKeySequenceNumber(keyHash);
        }

        /// <summary>
        /// Refresh maximum sequence number value for all virtual sublogs.
        /// </summary>
        public void UpdateMaxSequenceNumber()
        {
            // This should be called when multiple virtual sublogs are used
            Debug.Assert(vsrs.Length > 0);
            // Find maximum value across sublogs
            var maxSublogSequenceNumber = vsrs.Select(vsublog => vsublog.MaxSequenceNumber).Max();
            foreach (var vsublog in vsrs)
                vsublog.UpdateMaxSequenceNumber(maxSublogSequenceNumber);
        }

        /// <summary>
        /// Update max sequence number for all virtual sublogs of this physical sublog
        /// </summary>
        /// <param name="sequenceNumber"></param>
        public void UpdateMaxSequenceNumber(long sequenceNumber)
        {
            foreach (var vsublog in vsrs)
                vsublog.UpdateMaxSequenceNumber(sequenceNumber);
        }

        /// <summary>
        /// Update sequence number for the key described by provided key hash.
        /// </summary>
        /// <param name="keyHash"></param>
        /// <param name="sequenceNumber"></param>
        public void UpdateKeySequenceNumber(long keyHash, long sequenceNumber)
        {
            vsrs[keyHash % vsrs.Length].UpdateKeySequenceNumber(keyHash, sequenceNumber);
        }
    }

    internal struct VirtualSublogSketch
    {
        const int maxOffset = keySlotCount;
        public const int keySlotCount = 1 << 15;
        const int slotMask = keySlotCount - 1;
        readonly long[] replayState = new long[keySlotCount + 1];
        readonly ConcurrentQueue<ReadSessionWaiter> waitQs = new();

        public VirtualSublogSketch()
        {
            var size = keySlotCount;
            if ((size & (size - 1)) != 0)
                throw new InvalidOperationException($"Size ({keySlotCount}) must be a power of 2");
        }

        public readonly long MaxSequenceNumber => replayState[maxOffset];

        public readonly long Max => replayState.Max();

        public void AddWaiter(ReadSessionWaiter waiter)
            => waitQs.Enqueue(waiter);

        public readonly long GetFrontierSequenceNumber(long hash)
            => Math.Max(replayState[hash & slotMask], MaxSequenceNumber);

        public readonly long GetKeySequenceNumber(long hash)
            => replayState[hash & slotMask];

        /// <summary>
        /// Update max sequence number for this virtual sublog.
        /// </summary>
        /// <param name="sequenceNumber"></param>
        public void UpdateMaxSequenceNumber(long sequenceNumber)
        {
            _ = Utility.MonotonicUpdate(ref replayState[maxOffset], sequenceNumber, out _);
            SignalWaiters();
        }

        /// <summary>
        /// Update key sequence number for this virtual sublog.
        /// </summary>
        /// <param name="hash"></param>
        /// <param name="sequenceNumber"></param>
        public void UpdateKeySequenceNumber(long hash, long sequenceNumber)
        {
            _ = Utility.MonotonicUpdate(ref replayState[hash & slotMask], sequenceNumber, out _);
            _ = Utility.MonotonicUpdate(ref replayState[maxOffset], sequenceNumber, out _);
            SignalWaiters();
        }

        /// <summary>
        /// Signal any readers waiting for timestamp to progress
        /// </summary>        
        void SignalWaiters()
        {
            var waiterList = new List<ReadSessionWaiter>();
            while (waitQs.TryDequeue(out var waiter))
            {
                // If timestamp has not progressed enough will re-add this waiter to the waitQ
                if (waiter.rrsc.maximumSessionSequenceNumber > GetFrontierSequenceNumber(waiter.rrsc.lastHash))
                    waiterList.Add(waiter);
                else
                    // Signal for waiter to proceed
                    waiter.Set();
            }

            // Re-insert any waiters that have not been released yet
            foreach (var waiter in waiterList)
                waitQs.Enqueue(waiter);
        }
    }
}
