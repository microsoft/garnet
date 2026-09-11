// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    internal sealed class CompactionState
    {
        internal long RetryAfterTailAddress { get; private set; }

        internal bool ShouldSkip(long tailAddress)
            => RetryAfterTailAddress > tailAddress;

        internal bool TryResume(long tailAddress)
        {
            if (RetryAfterTailAddress == 0 || tailAddress < RetryAfterTailAddress)
                return false;

            RetryAfterTailAddress = 0;
            return true;
        }

        internal bool RecordCycle(long beginAddressBefore, long beginAddressAfter, long tailAddressBefore,
            long tailAddressAfter, long retryBytes, int minReclaimPercent)
        {
            var beginAddressAdvance = beginAddressAfter - beginAddressBefore;
            var tailAddressGrowth = tailAddressAfter - tailAddressBefore;
            var netReclaimed = beginAddressAdvance - tailAddressGrowth;

            // A cycle is only "productive" if it reclaims at least minReclaimPercent of the range it
            // scanned. A copy-forward pass over all-live data re-writes almost every byte it truncates,
            // so netReclaimed sits at a few hundred bytes of segment-alignment noise even after moving
            // tens of GB. The legacy "netReclaimed > 0" test treats that as progress and never backs
            // off, so the wasteful loop runs forever. Requiring a meaningful reclaim floor parks it.
            var minReclaim = minReclaimPercent > 0
                ? (long)Math.Ceiling(beginAddressAdvance * (minReclaimPercent / 100.0))
                : 1;

            if (beginAddressAdvance <= 0 || netReclaimed >= minReclaim)
            {
                RetryAfterTailAddress = 0;
                return false;
            }

            RetryAfterTailAddress = tailAddressAfter > long.MaxValue - retryBytes
                ? long.MaxValue
                : tailAddressAfter + retryBytes;
            return true;
        }
    }

    internal static class CompactionPolicy
    {
        internal static long GetBackoffBytes(long segmentSize, int backoffSegments)
            => backoffSegments > long.MaxValue / segmentSize
                ? long.MaxValue
                : segmentSize * backoffSegments;

        internal static long GetUntilAddress(long beginAddress, long readOnlyAddress, long segmentSize,
            int maxSegments, int numSegmentsToCompact, bool boundCycle)
        {
            if (boundCycle)
                return Math.Min(readOnlyAddress, beginAddress + segmentSize * numSegmentsToCompact);

            return readOnlyAddress - segmentSize * (maxSegments - numSegmentsToCompact);
        }
    }
}
