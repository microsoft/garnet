// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;
using NUnit.Framework;

namespace Garnet.test
{
    [TestFixture]
    internal class CompactionPolicyTests
    {
        [Test]
        public void LowYieldCycleBacksOffUntilForegroundGrowth()
        {
            const long segmentSize = 1024;
            const int minReclaimPercent = 20;
            var state = new CompactionState();

            Assert.That(state.RecordCycle(0, segmentSize, 10 * segmentSize, 11 * segmentSize, 4 * segmentSize, minReclaimPercent), Is.True);
            Assert.That(state.RetryAfterTailAddress, Is.EqualTo(15 * segmentSize));
            Assert.That(state.ShouldSkip(14 * segmentSize), Is.True);
            Assert.That(state.TryResume(14 * segmentSize), Is.False);
            Assert.That(state.TryResume(15 * segmentSize), Is.True);
            Assert.That(state.ShouldSkip(15 * segmentSize), Is.False);
        }

        [Test]
        public void TinyPositiveReclaimBacksOff()
        {
            // Regression for the all-live copy-forward loop: a cycle that truncates ~1000 segments
            // but only reclaims 256 bytes (alignment noise) must be treated as low-yield and back
            // off, even though net reclaim is strictly positive. The legacy "netReclaimed > 0" test
            // misclassified this as productive and never parked.
            const long segmentSize = 1024;
            const int minReclaimPercent = 20;
            var state = new CompactionState();

            var beginAdvance = 1000 * segmentSize;
            Assert.That(state.RecordCycle(0, beginAdvance, 0, beginAdvance - 256, 4 * segmentSize, minReclaimPercent), Is.True);
            Assert.That(state.RetryAfterTailAddress, Is.EqualTo(beginAdvance - 256 + 4 * segmentSize));
        }

        [Test]
        public void ProductiveCycleDoesNotBackOff()
        {
            const long segmentSize = 1024;
            const int minReclaimPercent = 20;
            var state = new CompactionState();

            Assert.That(state.RecordCycle(0, segmentSize, 10 * segmentSize, 10 * segmentSize + 256, 4 * segmentSize, minReclaimPercent), Is.False);
            Assert.That(state.RetryAfterTailAddress, Is.Zero);
            Assert.That(state.ShouldSkip(10 * segmentSize + 256), Is.False);
        }

        [Test]
        public void LowYieldStateIsPerDatabase()
        {
            const long segmentSize = 1024;
            const int minReclaimPercent = 20;
            var first = new CompactionState();
            var second = new CompactionState();

            Assert.That(first.RecordCycle(0, segmentSize, 0, segmentSize, segmentSize, minReclaimPercent), Is.True);
            Assert.That(first.ShouldSkip(segmentSize), Is.True);
            Assert.That(second.ShouldSkip(segmentSize), Is.False);
        }

        [Test]
        public void EnabledPolicyBoundsCycleToRequestedSegments()
        {
            const long segmentSize = 1024;
            const long beginAddress = 2 * segmentSize;
            const long readOnlyAddress = 40 * segmentSize;

            var untilAddress = CompactionPolicy.GetUntilAddress(beginAddress, readOnlyAddress, segmentSize,
                maxSegments: 32, numSegmentsToCompact: 1, boundCycle: true);

            Assert.That(untilAddress, Is.EqualTo(3 * segmentSize));
        }

        [Test]
        public void DisabledPolicyPreservesCatchUpBehavior()
        {
            const long segmentSize = 1024;
            const long readOnlyAddress = 40 * segmentSize;

            var untilAddress = CompactionPolicy.GetUntilAddress(0, readOnlyAddress, segmentSize,
                maxSegments: 32, numSegmentsToCompact: 1, boundCycle: false);

            Assert.That(untilAddress, Is.EqualTo(9 * segmentSize));
        }

        [Test]
        public void BackoffByteCalculationSaturates()
        {
            Assert.That(CompactionPolicy.GetBackoffBytes(1L << 40, int.MaxValue), Is.EqualTo(long.MaxValue));
        }
    }
}
