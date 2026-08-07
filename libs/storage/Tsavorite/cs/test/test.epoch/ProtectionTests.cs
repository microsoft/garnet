// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.epoch
{
    /// <summary>
    /// The protection lifecycle: what <see cref="LightEpoch.Resume"/>, <see cref="LightEpoch.Suspend"/>
    /// and the refresh path leave behind in the epoch table.
    /// </summary>
    [TestFixture]
    public class ProtectionTests : EpochTestBase
    {
        [Test]
        public void UnprotectedThreadHoldsNoSlot()
        {
            Assert.That(epoch.ThisInstanceProtected(), Is.False);
            Assert.That(epoch.TestHookThisThreadEntry(), Is.Zero);
            Assert.That(epoch.TestHookThisThreadAnnouncedEpoch(), Is.Zero);
            Assert.That(epoch.TrySuspend(), Is.False);
        }

        [Test]
        public void AProtectedThreadOwnsAValidSlot()
        {
            using (epoch.ProtectedScope())
                AssertProtectedAt(epoch.CurrentEpoch);
        }

        [Test]
        public void SuspendLeavesTheSlotCompletelyFree()
        {
            epoch.Resume();
            var entry = epoch.TestHookThisThreadEntry();
            epoch.Suspend();

            Assert.That(epoch.TestHookAnnouncedEpochAt(entry), Is.Zero, "the announced epoch was left behind");
            Assert.That(epoch.TestHookThreadIdAt(entry), Is.Zero, "the thread id was left behind");
            Assert.That(epoch.TestHookThisThreadEntry(), Is.Zero);
        }

        [Test]
        public void SuspendResumeKeepsTheThreadProtected()
        {
            using (epoch.ProtectedScope())
            {
                epoch.SuspendResume();

                AssertProtectedAt(epoch.CurrentEpoch);
            }
        }

        [Test]
        public void RefreshRepublishesTheLatestEpochEveryTime()
        {
            using (epoch.ProtectedScope())
            {
                for (var i = 0; i < 16; i++)
                {
                    var announced = epoch.TestHookThisThreadAnnouncedEpoch();

                    var bumped = epoch.BumpCurrentEpoch();
                    AssertProtectedAt(announced, "BumpCurrentEpoch must not republish the announced epoch");

                    epoch.ProtectAndDrain();
                    AssertProtectedAt(bumped);
                }
            }
        }

        [Test]
        public void ProtectionSurvivesRepeatedResumeSuspendCycles()
        {
            for (var i = 0; i < 128; i++)
            {
                epoch.Resume();
                Assert.That(epoch.ThisInstanceProtected(), Is.True);
                epoch.Suspend();
                Assert.That(epoch.ThisInstanceProtected(), Is.False);
            }
        }

        [Test]
        public void ResumeAndSuspendTrackProtectionState()
        {
            Assert.That(epoch.ThisInstanceProtected(), Is.False);

            epoch.Resume();
            Assert.That(epoch.ThisInstanceProtected(), Is.True);

            epoch.Suspend();
            Assert.That(epoch.ThisInstanceProtected(), Is.False);
        }

        [Test]
        public void ResumeIfNotProtectedIsIdempotent()
        {
            Assert.That(epoch.ResumeIfNotProtected(), Is.True);
            Assert.That(epoch.ResumeIfNotProtected(), Is.False);
            Assert.That(epoch.TrySuspend(), Is.True);
            Assert.That(epoch.TrySuspend(), Is.False);
        }

        /// <summary>Everything that must hold while this thread is inside a protected region.</summary>
        void AssertProtectedAt(long announcedEpoch, string because = null)
        {
            var entry = epoch.TestHookThisThreadEntry();

            Assert.That(epoch.ThisInstanceProtected(), Is.True, because);
            Assert.That(entry, Is.GreaterThan(0), because);
            Assert.That(entry, Is.LessThanOrEqualTo(epoch.EntryCount), because);
            Assert.That(epoch.TestHookThreadIdAt(entry), Is.EqualTo(Environment.CurrentManagedThreadId), because);
            Assert.That(epoch.TestHookThisThreadAnnouncedEpoch(), Is.EqualTo(announcedEpoch), because);
            Assert.That(epoch.TestHookAnnouncedEpochAt(entry), Is.EqualTo(announcedEpoch), because);
        }
    }
}