// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using BenchmarkDotNetTests;
using NUnit.Framework;

namespace Tsavorite.test.epoch
{
    [TestFixture]
    public class ReleaseWorkloadTests : TestBase
    {
        [TestCase(EpochReleasePressure.Stable, 0)]
        [TestCase(EpochReleasePressure.Stable, 8)]
        [TestCase(EpochReleasePressure.Drain, 0)]
        [TestCase(EpochReleasePressure.Drain, 8)]
        [TestCase(EpochReleasePressure.Waiter, 0)]
        [TestCase(EpochReleasePressure.Waiter, 8)]
        public void WorkersCompleteAndQuiesce(EpochReleasePressure pressure, int jitter)
        {
            using var workload = new EpochReleaseWorkload(2, jitter, pressure);
            workload.Initialize();
            for (var i = 0; i < 3; i++)
            {
                workload.Run();
                workload.Validate();
            }
        }

        [Test]
        public void JitterSequencesAreBoundedAndReproducible()
        {
            var first = EpochReleaseWorkload.CreateDelays(0, 8);
            Assert.That(EpochReleaseWorkload.CreateDelays(0, 8), Is.EqualTo(first));
            Assert.That(EpochReleaseWorkload.CreateDelays(1, 8), Is.Not.EqualTo(first));
            Assert.That(first, Has.All.InRange(0, 8));
        }

        [TestCase(1, 0)]
        [TestCase(3, 0)]
        [TestCase(2, -1)]
        [TestCase(2, 65)]
        public void InvalidParametersFailBeforeStartingWorkers(int threads, int jitter)
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new EpochReleaseWorkload(threads, jitter, EpochReleasePressure.Stable));
        }
    }
}