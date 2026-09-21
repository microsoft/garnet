// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Keeps the cluster port band mirrored in <see cref="TestUtils"/> in sync with
    /// <see cref="ClusterPortAssignment"/>, which lives here and is not compiled into Garnet.test. Without this
    /// the mirrored constants would silently go stale as cluster assignments are added.
    /// </summary>
    [TestFixture]
    public class ClusterPortBandTests : TestBase
    {
        [Test]
        public void MirroredClusterBandMatchesAssignments()
        {
            var ports = Enum.GetValues<ClusterPortAssignment>().Select(p => (int)p).ToArray();

            ClassicAssert.AreEqual(ports.Min(), TestUtils.ClusterPortBandBase,
                $"{nameof(TestUtils.ClusterPortBandBase)} no longer matches the lowest {nameof(ClusterPortAssignment)}.");
            ClassicAssert.AreEqual(ports.Max() + TestUtils.PortsPerAssignment, TestUtils.ClusterPortBandTop,
                $"{nameof(TestUtils.ClusterPortBandTop)} no longer matches the highest {nameof(ClusterPortAssignment)}.");
        }

        /// <summary>
        /// The cluster nodes of one sub-project must all fit inside its reserved width.
        /// </summary>
        [Test]
        public void ClusterAssignmentsAreSpacedByReservedWidth()
        {
            var ports = Enum.GetValues<ClusterPortAssignment>().Select(p => (int)p).OrderBy(p => p).ToArray();

            for (var i = 1; i < ports.Length; i++)
            {
                Assert.That(ports[i] - ports[i - 1], Is.GreaterThanOrEqualTo(TestUtils.PortsPerAssignment),
                    $"Cluster assignments {ports[i - 1]} and {ports[i]} are closer than the reserved width.");
            }
        }

        [Test]
        public void PortBandsAreValid() => TestUtils.ValidatePortBands();

        [Test]
        public void UnsetSlotKeepsUpstreamPort()
        {
            if (!string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(TestUtils.PortSlotEnvVar)))
                Assert.Ignore($"{TestUtils.PortSlotEnvVar} is set for this run.");

            ClassicAssert.AreEqual(0, TestUtils.PortOffset);
            ClassicAssert.AreEqual((int)ClusterPortAssignment.ClusterTest, ClusterTestContext.Port);
        }
    }
}