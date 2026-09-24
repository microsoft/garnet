// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Net;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers port slot parsing and the band invariants the slot arithmetic depends on. The band assertions are
    /// spelled out individually so a future port assignment names the invariant it broke.
    /// </summary>
    [TestFixture]
    public class PortSlotTests : TestBase
    {
        [TestCase("0", 0)]
        [TestCase("1", 1)]
        [TestCase("7", TestUtils.MaxPortSlot)]
        [TestCase(" 3 ", 3)]
        public void ParsesExplicitSlot(string raw, int expected)
            => ClassicAssert.AreEqual(expected, TestUtils.ParseExplicitPortSlot(raw));

        [TestCase("8")]
        [TestCase("-1")]
        [TestCase("auto")]
        [TestCase("abc")]
        [TestCase("1.5")]
        [TestCase("")]
        public void RejectsInvalidSlot(string raw)
        {
            var ex = Assert.Throws<InvalidOperationException>(() => TestUtils.ParseExplicitPortSlot(raw));
            Assert.That(ex.Message, Does.Contain(TestUtils.PortSlotEnvVar));
        }

        /// <summary>
        /// With the variable unset the ports must match upstream exactly, which is what CI relies on.
        /// </summary>
        [Test]
        public void UnsetSlotKeepsUpstreamPorts()
        {
            if (!string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(TestUtils.PortSlotEnvVar)))
                Assert.Ignore($"{TestUtils.PortSlotEnvVar} is set for this run.");

            ClassicAssert.AreEqual(0, TestUtils.PortOffset);
            ClassicAssert.AreEqual((int)TestPortAssignment.GarnetTest, TestUtils.TestPort);
            ClassicAssert.AreEqual((int)TestPortAssignment.GarnetTest, ((IPEndPoint)TestUtils.EndPoint).Port);
        }

        /// <summary>
        /// Runs unconditionally so CI enforces the bands even though CI never selects a slot.
        /// </summary>
        [Test]
        public void PortBandsAreValid() => TestUtils.ValidatePortBands();

        [Test]
        public void StandaloneBandFitsWithinSlotStride()
        {
            var ports = Enum.GetValues<TestPortAssignment>().Select(p => (int)p).ToArray();
            var width = ports.Max() + TestUtils.PortsPerAssignment - ports.Min();

            Assert.That(width, Is.LessThanOrEqualTo(TestUtils.PortSlotStride),
                $"The standalone port band grew to {width} ports, so slot n now overlaps slot n+1.");
        }

        [Test]
        public void ClusterBandFitsWithinSlotStride()
        {
            var width = TestUtils.ClusterPortBandTop - TestUtils.ClusterPortBandBase;

            Assert.That(width, Is.LessThanOrEqualTo(TestUtils.PortSlotStride),
                $"The cluster port band grew to {width} ports, so slot n now overlaps slot n+1.");
        }

        [Test]
        public void TopSlotStaysBelowEphemeralPortFloor()
        {
            var top = Enum.GetValues<TestPortAssignment>().Max(p => (int)p) + TestUtils.PortsPerAssignment
                + (TestUtils.MaxPortSlot * TestUtils.PortSlotStride);

            Assert.That(top, Is.LessThan(TestUtils.EphemeralPortFloor),
                $"Slot {TestUtils.MaxPortSlot} reaches {top}, which collides with the ephemeral port range.");
        }

        [Test]
        public void TopClusterSlotStaysBelowStandaloneBand()
        {
            var top = TestUtils.ClusterPortBandTop + (TestUtils.MaxPortSlot * TestUtils.PortSlotStride);

            Assert.That(top, Is.LessThan(Enum.GetValues<TestPortAssignment>().Min(p => (int)p)),
                $"Slot {TestUtils.MaxPortSlot} of the cluster band reaches {top}, inside the standalone band.");
        }

        /// <summary>
        /// Slot 0's offset is 0, which is what every checkout that does not set the variable already uses, so
        /// handing it to <c>auto</c> would produce a run with no isolation at all.
        /// </summary>
        [Test]
        public void AutoNeverClaimsTheUnshiftedSlot()
        {
            Assert.That(TestUtils.MinAutoPortSlot, Is.GreaterThan(0),
                "auto must not claim slot 0; its offset is 0, the port used by checkouts that set no slot.");
            Assert.That(TestUtils.MinAutoPortSlot, Is.LessThanOrEqualTo(TestUtils.MaxPortSlot),
                "There would be no slot left for auto to claim.");
        }

        /// <summary>
        /// The stride must not be a distance between two existing assignments, or one assignment under a slot
        /// would land exactly on another under a different slot. This is why the stride is not 1000: that is
        /// precisely the distance from GarnetTest to GarnetTestAlternate.
        /// </summary>
        [Test]
        public void SlotStrideIsNotADistanceBetweenAssignments()
        {
            var ports = Enum.GetValues<TestPortAssignment>().Select(p => (int)p).ToArray();

            foreach (var low in ports)
            {
                foreach (var high in ports)
                {
                    Assert.That(high - low, Is.Not.EqualTo(TestUtils.PortSlotStride),
                        $"Ports {low} and {high} are exactly one stride apart, so they collide across slots.");
                }
            }
        }
    }
}