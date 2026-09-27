// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
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

        /// <summary>
        /// A slot reserves one port per standalone assignment plus the cluster node range, so the probe must
        /// enumerate all of them. This is the test that fails when a new assignment is added and the probe stops
        /// covering the ports the projects actually bind.
        /// </summary>
        [Test]
        public void SlotPortsCoverEveryReservedPort()
        {
            const int slot = TestUtils.MaxPortSlot;
            var offset = slot * TestUtils.PortSlotStride;
            var ports = TestUtils.SlotPorts(slot).ToHashSet();

            foreach (var assignment in Enum.GetValues<TestPortAssignment>())
            {
                Assert.That(ports, Does.Contain((int)assignment + offset),
                    $"Slot {slot} hands {assignment} a port the claim-time probe never checks, so a server " +
                    $"stranded on it would not stop the slot from being claimed.");
            }

            for (var node = 0; node < TestUtils.MaxClusterNodesPerSubProject; node++)
            {
                Assert.That(ports, Does.Contain(TestUtils.ClusterPortBandBase + offset + node),
                    $"Cluster node {node} of slot {slot} is not probed, so a node stranded by a crashed run " +
                    $"would read as free.");
            }
        }

        /// <summary>
        /// A server stranded on a non-default assignment has to stop <c>auto</c> from taking that slot, rather
        /// than being discovered later when the owning project binds. GarnetTestAlternate is occupied because
        /// this assembly is its only user and NUnit runs these fixtures sequentially, so the bind cannot
        /// disturb a sibling project sharing the slot.
        /// </summary>
        [Test]
        public void OccupiedNonDefaultAssignmentBlocksTheSlot()
            => AssertOccupiedPortBlocksTheSlot(IPAddress.Loopback);

        /// <summary>
        /// Tests bind every address <see cref="Dns.GetHostAddresses(string)"/> returns, not just the loopbacks,
        /// so a server stranded on one of those has to block the slot as well.
        /// </summary>
        [Test]
        public void OccupiedNonLoopbackAddressBlocksTheSlot()
        {
            var address = NonLoopbackAddress();
            if (address is null)
                Assert.Ignore("This host has no non-loopback IPv4 address to occupy.");

            AssertOccupiedPortBlocksTheSlot(address);
        }

        /// <summary>
        /// The IPv4 wildcard cannot see an IPv6 listener, so this is the case that fails when the probe stops
        /// binding both families. Tests reach the IPv6 loopback through GarnetServerTcpTests and the
        /// multi-endpoint configuration tests.
        /// </summary>
        [Test]
        public void OccupiedIPv6LoopbackBlocksTheSlot()
        {
            if (!Socket.OSSupportsIPv6)
                Assert.Ignore("This host has IPv6 disabled, so no test can strand a listener there.");

            AssertOccupiedPortBlocksTheSlot(IPAddress.IPv6Loopback);
        }

        /// <summary>
        /// A stranded server has to be seen wherever it listens, which is any of the four kinds of address the
        /// tests bind. The port is one the OS hands out for this listener, so the case is exact and unaffected
        /// by other test projects running in parallel.
        /// </summary>
        /// <param name="addressKind">Which of the bound address kinds to strand the listener on.</param>
        [TestCase(BoundAddressKind.Loopback)]
        [TestCase(BoundAddressKind.IPv6Loopback)]
        [TestCase(BoundAddressKind.NonLoopback)]
        [TestCase(BoundAddressKind.Wildcard)]
        public void ListenerIsDetectedOnEveryBoundAddressKind(BoundAddressKind addressKind)
        {
            var address = ResolveAddress(addressKind);
            if (address is null)
                Assert.Ignore($"This host cannot bind {addressKind}.");

            var listener = new TcpListener(address, 0);

            try
            {
                listener.Start();
                var port = ((IPEndPoint)listener.LocalEndpoint).Port;

                Assert.That(TestUtils.IsPortFree(port), Is.False,
                    $"A listener on {address}:{port} reads as free, so a server stranded there would not stop " +
                    $"auto from claiming the slot that hands out that port.");
            }
            finally
            {
                listener.Stop();
            }
        }

        /// <summary>
        /// The kinds of address Garnet tests bind, which the probe has to cover.
        /// </summary>
        public enum BoundAddressKind
        {
            /// <summary>127.0.0.1, the default behind <see cref="TestUtils.EndPoint"/>.</summary>
            Loopback,

            /// <summary>::1, bound by GarnetServerTcpTests and the multi-endpoint configuration tests.</summary>
            IPv6Loopback,

            /// <summary>A routable address, bound by the multi-endpoint configuration tests.</summary>
            NonLoopback,

            /// <summary>0.0.0.0, bound by the cluster tests.</summary>
            Wildcard,
        }

        private static IPAddress ResolveAddress(BoundAddressKind addressKind) => addressKind switch
        {
            BoundAddressKind.Loopback => IPAddress.Loopback,
            BoundAddressKind.IPv6Loopback => Socket.OSSupportsIPv6 ? IPAddress.IPv6Loopback : null,
            BoundAddressKind.NonLoopback => NonLoopbackAddress(),
            _ => IPAddress.Any,
        };

        private static IPAddress NonLoopbackAddress()
            => Dns.GetHostAddresses(TestUtils.GetHostName())
                .FirstOrDefault(a => !IPAddress.IsLoopback(a) && a.AddressFamily == AddressFamily.InterNetwork);

        /// <summary>
        /// Occupies this slot's GarnetTestAlternate port on one address and asserts the slot stops reading as
        /// free. The listener is always released, so a failed assertion cannot strand one on a shared machine.
        /// </summary>
        /// <param name="address">Address to hold the port on.</param>
        private static void AssertOccupiedPortBlocksTheSlot(IPAddress address)
        {
            var slot = TestUtils.PortOffset / TestUtils.PortSlotStride;
            if (!TestUtils.ArePortsFree(slot))
                Assert.Ignore($"Slot {slot} already has ports in use, so blocking it proves nothing.");

            var port = TestUtils.GetTestPort(TestPortAssignment.GarnetTestAlternate);
            var listener = new TcpListener(address, port);

            try
            {
                listener.Start();

                Assert.That(TestUtils.ArePortsFree(slot), Is.False,
                    $"Port {port} ({TestPortAssignment.GarnetTestAlternate}) is occupied on {address}, but slot " +
                    $"{slot} still reads as free, so auto would claim a slot a project cannot bind.");
            }
            finally
            {
                listener.Stop();
            }
        }
    }
}