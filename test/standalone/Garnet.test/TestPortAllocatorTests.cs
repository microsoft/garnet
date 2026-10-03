// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers <see cref="TestPortAllocator"/>. The properties asserted here are the ones that make the
    /// allocator safe rather than merely functional: that allocatable ports stay clear of the ranges the OS
    /// assigns to outbound connections, that the probe sequence reaches every block, and that a port already
    /// spoken for is never handed out.
    /// </summary>
    [TestFixture]
    public class TestPortAllocatorTests : TestBase
    {
        /// <summary>
        /// Lowest ephemeral port on any supported platform: Linux defaults to 32768, Windows and macOS to
        /// 49152. A port at or above this can be handed to an outbound connection between the moment it is
        /// probed and the moment the server binds it, which no amount of probing can prevent.
        /// </summary>
        private const int LowestEphemeralPortAcrossPlatforms = 32768;

        [Test]
        public void UniverseStaysBelowEveryPlatformsEphemeralRange()
        {
            var top = TestPortAllocator.UniverseBase + TestPortAllocator.UniverseSize;

            Assert.That(top, Is.LessThanOrEqualTo(LowestEphemeralPortAcrossPlatforms),
                $"The allocatable range reaches {top}, at or above the lowest ephemeral floor across " +
                $"supported platforms ({LowestEphemeralPortAcrossPlatforms}). Ports there can be assigned to " +
                $"an outbound connection between the probe and the bind, which presents as a flaky test.");
        }

        [Test]
        public void UniverseStaysAboveThePrivilegedAndCrowdedRange()
        {
            Assert.That(TestPortAllocator.UniverseBase, Is.GreaterThanOrEqualTo(1024),
                "Ports below 1024 require privilege.");

            // 10000-10002 is Azurite, which the Azure tests in this suite connect to.
            Assert.That(TestPortAllocator.UniverseBase, Is.GreaterThan(10002),
                "The allocatable range must not overlap the Azurite ports the Azure tests use.");
        }

        /// <summary>
        /// The probe sequence advances by a fixed stride modulo <see cref="TestPortAllocator.BlockCount"/> and
        /// visits every block exactly once only when that stride is coprime with the count. Because the count
        /// is a power of two, that reduces to the stride being odd - which is why the allocator forces the low
        /// bit. An even stride would silently skip blocks, so a free one could be missed entirely.
        /// </summary>
        [Test]
        public void BlockCountIsAPowerOfTwoSoAnOddStrideCoversEveryBlock()
        {
            Assert.That(TestPortAllocator.BlockCount & (TestPortAllocator.BlockCount - 1), Is.Zero,
                $"BlockCount is {TestPortAllocator.BlockCount}, not a power of two, so an odd stride no " +
                $"longer guarantees the probe sequence reaches every block.");
        }

        [TestCase(1)]
        [TestCase(3)]
        [TestCase(511)]
        [TestCase(12345)]
        public void OddStrideVisitsEveryBlockExactlyOnce(int rawStride)
        {
            var count = TestPortAllocator.BlockCount;
            var stride = rawStride | 1;
            var visited = new HashSet<int>();
            var block = 7;

            for (var i = 0; i < count; i++, block = (block + stride) % count)
                Assert.That(visited.Add(block), Is.True, $"Block {block} was visited twice within one pass.");

            ClassicAssert.AreEqual(count, visited.Count);
        }

        /// <summary>
        /// The counterexample for the test above: an even stride cannot reach every block, so the allocator's
        /// exhaustion message would be wrong and a free block could be skipped forever.
        /// </summary>
        [Test]
        public void EvenStrideFailsToVisitEveryBlock()
        {
            var count = TestPortAllocator.BlockCount;
            var visited = new HashSet<int>();
            var block = 0;

            for (var i = 0; i < count; i++, block = (block + 2) % count)
                _ = visited.Add(block);

            Assert.That(visited.Count, Is.LessThan(count),
                "An even stride reached every block, so the power-of-two assumption no longer holds.");
        }

        [Test]
        public void BlockHoldsEnoughPortsForTheLargestCluster()
        {
            Assert.That(TestPortAllocator.BlockPorts,
                Is.GreaterThanOrEqualTo(TestUtils.MaxClusterNodesPerSubProject),
                "A cluster sub-project binds one contiguous port per node, so its nodes must fit in one block.");
            Assert.That(TestPortAllocator.BlockPorts, Is.GreaterThanOrEqualTo(TestUtils.StandalonePortCount));
        }

        [Test]
        public void BlockArithmeticRoundTrips()
        {
            for (var block = 0; block < TestPortAllocator.BlockCount; block += 37)
            {
                var basePort = TestPortAllocator.BlockBasePort(block);
                ClassicAssert.AreEqual(block, TestPortAllocator.PortBlock(basePort));
                ClassicAssert.AreEqual(block, TestPortAllocator.PortBlock(basePort + TestPortAllocator.BlockPorts - 1));
            }
        }

        /// <summary>
        /// The Docker image validation harness allocates container ports from a fixed base of its own rather
        /// than through this allocator, and can run at the same time as the managed suite. This reads the base
        /// out of the harness so that moving either side without the other fails here rather than as an
        /// intermittent container start failure.
        /// </summary>
        [Test]
        public void DockerTestPortsAreCarvedOut()
        {
            var harness = Path.Combine(TestUtils.RootTestsProjectPath, "docker-tests", "validate_docker_images.py");
            if (!File.Exists(harness))
                Assert.Ignore($"Docker validation harness not found at '{harness}'.");

            var match = Regex.Match(File.ReadAllText(harness), @"^BASE_PORT\s*=\s*(\d+)", RegexOptions.Multiline);
            Assert.That(match.Success, Is.True,
                $"Could not read BASE_PORT from '{harness}'; the carve-out can no longer be verified.");

            var harnessBase = int.Parse(match.Groups[1].Value);
            ClassicAssert.AreEqual(TestPortAllocator.DockerReservedBase, harnessBase,
                $"{nameof(TestPortAllocator.DockerReservedBase)} no longer matches the harness's BASE_PORT.");

            // The harness uses base + phaseOffset + imageIndex, with phase offsets up to 400.
            const int highestHarnessOffset = 500;
            Assert.That(TestPortAllocator.DockerReservedCount, Is.GreaterThanOrEqualTo(highestHarnessOffset),
                "The reservation is narrower than the span the harness actually uses.");

            for (var block = 0; block < TestPortAllocator.BlockCount; block++)
            {
                var basePort = TestPortAllocator.BlockBasePort(block);
                var overlaps = basePort < harnessBase + TestPortAllocator.DockerReservedCount
                    && harnessBase < basePort + TestPortAllocator.BlockPorts;

                if (overlaps)
                {
                    Assert.That(TestPortAllocator.OverlapsDockerReservation(block), Is.True,
                        $"Block {block} ({basePort}-{basePort + TestPortAllocator.BlockPorts - 1}) meets the " +
                        $"Docker harness range but is not carved out, so both could bind the same port.");
                }
            }
        }

        [Test]
        public void CarveOutCoversEveryPortTheDockerHarnessUses()
        {
            for (var port = TestPortAllocator.DockerReservedBase;
                 port < TestPortAllocator.DockerReservedBase + TestPortAllocator.DockerReservedCount;
                 port++)
            {
                if (port < TestPortAllocator.UniverseBase ||
                    port >= TestPortAllocator.UniverseBase + TestPortAllocator.UniverseSize)
                {
                    // Outside the universe entirely, so it can never be handed out.
                    continue;
                }

                Assert.That(TestPortAllocator.OverlapsDockerReservation(TestPortAllocator.PortBlock(port)), Is.True,
                    $"Port {port} is reserved for the Docker harness but sits in a block the allocator can hand out.");
            }
        }

        /// <summary>
        /// A listener has to be seen wherever it sits, which is any of the address kinds the suite binds. The
        /// port is one the OS hands out for this listener, so the case is exact and unaffected by other test
        /// projects running in parallel.
        /// </summary>
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

                Assert.That(TestPortAllocator.IsPortFree(port), Is.False,
                    $"A listener on {address}:{port} reads as free, so the allocator would hand out a port a " +
                    $"server cannot bind.");
                Assert.That(TestPortAllocator.PortsAreFree(port - 1, 3), Is.False,
                    "A run containing an occupied port must not read as free.");
            }
            finally
            {
                listener.Stop();
            }
        }

        [Test]
        public void ReservedPortsForThisHostAreInsideTheUniverseAndUsable()
        {
            Assert.That(TestUtils.TestPort, Is.GreaterThanOrEqualTo(TestPortAllocator.UniverseBase));
            Assert.That(TestUtils.TestPort,
                Is.LessThan(TestPortAllocator.UniverseBase + TestPortAllocator.UniverseSize));
            ClassicAssert.AreEqual(TestUtils.TestPort + 1, TestUtils.AlternateTestPort);

            // The reserved run must not straddle a block boundary, or part of it would be unleased.
            ClassicAssert.AreEqual(
                TestPortAllocator.PortBlock(TestUtils.TestPort),
                TestPortAllocator.PortBlock(TestUtils.TestPort + TestUtils.StandalonePortCount - 1));
        }

        [Test]
        public void ReserveRejectsRunsLargerThanABlock()
        {
            _ = Assert.Throws<ArgumentOutOfRangeException>(
                () => TestPortAllocator.Reserve(TestPortAllocator.BlockPorts + 1, "oversized"));
            _ = Assert.Throws<ArgumentOutOfRangeException>(() => TestPortAllocator.Reserve(0, "empty"));
        }

        /// <summary>
        /// A second reservation in the same process must not return ports already handed out here, which is
        /// what lets one test host hold both a standalone and a cluster run.
        /// </summary>
        [Test]
        public void SecondReservationDoesNotOverlapTheFirst()
        {
            var first = TestUtils.TestPort;
            var second = TestPortAllocator.Reserve(4, "TestPortAllocatorTests.second");

            Assert.That(TestPortAllocator.PortBlock(second),
                Is.Not.EqualTo(TestPortAllocator.PortBlock(first)),
                "A second reservation landed in the block already held by this host.");
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
    }
}