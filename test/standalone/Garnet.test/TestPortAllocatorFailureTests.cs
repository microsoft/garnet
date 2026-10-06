// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers the <see cref="TestPortAllocator"/> paths that only run when something has gone wrong: a block
    /// held by another test host, a pinned base port, and every block exhausted. They are the paths least
    /// likely to be exercised by an ordinary run and the most costly to get wrong, because each one decides
    /// whether a run fails clearly or proceeds on ports it does not own.
    /// <para>
    /// Every test here leases from a private directory via
    /// <see cref="TestPortAllocator.WithIsolatedLeases"/>. Holding blocks in the shared directory would stop
    /// concurrent runs in other checkouts from starting, which is exactly the failure this allocator exists to
    /// prevent.
    /// </para>
    /// </summary>
    [TestFixture, NonParallelizable]
    public class TestPortAllocatorFailureTests : TestBase
    {
        private static string leaseDirectory;

        /// <summary>
        /// Leases taken by a reservation are held for the life of the process by design, so this directory
        /// cannot be deleted while any test here has reserved from it. It therefore lives outside
        /// <see cref="TestUtils.MethodTestDir"/>, which per-test teardown deletes, and is cleaned up
        /// best-effort at the end of the fixture.
        /// </summary>
        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            leaseDirectory = Path.Combine(Path.GetTempPath(), $"garnet-test-leases-{Environment.ProcessId}");
            _ = Directory.CreateDirectory(leaseDirectory);
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            try
            {
                Directory.Delete(leaseDirectory, recursive: true);
            }
            catch (IOException)
            {
                // Leases this fixture reserved are still open, which is the documented lifetime.
            }
        }

        [TearDown]
        public void TearDown()
        {
            Environment.SetEnvironmentVariable(TestPortAllocator.PinnedBaseEnvVar, null);
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// A block whose lease another host holds must be skipped rather than handed out. This is the property
        /// that keeps concurrent test hosts apart, and nothing else enforces it: the ports themselves are free
        /// at that moment, because the holder has not bound them yet.
        /// </summary>
        [Test]
        public void BlockHeldByAnotherHostIsSkipped()
        {
            TestPortAllocator.WithIsolatedLeases(leaseDirectory, () =>
            {
                var first = TestPortAllocator.Reserve(2, "contention.first");
                var firstBlock = TestPortAllocator.PortBlock(first);

                // Release this process's record of the block but keep the lease held, which is what another
                // host's claim looks like from here.
                TestPortAllocator.WithIsolatedLeases(leaseDirectory, () =>
                {
                    var second = TestPortAllocator.Reserve(2, "contention.first");
                    Assert.That(TestPortAllocator.PortBlock(second), Is.Not.EqualTo(firstBlock),
                        "The same purpose reserved twice returned a block whose lease is still held, so two " +
                        "test hosts could bind the same ports.");
                });
            });
        }

        /// <summary>
        /// Two purposes that hash to the same start block must still end up on different blocks. Forcing the
        /// collision directly, rather than hoping for one, is the only way this path is covered.
        /// </summary>
        [Test]
        public void ReservationSkipsEveryBlockAlreadyLeased()
        {
            TestPortAllocator.WithIsolatedLeases(leaseDirectory, () =>
            {
                var held = new List<FileStream>();
                try
                {
                    // Hold a contiguous span of blocks so that whichever block the hash picks, the walk has to
                    // step over at least some of them.
                    for (var block = 0; block < TestPortAllocator.BlockCount; block += 2)
                    {
                        var lease = TestPortAllocator.HoldLeaseForTest(block);
                        if (lease is not null)
                            held.Add(lease);
                    }

                    var port = TestPortAllocator.Reserve(2, "skip-leased");
                    var chosen = TestPortAllocator.PortBlock(port);

                    Assert.That(chosen % 2, Is.EqualTo(1),
                        $"Block {chosen} was handed out although its lease is held by another host.");
                }
                finally
                {
                    foreach (var lease in held)
                        lease.Dispose();
                }
            });
        }

        /// <summary>
        /// With every block leased the reservation must fail, and fail informatively: a run that continued on
        /// unowned ports is the outcome this whole mechanism exists to avoid. The message has to say how many
        /// blocks were examined and why each was unavailable, because the two causes - genuinely concurrent
        /// runs, and servers stranded by killed runs - need different responses.
        /// </summary>
        [Test]
        public void ExhaustingEveryBlockFailsWithAnActionableMessage()
        {
            TestPortAllocator.WithIsolatedLeases(leaseDirectory, () =>
            {
                var held = new List<FileStream>();
                try
                {
                    for (var block = 0; block < TestPortAllocator.BlockCount; block++)
                    {
                        var lease = TestPortAllocator.HoldLeaseForTest(block);
                        if (lease is not null)
                            held.Add(lease);
                    }

                    var exception = Assert.Throws<InvalidOperationException>(
                        () => TestPortAllocator.Reserve(2, "exhausted"));

                    Assert.That(exception.Message, Does.Contain("exhausted"),
                        "The failure does not name the project that could not get ports.");
                    Assert.That(exception.Message, Does.Contain(TestPortAllocator.BlockCount.ToString()),
                        "The failure does not say how many blocks were examined.");
                    Assert.That(exception.Message, Does.Contain("leased by running test hosts"),
                        "The failure does not distinguish leased blocks from stranded ports.");
                    Assert.That(exception.Message, Does.Contain(TestPortAllocator.PinnedBaseEnvVar),
                        "The failure does not mention the escape hatch.");
                }
                finally
                {
                    foreach (var lease in held)
                        lease.Dispose();
                }
            });
        }

        /// <summary>
        /// A pinned base must be honored exactly, since the point of pinning is to name the port a firewall
        /// was opened for or a failure was seen on.
        /// </summary>
        [Test]
        public void PinnedBasePortIsUsedExactly()
        {
            TestPortAllocator.WithIsolatedLeases(leaseDirectory, () =>
            {
                var free = FindReservablePort();
                Environment.SetEnvironmentVariable(TestPortAllocator.PinnedBaseEnvVar, free.ToString());

                ClassicAssert.AreEqual(free, TestPortAllocator.Reserve(2, "pinned"));
            });
        }

        /// <summary>
        /// Pinning must not become a way to silently reintroduce a collision, so the pinned ports are still
        /// probed and an occupied one fails outright.
        /// </summary>
        [Test]
        public void PinnedBasePortThatIsOccupiedFails()
        {
            TestPortAllocator.WithIsolatedLeases(leaseDirectory, () =>
            {
                var free = FindReservablePort();
                var listener = new TcpListener(IPAddress.Loopback, free);

                try
                {
                    listener.Start();
                    Environment.SetEnvironmentVariable(TestPortAllocator.PinnedBaseEnvVar, free.ToString());

                    var exception = Assert.Throws<InvalidOperationException>(
                        () => TestPortAllocator.Reserve(2, "pinned-occupied"));
                    Assert.That(exception.Message, Does.Contain("already in use"));
                }
                finally
                {
                    listener.Stop();
                }
            });
        }

        [TestCase("not-a-port")]
        [TestCase("80")]
        [TestCase("0")]
        [TestCase("-1")]
        [TestCase("70000")]
        public void PinnedBasePortThatIsInvalidFails(string raw)
        {
            Environment.SetEnvironmentVariable(TestPortAllocator.PinnedBaseEnvVar, raw);

            TestPortAllocator.WithIsolatedLeases(leaseDirectory, () =>
            {
                var exception = Assert.Throws<InvalidOperationException>(
                    () => TestPortAllocator.Reserve(2, "pinned-invalid"));
                Assert.That(exception.Message, Does.Contain(TestPortAllocator.PinnedBaseEnvVar));
            });
        }

        /// <summary>
        /// The teardown report has to fire for a bind failure and stay silent otherwise, or it becomes noise
        /// attached to unrelated failures and stops being read at all.
        /// </summary>
        [Test]
        public void PortConflictReportIsEmittedOnlyForBindFailures()
        {
            Assert.That(TestUtils.BuildPortConflictReport(null), Is.Null);
            Assert.That(TestUtils.BuildPortConflictReport("Expected 3 but was 4"), Is.Null);

            var report = TestUtils.BuildPortConflictReport(
                $"Could not bind 127.0.0.1:{TestUtils.TestPort} (AddressAlreadyInUse).");

            Assert.That(report, Is.Not.Null);
            Assert.That(report, Does.Contain(TestUtils.TestPort.ToString()),
                "The report does not name the ports this host reserved.");
            Assert.That(report, Does.Contain("not a defect in Garnet"),
                "The report does not say that a conflict here is an environment problem, which is the one " +
                "thing the reader needs in order to stop looking for a bug in Garnet.");
        }

        /// <summary>
        /// Straddle points tried before the test gives up. The ports are probed and only then reserved, and
        /// this fixture leases from a private directory, so a concurrent test host is invisible to the probe
        /// and is free to bind or lease the chosen blocks in between. Trying another boundary keeps the
        /// property under test exercised rather than surrendering the run to whichever host got there first.
        /// </summary>
        private const int StraddleCandidates = 8;

        /// <summary>
        /// A pinned base need not be block-aligned, so a run can span two blocks. Leasing only the block
        /// holding the first port would leave the tail unclaimed, and another host could lease the next block
        /// and be handed an overlapping run - both probes passing because neither has bound yet.
        /// </summary>
        [Test]
        public void PinnedRunSpanningTwoBlocksLeasesBoth()
        {
            TestPortAllocator.WithIsolatedLeases(leaseDirectory, () =>
            {
                var ownBlock = TestPortAllocator.PortBlock(TestUtils.TestPort);
                var contended = new List<string>();
                var attempts = 0;

                // Walks the whole block range rather than a fixed window just above the host's own block.
                // That window can run off the end when the host sits near the top of the range, or fall
                // entirely inside the Docker reservation, and in either case the test would report nothing
                // tried on a completely idle host. Ineligible boundaries are stepped over without spending an
                // attempt, so the budget counts only boundaries actually reserved against.
                for (var step = 1; step < TestPortAllocator.BlockCount && attempts < StraddleCandidates; step++)
                {
                    var secondBlock = (ownBlock + step) % TestPortAllocator.BlockCount;
                    var firstBlock = secondBlock - 1;

                    // Block 0 has no predecessor to straddle from.
                    if (firstBlock < 0)
                        continue;
                    // This host's own block is already reserved by this process, so it is never free to pin.
                    if (firstBlock == ownBlock || secondBlock == ownBlock)
                        continue;
                    if (TestPortAllocator.OverlapsDockerReservation(firstBlock) ||
                        TestPortAllocator.OverlapsDockerReservation(secondBlock))
                        continue;

                    attempts++;

                    // One port below a block boundary, so a two-port run straddles it.
                    var basePort = TestPortAllocator.BlockBasePort(secondBlock) - 1;
                    ClassicAssert.AreNotEqual(firstBlock, secondBlock, "The run does not straddle a boundary.");

                    if (!TestPortAllocator.PortsAreFree(basePort, 2))
                    {
                        contended.Add($"{basePort}-{basePort + 1}: in use");
                        continue;
                    }

                    Environment.SetEnvironmentVariable(TestPortAllocator.PinnedBaseEnvVar, basePort.ToString());
                    try
                    {
                        ClassicAssert.AreEqual(basePort, TestPortAllocator.Reserve(2, "pinned-straddle"));
                    }
                    catch (InvalidOperationException ex)
                    {
                        // Another host bound the ports or took a block lease between the probe above and the
                        // reservation. Reserve releases everything it took before throwing, so the next
                        // candidate starts from a clean state.
                        contended.Add($"{basePort}-{basePort + 1}: {ex.Message}");
                        continue;
                    }

                    // The second block must now be unavailable to anyone else. Taking its lease from here stands
                    // in for another test host trying to claim it.
                    using var stolen = TestPortAllocator.HoldLeaseForTest(secondBlock);
                    Assert.That(stolen, Is.Null,
                        $"Block {secondBlock} holds the tail of pinned run {basePort}-{basePort + 1} but was " +
                        $"still available, so another host could reserve an overlapping run.");
                    return;
                }

                Assert.Ignore(
                    $"No straddling port pair could be reserved on this host after {attempts} attempt(s): " +
                    $"{string.Join("; ", contended)}");
            });
        }

        [Test]
        public void IntersectedBlocksCoversEveryBlockARunTouches()
        {
            var boundary = TestPortAllocator.BlockBasePort(10);

            CollectionAssert.AreEqual(new[] { 9, 10 },
                TestPortAllocator.IntersectedBlocks(boundary - 1, 2).ToArray(),
                "A run straddling a boundary must report both blocks.");
            CollectionAssert.AreEqual(new[] { 10 },
                TestPortAllocator.IntersectedBlocks(boundary, 4).ToArray(),
                "A run inside one block must report only that block.");

            // Ports outside the allocatable range belong to no block; PortBlock would return a negative or
            // past-the-end index for them, which names no lease file.
            CollectionAssert.IsEmpty(TestPortAllocator.IntersectedBlocks(1024, 4).ToArray());
            CollectionAssert.IsEmpty(TestPortAllocator.IntersectedBlocks(40000, 4).ToArray());
            CollectionAssert.AreEqual(new[] { 0 },
                TestPortAllocator.IntersectedBlocks(TestPortAllocator.UniverseBase - 1, 2).ToArray(),
                "A run entering the range from below must report only the in-range part.");
        }

        /// <summary>
        /// The report explains a port someone else is holding. The other bind failures have different causes -
        /// a platform-reserved range, an address absent from the machine - so attaching this reasoning to them
        /// would state a confident diagnosis that is wrong.
        /// </summary>
        [TestCase("AccessDenied")]
        [TestCase("AddressNotAvailable")]
        [TestCase("NetworkDown")]
        public void PortConflictReportIsSilentForNonConflictBindErrors(string socketError)
        {
            var message = $"Could not bind 127.0.0.1:{TestUtils.TestPort} ({socketError}). Something else.";

            Assert.That(TestUtils.BuildPortConflictReport(message), Is.Null,
                $"A {socketError} bind failure is not a port conflict, but the port-conflict report fired.");
        }

        /// <summary>
        /// Cluster projects reserve through <c>ClusterTestContext.ReservePorts</c>, which never touches the
        /// standalone port field, yet cluster teardown runs the same report. Consulting that field would tell
        /// a cluster host it had reserved nothing while it held an eight-port run.
        /// </summary>
        [Test]
        public void PortConflictReportNamesTheRunThatCoversThePort()
        {
            var reserved = TestPortAllocator.FindReservation(TestUtils.TestPort);
            Assert.That(reserved, Is.Not.Null, "This host's own reservation is not recorded.");

            var report = TestUtils.BuildPortConflictReport(
                $"Could not bind 127.0.0.1:{TestUtils.TestPort} (AddressAlreadyInUse).");

            Assert.That(report, Does.Contain(reserved.BasePort.ToString()));
            Assert.That(report, Does.Contain(reserved.Purpose),
                "The report does not name the project holding the run, so a cluster failure cannot be " +
                "attributed to its own reservation.");
            Assert.That(report, Does.Contain("not a defect in Garnet"));
        }

        /// <summary>
        /// An endpoint the test built itself carries none of the allocator's guarantees, so claiming the OS
        /// cannot have taken the port would send the reader hunting for software that is not there.
        /// </summary>
        [Test]
        public void PortConflictReportWithholdsGuaranteesForUnreservedPorts()
        {
            var unreserved = TestPortAllocator.UniverseBase + TestPortAllocator.UniverseSize + 5000;
            Assert.That(TestPortAllocator.FindReservation(unreserved), Is.Null);

            var report = TestUtils.BuildPortConflictReport(
                $"Could not bind 127.0.0.1:{unreserved} (AddressAlreadyInUse).");

            Assert.That(report, Is.Not.Null);
            Assert.That(report, Does.Contain("not reserved by this test host"));
            Assert.That(report, Does.Not.Contain("cannot have assigned"),
                "The report claims the non-ephemeral guarantee for a port the allocator never handed out.");
        }

        /// <summary>
        /// Pinning can place a run inside the ephemeral range, which is the one case where the OS really can
        /// take the port between the probe and the bind. Reporting that as external software would blame the
        /// wrong thing for a collision the operator opted into.
        /// </summary>
        [Test]
        public void PortConflictReportWithholdsGuaranteesForPinnedPortsOutsideTheUniverse()
        {
            var outside = TestPortAllocator.UniverseBase + TestPortAllocator.UniverseSize + 1000;
            if (!TestPortAllocator.PortsAreFree(outside, 2))
                Assert.Ignore($"Ports {outside}-{outside + 1} are in use on this host.");

            TestPortAllocator.WithIsolatedLeases(leaseDirectory, () =>
            {
                Environment.SetEnvironmentVariable(TestPortAllocator.PinnedBaseEnvVar, outside.ToString());
                ClassicAssert.AreEqual(outside, TestPortAllocator.Reserve(2, "pinned-outside"));

                var report = TestUtils.BuildPortConflictReport(
                    $"Could not bind 127.0.0.1:{outside} (AddressAlreadyInUse).");

                Assert.That(report, Is.Not.Null);
                Assert.That(report, Does.Contain("may itself have assigned"));
                Assert.That(report, Does.Not.Contain("not a defect in Garnet"),
                    "A pinned port inside the ephemeral range is exactly the case the allocator cannot rule " +
                    "out, so it must not be reported as certainly external software.");
            });
        }

        /// <summary>
        /// Finds a port inside the allocatable range that a pinned reservation could actually take: its ports
        /// must be free <em>and</em> its block's lease unheld. Checking only the ports is not enough, because a
        /// reservation earlier in this fixture holds its lease for the life of the process without binding
        /// anything, so its ports still read as free. Call from inside
        /// <see cref="TestPortAllocator.WithIsolatedLeases"/> so the lease check sees the private directory.
        /// </summary>
        /// <returns>A port a pinned reservation can take.</returns>
        private static int FindReservablePort()
        {
            for (var port = TestUtils.TestPort + TestPortAllocator.BlockPorts;
                 port < TestPortAllocator.UniverseBase + TestPortAllocator.UniverseSize - 2;
                 port += TestPortAllocator.BlockPorts)
            {
                var block = TestPortAllocator.PortBlock(port);
                if (TestPortAllocator.OverlapsDockerReservation(block) || !TestPortAllocator.PortsAreFree(port, 2))
                    continue;

                using var probe = TestPortAllocator.HoldLeaseForTest(block);
                if (probe is not null)
                    return port;
            }

            Assert.Ignore("No reservable port available inside the allocatable range.");
            return 0;
        }
    }
}