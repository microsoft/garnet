// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Garnet.server.Auth.Settings;
using Garnet.server.TLS;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Interfaces;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;
using Tsavorite.devices;

namespace Garnet.test
{
    public struct StoreAddressInfo
    {
        public long BeginAddress;
        public long HeadAddress;
        public long ReadOnlyAddress;
        public long FlushedUntilAddress;
        public long TailAddress;
        public long MemorySize;
        public long ReadCacheHeadAddress;
        public long ReadCacheBeginAddress;
        public long ReadCacheTailAddress;
    }

    /// <summary>
    /// Get all attributes that start with given prefix.
    /// </summary>
    [AttributeUsage(AttributeTargets.Parameter)]
    public sealed class ValuesPrefixAttribute : NUnitAttribute, IParameterDataSource
    {
        readonly string prefix;

        public ValuesPrefixAttribute(string prefix)
        {
            this.prefix = prefix;
        }

        public IEnumerable GetData(IParameterInfo parameter)
        {
            return new ValuesAttribute()
                .GetData(parameter)
                .Cast<object>()
                .Where(e => e.ToString().StartsWith(prefix));
        }
    }

    public enum RevivificationMode
    {
        NoReviv = 0,
        UseReviv = 1,
    }

    /// <summary>
    /// Unique base port for each test sub-project, enabling parallel test runs without port conflicts.
    /// </summary>
    public enum TestPortAssignment
    {
        GarnetTest = 33278,
        GarnetTestAlternate = 34278,    // Alternate port for GarnetTest; used by NetworkTests.cs
        GarnetTestAcl = 34300,
        GarnetTestCollections = 34400,
        GarnetTestComplexString = 34500,
        GarnetTestExtensions = 34600,
        GarnetTestRangeIndex = 34700,
        GarnetTestScripting = 34800,
        GarnetTestVectorSet = 34900,
        GarnetTestBfTreeInterop = 35000,
    }

    internal static class TestUtils
    {
        // Use 4KB page size for tests, independent of device sector size
        public const int MinKvLogPageSizeBits = 12; // TODO: Same as LogSettings.kMinPageSizeBits; need to centralize
        public const int MinKvLogPageSize = 1 << MinKvLogPageSizeBits;
        public const int MinKvLogPageSizeInKB = MinKvLogPageSize / 1024;

        /// <summary>
        /// Environment variable selecting a port slot, so checkouts sharing a machine do not bind the same
        /// ports. Unset or empty yields offset 0, which is identical to upstream behavior and is what CI uses.
        /// Accepts <c>auto</c> to claim a slot for this checkout, or an integer in [0, <see cref="MaxPortSlot"/>].
        /// </summary>
        internal const string PortSlotEnvVar = "GARNET_TEST_PORT_SLOT";

        /// <summary>
        /// Distance between consecutive port slots. Must exceed the width of both port bands, otherwise slot n
        /// overlaps slot n+1. A stride of 1000 would be actively wrong: 33278 + 1000 is
        /// <see cref="TestPortAssignment.GarnetTestAlternate"/>.
        /// </summary>
        internal const int PortSlotStride = 2000;

        /// <summary>
        /// Highest selectable port slot. <see cref="ValidatePortBands"/> verifies the resulting ports stay below
        /// <see cref="EphemeralPortFloor"/>.
        /// </summary>
        internal const int MaxPortSlot = 7;

        /// <summary>
        /// Lowest slot <c>auto</c> will claim. Slot 0 is excluded because its offset is 0, which is the port
        /// every checkout that does not set <see cref="PortSlotEnvVar"/> already uses, so claiming it would leave
        /// the run exposed to exactly the collisions slots exist to prevent. Slot 0 stays reachable explicitly,
        /// since pinning the upstream ports is occasionally useful.
        /// </summary>
        internal const int MinAutoPortSlot = 1;

        /// <summary>
        /// Ports a cluster sub-project can bind, one per node from its base. The largest cluster test today
        /// creates 5 nodes; the headroom stays well inside <see cref="PortsPerAssignment"/>.
        /// </summary>
        internal const int MaxClusterNodesPerSubProject = 8;

        /// <summary>
        /// Ports reserved per assignment, matching the spacing convention of <see cref="TestPortAssignment"/> and
        /// ClusterPortAssignment. The bands are validated against this reserved width rather than against current
        /// usage, so a test that grows to use more of its range does not silently invalidate the slot arithmetic.
        /// </summary>
        internal const int PortsPerAssignment = 100;

        /// <summary>
        /// Default lower bound of the Windows ephemeral port range. Binds at or above this collide intermittently
        /// with the OS handing the same port to an outbound socket, which presents as flaky tests.
        /// </summary>
        internal const int EphemeralPortFloor = 49152;

        /// <summary>
        /// Lowest cluster port assignment. Mirrored from ClusterPortAssignment, which lives in Garnet.test.cluster
        /// and is not compiled into Garnet.test; ClusterPortBandTests keeps these in sync with the enum.
        /// </summary>
        internal const int ClusterPortBandBase = 7000;

        /// <summary>
        /// Top of the cluster port band: the highest cluster assignment plus its reserved width.
        /// </summary>
        internal const int ClusterPortBandTop = 8100 + PortsPerAssignment;

        /// <summary>
        /// Amount added to every test port so concurrent checkouts do not collide. Declared before
        /// <see cref="TestPort"/> because static field initializers run in textual order.
        /// <para>
        /// Resolving this can fail — an unparseable <see cref="PortSlotEnvVar"/>, every slot held by another
        /// checkout, or the bookkeeping files being unwritable. The failure is deliberately left to propagate
        /// rather than falling back to offset 0: that fallback would put the run on the shared default ports and
        /// reintroduce exactly the silent cross-checkout corruption slots exist to prevent. Because the
        /// initializer is forced from an assembly-level <c>[OneTimeSetUp]</c> by
        /// <see cref="EnsurePortSlotResolved"/>, the effect is that NUnit fails that fixture, every test in the
        /// assembly is reported as errored, none of them run, and the process exits non-zero. The CLR also caches
        /// a failed type initializer, so every later use of this class rethrows; there is no path on which some
        /// tests proceed against unknown ports.
        /// </para>
        /// </summary>
        internal static readonly int PortOffset = ResolvePortOffset();

        public static int TestPort = GetTestPort(TestPortAssignment.GarnetTest);    // No OneTimeSetUp needed for "Garnet.test" to set this

        /// <summary>
        /// Test server end point
        /// </summary>
        public static EndPoint EndPoint = new IPEndPoint(IPAddress.Loopback, TestPort);

        /// <summary>
        /// Resolves an assignment to the port this test host should actually use.
        /// </summary>
        /// <param name="port">The sub-project's port assignment.</param>
        /// <returns>The assigned port shifted by <see cref="PortOffset"/>.</returns>
        internal static int GetTestPort(TestPortAssignment port) => (int)port + PortOffset;

        /// <summary>
        /// Sets the test port for the current sub-project, updating both <see cref="TestPort"/> and <see cref="EndPoint"/>.
        /// Call from a <c>[SetUpFixture]</c> in each sub-project.
        /// </summary>
        public static void SetTestPort(TestPortAssignment port)
        {
            TestPort = GetTestPort(port);
            EndPoint = new IPEndPoint(IPAddress.Loopback, TestPort);
            EnsurePortAvailable(TestPort, port.ToString());
        }

        /// <summary>
        /// Forces the port slot to be resolved now, at the point of this call.
        /// <para>
        /// The slot is claimed by the <see cref="PortOffset"/> field initializer, which the CLR runs as part of
        /// this class's static constructor the first time anything touches the class. <c>RunClassConstructor</c>
        /// is the mechanism: it runs that static constructor on demand, so <see cref="ResolvePortOffset"/>
        /// executes here rather than at some arbitrary later point. Nothing else pulls it forward — projects
        /// that rely on the static port defaults have no <c>[SetUpFixture]</c> calling
        /// <see cref="SetTestPort"/>, and the remaining members read only constants — so without this the slot
        /// would be claimed lazily and any failure would surface from whichever test first touched a port. See
        /// <see cref="PortOffset"/> for what a failure does to the run.
        /// </para>
        /// <para>
        /// The underlying exception is rethrown in place of the type initializer wrapper, so the actionable
        /// message is the one reported rather than "the type initializer threw an exception".
        /// </para>
        /// </summary>
        internal static void EnsurePortSlotResolved()
        {
            try
            {
                // Runs the static constructor, evaluating the PortOffset initializer and claiming the slot.
                RuntimeHelpers.RunClassConstructor(typeof(TestUtils).TypeHandle);
            }
            catch (TypeInitializationException e) when (e.InnerException is not null)
            {
                ExceptionDispatchInfo.Capture(e.InnerException).Throw();
            }
        }

        /// <summary>
        /// Reads <see cref="PortSlotEnvVar"/> and converts it to a port offset.
        /// </summary>
        /// <returns>The offset to add to every assigned port; 0 when no slot is selected.</returns>
        private static int ResolvePortOffset()
        {
            var raw = Environment.GetEnvironmentVariable(PortSlotEnvVar);
            if (string.IsNullOrWhiteSpace(raw))
                return 0;

            // Only validated when slots are in use, so the unset path cannot be broken by a band regression.
            ValidatePortBands();

            return raw.Trim().Equals("auto", StringComparison.OrdinalIgnoreCase)
                ? ClaimCheckoutPortSlot() * PortSlotStride
                : ParseExplicitPortSlot(raw) * PortSlotStride;
        }

        /// <summary>
        /// Parses an explicit slot number.
        /// </summary>
        /// <param name="raw">The raw environment variable value.</param>
        /// <returns>The slot number.</returns>
        internal static int ParseExplicitPortSlot(string raw)
        {
            if (int.TryParse(raw.Trim(), out var slot) && slot >= 0 && slot <= MaxPortSlot)
                return slot;

            throw new InvalidOperationException(
                $"{PortSlotEnvVar} must be 'auto' or an integer in [0, {MaxPortSlot}]; got '{raw}'.");
        }

        /// <summary>
        /// Verifies that the port bands still satisfy the invariants the slot arithmetic depends on. Every value
        /// is derived from the assignment enums, so adding an assignment that breaks a slot fails here loudly
        /// instead of silently reintroducing cross-checkout port collisions.
        /// </summary>
        internal static void ValidatePortBands()
        {
            var standalone = Enum.GetValues<TestPortAssignment>().Select(p => (int)p).ToArray();
            var standaloneBase = standalone.Min();
            var standaloneTop = standalone.Max() + PortsPerAssignment;

            ValidateBandWidth("standalone", standaloneBase, standaloneTop);
            ValidateBandWidth("cluster", ClusterPortBandBase, ClusterPortBandTop);

            var highestStandalone = standaloneTop + (MaxPortSlot * PortSlotStride);
            if (highestStandalone >= EphemeralPortFloor)
            {
                throw new InvalidOperationException(
                    $"Port slot {MaxPortSlot} puts the top of the standalone test port band at {highestStandalone}, " +
                    $"at or above the ephemeral port floor ({EphemeralPortFloor}), where binds fail intermittently. " +
                    $"Lower {nameof(MaxPortSlot)} to {(EphemeralPortFloor - 1 - standaloneTop) / PortSlotStride}, or " +
                    $"relocate the band into the unused range between {ClusterPortBandTop} and {standaloneBase}.");
            }

            var highestCluster = ClusterPortBandTop + (MaxPortSlot * PortSlotStride);
            if (highestCluster >= standaloneBase)
            {
                throw new InvalidOperationException(
                    $"Port slot {MaxPortSlot} puts the top of the cluster test port band at {highestCluster}, which " +
                    $"reaches the standalone band's slot 0 base ({standaloneBase}). Lower {nameof(MaxPortSlot)} to " +
                    $"{(standaloneBase - 1 - ClusterPortBandTop) / PortSlotStride}, or move the two bands apart.");
            }
        }

        /// <summary>
        /// Verifies one band is narrower than the slot stride.
        /// </summary>
        /// <param name="name">Band name, used in the failure message.</param>
        /// <param name="bandBase">Lowest port in the band.</param>
        /// <param name="bandTop">Highest port in the band, inclusive of its reserved width.</param>
        private static void ValidateBandWidth(string name, int bandBase, int bandTop)
        {
            var width = bandTop - bandBase;
            if (width > PortSlotStride)
            {
                throw new InvalidOperationException(
                    $"The {name} test port band spans {width} ports ({bandBase}-{bandTop}), which exceeds " +
                    $"{nameof(PortSlotStride)} ({PortSlotStride}), so port slot n would overlap slot n+1. Raise " +
                    $"{nameof(PortSlotStride)} to at least {width} and re-check {nameof(MaxPortSlot)}, or move the " +
                    $"band's outlying assignments closer together.");
            }
        }

        /// <summary>
        /// This process's claim on its port slot, one of possibly several held against the same slot by test
        /// hosts from this checkout. The field exists to root the stream for the lifetime of the test host: the
        /// lock lives in the open handle, so letting this be collected and finalized would silently release the
        /// slot mid-run. It is never read and must not be removed or replaced with a local. Deliberately never
        /// disposed either — the OS releases it on exit, crash, or kill, which is exactly how a slot becomes free.
        /// </summary>
        private static FileStream portSlotHolder;

        /// <summary>
        /// Claims a port slot for this checkout, or rejoins the one it already holds. Claiming per checkout rather
        /// than per process lets one checkout run all of its test projects in parallel on a single slot: their
        /// base ports already differ, so the slot only has to move the whole checkout clear of other checkouts.
        /// <para>
        /// Two kinds of file track a slot, and they identify different things. <c>slot{n}.owner</c> names a
        /// <em>directory</em>: the checkout the slot belongs to, recorded as text and kept indefinitely, so a
        /// checkout keeps returning to the same slot. <c>slot{n}.holder-{pid}</c> names a <em>process</em>: one
        /// per live test host, empty because the lock on it is the whole point, released by the OS when that
        /// process dies. So a slot has exactly one owner and any number of holders — one per concurrent
        /// <c>dotnet test</c> from that checkout, which the parallel test-project runsettings makes routine.
        /// </para>
        /// <para>
        /// Because holders are per live process, the supply limits how many checkouts can run tests
        /// simultaneously, not how many checkouts may exist. Exhaustion therefore means either more than
        /// <see cref="MaxPortSlot"/> concurrent runs, or servers stranded by earlier runs that still hold ports
        /// while holding no lock. Both are reported per slot so they can be told apart, and the throw stops the
        /// run outright — see <see cref="PortOffset"/>.
        /// </para>
        /// </summary>
        /// <returns>The claimed slot number.</returns>
        private static int ClaimCheckoutPortSlot()
        {
            var dir = Path.Combine(Path.GetTempPath(), "garnet-test-port-slots");
            _ = Directory.CreateDirectory(dir);

            var checkoutKey = GetCheckoutKey();
            using var claimLock = AcquireClaimLock(dir);

            // Rejoin a slot this checkout already owns. While another test host from this checkout still holds
            // the slot, its ports are legitimately in use by that host, so they are not probed. Once no holder
            // is left the ownership record is only a hint: another checkout may have taken those ports with an
            // explicit slot in the meantime, so they have to be re-checked before trusting it.
            for (var slot = MinAutoPortSlot; slot <= MaxPortSlot; slot++)
            {
                if (!string.Equals(ReadSlotOwner(dir, slot), checkoutKey, StringComparison.Ordinal))
                    continue;

                if (HasLiveHolder(dir, slot) || ArePortsFree(slot))
                    return TakeSlot(dir, slot, checkoutKey, "rejoined");
            }
            // Otherwise take a slot that no live test host is using and whose ports are actually free. The owner
            // record is not consulted here: a slot with no live holder is available whoever used it last.
            var blockedBy = new List<string>();
            for (var slot = MinAutoPortSlot; slot <= MaxPortSlot; slot++)
            {
                if (HasLiveHolder(dir, slot))
                {
                    blockedBy.Add($"slot {slot}: in use by {ReadSlotOwner(dir, slot) ?? "an unrecorded checkout"}");
                    continue;
                }

                if (!ArePortsFree(slot))
                {
                    // No lock, but something is on the ports: typically a server stranded by a crashed run.
                    blockedBy.Add($"slot {slot}: ports in use with no test host holding the slot");
                    continue;
                }

                return TakeSlot(dir, slot, checkoutKey, "claimed");
            }

            throw new InvalidOperationException(
                $"No Garnet test port slot is available; all {MaxPortSlot - MinAutoPortSlot + 1} are taken." +
                Environment.NewLine + string.Join(Environment.NewLine, blockedBy) + Environment.NewLine +
                $"Slots are only held while test hosts are running, so this means that many concurrent runs, or " +
                $"stray servers left by earlier ones. Wait for a run to finish, terminate stray processes by PID " +
                $"(never by name), or set {PortSlotEnvVar} to an explicit slot. Bookkeeping lives in '{dir}'.");
        }

        /// <summary>
        /// The ports a slot reserves. Enumerated from <see cref="TestPortAssignment"/> rather than listed here,
        /// so every assignment a project can bind is probed at claim time and a new one is covered without a
        /// second edit: claim time and bind time have to see the same set of ports.
        /// <para>
        /// Standalone assignments are probed at their base alone because no standalone project binds past it;
        /// one that starts to must widen this. The cluster band is probed across its whole node range, because
        /// a run can die leaving a non-base node listening while its base node is disposed, which a base-only
        /// probe would read as free.
        /// </para>
        /// </summary>
        /// <param name="slot">Slot whose ports to enumerate.</param>
        /// <returns>Every port the slot hands out.</returns>
        internal static IEnumerable<int> SlotPorts(int slot)
        {
            var offset = slot * PortSlotStride;

            foreach (var assignment in Enum.GetValues<TestPortAssignment>())
                yield return (int)assignment + offset;

            for (var node = 0; node < MaxClusterNodesPerSubProject; node++)
                yield return ClusterPortBandBase + offset + node;
        }

        /// <summary>
        /// Probes the ports a slot hands out. A server stranded by a crashed run holds no slot lock at all, so
        /// binding is the authority on whether a slot is actually usable.
        /// </summary>
        /// <param name="slot">Slot to probe.</param>
        /// <returns>True when every port the slot reserves is free.</returns>
        internal static bool ArePortsFree(int slot) => SlotPorts(slot).All(IsPortFree);

        /// <summary>
        /// Records the owning checkout and registers this process as a holder of the slot.
        /// </summary>
        /// <param name="dir">Slot bookkeeping directory.</param>
        /// <param name="slot">Slot being taken.</param>
        /// <param name="checkoutKey">Key identifying this checkout.</param>
        /// <param name="verb">Whether the slot was claimed or rejoined, for the progress message.</param>
        /// <returns>The slot number.</returns>
        private static int TakeSlot(string dir, int slot, string checkoutKey, string verb)
        {
            WriteSlotOwner(dir, slot, checkoutKey);
            portSlotHolder = new FileStream(
                Path.Combine(dir, $"slot{slot}.holder-{Environment.ProcessId}"),
                FileMode.Create, FileAccess.ReadWrite, FileShare.None, bufferSize: 1, FileOptions.DeleteOnClose);

            TestContext.Progress.WriteLine(
                $"Garnet test port slot {slot} {verb} (port offset {slot * PortSlotStride}) for '{checkoutKey}'.");
            return slot;
        }

        private static string SlotOwnerPath(string dir, int slot) => Path.Combine(dir, $"slot{slot}.owner");

        /// <summary>
        /// Records the checkout a slot belongs to. Failure propagates: continuing without the record would let a
        /// later run treat this slot as unowned and hand it to another checkout while this one is still on it.
        /// </summary>
        /// <param name="dir">Slot bookkeeping directory.</param>
        /// <param name="slot">Slot to record.</param>
        /// <param name="checkoutKey">Key identifying the owning checkout.</param>
        private static void WriteSlotOwner(string dir, int slot, string checkoutKey)
            => File.WriteAllText(SlotOwnerPath(dir, slot), checkoutKey);

        /// <summary>
        /// Reads the checkout key recorded against a slot.
        /// </summary>
        /// <param name="dir">Slot bookkeeping directory.</param>
        /// <param name="slot">Slot to read.</param>
        /// <returns>The owning checkout key, or null when the slot is unowned or unreadable.</returns>
        private static string ReadSlotOwner(string dir, int slot)
        {
            try
            {
                var path = SlotOwnerPath(dir, slot);
                return File.Exists(path) ? File.ReadAllText(path) : null;
            }
            catch (IOException)
            {
                return null;
            }
        }

        /// <summary>
        /// Determines whether any live test host still holds a slot. A holder lock is released by the kernel when
        /// its process dies, including on crash or kill, so a file that can now be opened exclusively belonged to
        /// a dead host and is removed.
        /// </summary>
        /// <param name="dir">Slot bookkeeping directory.</param>
        /// <param name="slot">Slot to test.</param>
        /// <returns>True when at least one live process still holds the slot.</returns>
        private static bool HasLiveHolder(string dir, int slot)
        {
            var live = false;
            foreach (var path in Directory.GetFiles(dir, $"slot{slot}.holder-*"))
            {
                try
                {
                    using (new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                    {
                    }

                    TryDeleteHolder(path);
                }
                catch (FileNotFoundException)
                {
                    // Removed by its owner between enumeration and opening.
                }
                catch (IOException)
                {
                    live = true;
                }
            }

            return live;
        }

        private static void TryDeleteHolder(string path)
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
                // Another test host reclaimed it first.
            }
        }

        /// <summary>
        /// Serializes slot claiming across processes. Without it two checkouts can both observe the same slot as
        /// free and both record themselves as its owner. Held only for the duration of a claim.
        /// </summary>
        /// <param name="dir">Slot bookkeeping directory.</param>
        /// <returns>The held lock, to be released once the claim completes.</returns>
        private static FileStream AcquireClaimLock(string dir)
        {
            var path = Path.Combine(dir, "claim.lock");
            var deadline = DateTime.UtcNow.AddSeconds(10);

            while (true)
            {
                try
                {
                    return new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
                }
                catch (IOException) when (DateTime.UtcNow < deadline)
                {
                    Thread.Sleep(50);
                }
                catch (IOException e)
                {
                    throw new InvalidOperationException(
                        $"Timed out acquiring the Garnet test port slot claim lock at '{path}'.", e);
                }
            }
        }

        /// <summary>
        /// Identifies the checkout — one working copy of the repo, meaning the directory containing
        /// <c>Garnet.slnx</c>, which is the worktree root under <c>git worktree</c> and the clone root otherwise.
        /// Every test host launched from that directory resolves the same key and so shares one slot. Uses
        /// <see cref="AppContext.BaseDirectory"/> rather than the NUnit test directory because the value is
        /// needed during static initialization. The solution file is the marker because in a worktree .git is a
        /// hidden file rather than a directory.
        /// </summary>
        /// <returns>A normalized key identifying this checkout.</returns>
        private static string GetCheckoutKey()
        {
            var dir = new DirectoryInfo(AppContext.BaseDirectory);
            while (dir != null && !File.Exists(Path.Combine(dir.FullName, "Garnet.slnx")))
                dir = dir.Parent;

            // Falling back to the assembly directory stays correct; it just narrows sharing to one sub-project.
            var root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(dir?.FullName ?? AppContext.BaseDirectory));
            return OperatingSystem.IsWindows() ? root.ToLowerInvariant() : root;
        }

        /// <summary>
        /// Reports whether a port can currently be bound. Both wildcards are probed, and exclusively, so that a
        /// listener is detected wherever it sits: tests bind the IPv4 and IPv6 loopbacks, every address
        /// <see cref="Dns.GetHostAddresses(string)"/> returns, and <see cref="IPAddress.Any"/>, and a wildcard
        /// bind conflicts with a specific-address one only when it asks for exclusive use.
        /// <para>
        /// Each family is bound separately rather than through one dual-mode socket, whose handling of
        /// IPv4-mapped addresses is platform-dependent. <see cref="IPAddress.Any"/> covers IPv4 alone, so a
        /// listener on <see cref="IPAddress.IPv6Loopback"/> needs the second bind to be seen.
        /// </para>
        /// </summary>
        /// <param name="port">Port to test.</param>
        /// <returns>True when the port is free.</returns>
        internal static bool IsPortFree(int port)
            => CanBindExclusively(IPAddress.Any, port)
                && (!Socket.OSSupportsIPv6 || CanBindExclusively(IPAddress.IPv6Any, port));

        /// <summary>
        /// Binds one address exclusively and releases it again. Exclusivity is what makes this a probe: a shared
        /// bind succeeds alongside a listener on a specific address under that wildcard and would report the
        /// port free. A port in <c>TIME_WAIT</c> is still reported free, since no listener can be there.
        /// </summary>
        /// <param name="address">Address to bind, normally a wildcard.</param>
        /// <param name="port">Port to bind.</param>
        /// <returns>True when the bind succeeded.</returns>
        private static bool CanBindExclusively(IPAddress address, int port)
        {
            try
            {
                var listener = new TcpListener(address, port) { ExclusiveAddressUse = true };
                listener.Start();
                listener.Stop();
                return true;
            }
            catch (SocketException)
            {
                return false;
            }
        }

        /// <summary>
        /// Fails immediately when a resolved test port is already taken, so a port conflict surfaces as an
        /// actionable error rather than as wrong results from another checkout's server or a SocketFailure
        /// partway through the run.
        /// </summary>
        /// <param name="port">The resolved port.</param>
        /// <param name="assignment">Name of the assignment that produced it.</param>
        internal static void EnsurePortAvailable(int port, string assignment)
        {
            if (IsPortFree(port))
                return;

            throw new InvalidOperationException(
                $"Test port {port} ({assignment}) is already in use. Likely causes: a server stranded by a crashed " +
                $"run; another checkout running this test project without {PortSlotEnvVar}; or this test project " +
                $"already running from this checkout. Set {PortSlotEnvVar}=auto in every checkout that shares this " +
                $"machine, and terminate stray processes by PID rather than by name.");
        }

        /// <summary>
        /// Fails when any port a cluster sub-project can bind is already taken. Cluster tests bind one port per
        /// node from their base, so checking the base alone would miss a node stranded by a crashed run and let
        /// the conflict surface later as a cluster that never forms.
        /// </summary>
        /// <param name="basePort">The sub-project's resolved base port.</param>
        /// <param name="assignment">Name of the assignment that produced it.</param>
        internal static void EnsureClusterPortsAvailable(int basePort, string assignment)
        {
            for (var node = 0; node < MaxClusterNodesPerSubProject; node++)
                EnsurePortAvailable(basePort + node, assignment);
        }

        /// <summary>
        /// Whether to use a test progress logger
        /// </summary>
        static readonly bool useTestLogger = false;

        internal static string CustomRespCommandInfoJsonPath = "CustomRespCommandsInfo.json";
        internal static string CustomRespCommandDocsJsonPath = "CustomRespCommandsDocs.json";

        private static bool CustomCommandsInfoInitialized;
        private static bool CustomCommandsDocsInitialized;
        private static IReadOnlyDictionary<string, RespCommandsInfo> RespCustomCommandsInfo;
        private static IReadOnlyDictionary<string, RespCommandDocs> RespCustomCommandsDocs;

        internal static string AzureTestContainer
        {
            get
            {
                var container = "Garnet.test".Replace('.', '-').ToLowerInvariant();
                return container;
            }
        }
        internal static string AzureTestDirectory => $"{Environment.ProcessId}_{TestContext.CurrentContext.Test.MethodName}";
        internal const string AzureEmulatedStorageString = "UseDevelopmentStorage=true;";
        internal static AzureStorageNamedDeviceFactoryCreator AzureStorageNamedDeviceFactoryCreator =
            IsRunningAzureTests ? new AzureStorageNamedDeviceFactoryCreator(AzureEmulatedStorageString, null) : null;

        public const string certFile = "testcert.pfx";
        public const string certPassword = "placeholder";

        static X509Certificate2 clientCertificate;
        static readonly object clientCertificateLock = new();

        /// <summary>
        /// Returns the client certificate used by TLS-enabled tests, importing it from disk on first use.
        /// Importing a PKCS#12 file is expensive - on Windows it materializes a key container - and the
        /// options factory a client calls runs once per connection attempt, so importing per connection
        /// spends that cost inside the client's connect timeout on every attempt and retry.
        /// </summary>
        public static X509Certificate2 GetClientCertificate()
        {
            if (clientCertificate is not null) return clientCertificate;
            lock (clientCertificateLock)
                return clientCertificate ??= CertificateUtils.GetMachineCertificateByFile(certFile, certPassword);
        }

        public const string pemCertFile = "testcert.pem";
        public const string pemCertKeyFile = "testcert.key.pem";

        internal static bool IsRunningAzureTests
        {
            get
            {
                if ("yes".Equals(Environment.GetEnvironmentVariable("RunAzureTests")) ||
                    "yes".Equals(Environment.GetEnvironmentVariable("RUNAZURETESTS")) ||
                    IsAzuriteRunning())
                {
                    return true;
                }
                return false;
            }
        }

        internal static bool IsRunningAsGitHubAction
        => "true".Equals(Environment.GetEnvironmentVariable("GITHUB_ACTIONS"), StringComparison.OrdinalIgnoreCase);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void AssertEqualUpToExpectedLength(string expectedResponse, byte[] response)
        {
            ClassicAssert.AreEqual(expectedResponse, Encoding.ASCII.GetString(response, 0, expectedResponse.Length));
        }

        /// <summary>
        /// Get command info for custom commands defined in custom commands json file
        /// </summary>
        /// <param name="customCommandsInfo">Mapping between command name and command info</param>
        /// <param name="logger">Logger</param>
        /// <returns></returns>
        internal static bool TryGetCustomCommandsInfo(out IReadOnlyDictionary<string, RespCommandsInfo> customCommandsInfo, ILogger logger = null)
        {
            customCommandsInfo = default;

            if (!CustomCommandsInfoInitialized && !TryInitializeCustomCommandsInfo(logger)) return false;

            customCommandsInfo = RespCustomCommandsInfo;
            return true;
        }

        /// <summary>
        /// Get command info for custom commands defined in custom commands json file
        /// </summary>
        /// <param name="customCommandsDocs">Mapping between command name and command info</param>
        /// <param name="logger">Logger</param>
        /// <returns></returns>
        internal static bool TryGetCustomCommandsDocs(out IReadOnlyDictionary<string, RespCommandDocs> customCommandsDocs, ILogger logger = null)
        {
            customCommandsDocs = default;

            if (!CustomCommandsDocsInitialized && !TryInitializeCustomCommandsDocs(logger)) return false;

            customCommandsDocs = RespCustomCommandsDocs;
            return true;
        }

        private static bool TryInitializeCustomCommandsInfo(ILogger logger)
        {
            if (!TryGetRespCommandData<RespCommandsInfo>(CustomRespCommandInfoJsonPath, logger, out var tmpCustomCommandsInfo))
                return false;

            RespCustomCommandsInfo = tmpCustomCommandsInfo;
            CustomCommandsInfoInitialized = true;
            return true;
        }

        private static bool TryInitializeCustomCommandsDocs(ILogger logger)
        {
            if (!TryGetRespCommandData<RespCommandDocs>(CustomRespCommandDocsJsonPath, logger, out var tmpCustomCommandsDocs))
                return false;

            RespCustomCommandsDocs = tmpCustomCommandsDocs;
            CustomCommandsDocsInitialized = true;
            return true;
        }

        private static bool TryGetRespCommandData<TData>(string resourcePath, ILogger logger, out IReadOnlyDictionary<string, TData> commandData)
        where TData : class, IRespCommandData<TData>
        {
            var streamProvider = StreamProviderFactory.GetStreamProvider(FileLocationType.Local);
            var commandsInfoProvider = RespCommandsDataProviderFactory.GetRespCommandsDataProvider<TData>();

            return commandsInfoProvider.TryImportRespCommandsData(resourcePath,
                streamProvider, out commandData, logger);
        }

        static bool IsAzuriteRunning()
        {
            // If Azurite is running, it will run on localhost and listen on port 10000 and/or 10001.
            var expectedIp = IPAddress.Loopback;
            var expectedPorts = new[] { 10000, 10001 };

            var activeTcpListeners = IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpListeners();

            var relevantListeners = activeTcpListeners.Where(t =>
                    expectedPorts.Contains(t.Port) &&
                    t.Address == expectedIp)
                .ToList();

            return relevantListeners.Any();
        }

        internal static void IgnoreIfNotRunningAzureTests()
        {
            // Need this environment variable set AND Azure Storage Emulator running
            if (!IsRunningAzureTests)
                Assert.Ignore("Environment variable RunAzureTests is not defined");
        }

        public static void IgnoreIfExceptionInjectionDisabled()
        {
#if !DEBUG
            Assert.Ignore("Relies on ExceptionInjectionHelper, only enabled in DEBUG builds");
#endif
        }

        public static void WaitUntilNextSecond(IDatabase db, long baseSeconds)
        {
            // LASTSAVE returns Unix seconds via DateTimeOffset.ToUnixTimeSeconds() so it has
            // only second-resolution. Loop on getting the server time and sleeping until the
            // server's time advances into the next Unix second.
            while (true)
            {
                var actualValue = db.Execute("TIME");
                var currentSeconds = (long)((RedisValue[])actualValue)[0];
                if (currentSeconds > baseSeconds)
                    break;
                Thread.Sleep(100);
            }
        }

        /// <summary>
        /// Create GarnetServer
        /// </summary>
        public static GarnetServer CreateGarnetServer(
            string logCheckpointDir,
            EndPoint[] endpoints = null,
            bool disablePubSub = false,
            bool tryRecover = false,
            bool lowMemory = false,
            string memorySize = default,
            string pageSize = default,
            int pageCount = 0,
            bool enableAOF = false,
            bool enableTLS = false,
            string tlsCertFileName = null,
            string tlsCertPassword = null,
            bool disableObjects = false,
            int metricsSamplingFreq = -1,
            bool latencyMonitor = false,
            int latencyMonitorPrecision = GarnetServerOptions.DefaultLatencyMonitorPrecision,
            bool commandStatsMonitor = false,
            int commitFrequencyMs = 0,
            bool commitWait = false,
            bool useAzureStorage = false,
            string defaultPassword = null,
            bool useAcl = false, // NOTE: Temporary until ACL is enforced as default
            string aclFile = null,
            bool aclStrictCustomCommands = true,
            string indexSize = "1m",
            string indexMaxSize = default,
            string[] extensionBinPaths = null,
            bool extensionAllowUnsignedAssemblies = true,
            bool getSG = false,
            int indexResizeFrequencySecs = 60,
            IAuthenticationSettings authenticationSettings = null,
            ConnectionProtectionOption enableDebugCommand = ConnectionProtectionOption.Yes,
            ConnectionProtectionOption enableModuleCommand = ConnectionProtectionOption.No,
            bool enableLua = false,
            bool enableReadCache = false,
            string readCacheMemorySize = default,
            string readCachePageSize = default,
            int readCachePageCount = 0,
            ILogger logger = null,
            IEnumerable<string> loadModulePaths = null,
            string pubSubPageSize = null,
            bool asyncReplay = false,
            LuaMemoryManagementMode luaMemoryMode = LuaMemoryManagementMode.Native,
            string luaMemoryLimit = "",
            TimeSpan? luaTimeout = null,
            LuaLoggingMode luaLoggingMode = LuaLoggingMode.Enable,
            IEnumerable<string> luaAllowedFunctions = null,
            string unixSocketPath = null,
            UnixFileMode unixSocketPermission = default,
            int slowLogThreshold = 0,
            TextWriter logTo = null,
            bool enableCluster = false,
            int expiredKeyDeletionScanFrequencySecs = -1,
            bool useReviv = false,
            bool useInChainRevivOnly = false,
            bool useLogNullDevice = false,
            bool enableVectorSetPreview = true,
            bool enableRangeIndexPreview = false,
            string aofMemorySize = "64m",
            string aofPageSize = null,
            int aofPhysicalSublogCount = 1,
            bool copyReadsToTail = false,
            int replayTaskCount = 1,
            bool failOnRecoveryError = false,
            bool fastAofTruncate = false,
            bool useAofNullDevice = false,
            LogCompactionType compactionType = LogCompactionType.None,
            int mutablePercent = 90,
            int compactionMaxSegments = 32,
            string segmentSize = "1g",
            bool? nativeAllocator = null,
            string bufferPoolMemoryBudget = null,
            string networkBufferSize = null,
            string networkBufferMemoryBudget = null,
            string networkReceiveBufferMinSize = null,
            string networkSendBufferMinSize = null,
            string sessionScratchBufferMaxRetainedSize = null,
            int? sessionParseStateMaxRetainedArgs = null
        )
        {
            if (useAzureStorage)
                IgnoreIfNotRunningAzureTests();
            var logDir = useLogNullDevice ? null : logCheckpointDir;
            if (useAzureStorage && !useLogNullDevice)
                logDir = $"{AzureTestContainer}/{AzureTestDirectory}";

            if (logCheckpointDir != null && !useAzureStorage && !useLogNullDevice)
                logDir = new DirectoryInfo(string.IsNullOrEmpty(logDir) ? "." : logDir).FullName;

            var checkpointDir = logCheckpointDir;
            if (useAzureStorage)
                checkpointDir = $"{AzureTestContainer}/{AzureTestDirectory}";

            if (logCheckpointDir != null && !useAzureStorage)
                checkpointDir = new DirectoryInfo(string.IsNullOrEmpty(checkpointDir) ? "." : checkpointDir).FullName;

            if (useAcl)
            {
                if (authenticationSettings != null)
                    throw new ArgumentException($"Cannot set both {nameof(useAcl)} and {nameof(authenticationSettings)}");
                authenticationSettings = new AclAuthenticationPasswordSettings(aclFile, defaultPassword);
            }
            else if (defaultPassword != null)
            {
                if (authenticationSettings != null)
                    throw new ArgumentException($"Cannot set both {nameof(defaultPassword)} and {nameof(authenticationSettings)}");
                authenticationSettings = new PasswordAuthenticationSettings(defaultPassword);
            }

            // Increase minimum thread pool size to 16 if needed
            int threadPoolMinThreads = 0;
            ThreadPool.GetMinThreads(out int workerThreads, out int completionPortThreads);
            if (workerThreads < 16 || completionPortThreads < 16)
                threadPoolMinThreads = 16;

            GarnetServerOptions opts = new(logger)
            {
                EnableStorageTier = logDir != null,
                LogDir = logDir,
                CheckpointDir = checkpointDir,
                EndPoints = endpoints ?? [EndPoint],
                DisablePubSub = disablePubSub,
                NetworkBufferSize = networkBufferSize,
                NetworkBufferMemoryBudget = networkBufferMemoryBudget,
                NetworkReceiveBufferMinSize = networkReceiveBufferMinSize,
                NetworkSendBufferMinSize = networkSendBufferMinSize,
                SessionScratchBufferMaxRetainedSize = sessionScratchBufferMaxRetainedSize,
                SessionParseStateMaxRetainedArgs = sessionParseStateMaxRetainedArgs ?? GarnetServerOptions.DefaultSessionParseStateMaxRetainedArgs,
                Recover = tryRecover,
                IndexMemorySize = indexSize,
                UseNativeAllocator = nativeAllocator ?? false,
                EnableAOF = enableAOF,
                EnableLua = enableLua,
                AofMemorySize = aofMemorySize,
                CommitFrequencyMs = commitFrequencyMs,
                WaitForCommit = commitWait,
                AclStrictCustomCommands = aclStrictCustomCommands,
                TlsOptions = enableTLS ? new GarnetTlsOptions(
                    certFileName: tlsCertFileName ?? certFile,
                    certPassword: tlsCertPassword ?? certPassword,
                    clientCertificateRequired: true,
                    certificateRevocationCheckMode: X509RevocationMode.NoCheck,
                    issuerCertificatePath: null,
                    null, 0, false, null, logger: logger)
                : null,
                DisableObjects = disableObjects,
                QuietMode = true,
                MetricsSamplingFrequency = metricsSamplingFreq,
                LatencyMonitor = latencyMonitor,
                LatencyMonitorPrecision = latencyMonitorPrecision,
                CommandStatsMonitor = commandStatsMonitor,
                DeviceFactoryCreator = useAzureStorage ?
                        logger == null ? TestUtils.AzureStorageNamedDeviceFactoryCreator : new AzureStorageNamedDeviceFactoryCreator(AzureEmulatedStorageString, logger)
                        : new LocalStorageNamedDeviceFactoryCreator(logger: logger),
                AuthSettings = authenticationSettings,
                ExtensionBinPaths = extensionBinPaths,
                ExtensionAllowUnsignedAssemblies = extensionAllowUnsignedAssemblies,
                EnableScatterGatherGet = getSG,
                IndexResizeFrequencySecs = indexResizeFrequencySecs,
                ThreadPoolMinThreads = threadPoolMinThreads,
                LoadModuleCS = loadModulePaths,
                EnableCluster = enableCluster,
                EnableDebugCommand = enableDebugCommand,
                EnableModuleCommand = enableModuleCommand,
                EnableReadCache = enableReadCache,
                AofReplayMaxLagBytes = asyncReplay ? -1 : 0,
                AofReplayTaskCount = replayTaskCount,
                AofPhysicalSublogCount = aofPhysicalSublogCount,
                LuaOptions = enableLua ? new LuaOptions(luaMemoryMode, luaMemoryLimit, luaTimeout ?? Timeout.InfiniteTimeSpan, luaLoggingMode, luaAllowedFunctions ?? [], logger) : null,
                UnixSocketPath = unixSocketPath,
                UnixSocketPermission = unixSocketPermission,
                SlowLogThreshold = slowLogThreshold,
                ExpiredKeyDeletionScanFrequencySecs = expiredKeyDeletionScanFrequencySecs,
                EnableVectorSetPreview = enableVectorSetPreview,
                EnableRangeIndexPreview = enableRangeIndexPreview,
                CopyReadsToTail = copyReadsToTail,
                FailOnRecoveryError = failOnRecoveryError,
                FastAofTruncate = fastAofTruncate,
                UseAofNullDevice = useAofNullDevice,
                CompactionType = compactionType,
                MutablePercent = mutablePercent,
                CompactionMaxSegments = compactionMaxSegments,
                SegmentSize = segmentSize,
            };

            if (!string.IsNullOrEmpty(memorySize))
                opts.LogMemorySize = memorySize;

            if (!string.IsNullOrEmpty(pageSize))
                opts.PageSize = pageSize;

            if (pageCount != 0)
            {
                opts.PageCount = pageCount;

                // If there is a pageCount and no memorySize, then we are bypassing the size tracker (which is automatically started if memorySize is specified).
                if (string.IsNullOrEmpty(memorySize))
                    opts.LogMemorySize = string.Empty;
            }

            if (!string.IsNullOrEmpty(pubSubPageSize))
                opts.PubSubPageSize = pubSubPageSize;

            if (indexMaxSize != default)
                opts.IndexMaxMemorySize = indexMaxSize;

            if (!string.IsNullOrEmpty(aofPageSize))
                opts.AofPageSize = aofPageSize;

            if (lowMemory)
            {
                opts.LogMemorySize = string.IsNullOrEmpty(memorySize) ? $"{MinKvLogPageSizeInKB * LogSizeTracker.MinTargetPageCount}k" : memorySize; // Must be LogSizeTracker.MinTargetPageCount pages due to memory size tracking
                opts.PageSize = pageSize == default ? $"{MinKvLogPageSize}" : pageSize;

                // If there is a pageCount and no memorySize, then we are bypassing the size tracker (which is automatically started if memorySize is specified).
                // This is especially useful for two-page tests, which is less than LogSizeTracker.MinTargetPageCount pages.
                if (pageCount != 0 && memorySize == default)
                    opts.LogMemorySize = string.Empty;
            }

            if (enableReadCache)
            {
                opts.ReadCacheMemorySize = readCacheMemorySize ?? opts.LogMemorySize;
                opts.ReadCachePageSize = readCachePageSize ?? opts.PageSize;
                opts.ReadCachePageCount = readCachePageCount != 0 ? readCachePageCount : opts.PageCount;

                // If there is a pageCount and no memorySize, then we are bypassing the size tracker (which is automatically started if memorySize is specified).
                if (opts.ReadCachePageCount != 0 && string.IsNullOrEmpty(opts.ReadCacheMemorySize))
                    opts.ReadCacheMemorySize = string.Empty;
            }

            ILoggerFactory loggerFactory = null;
            if (useTestLogger || logTo != null)
            {
                loggerFactory = LoggerFactory.Create(builder =>
                {
                    if (useTestLogger)
                    {
                        _ = builder.AddProvider(new NUnitLoggerProvider(TestContext.Progress, $"{Environment.ProcessId}_{TestContext.CurrentContext.Test.MethodName}", null, false, false, LogLevel.Trace));
                    }

                    if (logTo != null)
                    {
                        _ = builder.AddProvider(new NUnitLoggerProvider(logTo, logLevel: LogLevel.Trace));
                    }

                    _ = builder.SetMinimumLevel(LogLevel.Trace);
                });
            }

            if (useReviv)
            {
                opts.UseRevivBinsPowerOf2 = true;
                opts.RevivBinBestFitScanLimit = 0;
                opts.RevivNumberOfBinsToSearch = int.MaxValue;
                opts.RevivifiableFraction = 1;
                opts.RevivInChainOnly = false;
                opts.RevivBinRecordCounts = [];
                opts.RevivBinRecordSizes = [];
            }

            if (useInChainRevivOnly)
            {
                opts.RevivInChainOnly = true;
            }

            if (bufferPoolMemoryBudget != null)
                opts.BufferPoolMemoryBudget = bufferPoolMemoryBudget;

            return new GarnetServer(opts, loggerFactory);
        }

        /// <summary>
        /// Create logger factory for given TextWriter and loglevel
        /// E.g. Use with TestContext.Progress to print logs while test is running.
        /// </summary>
        /// <param name="textWriter"></param>
        /// <param name="logLevel"></param>
        /// <param name="scope"></param>
        /// <param name="skipCmd"></param>
        /// <param name="recvOnly"></param>
        /// <param name="matchLevel"></param>
        /// <returns></returns>
        public static (ILoggerFactory, NUnitLoggerProvider) CreateLoggerFactoryInstance(
            TextWriter textWriter,
            LogLevel logLevel,
            string scope = "",
            HashSet<string> skipCmd = null,
            bool recvOnly = false,
            bool matchLevel = false)
        {
            var provider = new NUnitLoggerProvider(textWriter, scope, skipCmd, recvOnly, matchLevel, logLevel);
            return (LoggerFactory.Create(builder =>
            {
                builder.AddProvider(provider);
                builder.SetMinimumLevel(logLevel);
            }), provider);
        }

        public static (GarnetServer[] Nodes, GarnetServerOptions[] Options) CreateGarnetCluster(
            string checkpointDir,
            EndPointCollection endpoints,
            bool enableCluster = true,
            bool disablePubSub = false,
            bool disableObjects = false,
            bool tryRecover = false,
            bool enableAOF = false,
            int timeout = -1,
            int gossipDelay = 1,
            bool UseAzureStorage = false,
            bool UseTLS = false,
            bool cleanClusterConfig = false,
            bool lowMemory = false,
            string MemorySize = default,
            string PageSize = default,
            string SegmentSize = "1g",
            bool FastAofTruncate = false,
            string AofMemorySize = "64m",
            string AofPageSize = default,
            bool OnDemandCheckpoint = false,
            int CommitFrequencyMs = 0,
            bool useAofNullDevice = false,
            bool DisableStorageTier = false,
            string authUsername = null,
            string authPassword = null,
            bool useAcl = false, // NOTE: Temporary until ACL is enforced as default
            string aclFile = null,
            X509CertificateCollection certificates = null,
            ILoggerFactory loggerFactory = null,
            AadAuthenticationSettings authenticationSettings = null,
            int metricsSamplingFrequency = 0,
            bool enableLua = false,
            bool asyncReplay = false,
            bool enableDisklessSync = false,
            int replicaDisklessSyncDelay = 1,
            string replicaDisklessSyncFullSyncAofThreshold = null,
            LuaMemoryManagementMode luaMemoryMode = LuaMemoryManagementMode.Native,
            string luaMemoryLimit = "",
            EndPoint clusterAnnounceEndpoint = null,
            bool luaTransactionMode = false,
            DeviceType deviceType = DeviceType.Default,
            int clusterReplicationReestablishmentTimeout = 0,
            string aofSizeLimit = "",
            int compactionFrequencySecs = 0,
            LogCompactionType compactionType = LogCompactionType.Scan,
            bool latencyMonitory = false,
            int metricSamplingFrequencySecs = 0,
            int loggingFrequencySecs = 5,
            int checkpointThrottleFlushDelayMs = 0,
            bool clusterReplicaResumeWithData = false,
            int replicaSyncTimeout = 60,
            int sublogCount = 1,
            int replayTaskCount = 1,
            int expiredObjectCollectionFrequencySecs = 0,
            ClusterPreferredEndpointType clusterPreferredEndpointType = ClusterPreferredEndpointType.Ip,
            string clusterAnnounceHostname = null,
            int vectorSetReplayTaskCount = 0,
            int threadPoolMinIOCompletionThreads = 0,
            bool enableRangeIndexPreview = false)
        {
            if (UseAzureStorage)
                IgnoreIfNotRunningAzureTests();
            var nodes = new GarnetServer[endpoints.Count];
            var opts = new GarnetServerOptions[nodes.Length];
            for (var i = 0; i < nodes.Length; i++)
            {
                var endpoint = (IPEndPoint)endpoints[i];

                opts[i] = GetGarnetServerOptions(
                    checkpointDir,
                    checkpointDir,
                    endpoint,
                    enableCluster: enableCluster,
                    disablePubSub,
                    disableObjects,
                    tryRecover,
                    enableAOF,
                    timeout,
                    gossipDelay,
                    UseAzureStorage,
                    useTLS: UseTLS,
                    cleanClusterConfig: cleanClusterConfig,
                    lowMemory: lowMemory,
                    memorySize: MemorySize,
                    pageSize: PageSize,
                    segmentSize: SegmentSize,
                    fastAofTruncate: FastAofTruncate,
                    aofMemorySize: AofMemorySize,
                    aofPageSize: AofPageSize,
                    onDemandCheckpoint: OnDemandCheckpoint,
                    commitFrequencyMs: CommitFrequencyMs,
                    useAofNullDevice: useAofNullDevice,
                    disableStorageTier: DisableStorageTier,
                    authUsername: authUsername,
                    authPassword: authPassword,
                    useAcl: useAcl,
                    aclFile: aclFile,
                    certificates: certificates,
                    logger: loggerFactory?.CreateLogger("GarnetServer"),
                    aadAuthenticationSettings: authenticationSettings,
                    metricsSamplingFrequency: metricsSamplingFrequency,
                    enableLua: enableLua,
                    asyncReplay: asyncReplay,
                    enableDisklessSync: enableDisklessSync,
                    replicaDisklessSyncDelay: replicaDisklessSyncDelay,
                    replicaDisklessSyncFullSyncAofThreshold: replicaDisklessSyncFullSyncAofThreshold,
                    luaMemoryMode: luaMemoryMode,
                    luaMemoryLimit: luaMemoryLimit,
                    clusterAnnounceEndpoint: clusterAnnounceEndpoint,
                    luaTransactionMode: luaTransactionMode,
                    deviceType: deviceType,
                    clusterReplicationReestablishmentTimeout: clusterReplicationReestablishmentTimeout,
                    aofSizeLimit: aofSizeLimit,
                    compactionFrequencySecs: compactionFrequencySecs,
                    compactionType: compactionType,
                    latencyMonitory: latencyMonitory,
                    loggingFrequencySecs: loggingFrequencySecs,
                    checkpointThrottleFlushDelayMs: checkpointThrottleFlushDelayMs,
                    clusterReplicaResumeWithData: clusterReplicaResumeWithData,
                    replicaSyncTimeout: replicaSyncTimeout,
                    sublogCount: sublogCount,
                    replayTaskCount: replayTaskCount,
                    expiredObjectCollectionFrequencySecs: expiredObjectCollectionFrequencySecs,
                    clusterPreferredEndpointType: clusterPreferredEndpointType,
                    clusterAnnounceHostname: clusterAnnounceHostname,
                    vectorSetReplayTaskCount: vectorSetReplayTaskCount,
                    threadPoolMinIOCompletionThreads: threadPoolMinIOCompletionThreads,
                    enableRangeIndexPreview: enableRangeIndexPreview);

                ClassicAssert.IsNotNull(opts);

                if (opts[i].EndPoints[0] is IPEndPoint ipEndpoint)
                {
                    var iter = 0;
                    while (!IsPortAvailable(ipEndpoint.Port))
                    {
                        ClassicAssert.Less(iter, 30, "Failed to connect within 30 seconds");
                        TestContext.Progress.WriteLine($"Waiting for Port {ipEndpoint.Port} to become available for {TestContext.CurrentContext.WorkerId}:{iter++}");
                        Thread.Sleep(1000);
                    }
                }

                nodes[i] = new GarnetServer(opts[i], loggerFactory);
            }
            return (nodes, opts);
        }

        public static GarnetServerOptions GetGarnetServerOptions(
            string checkpointDir,
            string logDir,
            EndPoint endpoint,
            bool enableCluster = true,
            bool disablePubSub = false,
            bool disableObjects = false,
            bool tryRecover = false,
            bool enableAOF = false,
            int timeout = -1,
            int gossipDelay = 5,
            bool useAzureStorage = false,
            bool useTLS = false,
            bool cleanClusterConfig = false,
            bool lowMemory = false,
            string memorySize = default,
            string pageSize = default,
            string segmentSize = "1g",
            bool fastAofTruncate = false,
            string aofMemorySize = "64m",
            string aofPageSize = default,
            bool onDemandCheckpoint = false,
            int commitFrequencyMs = 0,
            bool useAofNullDevice = false,
            bool disableStorageTier = false,
            string authUsername = null,
            string authPassword = null,
            bool useAcl = false, // NOTE: Temporary until ACL is enforced as default
            string aclFile = null,
            X509CertificateCollection certificates = null,
            AadAuthenticationSettings aadAuthenticationSettings = null,
            int metricsSamplingFrequency = 0,
            bool enableLua = false,
            bool asyncReplay = false,
            bool enableDisklessSync = false,
            int replicaDisklessSyncDelay = 1,
            string replicaDisklessSyncFullSyncAofThreshold = null,
            ILogger logger = null,
            LuaMemoryManagementMode luaMemoryMode = LuaMemoryManagementMode.Native,
            string luaMemoryLimit = "",
            TimeSpan? luaTimeout = null,
            LuaLoggingMode luaLoggingMode = LuaLoggingMode.Enable,
            IEnumerable<string> luaAllowedFunctions = null,
            string unixSocketPath = null,
            EndPoint clusterAnnounceEndpoint = null,
            bool luaTransactionMode = false,
            DeviceType deviceType = DeviceType.Default,
            int clusterReplicationReestablishmentTimeout = 0,
            string aofSizeLimit = "",
            int compactionFrequencySecs = 0,
            LogCompactionType compactionType = LogCompactionType.Scan,
            bool latencyMonitory = false,
            int loggingFrequencySecs = 5,
            int checkpointThrottleFlushDelayMs = 0,
            bool clusterReplicaResumeWithData = false,
            int replicaSyncTimeout = 60,
            int sublogCount = 1,
            int replayTaskCount = 1,
            int expiredObjectCollectionFrequencySecs = 0,
            ClusterPreferredEndpointType clusterPreferredEndpointType = ClusterPreferredEndpointType.Ip,
            string clusterAnnounceHostname = null,
            bool enableVectorSetPreview = true,
            int vectorSetReplayTaskCount = 0,
            bool enableRangeIndexPreview = false,
            int vectorSetQuantizationTaskCount = 0,
            int threadPoolMinIOCompletionThreads = 0)
        {
            if (useAzureStorage)
                IgnoreIfNotRunningAzureTests();

            if (useAzureStorage)
            {
                logDir = Path.Join(AzureTestContainer, AzureTestDirectory);
                checkpointDir = Path.Join(AzureTestContainer, AzureTestDirectory);
            }

            if (endpoint is IPEndPoint ipEndpoint)
            {
                logDir = Path.Join(logDir, ipEndpoint.Port.ToString());
                checkpointDir = Path.Join(checkpointDir, ipEndpoint.Port.ToString());
            }
            else if (endpoint is UnixDomainSocketEndPoint && !string.IsNullOrEmpty(unixSocketPath))
            {
                var socketFileName = Path.GetFileName(unixSocketPath);

                logDir = Path.Join(logDir, socketFileName);
                checkpointDir = Path.Join(checkpointDir, socketFileName);
            }
            else throw new NotSupportedException("Unsupported endpoint type.");

            if (!useAzureStorage)
            {
                logDir = Path.GetFullPath(logDir);
                checkpointDir = Path.GetFullPath(checkpointDir);
            }

            IAuthenticationSettings authenticationSettings = null;
            if (useAcl && aadAuthenticationSettings != null)
            {
                authenticationSettings = new AclAuthenticationAadSettings(aclFile, authPassword, aadAuthenticationSettings);
            }
            else if (useAcl)
            {
                authenticationSettings = new AclAuthenticationPasswordSettings(aclFile, authPassword);
            }
            else if (authPassword != null)
            {
                authenticationSettings = new PasswordAuthenticationSettings(authPassword);
            }

            GarnetServerOptions opts = new(logger)
            {
                ThreadPoolMinThreads = 512,
                SegmentSize = segmentSize,
                EnableStorageTier = useAzureStorage || (!disableStorageTier && logDir != null),
                LogDir = disableStorageTier ? null : logDir,
                CheckpointDir = checkpointDir,
                EndPoints = [endpoint],
                DisablePubSub = disablePubSub,
                DisableObjects = disableObjects,
                EnableDebugCommand = ConnectionProtectionOption.Yes,
                EnableModuleCommand = ConnectionProtectionOption.Yes,
                Recover = tryRecover,
                IndexMemorySize = "1m",
                EnableCluster = enableCluster,
                CleanClusterConfig = cleanClusterConfig,
                ClusterTimeout = timeout,
                QuietMode = true,
                EnableAOF = enableAOF,
                LogMemorySize = "1g",
                GossipDelay = gossipDelay,
                MetricsSamplingFrequency = metricsSamplingFrequency,
                TlsOptions = useTLS ? new GarnetTlsOptions(
                    certFileName: certFile,
                    certPassword: certPassword,
                    clientCertificateRequired: true,
                    certificateRevocationCheckMode: X509RevocationMode.NoCheck,
                    issuerCertificatePath: null,
                    certSubjectName: null,
                    certificateRefreshFrequency: 0,
                    enableCluster: true,
                    clientTargetHost: null,
                    serverCertificateRequired: true,
                    tlsServerOptionsOverride: null,
                    clusterTlsClientOptionsOverride: new SslClientAuthenticationOptions
                    {
                        ClientCertificates = certificates ?? [GetClientCertificate()],
                        TargetHost = "GarnetTest",
                        AllowRenegotiation = false,
                        RemoteCertificateValidationCallback = ValidateServerCertificate,
                    },
                    logger: logger)
                : null,
                DeviceFactoryCreator = useAzureStorage ?
                    logger == null ? TestUtils.AzureStorageNamedDeviceFactoryCreator : new AzureStorageNamedDeviceFactoryCreator(AzureEmulatedStorageString, logger)
                    : new LocalStorageNamedDeviceFactoryCreator(logger: logger),
                FastAofTruncate = fastAofTruncate,
                AofMemorySize = aofMemorySize,
                AofPageSize = aofPageSize ?? "32m",
                AofSizeLimit = aofSizeLimit,
                OnDemandCheckpoint = onDemandCheckpoint,
                CommitFrequencyMs = commitFrequencyMs,
                UseAofNullDevice = useAofNullDevice,
                AuthSettings = useAcl ? authenticationSettings : (authPassword != null ? authenticationSettings : null),
                ClusterUsername = authUsername,
                ClusterPassword = authPassword,
                EnableLua = enableLua,
                LuaTransactionMode = luaTransactionMode,
                AofReplayMaxLagBytes = asyncReplay ? -1 : 0,
                LuaOptions = enableLua ? new LuaOptions(luaMemoryMode, luaMemoryLimit, luaTimeout ?? Timeout.InfiniteTimeSpan, luaLoggingMode, luaAllowedFunctions ?? [], logger) : null,
                UnixSocketPath = unixSocketPath,
                ReplicaDisklessSync = enableDisklessSync,
                ReplicaDisklessSyncDelay = replicaDisklessSyncDelay,
                ReplicaDisklessSyncFullSyncAofThreshold = replicaDisklessSyncFullSyncAofThreshold,
                ClusterAnnounceEndpoint = clusterAnnounceEndpoint,
                ClusterAnnounceHostname = clusterAnnounceHostname,
                ClusterPreferredEndpointType = clusterPreferredEndpointType,
                DeviceType = deviceType,
                ClusterReplicationReestablishmentTimeout = clusterReplicationReestablishmentTimeout,
                CompactionFrequencySecs = compactionFrequencySecs,
                CompactionType = compactionType,
                LatencyMonitor = latencyMonitory,
                LoggingFrequency = loggingFrequencySecs,
                CheckpointThrottleFlushDelayMs = checkpointThrottleFlushDelayMs,
                ClusterReplicaResumeWithData = clusterReplicaResumeWithData,
                ReplicaSyncTimeout = replicaSyncTimeout <= 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(replicaSyncTimeout),
                AofPhysicalSublogCount = sublogCount,
                AofReplayTaskCount = replayTaskCount,
                EnableVectorSetPreview = enableVectorSetPreview,
                VectorSetReplayTaskCount = vectorSetReplayTaskCount,
                EnableRangeIndexPreview = enableRangeIndexPreview,
                VectorSetQuantizationTaskCount = vectorSetQuantizationTaskCount,
                ExpiredObjectCollectionFrequencySecs = expiredObjectCollectionFrequencySecs,
                ThreadPoolMinIOCompletionThreads = threadPoolMinIOCompletionThreads,
            };

            if (lowMemory)
            {
                opts.LogMemorySize = string.IsNullOrEmpty(memorySize) ? $"{MinKvLogPageSizeInKB * LogSizeTracker.MinTargetPageCount}k" : memorySize;  // Must be LogSizeTracker.MinTargetPageCount pages due to memory size tracking
                opts.PageSize = pageSize == default ? $"{MinKvLogPageSize}" : pageSize;
            }

            return opts;
        }

        public static bool IsPortAvailable(int port)
        {
            bool inUse = true;

            IPGlobalProperties ipProperties = IPGlobalProperties.GetIPGlobalProperties();
            IPEndPoint[] ipEndPoints = ipProperties.GetActiveTcpListeners();

            foreach (IPEndPoint endPoint in ipEndPoints)
            {
                if (endPoint.Port == port)
                {
                    inUse = false;
                    break;
                }
            }

            return inUse;
        }

        /// <summary>
        /// Create config options for SE.Redis client
        /// </summary>
        public static ConfigurationOptions GetConfig(
            EndPointCollection endpoints = default,
            bool allowAdmin = false,
            bool disablePubSub = false,
            bool useTLS = false,
            string authUsername = null,
            string authPassword = null,
            X509CertificateCollection certificates = null,
            RedisProtocol? protocol = null)
        {
            var cmds = RespCommandsInfo.TryGetRespCommandNames(out var names)
                ? new HashSet<string>(names)
                : new HashSet<string>();

            if (disablePubSub)
            {
                cmds.Remove("SUBSCRIBE");
                cmds.Remove("PUBLISH");
            }

            var defaultEndPoints = endpoints == default ? [EndPoint] : endpoints;
            var configOptions = new ConfigurationOptions
            {
                EndPoints = defaultEndPoints,
                CommandMap = CommandMap.Create(cmds),
                ConnectTimeout = (int)TimeSpan.FromSeconds(Debugger.IsAttached ? 100 : 2).TotalMilliseconds,
                SyncTimeout = (int)TimeSpan.FromSeconds(30).TotalMilliseconds,
                AsyncTimeout = (int)TimeSpan.FromSeconds(30).TotalMilliseconds,
                AllowAdmin = allowAdmin,
                // Gates how often the multiplexer may retry a dropped connection. Tests restart nodes
                // routinely, and the first command issued afterwards blocks until the next retry is
                // allowed, so a long interval is dead time added to every such test.
                ReconnectRetryPolicy = new LinearRetry((int)TimeSpan.FromMilliseconds(250).TotalMilliseconds),
                ConnectRetry = 5,
                IncludeDetailInExceptions = true,
                AbortOnConnectFail = true,
                Password = authPassword,
                User = authUsername,
                ClientName = $"{Environment.ProcessId}_{TestContext.CurrentContext.Test.MethodName}",
                Protocol = protocol,
            };

            if (Debugger.IsAttached)
            {
                configOptions.SyncTimeout = (int)TimeSpan.FromHours(2).TotalMilliseconds;
                configOptions.AsyncTimeout = (int)TimeSpan.FromHours(2).TotalMilliseconds;
            }

            if (useTLS)
            {
                configOptions.Ssl = true;
                configOptions.SslHost = "GarnetTest";
                configOptions.SslClientAuthenticationOptions = (host) =>
                (
                    new SslClientAuthenticationOptions
                    {
                        ClientCertificates = certificates ?? [GetClientCertificate()],
                        TargetHost = "GarnetTest",
                        AllowRenegotiation = false,
                        RemoteCertificateValidationCallback = ValidateServerCertificate,
                    }
                );
            }
            return configOptions;
        }

        public static GarnetClient GetGarnetClient(EndPoint endpoint = null, bool useTLS = false, bool recordLatency = false, client.LightEpoch epoch = null)
        {
            SslClientAuthenticationOptions sslOptions = null;
            if (useTLS)
            {
                sslOptions = new SslClientAuthenticationOptions
                {
                    ClientCertificates = [GetClientCertificate()],
                    TargetHost = "GarnetTest",
                    AllowRenegotiation = false,
                    RemoteCertificateValidationCallback = ValidateServerCertificate,
                };
            }
            return new GarnetClient(endpoint ?? EndPoint, sslOptions, recordLatency: recordLatency, epoch: epoch);
        }

        public static GarnetClientSession GetGarnetClientSession(bool useTLS = false, bool raw = false, EndPoint endPoint = null)
        {
            SslClientAuthenticationOptions sslOptions = null;
            if (useTLS)
            {
                sslOptions = new SslClientAuthenticationOptions
                {
                    ClientCertificates = [GetClientCertificate()],
                    TargetHost = "GarnetTest",
                    AllowRenegotiation = false,
                    RemoteCertificateValidationCallback = ValidateServerCertificate,
                };
            }
            return new GarnetClientSession(endPoint ?? EndPoint, new(), tlsOptions: sslOptions, rawResult: raw);
        }

        public static LightClientRequest CreateRequest(LightClient.OnResponseDelegateUnsafe onReceive = null, bool useTLS = false, CountResponseType countResponseType = CountResponseType.Tokens)
        {
            SslClientAuthenticationOptions sslOptions = null;
            if (useTLS)
            {
                sslOptions = new SslClientAuthenticationOptions
                {
                    ClientCertificates = [GetClientCertificate()],
                    TargetHost = "GarnetTest",
                    AllowRenegotiation = false,
                    RemoteCertificateValidationCallback = ValidateServerCertificate,
                };
            }
            return new LightClientRequest(EndPoint, 0, onReceive, sslOptions, countResponseType);
        }

        public static string GetHostName(ILogger logger = null)
        {
            try
            {
                var serverName = Environment.MachineName; // host name sans domain
                var fqhn = Dns.GetHostEntry(serverName).HostName; // fully qualified hostname
                return fqhn;
            }
            catch (SocketException ex)
            {
                logger?.LogError(ex, "GetHostName threw an error");
            }

            return "";
        }

        public static EndPointCollection GetShardEndPoints(int shards, IPAddress address, int port)
        {
            EndPointCollection endPoints = [];
            for (var i = 0; i < shards; i++)
                endPoints.Add(address, port + i);
            return endPoints;
        }

        internal static string MethodTestDir => UnitTestWorkingDir();

        /// <summary>
        /// Find root test directory (test/) based on prefix Garnet.test.
        /// After splitting on "Garnet.test", we land in test/standalone/ or test/cluster/,
        /// so navigate up one level to reach test/.
        /// </summary>
        internal static string RootTestsProjectPath =>
            Path.GetFullPath(Path.Combine(TestContext.CurrentContext.TestDirectory.Split("Garnet.test")[0], ".."));

        /// <summary>
        /// Build path for unit test working directory.
        /// </summary>
        /// <returns></returns>
        internal static string UnitTestWorkingDir()
        {
            // Include process id to avoid conflicts between parallel test runs, and remove the prefix to keep the length short.
            var testPath = $"{Environment.ProcessId}_{TestContext.CurrentContext.Test.ClassName.Split("Garnet.test")[0]}_{TestContext.CurrentContext.Test.MethodName}";

            // Incorporate arguments (as a hash code) so different runs of the same method get different folders
            //
            // Using hashes instead of the arguments themselves to keep length down
            if ((TestContext.CurrentContext.Test.Arguments?.Length ?? 0) > 0)
            {
                HashCode hash = new();
                foreach (var arg in TestContext.CurrentContext.Test.Arguments)
                {
                    if (arg is string str)
                    {
                        hash.Add(str);
                    }
                    else
                    {
                        var argAsStr = arg?.ToString() ?? "--EMPTY--";
                        hash.Add(argAsStr);
                    }
                }

                testPath += $"_{hash.ToHashCode()}";
            }

            var rootPath = Path.Combine(RootTestsProjectPath, ".tmp", testPath);

            return EnsureExtendedLengthPathIfNeeded(rootPath);
        }

        /// <summary>
        /// On Windows, rewrites <paramref name="path"/> as a Win32 extended-length path (prefixed with
        /// <c>\\?\</c>, or <c>\\?\UNC\</c> for a network share) when its fully-qualified length is close
        /// enough to the 260-char MAX_PATH limit that the files tests create beneath it could exceed it.
        /// Extended-length paths are exempt from that limit and are honored by the device layer (which
        /// passes them straight to CreateFileW) as well as the BCL file APIs.
        /// </summary>
        /// <remarks>
        /// This mirrors the equivalent helper in Tsavorite's TestUtils. Without it, a checkout under a long
        /// root makes the deepest files Garnet creates — checkpoint files such as
        /// "\Store\checkpoints\cpr-checkpoints\&lt;guid&gt;\snapshot.obj.dat" (~88 chars) — exceed the limit,
        /// and the device layer rejects them. Because the directory name embeds a per-process randomized
        /// <see cref="HashCode"/>, its length varies between runs, so such failures are intermittent.
        ///
        /// Only a path that actually needs rewriting is canonicalized: when it is close enough to the limit,
        /// it is fully qualified via <see cref="Path.GetFullPath(string)"/> so that relative segments and
        /// forward slashes are normalized to backslashes (as required by extended-length paths, which Windows
        /// does not normalize) before the <c>\\?\</c> prefix is applied. The input is returned unchanged on
        /// non-Windows platforms, when it is already extended-length, or when it is short enough that no child
        /// path can overflow; this keeps the common short-path case (normal checkouts and CI) on ordinary paths.
        ///
        /// The constants below are duplicated rather than taken from Tsavorite's Native32 because that type's
        /// members are internal and Garnet.test.cluster, which compiles this file via a linked Compile item,
        /// is not granted InternalsVisibleTo by Tsavorite.core.
        /// </remarks>
        internal static string EnsureExtendedLengthPathIfNeeded(string path)
        {
            if (string.IsNullOrEmpty(path) || !OperatingSystem.IsWindows() || path.StartsWith(ExtendedLengthPathPrefix, StringComparison.Ordinal))
                return path;

            var fullPath = Path.GetFullPath(path);

            // The device layer rejects non-extended paths longer than MAX_PATH - 11 (the 11 reserves room for
            // a ".<segmentId>" suffix). Once this directory's fully-qualified length is within the reserve
            // below of MAX_PATH, switch to an extended-length path so those children stay valid.
            const int win32MaxPath = 260;
            const int reservedForChildPaths = 100;
            if (fullPath.Length <= win32MaxPath - reservedForChildPaths)
                return path;

            // UNC paths (\\server\share\...) use the \\?\UNC\server\share\... form.
            if (fullPath.StartsWith(@"\\", StringComparison.Ordinal))
                return ExtendedLengthPathPrefix + "UNC" + fullPath[1..];

            return ExtendedLengthPathPrefix + fullPath;
        }

        /// <summary>The Win32 extended-length path prefix; paths using it bypass the MAX_PATH limit.</summary>
        private const string ExtendedLengthPathPrefix = @"\\?\";

        /// <summary>
        /// Delete a directory recursively
        /// </summary>
        /// <param name="path">The folder to delete</param>
        /// <param name="wait">If true, loop on exceptions that are retryable, and verify the directory no longer exists. Generally true on SetUp, false on TearDown</param>
        internal static void DeleteDirectory(string path, bool wait = false)
        {
            while (true)
            {
                try
                {
                    if (!Directory.Exists(path))
                        return;

                    // Recursively delete subdirectories, then fall through to delete this directory.
                    foreach (string directory in Directory.GetDirectories(path))
                        DeleteDirectory(directory, wait);
                    break;
                }
                catch
                {
                }
            }

            var retry = true;
            while (retry)
            {
                // Exceptions may happen due to a handle briefly remaining held after Dispose().
                retry = false;
                try
                {
                    if (Directory.Exists(path))
                        Directory.Delete(path, true);
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    // If we're not waiting, try once more then give up.
                    if (!wait)
                    {
                        try { Directory.Delete(path, true); }
                        catch { }
                        return;
                    }
                    retry = true;
                    _ = Thread.Yield();
                }
            }
        }

        /// <summary>
        /// Inserts filler string keys to advance the log past a captured address, then polls
        /// <c>INFO STORE</c> with exponential backoff until <c>Log.FlushedUntilAddress</c>
        /// reaches <paramref name="flushUntilAddress"/>. This guarantees that the record at
        /// that address has been written to disk.
        /// </summary>
        /// <param name="db">The Redis database to insert filler keys into.</param>
        /// <param name="server">The StackExchange.Redis server for issuing INFO commands.</param>
        /// <param name="flushUntilAddress">The log address to wait for (typically the TailAddress
        /// captured after inserting the record of interest).</param>
        /// <param name="fillerCount">Number of filler keys to insert before polling (default 200).</param>
        /// <param name="fillerPrefix">Prefix for filler key names (default "flushfiller").</param>
        /// <param name="timeoutMs">Maximum time in ms to wait for flush (default 5000).</param>
        public static async Task FlushAndWaitForStoreAsync(IDatabase db, IServer server,
            long flushUntilAddress, int fillerCount = 2000, string fillerPrefix = "flushfiller",
            int timeoutMs = 5000)
        {
            for (var i = 0; i < fillerCount; i++)
                await db.StringSetAsync($"{fillerPrefix}{i:D4}", $"data{i:D4}").ConfigureAwait(false);

            var deadline = Environment.TickCount64 + timeoutMs;
            var backoffMs = 10;
            while (Environment.TickCount64 < deadline)
            {
                var addressInfo = GetStoreAddressInfo(server);
                if (addressInfo.FlushedUntilAddress >= flushUntilAddress)
                    return;

                await Task.Delay(backoffMs).ConfigureAwait(false);
                backoffMs = Math.Min(backoffMs * 2, 500);
            }

            Assert.Fail($"Timed out waiting for FlushedUntilAddress to reach {flushUntilAddress} " +
                        $"(current: {GetStoreAddressInfo(server).FlushedUntilAddress})");
        }

        /// <summary>
        /// Delegate to use in TLS certificate validation
        /// Test certificate should be issued by "CN=Garnet"
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="certificate"></param>
        /// <param name="chain"></param>
        /// <param name="sslPolicyErrors"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public static bool ValidateServerCertificate(
          object sender,
          X509Certificate certificate,
          X509Chain chain,
          SslPolicyErrors sslPolicyErrors)
        {
            if (sslPolicyErrors == SslPolicyErrors.None)
                return true;

            if (sslPolicyErrors == SslPolicyErrors.RemoteCertificateChainErrors)
            {
                // Check chain elements
                foreach (var itemInChain in chain.ChainElements)
                {
                    if (itemInChain.Certificate.Issuer.Contains("CN=Garnet"))
                        return true;
                }
            }
            throw new Exception($"Certicate errors found {sslPolicyErrors}!");
        }

        public static void CreateTestLibrary(string[] namespaces, string[] referenceFiles, string[] filesToCompile, string dstFilePath)
        {
            if (File.Exists(dstFilePath))
            {
                File.Delete(dstFilePath);
            }

            foreach (var referenceFile in referenceFiles)
            {
                ClassicAssert.IsTrue(File.Exists(referenceFile), $"File '{Path.GetFullPath(referenceFile)}' does not exist.");
            }

            var references = referenceFiles.Select(f => MetadataReference.CreateFromFile(f));

            foreach (var fileToCompile in filesToCompile)
            {
                ClassicAssert.IsTrue(File.Exists(fileToCompile), $"File '{Path.GetFullPath(fileToCompile)}' does not exist.");
            }

            var explicitUsings = @"
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
";

            var parseFunc = new Func<string, SyntaxTree>(filePath =>
            {
                var source = $"{explicitUsings}{Environment.NewLine}{File.ReadAllText(filePath)}";
                var stringText = SourceText.From(source, Encoding.UTF8);
                return SyntaxFactory.ParseSyntaxTree(stringText,
                    CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Latest), string.Empty);
            });

            var syntaxTrees = filesToCompile.Select(f => parseFunc(f));

            var compilationOptions = new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
                .WithAllowUnsafe(true)
                .WithOverflowChecks(true)
                .WithOptimizationLevel(OptimizationLevel.Release)
                .WithUsings(namespaces);


            var compilation = CSharpCompilation.Create(Path.GetFileName(dstFilePath), syntaxTrees, references, compilationOptions);

            try
            {
                var result = compilation.Emit(dstFilePath);
                ClassicAssert.IsTrue(result.Success, string.Join(Environment.NewLine, result.Diagnostics.Select(d => d.ToString())));
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        public static StoreAddressInfo GetStoreAddressInfo(IServer server, bool includeReadCache = false)
        {
            StoreAddressInfo result = default;
            var info = server.Info("STORE");
            foreach (var section in info)
            {
                foreach (var entry in section)
                {
                    if (entry.Key.Equals("Log.BeginAddress"))
                        result.BeginAddress = long.Parse(entry.Value);
                    else if (entry.Key.Equals("Log.HeadAddress"))
                        result.HeadAddress = long.Parse(entry.Value);
                    else if (entry.Key.Equals("Log.SafeReadOnlyAddress"))
                        result.ReadOnlyAddress = long.Parse(entry.Value);
                    else if (entry.Key.Equals("Log.FlushedUntilAddress"))
                        result.FlushedUntilAddress = long.Parse(entry.Value);
                    else if (entry.Key.Equals("Log.TailAddress"))
                        result.TailAddress = long.Parse(entry.Value);
                    else if (entry.Key.Equals("Log.CurrentMemorySizeBytes"))
                        result.MemorySize = long.Parse(entry.Value);
                    else if (includeReadCache && entry.Key.Equals("ReadCache.HeadAddress"))
                        result.ReadCacheHeadAddress = long.Parse(entry.Value);
                    else if (includeReadCache && entry.Key.Equals("ReadCache.BeginAddress"))
                        result.ReadCacheBeginAddress = long.Parse(entry.Value);
                    else if (includeReadCache && entry.Key.Equals("ReadCache.TailAddress"))
                        result.ReadCacheTailAddress = long.Parse(entry.Value);
                }
            }
            return result;
        }

        /// <summary>
        /// Get effective memory size based on configured memory size and page size.
        /// </summary>
        /// <param name="memorySize">Memory size string</param>
        /// <param name="pageSize">Page size string</param>
        /// <param name="parsedPageSize">Parsed page size</param>
        /// <returns>Effective memory size</returns>
        public static long GetEffectiveMemorySize(string memorySize, string pageSize, out long parsedPageSize)
        {
            parsedPageSize = ServerOptions.PreviousPowerOf2(ServerOptions.ParseSize(pageSize, out _));
            return ServerOptions.ParseSize(memorySize, out _);
        }

        /// <summary>
        /// Get a random alphanumeric string of specified length
        /// </summary>
        /// <param name="len">Length of string</param>
        /// <returns>Random alphanumeric string</returns>
        public static string GetRandomString(int len)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return RandomNumberGenerator.GetString(chars, len);
        }

        internal static void OnTearDown(bool waitForDelete = false, ILogger logger = null, bool suppressFailure = false)
        {
            var failTestOnLeak = !suppressFailure && TestContext.CurrentContext.Result.Outcome.Status == TestStatus.Passed;

            DeleteDirectory(MethodTestDir, wait: waitForDelete);
            var count = Tsavorite.core.LightEpoch.ActiveInstanceCount();
            var failMsg = "";

            if (count != 0)
            {
                // Reset all instances to avoid impacting other tests
                Tsavorite.core.LightEpoch.ResetAllInstances();
                logger?.LogError("Tsavorite.core.LightEpoch instances still active: {count}", count);
                failMsg += $"Tsavorite.core.LightEpoch instances still active: {count}";
            }

            var count2 = client.LightEpoch.ActiveInstanceCount();
            if (count2 != 0)
            {
                // Reset all instances to avoid impacting other tests
                client.LightEpoch.ResetAllInstances();
                logger?.LogError("Garnet.client.LightEpoch instances still active: {count2}", count2);

                if (!string.IsNullOrEmpty(failMsg))
                {
                    failMsg += Environment.NewLine;
                }

                failMsg += $"Garnet.client.LightEpoch instances still active: {count2}";
            }

            if (failTestOnLeak && !string.IsNullOrEmpty(failMsg))
            {
                Assert.Fail(failMsg);
            }
            else if (logger is null && !string.IsNullOrEmpty(failMsg))
            {
                // Guarantee the leak message goes _somewhere_ if it doesn't fail the test
                TestContext.Out.WriteLine(failMsg);
            }
        }
    }
}