// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using NUnit.Framework;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Hands each test host a private, contiguous run of TCP ports and holds it for the life of the process.
    /// <para>
    /// Test ports must satisfy two unrelated constraints, and the older fixed-assignment scheme satisfied
    /// neither reliably. First, a port the OS can hand out for an outbound connection is unusable no matter how
    /// carefully it is chosen: between the moment a port is observed free and the moment the server binds it,
    /// the kernel can assign it to a <c>connect()</c> and the bind then fails. That race cannot be closed by
    /// probing, only by staying out of the ephemeral range entirely - hence <see cref="UniverseBase"/>. Second,
    /// several test hosts run at once, from one checkout and from several, and they must not pick the same
    /// ports; that is what the lease files provide.
    /// </para>
    /// <para>
    /// Ports are allocated in fixed-size blocks (<see cref="BlockPorts"/>) rather than singly. A cluster
    /// sub-project needs a contiguous run - <c>GetShardEndPoints</c> builds node endpoints as
    /// <c>base + nodeIndex</c> - and a fixed, aligned block keeps that run from straddling a boundary, so two
    /// hosts requesting runs can never partially overlap.
    /// </para>
    /// </summary>
    internal static class TestPortAllocator
    {
        /// <summary>
        /// Lowest allocatable port.
        /// <para>
        /// The upper bound is set by the ephemeral ranges: Linux defaults to 32768-60999
        /// (<c>net.ipv4.ip_local_port_range</c>) and Windows and macOS to 49152-65535. Anything at or above
        /// 32768 is therefore reachable by the kernel's outbound-connection allocator on some supported
        /// platform, which is the race described on this class. The universe stops below 32768 for that reason.
        /// </para>
        /// <para>
        /// The lower bound avoids the crowded region below 16384, which holds both ports a Garnet developer is
        /// likely to be running (6379 Redis, 5432, 11211, 8080, 9090) and ports the test suite itself depends
        /// on (10000-10002 Azurite, used by the Azure tests). Ports 1-1023 additionally require privilege.
        /// </para>
        /// </summary>
        internal const int UniverseBase = 16384;

        /// <summary>
        /// Number of allocatable ports. A power of two so that <see cref="BlockCount"/> is one as well, which
        /// is what makes the probe sequence in <see cref="Reserve"/> cover every block exactly once.
        /// </summary>
        internal const int UniverseSize = 16384;

        /// <summary>
        /// Ports per block. A cluster sub-project binds one port per node and the largest cluster test creates
        /// five, so this leaves room for growth, for a second server in the same host, and for the spare ports
        /// diagnostics rely on.
        /// </summary>
        internal const int BlockPorts = 32;

        /// <summary>
        /// Number of blocks the universe is divided into. A power of two, which <see cref="Reserve"/> depends on.
        /// </summary>
        internal const int BlockCount = UniverseSize / BlockPorts;

        /// <summary>
        /// First port reserved for the Docker image validation harness in
        /// <c>test/docker-tests/validate_docker_images.py</c>, which allocates container ports from a fixed base
        /// rather than through this allocator. It can run alongside the managed test suite, so blocks meeting
        /// this range are never handed out. <c>DockerTestPortsAreCarvedOut</c> keeps the two in sync.
        /// </summary>
        internal const int DockerReservedBase = 30000;

        /// <summary>
        /// Ports reserved for the Docker harness. It uses <c>base + phaseOffset + imageIndex</c> with phase
        /// offsets up to 400, so its true span is a few hundred ports; the reservation is rounded up to leave
        /// room for further phases without another edit here.
        /// </summary>
        internal const int DockerReservedCount = 1000;

        /// <summary>
        /// Environment variable pinning an explicit base port, bypassing the search. Intended for reproducing a
        /// failure on a known port or for a host whose firewall only opens a fixed range. The value is still
        /// probed, so a pinned port that is in use fails rather than silently colliding.
        /// </summary>
        internal const string PinnedBaseEnvVar = "GARNET_TEST_PORT_BASE";

        /// <summary>
        /// Leases held by this process. Each is an open, exclusively locked file whose existence is the claim;
        /// the OS releases it on exit, crash, or kill, which is how a block becomes available again. The list
        /// roots them for the process lifetime: letting one be collected and finalized would silently release
        /// the block mid-run. Deliberately never disposed.
        /// </summary>
        private static readonly List<FileStream> heldLeases = [];

        private static readonly object reservationLock = new();

        /// <summary>
        /// Blocks already reserved by this process, so a second request in the same host - a cluster project
        /// that also wants a standalone port, say - does not hand back ports already in use here.
        /// </summary>
        private static readonly HashSet<int> reservedBlocks = [];

        /// <summary>
        /// Reserves a contiguous run of ports and holds it until the process exits.
        /// <para>
        /// The search starts at a block derived from the checkout and the calling assembly, so a given test
        /// project returns to the same ports run after run, which keeps <c>netstat</c> output interpretable and
        /// makes a stranded server easy to attribute. On collision it advances by a stride derived from this
        /// process, so two hosts that start at the same block diverge immediately instead of walking in
        /// lockstep. The stride is forced odd and <see cref="BlockCount"/> is a power of two, which together
        /// guarantee the walk visits every block exactly once: completing it is therefore proof that no block
        /// is free, not merely that a retry budget ran out.
        /// </para>
        /// </summary>
        /// <param name="portCount">Contiguous ports required; must fit within a block.</param>
        /// <param name="purpose">Short name used in diagnostics and in the lease file name.</param>
        /// <returns>The first port of the reserved run.</returns>
        internal static int Reserve(int portCount, string purpose)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(portCount, 1);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(portCount, BlockPorts);

            lock (reservationLock)
            {
                var pinned = Environment.GetEnvironmentVariable(PinnedBaseEnvVar);
                if (!string.IsNullOrWhiteSpace(pinned))
                    return ReservePinned(pinned, portCount, purpose);

                var startHash = (ulong)Utility.GetHashCodeWithMix(Encoding.UTF8.GetBytes(CheckoutKey + "|" + purpose));
                var strideHash = (ulong)Utility.GetHashCodeWithMix(
                    Encoding.UTF8.GetBytes($"{Environment.ProcessId}|{Environment.TickCount64}"));

                var block = (int)(startHash % BlockCount);
                var stride = (int)(strideHash % BlockCount) | 1;

                var blockedByLease = 0;
                var blockedByPorts = 0;
                var blockedByCarveOut = 0;

                for (var attempt = 0; attempt < BlockCount; attempt++, block = (block + stride) % BlockCount)
                {
                    if (reservedBlocks.Contains(block))
                        continue;

                    if (OverlapsDockerReservation(block))
                    {
                        blockedByCarveOut++;
                        continue;
                    }

                    var basePort = BlockBasePort(block);
                    var lease = TryAcquireLease(block, purpose);
                    if (lease is null)
                    {
                        blockedByLease++;
                        continue;
                    }

                    if (!PortsAreFree(basePort, portCount))
                    {
                        // A lease is not proof the ports are usable: a service unrelated to the test suite, or
                        // a server stranded by a run that was killed, holds ports while holding no lease.
                        lease.Dispose();
                        blockedByPorts++;
                        continue;
                    }

                    heldLeases.Add(lease);
                    _ = reservedBlocks.Add(block);

                    TestContext.Progress.WriteLine(
                        $"Garnet test ports {basePort}-{basePort + portCount - 1} reserved for '{purpose}' " +
                        $"(block {block}) by process {Environment.ProcessId}.");
                    return basePort;
                }

                throw new InvalidOperationException(
                    $"No Garnet test port block is available for '{purpose}'; all {BlockCount} were examined. " +
                    $"{blockedByLease} are leased by running test hosts, {blockedByPorts} have ports in use " +
                    $"with no lease (typically servers stranded by a killed run), {blockedByCarveOut} are " +
                    $"reserved for the Docker harness, and {reservedBlocks.Count} are already held by this " +
                    $"process. Wait for other runs to finish, terminate stray servers by PID rather than by " +
                    $"name, or set {PinnedBaseEnvVar} to a known-free base port. Leases live in '{LeaseDirectory}'.");
            }
        }

        /// <summary>
        /// Honors <see cref="PinnedBaseEnvVar"/>. The base is used as given rather than snapped to a block, so
        /// an operator can name the exact port they have opened a firewall for. It is still leased and probed,
        /// so pinning cannot silently reintroduce a collision.
        /// </summary>
        /// <param name="raw">Raw environment variable value.</param>
        /// <param name="portCount">Contiguous ports required.</param>
        /// <param name="purpose">Short name used in diagnostics.</param>
        /// <returns>The first port of the reserved run.</returns>
        private static int ReservePinned(string raw, int portCount, string purpose)
        {
            if (!int.TryParse(raw.Trim(), out var basePort) || basePort < 1024 || basePort + portCount > 65536)
            {
                throw new InvalidOperationException(
                    $"{PinnedBaseEnvVar} must be a port between 1024 and {65536 - portCount}; got '{raw}'.");
            }

            if (basePort + portCount > UniverseBase + UniverseSize)
            {
                TestContext.Progress.WriteLine(
                    $"Warning: {PinnedBaseEnvVar}={basePort} puts test ports at or above " +
                    $"{UniverseBase + UniverseSize}, where the OS can assign the same port to an outbound " +
                    $"connection and the bind then fails intermittently.");
            }

            var lease = TryAcquireLease(PortBlock(basePort), purpose)
                ?? throw new InvalidOperationException(
                    $"{PinnedBaseEnvVar}={basePort} names a port block already leased by another test host.");

            if (!PortsAreFree(basePort, portCount))
            {
                lease.Dispose();
                throw new InvalidOperationException(
                    $"{PinnedBaseEnvVar}={basePort} names ports that are already in use ({basePort}-" +
                    $"{basePort + portCount - 1}), so '{purpose}' cannot bind them.");
            }

            heldLeases.Add(lease);
            return basePort;
        }

        /// <summary>First port of a block.</summary>
        /// <param name="block">Block index.</param>
        /// <returns>The block's base port.</returns>
        internal static int BlockBasePort(int block) => UniverseBase + (block * BlockPorts);

        /// <summary>Block containing a port, for ports inside the universe.</summary>
        /// <param name="port">Port to locate.</param>
        /// <returns>The containing block index.</returns>
        internal static int PortBlock(int port) => (port - UniverseBase) / BlockPorts;

        /// <summary>
        /// Whether any port in a block falls inside the Docker harness reservation. Compared as ranges rather
        /// than as block indices because the reservation is not block-aligned.
        /// </summary>
        /// <param name="block">Block index.</param>
        /// <returns>True when the block meets the reservation.</returns>
        internal static bool OverlapsDockerReservation(int block)
        {
            var basePort = BlockBasePort(block);
            return basePort < DockerReservedBase + DockerReservedCount && DockerReservedBase < basePort + BlockPorts;
        }

        /// <summary>
        /// Directory holding lease files. <see cref="Path.GetTempPath"/> is per-user on Windows and shared on
        /// Linux; both are correct for the case this exists to serve, which is several checkouts on one
        /// developer's machine. The one arrangement it would not cover is containers that share a network
        /// namespace but not a temp directory, where ports collide but leases cannot see each other.
        /// </summary>
        private static string LeaseDirectory => Path.Combine(Path.GetTempPath(), "garnet-test-ports");

        /// <summary>
        /// Takes the lease on a block, or returns null when another live test host holds it. The lock is the
        /// claim, so it is held open; a host that dies has its lock released by the OS, and the stale file is
        /// then reopened and reused by whoever claims the block next.
        /// </summary>
        /// <param name="block">Block index.</param>
        /// <param name="purpose">Short name recorded in the file name for diagnostics.</param>
        /// <returns>The held lease, or null when the block is taken.</returns>
        private static FileStream TryAcquireLease(int block, string purpose)
        {
            try
            {
                _ = Directory.CreateDirectory(LeaseDirectory);
                var path = Path.Combine(LeaseDirectory, $"block-{block:D4}.lease");
                var lease = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);

                // Recorded for diagnosis only; the lock, not the contents, is what reserves the block.
                var owner = Encoding.UTF8.GetBytes(
                    $"pid {Environment.ProcessId} purpose {purpose} checkout {CheckoutKey}");
                lease.SetLength(0);
                lease.Write(owner);
                lease.Flush();
                return lease;
            }
            catch (IOException)
            {
                // Held by a live test host.
                return null;
            }
            catch (UnauthorizedAccessException)
            {
                // A lease left by another user on a shared machine; treat it as taken.
                return null;
            }
        }

        /// <summary>
        /// Whether a run of ports can be bound right now.
        /// </summary>
        /// <param name="basePort">First port of the run.</param>
        /// <param name="portCount">Number of ports.</param>
        /// <returns>True when every port in the run is free.</returns>
        internal static bool PortsAreFree(int basePort, int portCount)
        {
            for (var port = basePort; port < basePort + portCount; port++)
            {
                if (!IsPortFree(port))
                    return false;
            }

            return true;
        }

        /// <summary>
        /// Whether a port can currently be bound. Both wildcards are probed, and exclusively, so a listener is
        /// detected wherever it sits: tests bind the IPv4 and IPv6 loopbacks, every address
        /// <see cref="Dns.GetHostAddresses(string)"/> returns, and <see cref="IPAddress.Any"/>, and a wildcard
        /// bind conflicts with a specific-address one only when it asks for exclusive use.
        /// <para>
        /// Each family is bound separately rather than through one dual-mode socket, whose treatment of
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
        /// Binds one address exclusively and releases it again. Exclusivity is what makes this a probe: a
        /// shared bind succeeds alongside a listener on a specific address under that wildcard and would report
        /// the port free.
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
                // AddressAlreadyInUse when something is listening, and on Windows AccessDenied when the port
                // falls in a range reserved by Hyper-V, WSL, or another netsh exclusion. Both mean unusable,
                // so the error code is deliberately not inspected.
                return false;
            }
        }

        /// <summary>
        /// Identifies the checkout - one working copy of the repo, meaning the directory containing
        /// <c>Garnet.slnx</c>, which is the worktree root under <c>git worktree</c> and the clone root
        /// otherwise. Every test host launched from that directory resolves the same key, so a project's start
        /// block is stable across runs while two checkouts of the same project start in different places.
        /// <see cref="AppContext.BaseDirectory"/> rather than the NUnit test directory because this is needed
        /// during static initialization. The solution file is the marker because in a worktree .git is a file.
        /// </summary>
        private static string CheckoutKey
        {
            get
            {
                var dir = new DirectoryInfo(AppContext.BaseDirectory);
                while (dir != null && !File.Exists(Path.Combine(dir.FullName, "Garnet.slnx")))
                    dir = dir.Parent;

                var root = Path.TrimEndingDirectorySeparator(
                    Path.GetFullPath(dir?.FullName ?? AppContext.BaseDirectory));
                return OperatingSystem.IsWindows() ? root.ToLowerInvariant() : root;
            }
        }

        /// <summary>
        /// Name identifying the test project asking for ports. The assembly name is used rather than a
        /// hand-maintained enum so that adding a test project needs no edit here and cannot collide with an
        /// existing entry.
        /// </summary>
        /// <returns>The calling test assembly's simple name.</returns>
        internal static string CallingProjectName()
            => Assembly.GetCallingAssembly().GetName().Name ?? "Garnet.test";

    }
}