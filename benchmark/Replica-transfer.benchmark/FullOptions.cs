// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Globalization;
using System.Net;
using Garnet.server;

namespace ReplicaTransfer.benchmark;

/// <summary>
/// Validated settings for one runner and its separately launched server build.
/// Sender cases select the wire transfer shape; replica write policy is local to the receiver.
/// </summary>
/// <param name="Role">Send (primary driver) or receive (replica controller).</param>
/// <param name="Server">Absolute DLL/executable path to the independently selected server build.</param>
/// <param name="Output">Existing storage parent for fresh, retained per-run directories.</param>
/// <param name="Bind">Private/loopback IP for this runner's owned server and receiver control listener.</param>
/// <param name="Host">Receiver IP used by the sender for control and replica connections.</param>
/// <param name="Port">Runner control port, separate from server RESP ports.</param>
/// <param name="ServerPort">RESP port of this runner's owned primary or replica.</param>
/// <param name="DataSize">Total logical value bytes, excluding key, record and checkpoint overhead.</param>
/// <param name="Repeats">Fresh sender runs per distinct chunk/depth case.</param>
/// <param name="Runs">Number of connections the receiver serves before exiting.</param>
/// <param name="WriteBuffers">Replica's bounded queued/in-flight chunk count; zero selects synchronous writes.</param>
/// <param name="Concurrently">Permit parallel positional writes independently of the buffer count.</param>
/// <param name="PhaseTiming">Opt in the owned process at startup to whole-shipment/recovery timing.</param>
/// <param name="Timeout">Whole-run deadline including preparation and verification outside measured sync.</param>
/// <param name="Csv">Optional new results file; an existing path is not overwritten.</param>
/// <param name="Cases">Distinct effective chunk-byte sizes and bounded pipeline depths.</param>
/// <param name="WriteBuffersSpecified">Reject missing external-build support rather than use an implicit default.</param>
/// <param name="ConcurrentlySpecified">Reject unsupported explicit policy rather than guess the build's default.</param>
/// <param name="Device">Startup device configuration forwarded to this runner's owned server.</param>
/// <param name="ServerTransferDefaults">Leave primary transfer sizing to the selected build and report it as unavailable.</param>
internal sealed record FullOptions(
    string Role, string Server, string Output, IPAddress Bind, IPAddress Host, int Port, int ServerPort,
    long DataSize, int Repeats, int Runs, int WriteBuffers, bool Concurrently, bool PhaseTiming,
    TimeSpan Timeout, string Csv, (int Chunk, int Depth)[] Cases, bool WriteBuffersSpecified, bool ConcurrentlySpecified,
    DeviceSettings Device, bool ServerTransferDefaults)
{
    /// <summary>Resolves role-specific options without starting servers or creating storage.</summary>
    /// <param name="args">Role followed by unique option/value pairs for the full scenario.</param>
    /// <param name="linux">Optional platform override for parsing tests; null uses the current OS.</param>
    internal static FullOptions Parse(string[] args, bool? linux = null)
    {
        if (args.Length == 0 || args[0] is not ("send" or "receive") || Program.SelectScenario(args) != "full")
            throw new ArgumentException("Full scenario requires send or receive.");
        string[] common = ["--scenario", "--server", "--output", "--bind", "--port", "--server-port",
            "--timeout-seconds", "--phase-timing", "--device-type"];
        string[] specific = args[0] == "send"
            ? ["--host", "--data-size", "--chunks", "--depths", "--repeats", "--csv", "--server-transfer-defaults"]
            : ["--runs", "--write-buffers", "--write-buffers-concurrently"];
        var allowed = common.Concat(specific).Concat(DeviceSettings.Switches).ToHashSet(StringComparer.Ordinal);
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 1; i < args.Length; i += 2)
            if (!allowed.Contains(args[i]) || i + 1 == args.Length || !values.TryAdd(args[i], args[i + 1]))
                throw new ArgumentException($"Unknown, repeated, or valueless full-scenario option: {args[i]}");
        string Get(string key, string fallback) => values.GetValueOrDefault(key, fallback);
        string Required(string key) => values.GetValueOrDefault(key) ?? throw new ArgumentException($"{key} is required.");
        int Number(string key, int fallback, int min, int max)
        {
            if (!int.TryParse(Get(key, fallback.ToString(CultureInfo.InvariantCulture)), NumberStyles.None,
                CultureInfo.InvariantCulture, out var value) || value < min || value > max)
                throw new ArgumentException($"{key} must be {min} through {max}.");
            return value;
        }
        bool Boolean(string key)
        {
            if (!bool.TryParse(Get(key, "false"), out var value))
                throw new ArgumentException($"{key} must be true or false.");
            return value;
        }
        var serverTransferDefaults = Boolean("--server-transfer-defaults");
        if (serverTransferDefaults && (values.ContainsKey("--chunks") || values.ContainsKey("--depths")))
            throw new ArgumentException("--server-transfer-defaults true cannot be combined with --chunks or --depths.");

        // Control and RESP endpoints are separate; both must stay on the trusted test network.
        var bind = PrivateAddress(Get("--bind", "127.0.0.1"));
        var host = PrivateAddress(Get("--host", "127.0.0.1"));
        var port = Number("--port", 7010, 1, 65535);
        var serverPort = Number("--server-port", args[0] == "send" ? 7012 : 7011, 1, 65535);
        if (port == serverPort)
            throw new ArgumentException("Control and server ports must differ.");
        var server = Path.GetFullPath(Required("--server"));
        if (!File.Exists(server))
            throw new FileNotFoundException("Server build does not exist.", server);
        var output = Path.GetFullPath(Required("--output"));
        if (!Directory.Exists(output))
            throw new DirectoryNotFoundException(output);

        // Device defaults are runner/platform-specific, while transfer normalization belongs to Garnet.
        var device = DeviceSettings.Parse(values, full: true, linux ?? OperatingSystem.IsLinux());
        if (!ServerOptions.TryParseSize(Get("--data-size", "512m"), out var size) || size < (16 << 10) || size > (64L << 30))
            throw new ArgumentException("--data-size must be 16k through 64g.");
        var cases = new List<(int, int)>();
        foreach (var chunk in Get("--chunks", "16m").Split(','))
            foreach (var depth in Get("--depths", "4").Split(','))
            {
                var options = new GarnetServerOptions
                {
                    ReplicaDiskBasedSyncChunkSize = chunk,
                    ReplicaDiskBasedSyncPipelineDepth = int.Parse(depth, CultureInfo.InvariantCulture)
                };
                cases.Add((options.GetReplicaDiskBasedSyncChunkSizeBytes(), options.GetReplicaDiskBasedSyncPipelineDepth()));
            }
        return new(args[0], server, output, bind, host, port, serverPort, size,
            Number("--repeats", 1, 1, 1000000), Number("--runs", 1, 1, 1000000),
            Number("--write-buffers", 1, 0, GarnetServerOptions.ReplicaDiskBasedSyncWriteBuffersMax),
            Boolean("--write-buffers-concurrently"), Boolean("--phase-timing"),
            TimeSpan.FromSeconds(Number("--timeout-seconds", 300, 1, 3600)),
            values.TryGetValue("--csv", out var csv) ? Path.GetFullPath(csv) : null, cases.Distinct().ToArray(),
            values.ContainsKey("--write-buffers"), values.ContainsKey("--write-buffers-concurrently"), device, serverTransferDefaults);
    }

    /// <summary>Restricts unauthenticated test endpoints to loopback or explicit private IPs.</summary>
    /// <param name="text">IP literal; DNS names and wildcard/public addresses are rejected.</param>
    internal static IPAddress PrivateAddress(string text)
    {
        if (!IPAddress.TryParse(text, out var address))
            throw new ArgumentException("Endpoints must be IP literals.");
        var bytes = address.GetAddressBytes();
        if (IPAddress.IsLoopback(address) || (bytes.Length == 4 &&
            (bytes[0] == 10 || (bytes[0] == 172 && bytes[1] is >= 16 and <= 31) || (bytes[0] == 192 && bytes[1] == 168))) ||
            (bytes.Length == 16 && (bytes[0] & 0xfe) == 0xfc))
            return address;
        throw new ArgumentException("Use loopback or an explicit private test-network address, never a public/wildcard address.");
    }
}