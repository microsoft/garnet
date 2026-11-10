// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Garnet.client;
using Garnet.common;

namespace ReplicaTransfer.benchmark;

/// <summary>
/// Coordinates fresh real primary/replica processes through production RESP replication.
/// The control connection exchanges case settings and completion, never checkpoint payloads.
/// Each server loads its own selected build and owns its data/checkpoints independently.
/// </summary>
internal static class FullReplication
{
    /// <summary>Identifies a wire-transfer case and acknowledges its verified completion.</summary>
    /// <param name="Protocol">Control protocol discriminator, separate from production replication.</param>
    /// <param name="Chunk">Control-case chunk size; not a physical setting when the primary uses server defaults.</param>
    /// <param name="Depth">Control-case depth; not a physical setting when the primary uses server defaults.</param>
    internal sealed record Request(string Protocol, int Chunk, int Depth);

    /// <summary>Reports the receiver's owned process identity and lifecycle stage.</summary>
    /// <param name="Protocol">Matching control protocol discriminator.</param>
    /// <param name="Stage">Ready after owned-server startup, or complete after verification and shutdown.</param>
    /// <param name="ServerPort">Receiver's real replica RESP port, not the control listener port.</param>
    /// <param name="BinaryHash">Fingerprint of the receiver's selected server build/dependencies.</param>
    /// <param name="ServerInfo">Owned replica's INFO SERVER output.</param>
    /// <param name="WriteBuffers">Configured replica buffer count, or null if an older build cannot report it.</param>
    /// <param name="Concurrently">Configured replica write policy, or null when unavailable.</param>
    /// <param name="ReplicationInfo">Post-verification replica INFO, empty in the ready response.</param>
    /// <param name="DeviceSettings">Forwarded replica I/O inputs, not measured kernel-normalized settings.</param>
    internal sealed record Response(string Protocol, string Stage, int ServerPort, string BinaryHash,
        string ServerInfo, int? WriteBuffers, bool? Concurrently, string ReplicationInfo, string DeviceSettings);
    const string ControlProtocol = "GARNET_FULL_BENCH_V2";
    const int ValueLength = 16 << 10;

    /// <summary>Runs the selected controller role without embedding either server in the runner process.</summary>
    /// <param name="options">Validated local runner/server settings.</param>
    /// <param name="token">Cancellation spanning startup, preparation, measured sync and verification.</param>
    internal static async Task RunAsync(FullOptions options, CancellationToken token)
    {
        if (options.Role == "receive")
            await ReceiveAsync(options, token);
        else
            await SendAsync(options, token);
    }

    /// <summary>Serves cases sequentially, with a new owned replica and retained storage directory per case.</summary>
    /// <param name="options">Receiver endpoint, server build, run count and local write/device policy.</param>
    /// <param name="token">Parent cancellation; each accepted case also has a whole-run deadline.</param>
    private static async Task ReceiveAsync(FullOptions options, CancellationToken token)
    {
        var listener = new TcpListener(options.Bind, options.Port);
        listener.Start();
        try
        {
            for (var run = 0; run < options.Runs; run++)
            {
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
                timeout.CancelAfter(options.Timeout);
                using var connection = await listener.AcceptTcpClientAsync(timeout.Token);
                var stream = connection.GetStream();

                // Validate the control case before starting the replica; checkpoint bytes use production RESP.
                var request = await ReadAsync<Request>(stream, timeout.Token);
                if (request.Protocol != ControlProtocol)
                    throw new InvalidDataException("Not a full-benchmark control connection.");
                var transfer = new Garnet.server.GarnetServerOptions
                {
                    ReplicaDiskBasedSyncChunkSize = request.Chunk.ToString(CultureInfo.InvariantCulture),
                    ReplicaDiskBasedSyncPipelineDepth = request.Depth
                };
                if (transfer.GetReplicaDiskBasedSyncChunkSizeBytes() != request.Chunk)
                    throw new InvalidDataException("Control request chunk is not an effective chunk size.");
                _ = transfer.GetReplicaDiskBasedSyncPipelineDepth();

                // Fresh replica storage cannot inherit a checkpoint from another case or selected version.
                await using var server = await OwnedServer.StartAsync(options, request.Chunk, request.Depth, timeout.Token);
                var info = await CommandAsync(server.Client, timeout.Token, "INFO", "SERVER");
                var ready = new Response(ControlProtocol, "ready", options.ServerPort, server.BinaryHash, info,
                    server.WriteBuffers, server.Concurrently, "", options.Device.Identity(OperatingSystem.IsLinux()));
                await WriteAsync(stream, ready, timeout.Token);

                // The sender signals only after marker visibility and exhaustive logical verification.
                var completed = await ReadAsync<Request>(stream, timeout.Token);
                if (completed.Protocol != ControlProtocol || completed.Chunk != request.Chunk || completed.Depth != request.Depth)
                    throw new InvalidDataException("Mismatched full-benchmark completion.");
                if (!Directory.Exists(server.CheckpointDirectory) ||
                    !Directory.EnumerateFiles(server.CheckpointDirectory, "*", SearchOption.AllDirectories).Any())
                    throw new InvalidDataException("Fresh replica has no received checkpoint files; full disk-based sync was not verified.");
                var replication = await CommandAsync(server.Client, timeout.Token, "INFO", "REPLICATION");

                // Keep received files/logs for inspection, but stop the owned process before confirming completion.
                await server.DisposeAsync();
                await WriteAsync(stream, ready with { Stage = "complete", ReplicationInfo = replication }, timeout.Token);
            }
        }
        finally
        {
            listener.Stop();
        }
    }

    /// <summary>
    /// Drives each fresh primary through untimed preparation, measured production sync, and untimed validation.
    /// Publishes a verified row only after both controllers finish their completion checks.
    /// </summary>
    /// <param name="options">Primary build/storage, peer endpoint and transfer cases.</param>
    /// <param name="token">Cancellation for the sweep; each case additionally uses its configured deadline.</param>
    private static async Task SendAsync(FullOptions options, CancellationToken token)
    {
        await using var csv = options.Csv == null ? null : new StreamWriter(
            new FileStream(options.Csv, FileMode.CreateNew, FileAccess.Write, FileShare.Read), Encoding.UTF8);
        const string header = "scenario,status,chunk_bytes,depth,write_buffers,concurrently,payload_bytes,keys,sync_seconds,barrier_seconds,primary_binary_sha256,replica_binary_sha256,checkpoint_transfer_ms,checkpoint_bytes,recovery_ms,phase_timing,primary_info,replica_info,error,primary_io,replica_io,primary_transfer_configuration";
        if (csv != null)
            await csv.WriteLineAsync(header);
        foreach (var (chunk, depth) in options.Cases)
        {
            for (var repeat = 0; repeat < options.Repeats; repeat++)
            {
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
                timeout.CancelAfter(options.Timeout);
                var ct = timeout.Token;
                var request = new Request(ControlProtocol, chunk, depth);
                var keys = checked((int)((options.DataSize + ValueLength - 1) / ValueLength));
                Response ready = null;
                OwnedServer primary = null;
                try
                {
                    // Establish the test-only control handshake and obtain the actual replica build/settings.
                    using var control = new TcpClient(options.Host.AddressFamily);
                    await control.ConnectAsync(options.Host, options.Port, ct);
                    var stream = control.GetStream();
                    await WriteAsync(stream, request, ct);
                    ready = await ReadAsync<Response>(stream, ct);
                    if (ready.Protocol != ControlProtocol || ready.Stage != "ready")
                        throw new InvalidDataException("Not a ready full-benchmark receiver.");
                    primary = await OwnedServer.StartAsync(options, chunk, depth, ct);
                    using var replica = await ConnectAsync(new IPEndPoint(options.Host, ready.ServerPort), ct);
                    await ExpectAsync(replica, "OK", ct, "READONLY");
                    var primaryInfo = await CommandAsync(primary.Client, ct, "INFO", "SERVER");
                    await ExpectAsync(primary.Client, "OK", ct, "CLUSTER", "ADDSLOTSRANGE", "0", "16383");

                    // Untimed preparation: bounded RESP batches seed this primary, then its own SAVE creates files.
                    // Alternating strings/hashes exercise inline values and serialized objects in the unified store.
                    var operations = new List<Task>(128);
                    for (var key = 0; key < keys; key++)
                    {
                        var value = Value(key, options.DataSize);
                        if ((key & 1) == 0)
                            operations.Add(ExpectAsync(primary.Client, "OK", ct, "SET", Key(key), value));
                        else
                            operations.Add(ExpectAsync(primary.Client, "1", ct, "HSET", Key(key), "field", value));
                        if (operations.Count == 128)
                        {
                            await Task.WhenAll(operations);
                            operations.Clear();
                        }
                    }
                    await Task.WhenAll(operations);
                    operations.Clear();
                    await ExpectAsync(primary.Client, "OK", ct, "SAVE");
                    var id = await CommandAsync(primary.Client, ct, "CLUSTER", "MYID");
                    await ExpectAsync(replica, "OK", ct, "CLUSTER", "MEET", options.Bind.ToString(), options.ServerPort.ToString(CultureInfo.InvariantCulture));
                    await WaitForAsync(replica, text => text.Contains(id, StringComparison.Ordinal), ct, "CLUSTER", "NODES");

                    // Measured interval: production attach includes checkpoint shipment/recovery, followed by
                    // a new replicated marker. Read visibility confirms catch-up, not fsync/crash durability.
                    var started = Stopwatch.GetTimestamp();
                    await ExpectAsync(replica, "OK", ct, "CLUSTER", "REPLICATE", id, "SYNC");
                    var attached = Stopwatch.GetTimestamp();
                    var marker = Guid.NewGuid().ToString("N");
                    await ExpectAsync(primary.Client, "OK", ct, "SET", "{replica-bench}:barrier", marker);
                    await WaitForAsync(replica, text => text == marker, ct, "GET", "{replica-bench}:barrier");
                    var finished = Stopwatch.GetTimestamp();

                    // Untimed verification: check recovery state and every deterministic value in bounded batches.
                    // INFO phases use each server's local clock and are not additive with this interval.
                    var replicaInfo = await CommandAsync(replica, ct, "INFO", "REPLICATION");
                    var fields = Info(replicaInfo);
                    if (!fields.TryGetValue("master_sync_in_progress", out var recovering) ||
                        !recovering.Equals("false", StringComparison.OrdinalIgnoreCase))
                        throw new InvalidDataException("Replica did not confirm completed synchronization.");
                    for (var key = 0; key < keys; key++)
                    {
                        operations.Add(VerifyKeyAsync(key));
                        if (operations.Count == 128)
                        {
                            await Task.WhenAll(operations);
                            operations.Clear();
                        }
                    }
                    await Task.WhenAll(operations);

                    // Let the receiver verify checkpoint presence and close its owned server before publishing.
                    await WriteAsync(stream, request, ct);
                    var completed = await ReadAsync<Response>(stream, ct);
                    if (completed.Protocol != ControlProtocol || completed.Stage != "complete")
                        throw new InvalidDataException("Receiver did not confirm completion.");
                    var primaryReplication = Info(await CommandAsync(primary.Client, ct, "INFO", "REPLICATION"));
                    await primary.DisposeAsync();
                    string Phase(Dictionary<string, string> values, string name) => values.GetValueOrDefault(name, "unavailable");
                    await RowAsync(csv, ["full", "verified",
                        options.ServerTransferDefaults ? "unavailable" : chunk,
                        options.ServerTransferDefaults ? "unavailable" : depth, ready.WriteBuffers?.ToString(CultureInfo.InvariantCulture) ?? "unavailable",
                        ready.Concurrently?.ToString() ?? "unavailable",
                        options.DataSize, keys, Stopwatch.GetElapsedTime(started, finished).TotalSeconds,
                        Stopwatch.GetElapsedTime(attached, finished).TotalSeconds, primary.BinaryHash, ready.BinaryHash,
                        Phase(primaryReplication, "benchmark_checkpoint_transfer_ms"), Phase(primaryReplication, "benchmark_checkpoint_bytes"),
                        Phase(fields, "benchmark_recovery_ms"), options.PhaseTiming, primaryInfo, ready.ServerInfo, "",
                        options.Device.Identity(OperatingSystem.IsLinux()), ready.DeviceSettings,
                        options.ServerTransferDefaults ? "server-defaults" : "configured"]);

                    async Task VerifyKeyAsync(int key)
                    {
                        var actual = (key & 1) == 0
                            ? await CommandAsync(replica, ct, "GET", Key(key))
                            : await CommandAsync(replica, ct, "HGET", Key(key), "field");
                        if (actual != Value(key, options.DataSize))
                            throw new InvalidDataException($"Logical verification failed for {Key(key)}.");
                    }
                }
                catch (Exception ex)
                {
                    // Preserve partial identity/error evidence, but propagate failure rather than claim throughput.
                    await RowAsync(csv, ["full", "failed",
                        options.ServerTransferDefaults ? "unavailable" : chunk,
                        options.ServerTransferDefaults ? "unavailable" : depth, ready?.WriteBuffers, ready?.Concurrently,
                        options.DataSize, keys, "", "", primary?.BinaryHash, ready?.BinaryHash, "unavailable",
                        "unavailable", "unavailable", options.PhaseTiming, "", ready?.ServerInfo, ex.Message,
                        options.Device.Identity(OperatingSystem.IsLinux()), ready?.DeviceSettings,
                        options.ServerTransferDefaults ? "server-defaults" : "configured"]);
                    throw;
                }
                finally
                {
                    if (primary != null)
                        await primary.DisposeAsync();
                }
            }
        }
    }

    /// <summary>Places deterministic workload keys in one cluster hash slot.</summary>
    /// <param name="key">Zero-based logical record index shared by preparation and verification.</param>
    internal static string Key(int key) => $"{{replica-bench}}:key:{key}";

    /// <summary>Recreates one seeded ASCII value, including a short final value to match total payload exactly.</summary>
    /// <param name="key">Zero-based record index used as the deterministic seed.</param>
    /// <param name="payload">Total logical bytes across all generated values.</param>
    internal static string Value(int key, long payload)
    {
        var length = (int)Math.Min(ValueLength, payload - (long)key * ValueLength);
        var chars = new char[length];
        var random = new Random(42 + key);
        for (var i = 0; i < chars.Length; i++)
            chars[i] = (char)('!' + random.Next(90));
        return new string(chars);
    }

    /// <summary>Extracts flat INFO fields while ignoring section headings.</summary>
    /// <param name="info">RESP INFO text from one server process.</param>
    internal static Dictionary<string, string> Info(string info)
        => info.Split('\n').Select(line => line.TrimEnd('\r')).Where(line => !line.StartsWith('#') && line.Contains(':'))
            .Select(line => (Index: line.IndexOf(':'), Line: line))
            .ToDictionary(pair => pair.Line[..pair.Index], pair => pair.Line[(pair.Index + 1)..], StringComparer.Ordinal);

    /// <summary>Publishes one invariant, escaped CSV row to stdout and optionally the retained results file.</summary>
    /// <param name="csv">Optional caller-owned writer; the row is flushed for post-failure inspection.</param>
    /// <param name="values">Fields in the full results header order.</param>
    private static async Task RowAsync(StreamWriter csv, object[] values)
    {
        var row = string.Join(',', values.Select(value => "\"" +
            Convert.ToString(value, CultureInfo.InvariantCulture)?.Replace("\"", "\"\"") + "\""));
        Console.WriteLine(row);
        if (csv != null)
        {
            await csv.WriteLineAsync(row);
            await csv.FlushAsync();
        }
    }

    /// <summary>Executes RESP with cancellation closing the connection and late faults still observed.</summary>
    /// <param name="client">Caller-owned session; cancellation makes it unusable for subsequent commands.</param>
    /// <param name="token">Current case/operation deadline.</param>
    /// <param name="command">Command name and arguments as RESP strings.</param>
    internal static async Task<string> CommandAsync(GarnetClientSession client, CancellationToken token, params string[] command)
    {
        using var close = token.Register(client.CloseConnection);
        var response = client.ExecuteAsync(command);
        try
        {
            return await response.WaitAsync(token);
        }
        finally
        {
            _ = response.ContinueWith(task => _ = task.Exception, CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        }
    }

    /// <summary>Rejects an unexpected RESP result rather than allowing the case to proceed.</summary>
    /// <param name="client">Caller-owned server connection.</param>
    /// <param name="expected">Exact successful response text.</param>
    /// <param name="token">Case deadline/cancellation.</param>
    /// <param name="command">RESP command and arguments.</param>
    private static async Task ExpectAsync(GarnetClientSession client, string expected, CancellationToken token, params string[] command)
    {
        var actual = await CommandAsync(client, token, command);
        if (actual != expected)
            throw new InvalidDataException($"{command[0]} {command.ElementAtOrDefault(1)} returned {actual}, expected {expected}.");
    }

    /// <summary>Polls readiness/catch-up without busy spinning, bounded by the case cancellation token.</summary>
    /// <param name="client">Server session to query.</param>
    /// <param name="predicate">Condition applied to each successful command result.</param>
    /// <param name="token">Deadline that ends an otherwise unbounded readiness wait.</param>
    /// <param name="command">RESP query to repeat.</param>
    private static async Task WaitForAsync(GarnetClientSession client, Func<string, bool> predicate, CancellationToken token, params string[] command)
    {
        while (!predicate(await CommandAsync(client, token, command)))
            await Task.Delay(25, token);
    }

    /// <summary>Connects a caller-owned RESP session, disposing it if connection setup fails.</summary>
    /// <param name="endpoint">Actual server RESP endpoint, not the runner control endpoint.</param>
    /// <param name="token">Connection deadline/cancellation.</param>
    internal static async Task<GarnetClientSession> ConnectAsync(IPEndPoint endpoint, CancellationToken token)
    {
        var client = new GarnetClientSession(endpoint, new NetworkBufferSettings(1 << 20, 1 << 17));
        try
        {
            await client.ConnectAsync(token: token);
            return client;
        }
        catch
        {
            client.Dispose();
            throw;
        }
    }

    /// <summary>Writes a length-prefixed JSON control message, separate from production RESP traffic.</summary>
    /// <typeparam name="T">Control request/response shape.</typeparam>
    /// <param name="stream">Caller-owned controller connection.</param>
    /// <param name="message">Case or lifecycle message to serialize.</param>
    /// <param name="token">Case deadline/cancellation.</param>
    private static async Task WriteAsync<T>(NetworkStream stream, T message, CancellationToken token)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(message);
        var length = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(length, bytes.Length);
        await stream.WriteAsync(length, token);
        await stream.WriteAsync(bytes, token);
    }

    /// <summary>Reads exactly one bounded control frame; truncation/invalid lengths fail the case.</summary>
    /// <typeparam name="T">Expected control request/response shape.</typeparam>
    /// <param name="stream">Caller-owned controller connection.</param>
    /// <param name="token">Case deadline/cancellation.</param>
    private static async Task<T> ReadAsync<T>(NetworkStream stream, CancellationToken token)
    {
        var header = new byte[4];
        await stream.ReadExactlyAsync(header, token);
        var length = BinaryPrimitives.ReadInt32LittleEndian(header);
        if (length is < 1 or > 65536)
            throw new InvalidDataException("Invalid control-frame length.");
        var bytes = new byte[length];
        await stream.ReadExactlyAsync(bytes, token);
        return JsonSerializer.Deserialize<T>(bytes) ?? throw new InvalidDataException("Empty control message.");
    }
}

/// <summary>
/// Owns one independently selected server process, its RESP connection and stdout/stderr capture.
/// Shutdown terminates only this process tree; data/checkpoints and diagnostic files remain on disk.
/// Direct internal access in the runner cannot configure a manager inside this separate process.
/// </summary>
internal sealed class OwnedServer : IAsyncDisposable
{
    private readonly Process process;
    private readonly Task stdout;
    private readonly Task stderr;
    private int disposed;
    internal GarnetClientSession Client { get; private set; }
    internal string BinaryHash { get; private set; }
    internal string CheckpointDirectory { get; private set; }
    internal int? WriteBuffers { get; private set; }
    internal bool? Concurrently { get; private set; }

    /// <summary>Takes ownership of a started process and its output-drain tasks.</summary>
    /// <param name="process">The server process created by this runner, never an existing endpoint occupant.</param>
    /// <param name="stdout">Capture task drained through process shutdown.</param>
    /// <param name="stderr">Capture task drained through process shutdown.</param>
    private OwnedServer(Process process, Task stdout, Task stderr)
    {
        this.process = process;
        this.stdout = stdout;
        this.stderr = stderr;
    }

    /// <summary>
    /// Starts a selected build in fresh version-owned storage and returns only after its own readiness/PING.
    /// Unsupported configuration and startup errors fail explicitly; failure disposes any started process.
    /// </summary>
    /// <param name="options">Local role, build path, output parent and startup device/write/timing settings.</param>
    /// <param name="chunk">Normalized byte size forwarded to a primary's checkpoint sender.</param>
    /// <param name="depth">Bounded pipeline depth forwarded to a primary's checkpoint sender.</param>
    /// <param name="token">Whole-case deadline including startup and build fingerprinting.</param>
    internal static async Task<OwnedServer> StartAsync(FullOptions options, int chunk, int depth, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();

        // Each launch owns fresh files; no checkpoint is preinstalled or read by another version in this runner.
        var path = Path.Combine(options.Output, $"full-{options.Role}-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        var log = Path.Combine(path, "log");
        var checkpoint = Path.Combine(path, "checkpoints");
        var start = new ProcessStartInfo
        {
            FileName = Path.GetExtension(options.Server).Equals(".dll", StringComparison.OrdinalIgnoreCase) ? "dotnet" : options.Server,
            WorkingDirectory = path,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };

        // Probe for an occupied endpoint, but do not treat a free probe as a lasting port reservation.
        var reservation = new TcpListener(options.Bind, options.ServerPort);
        reservation.Start();
        reservation.Stop();
        if (start.FileName == "dotnet")
            start.ArgumentList.Add(options.Server);

        // Ask this selected executable about capabilities instead of assuming it matches runner assemblies.
        var supported = await SupportedOptionsAsync(start, token);
        string[] args = ["--bind", options.Bind.ToString(), "--port", options.ServerPort.ToString(CultureInfo.InvariantCulture),
            "--cluster", "--aof", "--storage-tier", "--memory", "64m", "--page", "1m", "--segment", "64m", "--index", "16m",
            "--logdir", log, "--checkpointdir", checkpoint, "--repl-diskless-sync", "false", "--logger-level", "Warning",
            "--repl-attach-timeout", ((int)options.Timeout.TotalSeconds).ToString(CultureInfo.InvariantCulture)];
        foreach (var arg in args)
            start.ArgumentList.Add(arg);
        options.Device.AddServerArguments(start.ArgumentList, supported, OperatingSystem.IsLinux());
        if (options.Role == "send")
            ConfigureTransfer(start, supported, chunk, depth, options.ServerTransferDefaults);
        else
        {
            AddReplicaOption("repl-disk-based-sync-write-buffers", options.WriteBuffers.ToString(CultureInfo.InvariantCulture), options.WriteBuffersSpecified);
            AddReplicaOption("repl-disk-based-sync-write-buffers-concurrently", options.Concurrently.ToString(), options.ConcurrentlySpecified);
        }

        // Select timing through the startup flag; old reference builds remain valid only with timing off.
        ConfigureTiming(start, supported, options.PhaseTiming);
        await File.WriteAllTextAsync(Path.Combine(path, "launch.json"), JsonSerializer.Serialize(new
        {
            options.Server,
            Arguments = start.ArgumentList.ToArray(),
            options.PhaseTiming,
            options.ServerTransferDefaults,
            UnavailableReplicaDefaults = options.Role == "receive" ? new[]
            {
                "repl-disk-based-sync-write-buffers", "repl-disk-based-sync-write-buffers-concurrently"
            }.Where(name => !supported.Contains(name)).ToArray() : []
        }), token);
        var process = Process.Start(start) ?? throw new InvalidOperationException("Failed to start server.");
        var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var owner = new OwnedServer(process, CaptureAsync(process.StandardOutput, Path.Combine(path, "stdout.log"), ready),
            CaptureAsync(process.StandardError, Path.Combine(path, "stderr.log")));
        owner.CheckpointDirectory = checkpoint;
        owner.WriteBuffers = supported.Contains("repl-disk-based-sync-write-buffers") ? options.WriteBuffers : null;
        owner.Concurrently = supported.Contains("repl-disk-based-sync-write-buffers-concurrently") ? options.Concurrently : null;
        try
        {
            // Readiness must originate in this process's stdout before connecting to its RESP port.
            var endpoint = new IPEndPoint(options.Bind, options.ServerPort);
            while (!ready.Task.IsCompleted)
            {
                token.ThrowIfCancellationRequested();
                if (process.HasExited)
                    throw new InvalidOperationException($"Server exited with {process.ExitCode}; see {path}.");
                _ = await Task.WhenAny(ready.Task, Task.Delay(50, token));
            }
            await ready.Task.WaitAsync(token);
            owner.Client = await FullReplication.ConnectAsync(endpoint, token);
            if (await FullReplication.CommandAsync(owner.Client, token, "PING") != "PONG")
                throw new InvalidDataException("Owned server did not respond to PING.");
            if (process.HasExited)
                throw new InvalidOperationException($"Owned server exited; see {path}.");
            owner.BinaryHash = await FingerprintAsync(options.Server, token);
            return owner;
        }
        catch
        {
            await owner.DisposeAsync();
            throw;
        }

        // Older builds may own an unreported implicit policy, but never silently ignore an explicit request.
        void AddReplicaOption(string name, string value, bool explicitlySelected)
        {
            if (supported.Contains(name))
            {
                start.ArgumentList.Add("--" + name);
                start.ArgumentList.Add(value);
            }
            else if (explicitlySelected)
                throw new ArgumentException($"Selected replica build does not support requested --{name}.");
            else
                Console.Error.WriteLine($"UNAVAILABLE: selected replica build does not expose --{name}; its version-owned default is unreported.");
        }
    }

    /// <summary>Preserves version-owned defaults only when explicitly selected; unsupported tuning fails before startup.</summary>
    /// <param name="start">Selected primary's launch arguments.</param>
    /// <param name="supported">Help-advertised option names without leading dashes.</param>
    /// <param name="chunk">Requested normalized chunk bytes.</param>
    /// <param name="depth">Requested pipeline depth.</param>
    /// <param name="serverDefaults">Omit transfer options and leave physical sizing unreported.</param>
    internal static void ConfigureTransfer(ProcessStartInfo start, HashSet<string> supported, int chunk, int depth, bool serverDefaults)
    {
        if (serverDefaults)
        {
            Console.Error.WriteLine("UNAVAILABLE: using the primary build's transfer defaults; chunk size and pipeline depth are unreported.");
            return;
        }
        foreach (var (name, value) in new[] { ("repl-disk-based-sync-chunk-size", chunk), ("repl-disk-based-sync-pipeline-depth", depth) })
        {
            if (!supported.Contains(name))
                throw new ArgumentException($"Selected primary build does not support --{name}; explicitly select --server-transfer-defaults true to use its unreported defaults.");
            start.ArgumentList.Add("--" + name);
            start.ArgumentList.Add(value.ToString(CultureInfo.InvariantCulture));
        }
    }

    /// <summary>Applies the single startup activation mechanism, with explicit rejection of unsupported timing.</summary>
    /// <param name="start">Owned server launch; legacy environment activation is removed for reference-build hygiene.</param>
    /// <param name="supported">Selected build's help-advertised switches, without leading dashes.</param>
    /// <param name="enabled">Requested whole-operation recorder state, captured at server startup.</param>
    internal static void ConfigureTiming(ProcessStartInfo start, HashSet<string> supported, bool enabled)
    {
        start.Environment.Remove("GARNET_REPLICA_BENCH_PHASE_TIMING");
        const string name = "repl-enable-benchmark-timing";
        if (supported.Contains(name))
        {
            start.ArgumentList.Add("--" + name);
            start.ArgumentList.Add(enabled ? "true" : "false");
        }
        else if (enabled)
            throw new ArgumentException($"Selected server build does not support requested --{name} true.");
    }

    /// <summary>Queries a separate short-lived help process without loading the selected build in the runner.</summary>
    /// <param name="serverStart">Executable/DLL launch prefix and fresh working directory.</param>
    /// <param name="token">Case deadline/cancellation, also bounding help execution.</param>
    private static async Task<HashSet<string>> SupportedOptionsAsync(ProcessStartInfo serverStart, CancellationToken token)
    {
        var help = new ProcessStartInfo
        {
            FileName = serverStart.FileName,
            WorkingDirectory = serverStart.WorkingDirectory,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
        foreach (var arg in serverStart.ArgumentList)
            help.ArgumentList.Add(arg);
        help.ArgumentList.Add("--help");
        using var process = Process.Start(help) ?? throw new InvalidOperationException("Cannot query server options.");
        var stdout = process.StandardOutput.ReadToEndAsync(token);
        var stderr = process.StandardError.ReadToEndAsync(token);
        try
        {
            await process.WaitForExitAsync(token);
            var text = await stdout;
            var errors = await stderr;
            if (process.ExitCode != 0)
                throw new InvalidOperationException($"Server --help exited with {process.ExitCode}: {errors}");
            return Regex.Matches(text, @"(?m)^\s*--([a-z0-9-]+)(?:\s|,|$)", RegexOptions.CultureInvariant)
                .Select(match => match.Groups[1].Value).ToHashSet(StringComparer.Ordinal);
        }
        finally
        {
            KillIfRunning(process);
            await process.WaitForExitAsync();
        }
    }

    /// <summary>Hashes sorted binary paths/content and runtime manifests so version labels cannot hide build skew.</summary>
    /// <param name="server">Selected executable/DLL whose output directory and native runtimes define the build.</param>
    /// <param name="token">Case deadline/cancellation during file hashing.</param>
    private static async Task<string> FingerprintAsync(string server, CancellationToken token)
    {
        var directory = Path.GetDirectoryName(server);
        var candidates = Directory.EnumerateFiles(directory, "*", SearchOption.TopDirectoryOnly);
        var runtimes = Path.Combine(directory, "runtimes");
        if (Directory.Exists(runtimes))
            candidates = candidates.Concat(Directory.EnumerateFiles(runtimes, "*", SearchOption.AllDirectories));
        var files = candidates
            .Where(file => Path.GetExtension(file) is ".dll" or ".exe" or ".so" or ".dylib" ||
                file.Contains(".so.", StringComparison.Ordinal) ||
                file.Equals(server, OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal) ||
                file.EndsWith(".deps.json", StringComparison.Ordinal) || file.EndsWith(".runtimeconfig.json", StringComparison.Ordinal))
            .OrderBy(file => Path.GetRelativePath(directory, file).Replace('\\', '/'), StringComparer.Ordinal);
        using var fingerprint = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var path in files)
        {
            fingerprint.AppendData(Encoding.UTF8.GetBytes(Path.GetRelativePath(directory, path).Replace('\\', '/')));
            fingerprint.AppendData([0]);
            using var file = File.OpenRead(path);
            fingerprint.AppendData(await SHA256.HashDataAsync(file, token));
        }
        return Convert.ToHexString(fingerprint.GetHashAndReset());
    }

    /// <summary>Drains redirected output to a retained new file and optionally recognizes the owned startup banner.</summary>
    /// <param name="reader">Process-owned stdout/stderr stream, read until process shutdown.</param>
    /// <param name="path">New diagnostic log path; an existing file is not overwritten.</param>
    /// <param name="ready">Optional readiness signal supplied only for this process's stdout.</param>
    private static async Task CaptureAsync(StreamReader reader, string path, TaskCompletionSource ready = null)
    {
        await using var writer = new StreamWriter(new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.Read));
        string line;
        while ((line = await reader.ReadLineAsync()) != null)
        {
            await writer.WriteLineAsync(line);
            if (line.Contains("Ready to accept connections", StringComparison.Ordinal))
                ready?.TrySetResult();
        }
    }

    /// <summary>Idempotently closes the client, terminates the owned process tree and drains captures; retains files.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
            return;
        try
        {
            Client?.Dispose();
        }
        finally
        {
            try
            {
                KillIfRunning(process);
                await process.WaitForExitAsync();
                await Task.WhenAll(stdout, stderr);
            }
            finally
            {
                process.Dispose();
            }
        }
    }

    /// <summary>Terminates only the specified owned process tree, tolerating its concurrent normal exit.</summary>
    /// <param name="process">Server/help process started by this runner.</param>
    private static void KillIfRunning(Process process)
    {
        try
        {
            if (!process.HasExited)
                process.Kill(entireProcessTree: true);
        }
        catch (InvalidOperationException) when (process.HasExited)
        {
            // The process exited between the state check and termination.
        }
    }
}