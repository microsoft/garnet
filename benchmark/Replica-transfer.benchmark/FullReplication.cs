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

internal static class FullReplication
{
    internal sealed record Request(string Protocol, int Chunk, int Depth);
    internal sealed record Response(string Protocol, string Stage, int ServerPort, string BinaryHash,
        string ServerInfo, int? WriteBuffers, bool? Concurrently, string ReplicationInfo);
    const string ControlProtocol = "GARNET_FULL_BENCH_V1";
    const int ValueLength = 16 << 10;

    internal static async Task RunAsync(FullOptions options, CancellationToken token)
    {
        if (options.Role == "receive")
            await ReceiveAsync(options, token);
        else
            await SendAsync(options, token);
    }

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
                await using var server = await OwnedServer.StartAsync(options, request.Chunk, request.Depth, timeout.Token);
                var info = await CommandAsync(server.Client, timeout.Token, "INFO", "SERVER");
                var ready = new Response(ControlProtocol, "ready", options.ServerPort, server.BinaryHash, info,
                    server.WriteBuffers, server.Concurrently, "");
                await WriteAsync(stream, ready, timeout.Token);
                var completed = await ReadAsync<Request>(stream, timeout.Token);
                if (completed.Protocol != ControlProtocol || completed.Chunk != request.Chunk || completed.Depth != request.Depth)
                    throw new InvalidDataException("Mismatched full-benchmark completion.");
                if (!Directory.Exists(server.CheckpointDirectory) ||
                    !Directory.EnumerateFiles(server.CheckpointDirectory, "*", SearchOption.AllDirectories).Any())
                    throw new InvalidDataException("Fresh replica has no received checkpoint files; full disk-based sync was not verified.");
                var replication = await CommandAsync(server.Client, timeout.Token, "INFO", "REPLICATION");
                await server.DisposeAsync();
                await WriteAsync(stream, ready with { Stage = "complete", ReplicationInfo = replication }, timeout.Token);
            }
        }
        finally
        {
            listener.Stop();
        }
    }

    private static async Task SendAsync(FullOptions options, CancellationToken token)
    {
        await using var csv = options.Csv == null ? null : new StreamWriter(
            new FileStream(options.Csv, FileMode.CreateNew, FileAccess.Write, FileShare.Read), Encoding.UTF8);
        const string header = "scenario,status,chunk_bytes,depth,write_buffers,concurrently,payload_bytes,keys,sync_seconds,barrier_seconds,primary_binary_sha256,replica_binary_sha256,checkpoint_transfer_ms,checkpoint_bytes,recovery_ms,phase_timing,primary_info,replica_info,error";
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
                    var started = Stopwatch.GetTimestamp();
                    await ExpectAsync(replica, "OK", ct, "CLUSTER", "REPLICATE", id, "SYNC");
                    var attached = Stopwatch.GetTimestamp();
                    var marker = Guid.NewGuid().ToString("N");
                    await ExpectAsync(primary.Client, "OK", ct, "SET", "{replica-bench}:barrier", marker);
                    await WaitForAsync(replica, text => text == marker, ct, "GET", "{replica-bench}:barrier");
                    var finished = Stopwatch.GetTimestamp();
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
                    await WriteAsync(stream, request, ct);
                    var completed = await ReadAsync<Response>(stream, ct);
                    if (completed.Protocol != ControlProtocol || completed.Stage != "complete")
                        throw new InvalidDataException("Receiver did not confirm completion.");
                    var primaryReplication = Info(await CommandAsync(primary.Client, ct, "INFO", "REPLICATION"));
                    await primary.DisposeAsync();
                    string Phase(Dictionary<string, string> values, string name) => values.GetValueOrDefault(name, "unavailable");
                    await RowAsync(csv, ["full", "verified", chunk, depth, ready.WriteBuffers?.ToString(CultureInfo.InvariantCulture) ?? "unavailable",
                        ready.Concurrently?.ToString() ?? "unavailable",
                        options.DataSize, keys, Stopwatch.GetElapsedTime(started, finished).TotalSeconds,
                        Stopwatch.GetElapsedTime(attached, finished).TotalSeconds, primary.BinaryHash, ready.BinaryHash,
                        Phase(primaryReplication, "benchmark_checkpoint_transfer_ms"), Phase(primaryReplication, "benchmark_checkpoint_bytes"),
                        Phase(fields, "benchmark_recovery_ms"), options.PhaseTiming, primaryInfo, ready.ServerInfo, ""]);

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
                    await RowAsync(csv, ["full", "failed", chunk, depth, ready?.WriteBuffers, ready?.Concurrently,
                        options.DataSize, keys, "", "", primary?.BinaryHash, ready?.BinaryHash, "unavailable",
                        "unavailable", "unavailable", options.PhaseTiming, "", ready?.ServerInfo, ex.Message]);
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

    internal static string Key(int key) => $"{{replica-bench}}:key:{key}";

    internal static string Value(int key, long payload)
    {
        var length = (int)Math.Min(ValueLength, payload - (long)key * ValueLength);
        var chars = new char[length];
        var random = new Random(42 + key);
        for (var i = 0; i < chars.Length; i++)
            chars[i] = (char)('!' + random.Next(90));
        return new string(chars);
    }

    internal static Dictionary<string, string> Info(string info)
        => info.Split('\n').Select(line => line.TrimEnd('\r')).Where(line => !line.StartsWith('#') && line.Contains(':'))
            .Select(line => (Index: line.IndexOf(':'), Line: line))
            .ToDictionary(pair => pair.Line[..pair.Index], pair => pair.Line[(pair.Index + 1)..], StringComparer.Ordinal);

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

    private static async Task ExpectAsync(GarnetClientSession client, string expected, CancellationToken token, params string[] command)
    {
        var actual = await CommandAsync(client, token, command);
        if (actual != expected)
            throw new InvalidDataException($"{command[0]} {command.ElementAtOrDefault(1)} returned {actual}, expected {expected}.");
    }

    private static async Task WaitForAsync(GarnetClientSession client, Func<string, bool> predicate, CancellationToken token, params string[] command)
    {
        while (!predicate(await CommandAsync(client, token, command)))
            await Task.Delay(25, token);
    }

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

    private static async Task WriteAsync<T>(NetworkStream stream, T message, CancellationToken token)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(message);
        var length = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(length, bytes.Length);
        await stream.WriteAsync(length, token);
        await stream.WriteAsync(bytes, token);
    }

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

    private OwnedServer(Process process, Task stdout, Task stderr)
    {
        this.process = process;
        this.stdout = stdout;
        this.stderr = stderr;
    }

    internal static async Task<OwnedServer> StartAsync(FullOptions options, int chunk, int depth, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
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
        var reservation = new TcpListener(options.Bind, options.ServerPort);
        reservation.Start();
        reservation.Stop();
        if (start.FileName == "dotnet")
            start.ArgumentList.Add(options.Server);
        var supported = await SupportedOptionsAsync(start, token);
        string[] args = ["--bind", options.Bind.ToString(), "--port", options.ServerPort.ToString(CultureInfo.InvariantCulture),
            "--cluster", "--aof", "--storage-tier", "--memory", "64m", "--page", "1m", "--segment", "64m", "--index", "16m",
            "--logdir", log, "--checkpointdir", checkpoint, "--repl-diskless-sync", "false", "--logger-level", "Warning",
            "--device-type", options.DeviceType.ToString(),
            "--repl-attach-timeout", ((int)options.Timeout.TotalSeconds).ToString(CultureInfo.InvariantCulture)];
        foreach (var arg in args)
            start.ArgumentList.Add(arg);
        if (options.Role == "send")
        {
            start.ArgumentList.Add("--repl-disk-based-sync-chunk-size");
            start.ArgumentList.Add(chunk.ToString(CultureInfo.InvariantCulture));
            start.ArgumentList.Add("--repl-disk-based-sync-pipeline-depth");
            start.ArgumentList.Add(depth.ToString(CultureInfo.InvariantCulture));
        }
        else
        {
            AddReplicaOption("repl-disk-based-sync-write-buffers", options.WriteBuffers.ToString(CultureInfo.InvariantCulture), options.WriteBuffersSpecified);
            AddReplicaOption("repl-disk-based-sync-write-buffers-concurrently", options.Concurrently.ToString(), options.ConcurrentlySpecified);
        }
        start.Environment["GARNET_REPLICA_BENCH_PHASE_TIMING"] = options.PhaseTiming ? "1" : "0";
        await File.WriteAllTextAsync(Path.Combine(path, "launch.json"), JsonSerializer.Serialize(new
        {
            options.Server,
            Arguments = start.ArgumentList.ToArray(),
            options.PhaseTiming,
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