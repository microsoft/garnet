// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Globalization;
using Garnet.server;
using Tsavorite.core;

namespace Checkpoint.benchmark;

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        try
        {
            if (args.Length == 0 || args.Contains("--help"))
            {
                Console.WriteLine("""
                    Standalone checkpoint-file transfer benchmark (NOT a Garnet server).
                    receive --output <existing directory> [--bind 127.0.0.1] [--port 7010]
                            [--runs 1] [--max-chunk 16m] [--ack-delay-ms 0]
                    send    --source-prefix <prefix for existing prefix.0> [--host 127.0.0.1]
                            [--port 7010] [--chunks 128k,512k,1m,4m] [--depths 1,2,4,8]
                            [--repeats 3] [--csv <new file>]
                    Both:   [--timeout-seconds 30] [--device RandomAccess|Native|FileStream]
                            [--buffered false]
                    No TLS or authentication: use loopback or an isolated, trusted private test network.
                    See benchmark\Checkpoint.benchmark\README.md for preparation and interpretation.
                    """);
                return 0;
            }

            var options = Options.Parse(args);
            using var stop = new CancellationTokenSource();
            ConsoleCancelEventHandler onCancel = (_, e) =>
            {
                e.Cancel = true;
                stop.Cancel();
            };
            Console.CancelKeyPress += onCancel;
            try
            {
                if (options.Mode == "receive")
                    await Receiver.RunAsync(options, stop.Token);
                else
                    await Sender.RunAsync(options, stop.Token);
            }
            finally
            {
                Console.CancelKeyPress -= onCancel;
            }
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"FAILED: {ex.GetType().Name}: {ex.Message}");
            return 1;
        }
    }
}

internal sealed class Options
{
    internal string Mode { get; private init; }
    internal string Host { get; private init; }
    internal string SourcePrefix { get; private init; }
    internal string Output { get; private init; }
    internal string Csv { get; private init; }
    internal int Port { get; private init; }
    internal int Runs { get; private init; }
    internal int Repeats { get; private init; }
    internal int MaxChunk { get; private init; }
    internal int AckDelayMs { get; private init; }
    internal TimeSpan Timeout { get; private init; }
    internal DeviceType DeviceType { get; private init; }
    internal bool Buffered { get; private init; }
    internal (int Chunk, int Depth)[] Cases { get; private init; }

    internal static Options Parse(string[] args)
    {
        var mode = args[0];
        if (mode is not ("send" or "receive"))
            throw new ArgumentException("First argument must be send or receive.");
        string[] common = ["--port", "--timeout-seconds", "--device", "--buffered"];
        string[] specific = mode == "send"
            ? ["--source-prefix", "--host", "--chunks", "--depths", "--repeats", "--csv"]
            : ["--output", "--bind", "--runs", "--max-chunk", "--ack-delay-ms"];
        var allowed = common.Concat(specific).ToHashSet(StringComparer.Ordinal);
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 1; i < args.Length; i += 2)
        {
            if (!allowed.Contains(args[i]) || i + 1 == args.Length || !values.TryAdd(args[i], args[i + 1]))
                throw new ArgumentException($"Unknown, repeated, or valueless option: {args[i]}");
        }

        string Get(string key, string fallback = null) => values.GetValueOrDefault(key, fallback);
        string Required(string key) => Get(key) ?? throw new ArgumentException($"{key} is required.");
        int Number(string key, int fallback, int min, int max)
        {
            var result = int.Parse(Get(key, fallback.ToString(CultureInfo.InvariantCulture)), CultureInfo.InvariantCulture);
            if (result < min || result > max)
                throw new ArgumentOutOfRangeException(key, $"Expected {min} through {max}.");
            return result;
        }

        var device = Enum.Parse<DeviceType>(Get("--device", "RandomAccess"), ignoreCase: true);
        if (device is not (DeviceType.RandomAccess or DeviceType.Native or DeviceType.FileStream))
            throw new ArgumentException("--device must be RandomAccess, Native, or FileStream (real local storage only).");
        if (mode == "send" && device == DeviceType.Native && !OperatingSystem.IsWindows())
            throw new ArgumentException("Native source devices do not support read-only opens on this platform; use RandomAccess.");

        var timeout = TimeSpan.FromSeconds(Number("--timeout-seconds", 30, 1, 3600));
        var ackDelay = Number("--ack-delay-ms", 0, 0, 60000);
        if (ackDelay >= timeout.TotalMilliseconds)
            throw new ArgumentException("--ack-delay-ms must be below --timeout-seconds.");
        var cases = new List<(int, int)>();
        if (mode == "send")
        {
            foreach (var size in Get("--chunks", "128k,512k,1m,4m").Split(','))
            {
                foreach (var depth in Get("--depths", "1,2,4,8").Split(','))
                {
                    var config = new GarnetServerOptions
                    {
                        ReplicaDiskBasedSyncChunkSize = size,
                        ReplicaDiskBasedSyncPipelineDepth = int.Parse(depth, CultureInfo.InvariantCulture)
                    };
                    cases.Add((config.GetReplicaDiskBasedSyncChunkSizeBytes(), config.GetReplicaDiskBasedSyncPipelineDepth()));
                }
            }
        }
        return new Options
        {
            Mode = mode,
            Host = Get(mode == "send" ? "--host" : "--bind", "127.0.0.1"),
            Port = Number("--port", 7010, 1, 65535),
            SourcePrefix = mode == "send" ? Path.GetFullPath(Required("--source-prefix")) : null,
            Output = mode == "receive" ? Path.GetFullPath(Required("--output")) : null,
            Csv = Get("--csv") is { } csv ? Path.GetFullPath(csv) : null,
            Runs = Number("--runs", 1, 1, 1000000),
            Repeats = Number("--repeats", 3, 1, 1000000),
            MaxChunk = new GarnetServerOptions { ReplicaDiskBasedSyncChunkSize = Get("--max-chunk", "16m") }.GetReplicaDiskBasedSyncChunkSizeBytes(),
            AckDelayMs = ackDelay,
            Timeout = timeout,
            DeviceType = device,
            Buffered = bool.Parse(Get("--buffered", "false")),
            Cases = cases.Distinct().ToArray()
        };
    }
}