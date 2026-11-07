// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Globalization;
using Garnet.server;
using Tsavorite.core;

namespace ReplicaTransfer.benchmark;

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
                    [--scenario transfer-only|full] Default: transfer-only.
                    Full is reserved for the production-driver follow-up and is not implemented yet.
                    receive --output <existing directory> [--bind 127.0.0.1] [--port 7010]
                            [--runs 1] [--max-chunk 16m] [--ack-delay-us 0] [--write-buffers 1]
                            [--write-buffers-concurrently false]
                    send    --source-prefix <prefix for existing prefix.0> [--host 127.0.0.1]
                            [--port 7010] [--chunks 128k,1m,4m,16m] [--depths 1,2,4,8]
                            [--repeats 3] [--csv <new file>]
                    Or:     send --generate <existing directory> [--data-size 512m] ...
                    Prepare only:
                            generate --output <existing directory> [--data-size 512m]
                    Send/receive: [--timeout-seconds 30]
                    All:    [--device RandomAccess|Native|FileStream] [--buffered false]
                    No TLS or authentication: use loopback or an isolated, trusted private test network.
                    Generation creates an offline Tsavorite FoldOver checkpoint before transfer timing.
                    See benchmark\Replica-transfer.benchmark\README.md for preparation and interpretation.
                    """);
                return 0;
            }

            using var stop = new CancellationTokenSource();
            ConsoleCancelEventHandler onCancel = (_, e) =>
            {
                e.Cancel = true;
                stop.Cancel();
            };
            Console.CancelKeyPress += onCancel;
            try
            {
                if (SelectScenario(args) == "full")
                    throw new NotSupportedException("The full production scenario is not implemented yet; use --scenario transfer-only.");
                else
                {
                    var options = Options.Parse(args);
                    if (options.Mode == "receive")
                        await Receiver.RunAsync(options, stop.Token);
                    else if (options.Mode == "generate")
                        _ = await CheckpointGenerator.RunAsync(options, stop.Token);
                    else
                        await Sender.RunAsync(options, stop.Token);
                }
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

    internal static string SelectScenario(string[] args)
    {
        var selectors = args.Select((value, index) => (value, index)).Where(pair => pair.value == "--scenario").ToArray();
        if (selectors.Length > 1)
            throw new ArgumentException("Repeated --scenario.");
        var scenario = selectors.Length == 0 ? "transfer-only"
            : selectors[0].index + 1 < args.Length ? args[selectors[0].index + 1] : "";
        if (scenario is not ("full" or "transfer-only") || (args[0] == "generate" && scenario != "transfer-only"))
            throw new ArgumentException("--scenario must be full or transfer-only; generate is transfer-only.");
        return scenario;
    }
}

internal sealed class Options
{
    internal string Mode { get; private init; }
    internal string Host { get; private init; }
    internal string SourcePrefix { get; private init; }
    internal string GenerateDirectory { get; private init; }
    internal long DataSize { get; private init; }
    internal string Output { get; private init; }
    internal string Csv { get; private init; }
    internal int Port { get; private init; }
    internal int Runs { get; private init; }
    internal int Repeats { get; private init; }
    internal int MaxChunk { get; private init; }
    internal long AckDelayUs { get; private init; }
    internal int WriteBuffers { get; private init; }
    internal bool WriteBuffersConcurrently { get; private init; }
    internal TimeSpan Timeout { get; private init; }
    internal DeviceType DeviceType { get; private init; }
    internal bool Buffered { get; private init; }
    internal (int Chunk, int Depth)[] Cases { get; private init; }

    internal static Options Parse(string[] args)
    {
        var mode = args[0];
        if (mode is not ("send" or "receive" or "generate"))
            throw new ArgumentException("First argument must be send, receive, or generate.");
        if (args.Contains("--scenario") && Program.SelectScenario(args) != "transfer-only")
            throw new ArgumentException("Component options require --scenario transfer-only.");
        string[] common = ["--device", "--buffered", "--scenario"];
        string[] specific = mode switch
        {
            "send" => ["--source-prefix", "--generate", "--data-size", "--host", "--port", "--timeout-seconds", "--chunks", "--depths", "--repeats", "--csv"],
            "receive" => ["--output", "--bind", "--port", "--timeout-seconds", "--runs", "--max-chunk", "--ack-delay-us", "--write-buffers", "--write-buffers-concurrently"],
            _ => ["--output", "--data-size"]
        };
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
            // Accept C#-style digit separators between digits (e.g. 20_000); no sign, other separators or edge underscores.
            var text = Get(key, fallback.ToString(CultureInfo.InvariantCulture));
            if (text.Length == 0 || !char.IsAsciiDigit(text[0]) || !char.IsAsciiDigit(text[^1]) ||
                !int.TryParse(text.Replace("_", ""), NumberStyles.None, CultureInfo.InvariantCulture, out var result) ||
                result < min || result > max)
                throw new ArgumentOutOfRangeException(key, $"Expected an integer from {min} through {max}; underscores may separate digits, e.g. 20_000.");
            return result;
        }

        var device = Enum.Parse<DeviceType>(Get("--device", "RandomAccess"), ignoreCase: true);
        if (device is not (DeviceType.RandomAccess or DeviceType.Native or DeviceType.FileStream))
            throw new ArgumentException("--device must be RandomAccess, Native, or FileStream (real local storage only).");
        if (mode == "send" && device == DeviceType.Native && !OperatingSystem.IsWindows())
            throw new ArgumentException("Native source devices do not support read-only opens on this platform; use RandomAccess.");

        if (mode == "send" && (Get("--source-prefix") == null) == (Get("--generate") == null))
            throw new ArgumentException("Specify exactly one of --source-prefix and --generate.");
        if (mode == "send" && Get("--data-size") != null && Get("--generate") == null)
            throw new ArgumentException("--data-size requires --generate.");
        if (!ServerOptions.TryParseSize(Get("--data-size", "512m"), out var dataSize) ||
            dataSize < (16 << 10) || dataSize > (64L << 30))
            throw new ArgumentOutOfRangeException("--data-size", "Expected 16 KiB through 64 GiB of generated value payload.");

        var timeout = TimeSpan.FromSeconds(Number("--timeout-seconds", 30, 1, 3600));
        long ackDelayUs = Number("--ack-delay-us", 0, 0, 60_000_000);
        if (ackDelayUs >= timeout.TotalMilliseconds * 1000)
            throw new ArgumentException("--ack-delay-us must be below --timeout-seconds.");
        var cases = new List<(int, int)>();
        if (mode == "send")
        {
            foreach (var size in Get("--chunks", "128k,1m,4m,16m").Split(','))
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
            SourcePrefix = Get("--source-prefix") is { } source ? Path.GetFullPath(source) : null,
            GenerateDirectory = mode == "generate" ? Path.GetFullPath(Required("--output"))
                : Get("--generate") is { } generate ? Path.GetFullPath(generate) : null,
            DataSize = dataSize,
            Output = mode == "receive" ? Path.GetFullPath(Required("--output")) : null,
            Csv = Get("--csv") is { } csv ? Path.GetFullPath(csv) : null,
            Runs = Number("--runs", 1, 1, 1000000),
            Repeats = Number("--repeats", 3, 1, 1000000),
            MaxChunk = new GarnetServerOptions { ReplicaDiskBasedSyncChunkSize = Get("--max-chunk", "16m") }.GetReplicaDiskBasedSyncChunkSizeBytes(),
            AckDelayUs = ackDelayUs,
            WriteBuffers = Number("--write-buffers", 1, 0, GarnetServerOptions.ReplicaDiskBasedSyncWriteBuffersMax),
            WriteBuffersConcurrently = bool.Parse(Get("--write-buffers-concurrently", "false")),
            Timeout = timeout,
            DeviceType = device,
            Buffered = bool.Parse(Get("--buffered", "false")),
            Cases = cases.Distinct().ToArray()
        };
    }
}