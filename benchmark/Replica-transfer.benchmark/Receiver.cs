// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using Garnet.cluster;
using Tsavorite.core;

namespace ReplicaTransfer.benchmark;

internal static class Receiver
{
    internal static async Task RunAsync(Options options, CancellationToken token)
    {
        if (!Directory.Exists(options.Output))
            throw new DirectoryNotFoundException("--output must name an existing directory on the destination storage.");
        var endpoint = new IPEndPoint(IPAddress.Parse(options.Host), options.Port);
        if (!IPAddress.IsLoopback(endpoint.Address))
            Console.Error.WriteLine("WARNING: No TLS/authentication. Only bind on an isolated, trusted private test network.");
        var listener = new TcpListener(endpoint);
        listener.Start(1);
        try
        {
            Console.WriteLine($"Listening on {endpoint}; expecting {options.Runs} verified runs; device={options.DeviceType}, buffered={options.Buffered}, ACK delay={options.AckDelayUs} us, write buffers={options.WriteBuffers}.");
            for (var run = 1; run <= options.Runs; run++)
            {
                using var acceptDeadline = CancellationTokenSource.CreateLinkedTokenSource(token);
                acceptDeadline.CancelAfter(options.Timeout);
                using var client = await listener.AcceptTcpClientAsync(acceptDeadline.Token);
                client.NoDelay = true;
                await ReceiveAsync(client, options, run, token);
            }
        }
        finally
        {
            listener.Stop();
        }
    }

    private static async Task ReceiveAsync(TcpClient client, Options options, int run, CancellationToken token)
    {
        using var abort = CancellationTokenSource.CreateLinkedTokenSource(token);
        using var close = abort.Token.Register(client.Dispose);
        var stream = client.GetStream();
        var protocol = new Protocol(stream, options.Timeout, options.MaxChunk);
        var replies = new Acknowledgements(options.AckDelayUs);
        var writer = replies.WriteAsync(stream, abort, options.Timeout);
        Transfer transfer = null;
        try
        {
            var hello = await protocol.ReadAsync(abort.Token);
            Require(hello.Length == 1 && Text(hello[0]) is Protocol.Magic or Protocol.LegacyMagic, "Expected benchmark-only handshake.");
            var magic = Text(hello[0]);
            var identity = magic == Protocol.Magic ? $"|{options.Device.Identity(OperatingSystem.IsLinux())}" : "";
            await replies.EnqueueAsync($"{magic}|{options.AckDelayUs}|{options.MaxChunk}|{options.WriteBuffers}|{options.WriteBuffersConcurrently}{identity}", false, abort.Token);

            var begin = await protocol.ReadAsync(abort.Token);
            Require(begin.Length == 6 && Text(begin[0]) == "BEGIN", "Expected BEGIN token bytes chunk depth sha256.");
            var id = Guid.Parse(Text(begin[1]));
            var length = long.Parse(Text(begin[2]), CultureInfo.InvariantCulture);
            var chunk = int.Parse(Text(begin[3]), CultureInfo.InvariantCulture);
            var depth = int.Parse(Text(begin[4]), CultureInfo.InvariantCulture);
            var hash = Text(begin[5]);
            var config = new Garnet.server.GarnetServerOptions
            {
                ReplicaDiskBasedSyncChunkSize = chunk.ToString(CultureInfo.InvariantCulture),
                ReplicaDiskBasedSyncPipelineDepth = depth
            };
            Require(config.GetReplicaDiskBasedSyncChunkSizeBytes() == chunk, "BEGIN chunk must be an effective power-of-two chunk size.");
            _ = config.GetReplicaDiskBasedSyncPipelineDepth();
            Require(length > 0 && chunk <= options.MaxChunk, "Invalid length or chunk exceeds --max-chunk.");
            Require(hash.Length == 64 && Convert.FromHexString(hash).Length == 32, "Expected a SHA-256 hash.");
            transfer = Transfer.Create(options, id, length, chunk, hash, abort.Token);
            Console.WriteLine($"Receiving run {run}: token={id}, bytes={length}, chunk={chunk}, depth={depth}, destination={transfer.Path}");
            await replies.EnqueueAsync("OK", false, abort.Token);

            while (true)
            {
                var command = await protocol.ReadAsync(abort.Token);
                if (command.Length == 1 && Text(command[0]) == "VERIFY")
                {
                    Require(transfer.Completed, "VERIFY requires a valid EOF marker and completed device writes.");
                    using var verifyDeadline = CancellationTokenSource.CreateLinkedTokenSource(abort.Token);
                    verifyDeadline.CancelAfter(options.Timeout);
                    var verifiedHash = await HashAsync(transfer.Path, transfer.Length, verifyDeadline.Token);
                    Require(verifiedHash == transfer.Hash, "Destination SHA-256 differs from the source; this run has no valid throughput result.");
                    await replies.EnqueueAsync($"VERIFIED|{transfer.NextAddress}|{transfer.Chunks}|{verifiedHash}", false, abort.Token);
                    replies.Complete();
                    await writer;
                    Console.WriteLine($"VERIFIED run {run}: {transfer.NextAddress} completed bytes, {transfer.Chunks} chunks, SHA256={verifiedHash}; {replies.Report()}");
                    return;
                }

                Require(command.Length == 6 && command[0].AsSpan().SequenceEqual("CLUSTER"u8) &&
                    command[1].AsSpan().SequenceEqual("SNAPSHOT_DATA"u8), "Expected CLUSTER SNAPSHOT_DATA.");
                Require(!transfer.EndReceived, "Data or duplicate EOF after completion.");
                Require(command[2].AsSpan().SequenceEqual(transfer.TokenBytes), "Checkpoint token changed.");
                Require(Text(command[3]) == ((int)CheckpointFileType.STORE_HLOG).ToString(CultureInfo.InvariantCulture), "Unexpected checkpoint file type.");
                var address = long.Parse(Text(command[4]), CultureInfo.InvariantCulture);
                Require(address == transfer.NextAddress, "Chunk address is out of order, repeated, or missing.");
                var payload = command[5];
                if (payload.Count == 0)
                    Require(address == transfer.Length, "EOF before all expected bytes were written.");
                else
                    Require(payload.Count == Math.Min(transfer.Chunk, transfer.Length - address), "Incorrect chunk size or bytes beyond declared EOF.");

                // Chunk OKs confirm queue acceptance. EOF confirms all device writes and close.
                if (payload.Count == 0)
                    await transfer.CompleteAsync();
                else
                    transfer.Write(payload);
                await replies.EnqueueAsync("OK", true, abort.Token);
            }
        }
        catch
        {
            await abort.CancelAsync();
            replies.Complete();
            try { await writer; }
            catch { /* The connection is already failing; preserve the receive/write/verification error. */ }
            throw;
        }
        finally
        {
            transfer?.Dispose();
        }
    }

    internal static async Task<string> HashAsync(string path, long expectedLength, CancellationToken token)
    {
        await using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 20,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        Require(file.Length == expectedLength, $"File length is {file.Length}; expected {expectedLength}: {path}");
        return Convert.ToHexString(await SHA256.HashDataAsync(file, token));
    }

    private static string Text(ArraySegment<byte> value) => Encoding.ASCII.GetString(value.AsSpan());

    internal static void Require(bool condition, string message)
    {
        if (!condition)
            throw new InvalidDataException(message);
    }

    private sealed class Transfer : IDisposable
    {
        internal string Path { get; private init; }
        internal long Length { get; private init; }
        internal int Chunk { get; private init; }
        internal string Hash { get; private init; }
        internal byte[] TokenBytes { get; private init; }
        internal long NextAddress { get; private set; }
        internal long Chunks { get; private set; }
        internal bool EndReceived => completion != null;
        internal bool Completed => completion?.IsCompletedSuccessfully == true;
        private ISnapshotDataSink sink;
        private SectorAlignedBufferPool pool;
        private SemaphoreSlim writeCompletion;
        private Task completion;

        internal static Transfer Create(Options options, Guid id, long length, int chunk, string hash, CancellationToken token)
        {
            var directory = System.IO.Path.Combine(options.Output, $"checkpoint-bench-{id:N}");
            Require(!Directory.Exists(directory) && !File.Exists(directory), "Refusing to reuse a destination run path.");
            Directory.CreateDirectory(directory);
            var prefix = System.IO.Path.Combine(directory, "payload");
            var path = prefix + ".0";
            // Reserve the exact file with CreateNew before handing it to the device's OpenOrCreate path.
            using (var reservation = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None)) { }
            var device = options.Device.CreateDevice(prefix, options.Buffered);
            SectorAlignedBufferPool pool = null;
            SemaphoreSlim writeCompletion = null;
            try
            {
                device.Initialize(-1);
                Require(length % device.SectorSize == 0 && chunk % device.SectorSize == 0,
                    $"Source length and chunk must be multiples of destination sector size {device.SectorSize}.");
                pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);
                writeCompletion = new SemaphoreSlim(0);
                // The same sinks, chosen the same way, as Garnet's ReceiveCheckpointHandler.
                ISnapshotDataSink sink = options.WriteBuffers > 0
                    ? new OverlappedFileDataSink(CheckpointFileType.STORE_HLOG, id, device, pool, options.WriteBuffers, options.Timeout, token,
                        writeBuffersConcurrently: options.WriteBuffersConcurrently)
                    : new FileDataSink(CheckpointFileType.STORE_HLOG, id, device, pool, writeCompletion, options.Timeout, token);
                return new Transfer
                {
                    Path = path,
                    Length = length,
                    Chunk = chunk,
                    Hash = hash,
                    TokenBytes = id.ToByteArray(),
                    pool = pool,
                    writeCompletion = writeCompletion,
                    sink = sink
                };
            }
            catch
            {
                device.Dispose();
                pool?.Free();
                writeCompletion?.Dispose();
                throw;
            }
        }

        /// <summary>Consumes (copies) and accepts <paramref name="payload"/> for writing.</summary>
        internal void Write(ReadOnlySpan<byte> payload)
        {
            sink.WriteChunk(NextAddress, payload);
            NextAddress += payload.Length;
            Chunks++;
        }

        /// <summary>Completes once every write has completed and the device is closed.</summary>
        internal Task CompleteAsync()
        {
            if (sink is OverlappedFileDataSink overlapped)
            {
                completion = overlapped.CompleteAsync();
            }
            else
            {
                sink.Complete();
                completion = Task.CompletedTask;
            }
            return completion;
        }

        public void Dispose()
        {
            if (!Completed)
                sink.Dispose();
            pool.Free();
            writeCompletion.Dispose();
        }
    }
}