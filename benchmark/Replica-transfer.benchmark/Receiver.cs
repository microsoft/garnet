// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Channels;
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
        using var responder = new Responder(replies);
        var responding = responder.RunAsync(abort);
        Transfer transfer = null;
        try
        {
            var hello = await protocol.ReadAsync(abort.Token);
            Require(hello.Length == 1 && Text(hello[0]) == Protocol.Magic, "Expected benchmark-only handshake.");
            responder.Reply($"{Protocol.Magic}|{options.AckDelayUs}|{options.MaxChunk}|{options.WriteBuffers}");

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
            responder.Reply("OK");

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
                    responder.Reply($"VERIFIED|{transfer.NextAddress}|{transfer.Chunks}|{verifiedHash}");
                    responder.Complete();
                    await responding;
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

                // Garnet's receiving thread holds the session's network sender while it processes received commands, so
                // no reply can be sent meanwhile. Hold the stand-in for the copy, and for any wait for a write buffer.
                await responder.Sender.WaitAsync(abort.Token);
                try
                {
                    // The ACK waits for the write (or, at EOF, for completion and close), then for the ACK delay.
                    responder.Acknowledge(payload.Count == 0 ? transfer.CompleteAsync() : transfer.Write(payload));
                }
                finally
                {
                    _ = responder.Sender.Release();
                }
            }
        }
        catch
        {
            await abort.CancelAsync();
            responder.Complete();
            replies.Complete();
            try { await responding; }
            catch { /* The connection is already failing; preserve the receive/write/verification error. */ }
            try { await writer; }
            catch { /* As above. */ }
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

    /// <summary>
    /// Sends replies in command order, each once its write completes, as Garnet's <c>ClusterSession</c> defers CLUSTER
    /// SNAPSHOT_DATA replies. Garnet's receiving thread holds the session's network sender for as long as it processes
    /// received commands, so a reply can only be sent between commands; <see cref="Sender"/> stands in for that network
    /// sender. The ACK delay starts when the reply is sent, as a network round trip would.
    /// </summary>
    private sealed class Responder(Acknowledgements replies) : IDisposable
    {
        private readonly Channel<(string Reply, Task Written, bool Delayed)> pending =
            Channel.CreateUnbounded<(string, Task, bool)>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });

        internal SemaphoreSlim Sender { get; } = new(1, 1);

        /// <summary>Replies to a control command without delay.</summary>
        internal void Reply(string reply) => Enqueue(reply, Task.CompletedTask, delayed: false);

        /// <summary>Acknowledges a chunk or EOF once <paramref name="written"/> completes, after the ACK delay.</summary>
        internal void Acknowledge(Task written) => Enqueue("OK", written, delayed: true);

        internal void Complete() => pending.Writer.TryComplete();

        internal async Task RunAsync(CancellationTokenSource abort)
        {
            try
            {
                await foreach (var (reply, written, delayed) in pending.Reader.ReadAllAsync(abort.Token))
                {
                    // A failed write fails the run: the sender sees the connection close, never a success-shaped ACK.
                    await written.WaitAsync(abort.Token);
                    await Sender.WaitAsync(abort.Token);
                    try
                    {
                        await replies.EnqueueAsync(reply, delayed, abort.Token);
                    }
                    finally
                    {
                        _ = Sender.Release();
                    }
                }
            }
            catch
            {
                await abort.CancelAsync();
                throw;
            }
        }

        public void Dispose() => Sender.Dispose();

        private void Enqueue(string reply, Task written, bool delayed)
        {
            if (!pending.Writer.TryWrite((reply, written, delayed)))
                throw new InvalidOperationException("The run's replies are already complete.");
        }
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
            var device = Devices.CreateLogDevice(prefix, deviceType: options.DeviceType,
                deleteOnClose: false, disableFileBuffering: !options.Buffered);
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
                    ? new OverlappedFileDataSink(CheckpointFileType.STORE_HLOG, id, device, pool, options.WriteBuffers, options.Timeout, token)
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

        /// <summary>Consumes (copies) <paramref name="payload"/>; the task completes once it is written.</summary>
        internal Task Write(ReadOnlySpan<byte> payload)
        {
            var written = Task.CompletedTask;
            if (sink is OverlappedFileDataSink overlapped)
                written = overlapped.WriteChunkAsync(NextAddress, payload);
            else
                sink.WriteChunk(NextAddress, payload);
            NextAddress += payload.Length;
            Chunks++;
            return written;
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