// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Text;
using Garnet.client;
using Garnet.cluster;
using Garnet.common;
using Tsavorite.core;

namespace ReplicaTransfer.benchmark;

internal static class Sender
{
    internal static async Task RunAsync(Options options, CancellationToken token)
    {
        var sourcePrefix = options.SourcePrefix ?? await CheckpointGenerator.RunAsync(options, token);
        var sourcePath = sourcePrefix + ".0";
        // Generation is complete before opening the immutable source for the sweep.
        await using var sourceGuard = new FileStream(sourcePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        var length = sourceGuard.Length;
        Receiver.Require(length > 0, "Source must be nonempty.");
        Receiver.Require(!File.Exists(sourcePrefix + ".1"), "Only an unsegmented prefix.0 source is supported.");
        using var hashDeadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        hashDeadline.CancelAfter(options.Timeout);
        var hash = await Receiver.HashAsync(sourcePath, length, hashDeadline.Token);
        StreamWriter csv = null;
        try
        {
            if (options.Csv != null)
            {
                csv = new StreamWriter(new FileStream(options.Csv, FileMode.CreateNew, FileAccess.Write, FileShare.Read), Encoding.UTF8);
                await csv.WriteLineAsync("utc,token,repeat,chunk_bytes,pipeline_depth,completed_bytes,completed_chunks,elapsed_seconds,MB_per_second,ack_delay_ms,send_buffer_bytes,source_device,source_buffered,sha256");
                await csv.FlushAsync(token);
            }
            Console.WriteLine($"Source={sourcePath}; bytes={length}; SHA256={hash}; {options.Cases.Length * (long)options.Repeats} runs; device={options.DeviceType}, buffered={options.Buffered}.");
            foreach (var (chunk, depth) in options.Cases)
            {
                for (var repeat = 1; repeat <= options.Repeats; repeat++)
                {
                    token.ThrowIfCancellationRequested();
                    var id = Guid.NewGuid();
                    var device = Devices.CreateLogDevice(sourcePrefix, deviceType: options.DeviceType,
                        readOnly: true, deleteOnClose: false, disableFileBuffering: !options.Buffered);
                    var pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);
                    using var completion = new SemaphoreSlim(0);
                    FileTransmitSource transmitter = null;
                    try
                    {
                        device.Initialize(-1);
                        Receiver.Require(length % device.SectorSize == 0 && chunk % device.SectorSize == 0,
                            $"Source length and chunk must be multiples of source sector size {device.SectorSize}.");
                        var dataSource = new FileDataSource(CheckpointFileType.STORE_HLOG, id, device, 0, length,
                            chunk, options.Timeout, pool, completion);
                        transmitter = new FileTransmitSource(dataSource, pipelineDepth: depth);
                        var sendBufferSize = Math.Max(1 << 20, checked(2 * chunk));
                        using var client = new GarnetClientSession(new IPEndPoint(IPAddress.Parse(options.Host), options.Port),
                            new NetworkBufferSettings(sendBufferSize, 1 << 17));
                        using var close = token.Register(client.CloseConnection);
                        await client.ConnectAsync(checked((int)options.Timeout.TotalMilliseconds), token);
                        var hello = (await ExecuteAsync(client, options.Timeout, token, Protocol.Magic)).Split('|');
                        Receiver.Require(hello.Length == 3 && hello[0] == Protocol.Magic,
                            "Peer is not the standalone benchmark receiver. Do not use a Garnet server port.");
                        var ackDelay = int.Parse(hello[1], CultureInfo.InvariantCulture);
                        var maxChunk = int.Parse(hello[2], CultureInfo.InvariantCulture);
                        Receiver.Require(chunk <= maxChunk, "Chunk exceeds receiver --max-chunk.");
                        Receiver.Require(ackDelay < options.Timeout.TotalMilliseconds, "Receiver ACK delay exceeds sender timeout.");
                        var begin = await ExecuteAsync(client, options.Timeout, token, "BEGIN", id.ToString(),
                            length.ToString(CultureInfo.InvariantCulture), chunk.ToString(CultureInfo.InvariantCulture),
                            depth.ToString(CultureInfo.InvariantCulture), hash);
                        Receiver.Require(begin == "OK", "Receiver did not accept the transfer.");

                        var timer = Stopwatch.StartNew();
                        await transmitter.TransmitAsync(client, options.Timeout, token);
                        timer.Stop();

                        var verified = (await ExecuteAsync(client, options.Timeout, token, "VERIFY")).Split('|');
                        Receiver.Require(verified.Length == 4 && verified[0] == "VERIFIED" &&
                            long.Parse(verified[1], CultureInfo.InvariantCulture) == length &&
                            long.Parse(verified[2], CultureInfo.InvariantCulture) == (length + chunk - 1) / chunk &&
                            verified[3] == hash && dataSource.CurrentOffset == length,
                            "Receiver did not confirm correct bytes, chunk count, EOF, and SHA-256.");
                        var seconds = timer.Elapsed.TotalSeconds;
                        var rate = length / seconds / 1000000d;
                        Console.WriteLine(FormattableString.Invariant(
                            $"VERIFIED token={id} repeat={repeat} chunk={chunk} depth={depth} bytes={length} seconds={seconds:F6} MB/s={rate:F3} ACK-delay-ms={ackDelay}"));
                        if (csv != null)
                        {
                            await csv.WriteLineAsync(FormattableString.Invariant(
                                $"{DateTimeOffset.UtcNow:O},{id},{repeat},{chunk},{depth},{length},{verified[2]},{seconds:F9},{rate:F6},{ackDelay},{sendBufferSize},{options.DeviceType},{options.Buffered},{hash}"));
                            await csv.FlushAsync(token);
                        }
                    }
                    finally
                    {
                        if (transmitter != null)
                            transmitter.Dispose();
                        else
                            device.Dispose();
                        pool.Free();
                    }
                }
            }
        }
        finally
        {
            if (csv != null)
                await csv.DisposeAsync();
        }
    }

    private static async Task<string> ExecuteAsync(GarnetClientSession client, TimeSpan timeout, CancellationToken token, params string[] command)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(timeout);
        using var close = deadline.Token.Register(client.CloseConnection);
        var response = client.ExecuteAsync(command);
        try
        {
            return await response.WaitAsync(deadline.Token);
        }
        finally
        {
            _ = response.ContinueWith(task => _ = task.Exception, CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        }
    }
}