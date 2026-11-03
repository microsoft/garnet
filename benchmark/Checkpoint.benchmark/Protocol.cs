// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net.Sockets;
using System.Threading.Channels;
using Garnet.common;

namespace Checkpoint.benchmark;

internal sealed class Protocol
{
    internal const string Magic = "CHECKPOINT_BENCH_V1";
    private readonly NetworkStream stream;
    private readonly TimeSpan timeout;
    private readonly int maxFrame;
    private byte[] buffer = new byte[1 << 17];
    private int start;
    private int end;

    internal Protocol(NetworkStream stream, TimeSpan timeout, int maxChunk)
    {
        this.stream = stream;
        this.timeout = timeout;
        maxFrame = checked(maxChunk + 1024);
        if (buffer.Length > maxFrame)
            buffer = new byte[maxFrame];
    }

    // Returned slices remain valid only until the next ReadAsync.
    internal async Task<ArraySegment<byte>[]> ReadAsync(CancellationToken cancellationToken)
    {
        using var idle = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        idle.CancelAfter(timeout);
        while (true)
        {
            if (TryParse(out var command, out var consumed))
            {
                start += consumed;
                return command;
            }
            if (start != 0)
            {
                Buffer.BlockCopy(buffer, start, buffer, 0, end - start);
                end -= start;
                start = 0;
            }
            if (end == buffer.Length)
            {
                if (buffer.Length == maxFrame)
                    throw new InvalidDataException("RESP frame exceeds the receiver's --max-chunk limit.");
                Array.Resize(ref buffer, (int)Math.Min(maxFrame, 2L * buffer.Length));
            }
            var count = await stream.ReadAsync(buffer.AsMemory(end), idle.Token);
            if (count == 0)
                throw new EndOfStreamException("Peer disconnected before the verified transfer completed.");
            end += count;
        }
    }

    private unsafe bool TryParse(out ArraySegment<byte>[] command, out int consumed)
    {
        command = null;
        consumed = 0;
        fixed (byte* origin = buffer)
        {
            var ptr = origin + start;
            var limit = origin + end;
            if (!RespReadUtils.TryReadUnsignedArrayLength(out var count, ref ptr, limit))
                return false;
            if (count is < 1 or > 6)
                throw new InvalidDataException("Expected a RESP array with 1 through 6 bulk-string arguments.");
            Span<(int Offset, int Length)> slices = stackalloc (int, int)[6];
            for (var i = 0; i < count; i++)
            {
                byte* argument = null;
                var length = 0;
                if (!RespReadUtils.TryReadPtrWithLengthHeader(ref argument, ref length, ref ptr, limit))
                    return false;
                slices[i] = ((int)(argument - origin), length);
            }
            command = new ArraySegment<byte>[count];
            for (var i = 0; i < count; i++)
                command[i] = new ArraySegment<byte>(buffer, slices[i].Offset, slices[i].Length);
            consumed = (int)(ptr - origin) - start;
            return true;
        }
    }

    internal static unsafe byte[] Reply(string value)
    {
        var bytes = new byte[value.Length + 3];
        fixed (byte* origin = bytes)
        {
            var ptr = origin;
            if (!RespWriteUtils.TryWriteSimpleString(value.AsSpan(), ref ptr, origin + bytes.Length))
                throw new InvalidOperationException("Could not format benchmark reply.");
        }
        return bytes;
    }
}

internal sealed class Acknowledgements
{
    private readonly Channel<(byte[] Reply, long ReadyAt)> queue =
        Channel.CreateBounded<(byte[], long)>(new BoundedChannelOptions(68)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait
        });
    private readonly int delayMs;

    internal Acknowledgements(int delayMs) => this.delayMs = delayMs;

    internal ValueTask EnqueueAsync(string reply, bool delayed, CancellationToken token)
    {
        var readyAt = Stopwatch.GetTimestamp() + (delayed ? (long)(delayMs * (double)Stopwatch.Frequency / 1000) : 0);
        return queue.Writer.WriteAsync((Protocol.Reply(reply), readyAt), token);
    }

    internal void Complete() => queue.Writer.TryComplete();

    internal async Task WriteAsync(NetworkStream stream, CancellationTokenSource abort, TimeSpan timeout)
    {
        try
        {
            await foreach (var item in queue.Reader.ReadAllAsync(abort.Token))
            {
                var delay = TimeSpan.FromSeconds((item.ReadyAt - Stopwatch.GetTimestamp()) / (double)Stopwatch.Frequency);
                if (delay > TimeSpan.Zero)
                    await Task.Delay(TimeSpan.FromMilliseconds(Math.Ceiling(delay.TotalMilliseconds)), abort.Token);
                using var deadline = CancellationTokenSource.CreateLinkedTokenSource(abort.Token);
                deadline.CancelAfter(timeout);
                await stream.WriteAsync(item.Reply, deadline.Token);
            }
        }
        catch
        {
            await abort.CancelAsync();
            throw;
        }
    }
}