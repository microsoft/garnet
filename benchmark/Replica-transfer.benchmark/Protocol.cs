// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net.Sockets;
using System.Threading.Channels;
using Garnet.common;

namespace ReplicaTransfer.benchmark;

internal sealed class Protocol
{
    // V3: the handshake also reports the receiver's write buffers; an older peer fails the handshake instead of misreading it.
    internal const string Magic = "CHECKPOINT_BENCH_V4";
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
    // Waits longer than this sleep first and spin only for the remainder; it exceeds one ~15.6 ms Windows timer tick.
    private static readonly long CoarseSleepMarginTicks = Stopwatch.Frequency / 50;

    private readonly Channel<(byte[] Reply, long ReadyAt, bool Delayed)> queue =
        Channel.CreateBounded<(byte[], long, bool)>(new BoundedChannelOptions(68)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait
        });
    private readonly long delayTicks;
    private long delayedCount, lateTicksSum, lateTicksMax;

    internal Acknowledgements(long delayUs) => delayTicks = (long)(delayUs * (double)Stopwatch.Frequency / 1_000_000);

    internal ValueTask EnqueueAsync(string reply, bool delayed, CancellationToken token)
    {
        var readyAt = Stopwatch.GetTimestamp() + (delayed ? delayTicks : 0);
        return queue.Writer.WriteAsync((Protocol.Reply(reply), readyAt, delayed && delayTicks > 0), token);
    }

    internal void Complete() => queue.Writer.TryComplete();

    /// <summary>
    /// Measured release lateness of delayed ACKs beyond their targets; read only after <see cref="WriteAsync"/> completes.
    /// </summary>
    internal string Report() => delayedCount == 0 ? "no delayed ACKs" : FormattableString.Invariant(
        $"ACK lateness mean={lateTicksSum * 1e6 / Stopwatch.Frequency / delayedCount:F1}us max={lateTicksMax * 1e6 / Stopwatch.Frequency:F1}us over {delayedCount} ACKs");

    /// <summary>
    /// Return no earlier than <paramref name="readyAt"/> (a <see cref="Stopwatch"/> timestamp), and within microseconds of it.
    /// Timer-based waits alone are too coarse: Windows timers fire on ~15.6 ms ticks, so a 1 ms Task.Delay can take a full tick.
    /// Long waits therefore sleep until shortly before the target and spin only for the remainder, keeping a core busy briefly.
    /// </summary>
    internal static async ValueTask WaitUntilAsync(long readyAt, CancellationToken token)
    {
        var remaining = readyAt - Stopwatch.GetTimestamp();
        if (remaining > 2 * CoarseSleepMarginTicks)
            await Task.Delay(TimeSpan.FromSeconds((remaining - CoarseSleepMarginTicks) / (double)Stopwatch.Frequency), token);
        var spinner = new SpinWait();
        while (Stopwatch.GetTimestamp() < readyAt)
        {
            token.ThrowIfCancellationRequested();
            // Never Sleep(1): that would reintroduce the timer tick this wait exists to avoid.
            spinner.SpinOnce(sleep1Threshold: -1);
        }
    }

    internal async Task WriteAsync(NetworkStream stream, CancellationTokenSource abort, TimeSpan timeout)
    {
        try
        {
            await foreach (var item in queue.Reader.ReadAllAsync(abort.Token))
            {
                await WaitUntilAsync(item.ReadyAt, abort.Token);
                if (item.Delayed)
                {
                    // Only this single reader updates the statistics.
                    var late = Stopwatch.GetTimestamp() - item.ReadyAt;
                    delayedCount++;
                    lateTicksSum += late;
                    lateTicksMax = Math.Max(lateTicksMax, late);
                }
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