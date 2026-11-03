// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers.Binary;
using Garnet.common;
using Tsavorite.core;
using GeneratedStoreFunctions = Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.SpanByteRecordTriggers>;

namespace ReplicaTransfer.benchmark;

internal static class CheckpointGenerator
{
    internal static async Task<string> RunAsync(Options options, CancellationToken token)
    {
        if (!Directory.Exists(options.GenerateDirectory))
            throw new DirectoryNotFoundException("Checkpoint generation requires an existing --output or --generate directory.");
        token.ThrowIfCancellationRequested();
        var directory = Path.Combine(options.GenerateDirectory, $"generated-{Guid.NewGuid():N}");
        Receiver.Require(!Directory.Exists(directory) && !File.Exists(directory), "Refusing to reuse a generated checkpoint path.");
        Directory.CreateDirectory(directory);
        var prefix = Path.Combine(directory, "hlog");
        using (var reservation = new FileStream(prefix + ".0", FileMode.CreateNew, FileAccess.Write, FileShare.None)) { }
        Console.WriteLine($"Generating checkpoint: value-payload-bytes={options.DataSize}; directory={directory}; preparation is NOT timed.");
        Guid checkpoint;
        using (var device = Devices.CreateLogDevice(prefix, deviceType: options.DeviceType,
            deleteOnClose: false, disableFileBuffering: !options.Buffered))
        using (var store = new TsavoriteKV<GeneratedStoreFunctions, SpanByteAllocator<GeneratedStoreFunctions>>(new()
        {
            IndexSize = 1 << 26,
            LogDevice = device,
            PageSize = 1 << 20,
            LogMemorySize = 1 << 26,
            SegmentSize = 1L << 40,
            CheckpointDir = Path.Combine(directory, "checkpoints")
        }, StoreFunctions.Create(SpanByteComparer.Instance, SpanByteRecordTriggers.Instance),
            (settings, functions) => new(settings, functions)))
        {
            Populate(store, options.DataSize, token);
            var (success, id) = await store.TakeFullCheckpointAsync(CheckpointType.FoldOver, token);
            Receiver.Require(success, "Could not create the generated store's full FoldOver checkpoint.");
            checkpoint = id;
        }
        long length;
        // Keep generated files portable between 512-byte and 4-KiB transfer devices.
        using (var file = new FileStream(prefix + ".0", FileMode.Open, FileAccess.Write, FileShare.None))
        {
            file.SetLength((file.Length + 4095) & ~4095L);
            length = file.Length;
        }
        Receiver.Require(length > 0 && !File.Exists(prefix + ".1"), "Generated checkpoint must have a nonempty single-segment HLOG.");
        Console.WriteLine($"GENERATED checkpoint={checkpoint} value-payload-bytes={options.DataSize} file-bytes={length} source-prefix={prefix}");
        return prefix;
    }

    private static void Populate(TsavoriteKV<GeneratedStoreFunctions, SpanByteAllocator<GeneratedStoreFunctions>> store,
        long dataSize, CancellationToken token)
    {
        using var session = store.NewSession<FixedSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new());
        var context = session.BasicContext;
        var random = new Random(42);
        var value = new byte[16 << 10];
        Span<byte> key = stackalloc byte[sizeof(long)];
        for (long offset = 0, record = 0; offset < dataSize; offset += value.Length, record++)
        {
            token.ThrowIfCancellationRequested();
            random.NextBytes(value);
            BinaryPrimitives.WriteInt64LittleEndian(key, record);
            var status = context.Upsert((FixedSpanByteKey)(ReadOnlySpan<byte>)key,
                value.AsSpan(0, (int)Math.Min(value.Length, dataSize - offset)));
            Receiver.Require(status.IsCompletedSuccessfully, $"Generated record {record} failed: {status}");
        }
    }
}