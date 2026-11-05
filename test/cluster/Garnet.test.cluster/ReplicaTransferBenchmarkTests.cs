// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using NUnit.Framework;
using ReplicaTransfer.benchmark;
using Tsavorite.core;
using BenchmarkOptions = ReplicaTransfer.benchmark.Options;
using GeneratedStoreFunctions = Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.SpanByteRecordTriggers>;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class ReplicaTransferBenchmarkTests : TestBase
    {
        [SetUp]
        public void Setup() => TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

        [TearDown]
        public void TearDown()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            TestUtils.OnTearDown();
        }

        [TestCase("send")]
        [TestCase("send --source-prefix x --generate .")]
        [TestCase("send --source-prefix x --data-size 64m")]
        [TestCase("send --generate . --data-size 0")]
        [TestCase("send --generate . --data-size 65g")]
        [TestCase("generate --output . --data-size invalid")]
        [TestCase("generate --source-prefix x")]
        [TestCase("receive --output . --timeout-seconds 1 --ack-delay-us 1000000")]
        [TestCase("receive --output . --ack-delay-us 20_000_")]
        [TestCase("receive --output . --ack-delay-us 20,000")]
        [TestCase("receive --output . --write-buffers 9")]
        [TestCase("receive --output . --write-buffers -1")]
        [TestCase("send --source-prefix x --write-buffers 1")]
        public void InvalidOptions(string command)
            => Assert.Catch<ArgumentException>(() => BenchmarkOptions.Parse(command.Split(' ')));

        [TestCase("receive --output .", 0L)]
        [TestCase("receive --output . --ack-delay-us 1050", 1050L)]
        [TestCase("receive --output . --ack-delay-us 20_000", 20000L)]
        public void AckDelayOptions(string command, long delayUs)
            => Assert.That(BenchmarkOptions.Parse(command.Split(' ')).AckDelayUs, Is.EqualTo(delayUs));

        [TestCase("receive --output .", 1)]
        [TestCase("receive --output . --write-buffers 0", 0)]
        [TestCase("receive --output . --write-buffers 8", 8)]
        public void WriteBufferOptions(string command, int writeBuffers)
            => Assert.That(BenchmarkOptions.Parse(command.Split(' ')).WriteBuffers, Is.EqualTo(writeBuffers));

        [Test]
        public async Task AckDelayIsReleasedPrecisely()
        {
            // A timer-based wait rounds a 1 ms delay up to the next ~15.6 ms Windows timer tick; the spin-wait must not.
            var lateness = new List<double>();
            for (var i = 0; i < 50; i++)
            {
                var readyAt = Stopwatch.GetTimestamp() + Stopwatch.Frequency / 1000;
                await Acknowledgements.WaitUntilAsync(readyAt, CancellationToken.None);
                var releasedAt = Stopwatch.GetTimestamp();
                Assert.That(releasedAt, Is.GreaterThanOrEqualTo(readyAt), "An ACK must never be released early");
                lateness.Add((releasedAt - readyAt) * 1000.0 / Stopwatch.Frequency);
            }
            lateness.Sort();
            Assert.That(lateness[lateness.Count / 2], Is.LessThan(1.0), "Median lateness in milliseconds");

            // Long waits sleep first, then spin; they must still not return early.
            var longReadyAt = Stopwatch.GetTimestamp() + Stopwatch.Frequency / 20;
            await Acknowledgements.WaitUntilAsync(longReadyAt, CancellationToken.None);
            Assert.That(Stopwatch.GetTimestamp(), Is.GreaterThanOrEqualTo(longReadyAt));
        }

        [Test]
        public void GeneratedSweepOptions()
        {
            var options = BenchmarkOptions.Parse(["send", "--generate", ".", "--data-size", "64m",
                "--chunks", "128k,1m", "--depths", "1,8", "--repeats", "1"]);
            Assert.That(options.SourcePrefix, Is.Null);
            Assert.That(options.GenerateDirectory, Is.EqualTo(Path.GetFullPath(".")));
            Assert.That(options.DataSize, Is.EqualTo(64L << 20));
            Assert.That(options.Cases, Is.EqualTo([(128 << 10, 1), (128 << 10, 8), (1 << 20, 1), (1 << 20, 8)]));
        }

        [Test]
        public void ExistingSourceOptions()
        {
            var options = BenchmarkOptions.Parse(["send", "--source-prefix", "sample"]);
            Assert.That(options.SourcePrefix, Is.EqualTo(Path.GetFullPath("sample")));
            Assert.That(options.GenerateDirectory, Is.Null);
        }

        [TestCase("16k", 16L << 10)]
        [TestCase("64g", 64L << 30)]
        public void GenerationSizeLimits(string size, long bytes)
        {
            var options = BenchmarkOptions.Parse(["generate", "--output", ".", "--data-size", size]);
            Assert.That(options.DataSize, Is.EqualTo(bytes));
        }

        [Test]
        public async Task GenerateCheckpoint()
        {
            Directory.CreateDirectory(TestUtils.MethodTestDir);
            var options = BenchmarkOptions.Parse(["generate", "--output", TestUtils.MethodTestDir, "--data-size", "16k"]);
            var first = await CheckpointGenerator.RunAsync(options, CancellationToken.None);
            Assert.That(new FileInfo(first + ".0").Length % 4096, Is.Zero);
            var hash = await Receiver.HashAsync(first + ".0", new FileInfo(first + ".0").Length, CancellationToken.None);
            var second = await CheckpointGenerator.RunAsync(options, CancellationToken.None);
            Assert.That(second, Is.Not.EqualTo(first));
            Assert.That(await Receiver.HashAsync(first + ".0", new FileInfo(first + ".0").Length, CancellationToken.None), Is.EqualTo(hash));
            Assert.That(File.Exists(first + ".1"), Is.False);

            using var device = Devices.CreateLogDevice(first, deviceType: DeviceType.RandomAccess);
            using var store = new TsavoriteKV<GeneratedStoreFunctions, SpanByteAllocator<GeneratedStoreFunctions>>(new()
            {
                IndexSize = 1 << 26,
                LogDevice = device,
                PageSize = 1 << 20,
                LogMemorySize = 1 << 26,
                SegmentSize = 1L << 40,
                CheckpointDir = Path.Combine(Path.GetDirectoryName(first), "checkpoints")
            }, StoreFunctions.Create(SpanByteComparer.Instance, SpanByteRecordTriggers.Instance),
                (settings, functions) => new(settings, functions));
            await store.RecoverAsync();
            Assert.That(store.Log.TailAddress, Is.GreaterThan(options.DataSize));
            using var session = store.NewSession<FixedSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new());
            Span<byte> key = stackalloc byte[sizeof(long)];
            key.Clear();
            PinnedSpanByte input = default;
            SpanByteAndMemory output = default;
            try
            {
                Assert.That(session.BasicContext.Read((FixedSpanByteKey)(ReadOnlySpan<byte>)key, ref input, ref output).Found, Is.True);
                var expected = new byte[16 << 10];
                new Random(42).NextBytes(expected);
                Assert.That(output.ReadOnlySpan.ToArray(), Is.EqualTo(expected));
            }
            finally
            {
                output.Dispose();
            }
        }

        [TestCase(0)]
        [TestCase(1)]
        [TestCase(2)]
        public async Task TransfersAndVerifiesFileWithAcceptanceReplies(int writeBuffers)
        {
            var sourcePrefix = Path.Combine(TestUtils.MethodTestDir, "source");
            var destination = Path.Combine(TestUtils.MethodTestDir, "destination");
            Directory.CreateDirectory(destination);
            var data = new byte[64 << 10];
            new Random(42).NextBytes(data);
            await File.WriteAllBytesAsync(sourcePrefix + ".0", data);

            var port = ClusterTestContext.Port.ToString();
            var receiverOptions = BenchmarkOptions.Parse(["receive", "--output", destination, "--bind", "127.0.0.1",
                "--port", port, "--runs", "2", "--write-buffers", writeBuffers.ToString(), "--timeout-seconds", "20",
                "--ack-delay-us", "1000"]);
            var senderOptions = BenchmarkOptions.Parse(["send", "--source-prefix", sourcePrefix, "--host", "127.0.0.1",
                "--port", port, "--chunks", "4k", "--depths", "1,3", "--repeats", "1", "--timeout-seconds", "20"]);
            using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var receiver = Receiver.RunAsync(receiverOptions, cancellation.Token);
            try
            {
                await Sender.RunAsync(senderOptions, cancellation.Token);
                await receiver;
                var files = Directory.GetFiles(destination, "payload.0", SearchOption.AllDirectories);
                Assert.That(files, Has.Length.EqualTo(2));
                foreach (var file in files)
                    Assert.That(await File.ReadAllBytesAsync(file), Is.EqualTo(data));
            }
            finally
            {
                await cancellation.CancelAsync();
                await receiver.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }
        }

        [Test]
        public void CancelBeforeGeneration()
        {
            Directory.CreateDirectory(TestUtils.MethodTestDir);
            var options = BenchmarkOptions.Parse(["generate", "--output", TestUtils.MethodTestDir, "--data-size", "16k"]);
            Assert.ThrowsAsync<OperationCanceledException>(async () =>
                await CheckpointGenerator.RunAsync(options, new CancellationToken(true)));
            Assert.That(Directory.GetFileSystemEntries(TestUtils.MethodTestDir), Is.Empty);
        }

        [Test]
        public void MissingGenerationDirectory()
        {
            var options = BenchmarkOptions.Parse(["generate", "--output", TestUtils.MethodTestDir]);
            Assert.ThrowsAsync<DirectoryNotFoundException>(async () =>
                await CheckpointGenerator.RunAsync(options, CancellationToken.None));
            Assert.That(Directory.Exists(TestUtils.MethodTestDir), Is.False);
        }
    }
}