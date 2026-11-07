// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.cluster;
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

        [Test]
        public void ProductionScenarioIsDefault()
        {
            Assert.That(Program.SelectScenario(["send"]), Is.EqualTo("full"));
            Assert.That(Program.SelectScenario(["receive"]), Is.EqualTo("full"));
            Assert.That(Program.SelectScenario(["generate"]), Is.EqualTo("transfer-only"));
            Assert.That(Program.SelectScenario(["send", "--scenario", "transfer-only"]), Is.EqualTo("transfer-only"));
            Assert.Throws<ArgumentException>(() => Program.SelectScenario(["send", "--scenario", "invalid"]));
            Assert.Throws<ArgumentException>(() => Program.SelectScenario(["send", "--scenario"]));
            Assert.Throws<ArgumentException>(() => Program.SelectScenario(["send", "--scenario", "full", "--scenario", "full"]));
        }

        [TestCase("0.0.0.0")]
        [TestCase("::")]
        [TestCase("8.8.8.8")]
        [TestCase("example.com")]
        public void FullScenarioRejectsUnsafeAddresses(string address)
            => Assert.Throws<ArgumentException>(() => FullOptions.PrivateAddress(address));

        [TestCase("127.0.0.1")]
        [TestCase("::1")]
        [TestCase("10.1.2.3")]
        [TestCase("172.16.0.2")]
        [TestCase("192.168.1.2")]
        public void FullScenarioAcceptsPrivateAddresses(string address)
            => Assert.That(FullOptions.PrivateAddress(address).ToString(), Is.EqualTo(address));

        [Test]
        public void ProductionDatasetIsRepeatableAndPayloadBounded()
        {
            Assert.That(FullReplication.Value(0, 16385), Has.Length.EqualTo(16384));
            Assert.That(FullReplication.Value(1, 16385), Has.Length.EqualTo(1));
            Assert.That(FullReplication.Value(0, 16385), Is.EqualTo(FullReplication.Value(0, 16385)));
            Assert.That(FullReplication.Info("# Replication\r\nrole:slave\r\nmaster_sync_in_progress:False\r\n")["role"], Is.EqualTo("slave"));
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task FullScenarioUsesRealServerProcessesAndFreshCheckpoints(bool concurrently)
        {
            Directory.CreateDirectory(TestUtils.MethodTestDir);
            var root = new DirectoryInfo(TestContext.CurrentContext.TestDirectory);
            while (root != null && !File.Exists(Path.Combine(root.FullName, "Garnet.slnx")))
                root = root.Parent;
            Assert.That(root, Is.Not.Null, "The external server build is in this source workspace.");
#if DEBUG
            const string configuration = "Debug";
#else
            const string configuration = "Release";
#endif
            var server = Path.Combine(root.FullName, "main", "GarnetServer", "bin", configuration, "net10.0", "GarnetServer.dll");
            var port = ClusterTestContext.Port;
            var receive = FullOptions.Parse(["receive", "--server", server, "--output", TestUtils.MethodTestDir,
                "--port", port.ToString(), "--server-port", (port + 1).ToString(), "--write-buffers", "2",
                "--write-buffers-concurrently", concurrently.ToString(), "--phase-timing", "true", "--timeout-seconds", "60"]);
            var csv = Path.Combine(TestUtils.MethodTestDir, "full.csv");
            var send = FullOptions.Parse(["send", "--server", server, "--output", TestUtils.MethodTestDir,
                "--port", port.ToString(), "--server-port", (port + 2).ToString(), "--data-size", "4m",
                "--chunks", "128k", "--depths", "2", "--phase-timing", "true", "--timeout-seconds", "60", "--csv", csv]);
            using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(90));
            var receiver = FullReplication.RunAsync(receive, cancellation.Token);
            try
            {
                await FullReplication.RunAsync(send, cancellation.Token);
                await receiver;
                var result = await File.ReadAllTextAsync(csv);
                Assert.That(result, Does.Contain("\"full\",\"verified\""));
                Assert.That(result, Does.Not.Contain("unavailable"));
                var fingerprints = Regex.Matches(result, "\"([0-9A-F]{64})\"");
                Assert.That(fingerprints, Has.Count.EqualTo(2));
                Assert.That(fingerprints[0].Value, Is.EqualTo(fingerprints[1].Value));
                Assert.That(Directory.GetDirectories(TestUtils.MethodTestDir, "full-send-*"), Has.Length.EqualTo(1));
                Assert.That(Directory.GetDirectories(TestUtils.MethodTestDir, "full-receive-*"), Has.Length.EqualTo(1));
            }
            catch
            {
                foreach (var file in Directory.EnumerateFiles(TestUtils.MethodTestDir, "*.log", SearchOption.AllDirectories))
                {
                    TestContext.Out.WriteLine(file);
                    TestContext.Out.WriteLine(await File.ReadAllTextAsync(file));
                }
                throw;
            }
            finally
            {
                await cancellation.CancelAsync();
                await receiver.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }
        }

        [Test]
        public void FullOptionsRejectComponentSwitches()
        {
            Assert.Throws<ArgumentException>(() => FullOptions.Parse(["send", "--source-prefix", "existing"]));
            Assert.Throws<ArgumentException>(() => FullOptions.Parse(["receive", "--ack-delay-us", "1000"]));
            Assert.Throws<ArgumentException>(() => FullOptions.Parse(["receive", "--write-buffers-concurrently", "invalid"]));
        }

        [Test]
        public void PhaseMetricsAreUnavailableUntilSuccessfulPublication()
        {
            var timing = new ReplicaBenchmarkTiming();
            Assert.That(timing.GetMetrics()[0].Value, Is.EqualTo("unavailable"));
            Assert.That(timing.GetMetrics()[2].Value, Is.EqualTo("unavailable"));
            var started = Stopwatch.GetTimestamp();
            timing.RecordShipment(started, 4096);
            timing.RecordRecovery(started);
            Assert.That(timing.GetMetrics()[1].Value, Is.EqualTo("4096"));
            Assert.That(timing.GetMetrics()[0].Value, Is.Not.EqualTo("unavailable"));
            Assert.That(timing.GetMetrics()[2].Value, Is.Not.EqualTo("unavailable"));
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task ShipmentMetricsPublishOnlyAfterAllReadersSucceed(bool fail)
        {
            var timing = new ReplicaBenchmarkTiming();
            if (fail)
                timing.RecordShipment(Stopwatch.GetTimestamp(), 4096);
            var previous = timing.GetMetrics()[1].Value;
            using var driver = new SnapshotTransmissionDriver(null, TimeSpan.FromSeconds(1), benchmarkTiming: timing);
            driver.AddReader(new ScriptedSnapshotSource(null));
            driver.AddReader(new ScriptedSnapshotSource(() =>
            {
                Assert.That(timing.GetMetrics()[1].Value, Is.EqualTo(previous));
                if (fail)
                    throw new IOException("Shipment failed.");
            }));
            if (fail)
                Assert.ThrowsAsync<IOException>(() => driver.SendCheckpointAsync());
            else
                await driver.SendCheckpointAsync();
            Assert.That(timing.GetMetrics()[1].Value, Is.EqualTo(fail ? previous : "1024"),
                "Successful shipment bytes include padding, not just logical source length.");
        }

        private sealed class ScriptedSnapshotSource(Action transmit) : ISnapshotReader, ISnapshotTransmitSource, ISnapshotDataSource
        {
            public ISnapshotDataSource DataSource => this;
            public CheckpointFileType Type => default;
            public Guid Token => Guid.Empty;
            public long StartOffset => 128;
            public long EndOffset => StartOffset + 1;
            public long CurrentOffset { get; private set; } = 128;
            public bool HasNextChunk => false;
            public IEnumerable<ISnapshotTransmitSource> GetTransmitSources() => [this];
            public Task<DataSourceReadResult> ReadNextChunkAsync(CancellationToken cancellationToken = default)
                => throw new NotSupportedException();
            public Task TransmitAsync(GarnetClientSession gcs, TimeSpan timeout, CancellationToken cancellationToken = default)
            {
                CurrentOffset = StartOffset + 512;
                transmit?.Invoke();
                return Task.CompletedTask;
            }
            public void Dispose() { }
        }

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

        [TestCase(0, false)]
        [TestCase(1, false)]
        [TestCase(2, false)]
        [TestCase(4, false)]
        [TestCase(0, true)]
        [TestCase(1, true)]
        [TestCase(2, true)]
        [TestCase(4, true)]
        public async Task TransfersAndVerifiesFileWithAcceptanceReplies(int writeBuffers, bool concurrently)
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
                "--ack-delay-us", "1000", "--write-buffers-concurrently", concurrently.ToString()]);
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