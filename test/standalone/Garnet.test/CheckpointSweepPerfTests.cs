// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Wall-clock comparison of the Snapshot checkpoint phase, used to measure the post-checkpoint
    /// <c>ClearSerializedObjectData</c> sweep across two builds.
    /// </summary>
    /// <remarks>
    /// <para>
    /// WHY THIS IS A TEST AND NOT A BENCHMARK. The work being measured is timing-dependent: how much the sweep has to
    /// examine depends on how many CopyUpdate sources cached a <c>(v)</c> image while the checkpoint was running, which
    /// varies run to run when updates and checkpointing overlap. Sized large enough for the walk to matter - enough
    /// records that scanning the log misses cache - it is also too large to sit in BDN. So this loads the database once
    /// and then checkpoints repeatedly **without adding more data**, which removes the concurrent-update variance and
    /// leaves a repeatable measurement.
    /// </para>
    /// <para>
    /// SNAPSHOT ONLY. <c>FoldOverSMTask</c> shifts ReadOnly to the tail at <c>WAIT_FLUSH</c>, so under FoldOver the
    /// swept range <c>[ReadOnlyAddress, TailAddress)</c> is already nearly empty and this would measure nothing. The
    /// fixture therefore leaves the server at its default Snapshot checkpoints; do not add FoldOver here.
    /// </para>
    /// <para>
    /// WHAT THE NUMBER INCLUDES. <c>SAVE</c> is synchronous and
    /// <c>DatabaseManagerBase.RunPostCheckpointCleanup</c> runs inline on that path, so the round-trip covers the
    /// sweep. It also covers writing the snapshot, which is why the values are deliberately short: snapshot bytes scale
    /// with value size while the walk scales with record count, so small values maximise the sweep's share of the
    /// total. Compare medians across builds, not single iterations.
    /// </para>
    /// <para>
    /// HOW TO RUN A BEFORE/AFTER COMPARISON. This test does not exist on <c>main</c>, so copy this one file onto the
    /// baseline build rather than trying to check it out there. It deliberately uses only APIs that exist on both:
    /// <c>TestUtils.CreateGarnetServer</c> without <c>useFoldOverCheckpoints</c>, and the <c>TestBase</c> alias that
    /// <c>test/standalone/Directory.Build.props</c> defines on both branches.
    /// <code>
    ///   # 1. Baseline. From a clean checkout of the comparison point:
    ///   git checkout main
    ///   copy this file to test\standalone\Garnet.test\CheckpointSweepPerfTests.cs
    ///   dotnet build test\standalone\Garnet.test -c Release
    ///   $env:GARNET_PERF_TAG = "before"
    ///   dotnet test test\standalone\Garnet.test -f net10.0 -c Release --no-build ^
    ///       --filter "FullyQualifiedName~SnapshotCheckpointPhaseTiming" -- NUnit.DefaultTestNamePattern="{m}"
    ///
    ///   # 2. The change. Same machine, same shell, nothing else running:
    ///   git checkout tedhar/aof-chunk
    ///   dotnet build test\standalone\Garnet.test -c Release
    ///   $env:GARNET_PERF_TAG = "after"
    ///   dotnet test test\standalone\Garnet.test -f net10.0 -c Release --no-build ^
    ///       --filter "FullyQualifiedName~SnapshotCheckpointPhaseTiming"
    ///
    ///   # 3. Compare the two summary lines, which are tagged and greppable:
    ///   #      [SWEEPPERF] tag=before summary records=... medianMs=...
    ///   #      [SWEEPPERF] tag=after  summary records=... medianMs=...
    /// </code>
    /// Run it Release, on an otherwise idle machine, and take the median. Because the test is
    /// <see cref="ExplicitAttribute"/> it never runs in CI or in an unfiltered local pass; it must be named.
    /// </para>
    /// <para>
    /// TUNING. All four knobs are environment variables so the same binary can be re-run without an edit:
    /// <c>GARNET_PERF_TAG</c>, <c>GARNET_PERF_RECORDS</c>, <c>GARNET_PERF_CHECKPOINTS</c>,
    /// <c>GARNET_PERF_VALUE_LEN</c>. Raise the record count until the baseline shows a checkpoint time well clear of
    /// run-to-run noise. For scale, the 300,000-record default puts the checkpoint phase in the hundreds of
    /// milliseconds on a developer machine, which separates two builds cleanly; 20,000 records puts it near 30ms,
    /// which is too close to the noise floor to compare.
    /// </para>
    /// </remarks>
    [TestFixture]
    public class CheckpointSweepPerfTests : TestBase
    {
        GarnetServer server;

        const int DefaultRecords = 300_000;
        const int DefaultCheckpoints = 10;
        const int DefaultValueLength = 16;
        const int LoadBatchSize = 10_000;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            // Large enough that the loaded records stay in the mutable region, so every checkpoint sweeps the same
            // large [ReadOnlyAddress, TailAddress) range rather than a range that shrinks as pages go read-only.
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, memorySize: "1g", indexSize: "64m");
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            TestUtils.OnTearDown();
        }

        static int EnvInt(string name, int fallback)
            => int.TryParse(Environment.GetEnvironmentVariable(name), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) && value > 0
                ? value
                : fallback;

        [Test]
        [Explicit("Wall-clock perf comparison. Run by name on an idle machine and compare the tagged summary across builds.")]
        [Category("GarnetServer")]
        public void SnapshotCheckpointPhaseTiming()
        {
            var tag = Environment.GetEnvironmentVariable("GARNET_PERF_TAG") ?? "untagged";
            var records = EnvInt("GARNET_PERF_RECORDS", DefaultRecords);
            var checkpoints = EnvInt("GARNET_PERF_CHECKPOINTS", DefaultCheckpoints);
            var valueLength = EnvInt("GARNET_PERF_VALUE_LEN", DefaultValueLength);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            var value = new string('v', valueLength);
            var loadStopwatch = Stopwatch.StartNew();
            for (var start = 0; start < records; start += LoadBatchSize)
            {
                var count = Math.Min(LoadBatchSize, records - start);
                var batch = db.CreateBatch();
                var pending = new List<Task>(count);
                for (var i = 0; i < count; i++)
                    pending.Add(batch.HashSetAsync($"h:{start + i}", "f", value));
                batch.Execute();
                Task.WaitAll([.. pending]);
            }
            loadStopwatch.Stop();

            var dbSize = (long)db.Execute("DBSIZE");
            ClassicAssert.AreEqual(records, dbSize, "the load did not produce the expected record count");

            TestContext.Progress.WriteLine(
                $"[SWEEPPERF] tag={tag} load records={records} valueLength={valueLength} loadMs={loadStopwatch.Elapsed.TotalMilliseconds:F1}");

            // One untimed checkpoint first: it pays the one-off costs of creating the snapshot files and of growing
            // whatever buffers the checkpoint path needs, which would otherwise land entirely on iteration 1.
            _ = db.Execute("SAVE");

            var elapsed = new double[checkpoints];
            for (var i = 0; i < checkpoints; i++)
            {
                var stopwatch = Stopwatch.StartNew();
                var result = (string)db.Execute("SAVE");
                stopwatch.Stop();

                ClassicAssert.AreEqual("OK", result, "SAVE did not report success");
                elapsed[i] = stopwatch.Elapsed.TotalMilliseconds;
                TestContext.Progress.WriteLine(
                    $"[SWEEPPERF] tag={tag} iteration={i} checkpointMs={elapsed[i]:F3}");
            }

            var ordered = (double[])elapsed.Clone();
            Array.Sort(ordered);
            var median = ordered.Length % 2 == 1
                ? ordered[ordered.Length / 2]
                : (ordered[(ordered.Length / 2) - 1] + ordered[ordered.Length / 2]) / 2;

            var total = 0.0;
            foreach (var ms in elapsed)
                total += ms;

            TestContext.Progress.WriteLine(
                $"[SWEEPPERF] tag={tag} summary records={records} checkpoints={checkpoints} valueLength={valueLength} " +
                $"minMs={ordered[0]:F3} medianMs={median:F3} maxMs={ordered[^1]:F3} meanMs={total / elapsed.Length:F3}");
        }
    }
}