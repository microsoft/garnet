// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Upgrade tests driven by a checkpoint produced by GENUINELY downlevel (cv7) code, rather than one synthesized by
    /// <see cref="V7CheckpointFixture"/>. The data written and verified is defined once in <see cref="V7Corpus"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// WHY THIS EXISTS SEPARATELY. <see cref="V7CheckpointFixture"/> fabricates a cv7 checkpoint by rewriting a
    /// current-format one in place. That is only valid while every out-of-line component is exact-size and headerless,
    /// because only then are the current object-log bytes byte-identical to the cv7 dense encoding. It therefore cannot
    /// cover the cases that actually differ between the formats: components at and above the 511-byte exact-size
    /// cutoff, multi-page components, components crossing a 4 MB object-log segment, and overflow keys at those sizes.
    /// Those need bytes written by the cv7 writer itself, which is what these tests consume. The full permutation sweep
    /// -- overflow key, overflow value, both, object value, and object value with overflow key, across every size seam --
    /// is defined in <see cref="V7Corpus"/>; one object type (Hash) suffices because the framing is per out-of-line
    /// component and does not depend on the object kind.
    /// </para>
    /// <para>
    /// WHY THE ARTIFACTS ARE NOT CHECKED IN. By decision (2026-10-08) we generate on demand and discard, rather than
    /// persisting fixtures in LFS. The generator is a pinned branch, so the artifacts are reproducible at any time and
    /// nothing large lives in the repo. These tests therefore SKIP when no artifact directory is supplied, and the
    /// recipe to produce one is below.
    /// </para>
    ///
    /// <para>
    /// THE GENERATOR BRANCH.
    /// <code>
    ///   branch: tedhar/cv7-save
    ///   commit: 4b1a49f1d8770b218087efc49dc00af2061cf1ec
    /// </code>
    /// That commit is <c>origin/main</c> as of 2026-10-08, verified downlevel: <c>HybridLogRecoveryInfo.CheckpointVersion</c>
    /// is 7 there, against 8 on this branch. The SHA is recorded here so the branch can be recreated from main's history
    /// if it is ever deleted:
    /// <code>
    ///   git branch tedhar/cv7-save 4b1a49f1d8770b218087efc49dc00af2061cf1ec
    ///   git push origin tedhar/cv7-save
    /// </code>
    /// </para>
    ///
    /// <para>
    /// CHECKPOINT TYPE AND TIER. The recovering store MUST be configured with the same log geometry the generator used,
    /// because a cv7 checkpoint predates <c>HybridLogRecoveryInfo.LogGeometryCheckpointVersion</c>, so
    /// <c>AllocatorBase.VerifyLogGeometry</c> does not and cannot check it -- a mismatch is silently misparsed, not
    /// reported. Both sides therefore pin page, memory, main-log segment, and object-log segment sizes explicitly (the
    /// recovering tests use <c>TestUtils.lowMemory</c> plus <c>objectLogSegmentSize</c>, and the recipe mirrors those on
    /// the generator command line), and both run with the storage tier ON: the object log must have been evicted to its
    /// device for the cv7 bytes to exist there for the upgrade to convert. The stock cv7 server emits a Snapshot
    /// checkpoint; it has no command-line or config switch for FoldOver (<c>UseFoldOverCheckpoints</c> is a server-options
    /// field with no bound option), so these generated tests exercise the Snapshot recovery path. FoldOver recovery of the
    /// chunked object log is covered without a cv7 artifact by
    /// <c>ObjectLogUpgradeTests.UpgradeConvertsDownlevelStoreEndToEnd</c> (synthetic cv7, both checkpoint types,
    /// headerless sizes) and by <see cref="GeneratedV7CorpusRoundTripsOnCurrentBinary"/> (current writer, both types, all
    /// sizes, in-process). Because the object-log byte format is identical for the two checkpoint types, the only gap is a
    /// cv7 FoldOver at above-cutoff sizes, which would require teaching the generator branch to emit FoldOver.
    /// </para>
    ///
    /// <para>
    /// HOW TO REGENERATE THE ARTIFACTS (instructions for a future agent or developer).
    /// <code>
    ///   # 1. Get a worktree on the downlevel generator. Keep the path SHORT: NativeStorageDevice enforces a
    ///   #    249-character limit (WIN32_MAX_PATH - 11) on every platform, and checkpoint paths nest deeply.
    ///   git fetch origin tedhar/cv7-save
    ///   git worktree add --detach C:\cv7 origin/tedhar/cv7-save      # or ~/cv7 on Linux
    ///
    ///   # 2. Build the downlevel server.
    ///   cd C:\cv7
    ///   dotnet build Garnet.slnx -c Debug
    ///
    ///   # 3. Run it against a scratch directory with the EXACT geometry the tests recover with (TestUtils.lowMemory,
    ///   #    i.e. 16 KB of log memory over 4 KB pages, plus a 4 MB object-log segment), the storage tier ON, and a
    ///   #    256-byte inline cutoff so every 510-byte-or-larger component is stored out-of-line:
    ///   cd C:\cv7\main\GarnetServer
    ///   dotnet run -c Debug -f net10.0 -- --port 7777 -c C:\cv7out -m 16k -p 4096 -s 1g --object-log-segment 4m -i 1m --max-inline-key-size 256 --max-inline-value-size 256
    ///
    ///   # 4. Populate it with the shared corpus and SAVE, by running the [Explicit] generator against the running server:
    ///   $env:GARNET_CV7_GEN_ENDPOINT = "127.0.0.1:7777"
    ///   dotnet test test/standalone/Garnet.test -f net10.0 -c Debug --filter FullyQualifiedName~GenerateV7Corpus
    ///
    ///   # 5. Point the upgrade tests at the result and run them.
    ///   $env:GARNET_CV7_ARTIFACTS = "C:\cv7out"
    ///   dotnet test test/standalone/Garnet.test -f net10.0 -c Debug --filter FullyQualifiedName~GeneratedV7UpgradeTests
    ///
    ///   # 6. Discard when done; nothing here is meant to be retained.
    ///   git worktree remove C:\cv7
    /// </code>
    /// </para>
    ///
    /// <para>
    /// A NOTE ON THE SILENT-EMPTY-STORE CASE. A FoldOver (log-only) checkpoint written with no real device, then
    /// recovered, comes back empty with no error: FoldOver only shifts the read-only region to the tail and flushes it to
    /// the log device, so a null device discards it and recovery reads nothing. That is a different failure from this
    /// recipe, which uses a real device and a self-contained Snapshot, and it is why both sides here keep the tier ON.
    /// </para>
    ///
    /// <para>
    /// WHAT TO ASSERT, beyond "it recovered". A record left in the downlevel encoding does NOT surface as a recovery
    /// error: it is decoded with the current reader on a later live read and yields garbage lengths. So the meaningful
    /// checks are <see cref="V7CheckpointFixture.CountDownlevelRecordsOnMainLog"/> returning zero after the upgrade, and
    /// <see cref="V7Corpus.Verify"/> reading every value back and comparing it byte-for-byte with what was written.
    /// </para>
    /// </remarks>
    [TestFixture]
    public class GeneratedV7UpgradeTests : TestBase
    {
        /// <summary>Environment variable naming a directory that holds a checkpoint produced by the cv7 generator branch.</summary>
        internal const string ArtifactsEnvVar = "GARNET_CV7_ARTIFACTS";

        const string SkipReason =
            "No genuinely-downlevel (cv7) artifacts supplied. Set " + ArtifactsEnvVar + " to a checkpoint directory produced by "
            + "branch tedhar/cv7-save (4b1a49f1d8770b218087efc49dc00af2061cf1ec); see the regeneration recipe in the comments on "
            + nameof(GeneratedV7UpgradeTests) + ".";

        string artifactsRoot;

        /// <summary>Environment variable naming the host:port of a running cv7 generator server, for <see cref="GenerateV7Corpus"/>.</summary>
        internal const string GeneratorEndpointEnvVar = "GARNET_CV7_GEN_ENDPOINT";

        /// <summary>
        /// The object-log segment size the generator and every recovering store must agree on. A cv7 checkpoint records no log
        /// geometry, so this cannot be verified on recovery and a mismatch is silently misparsed; it is small enough that the
        /// multi-megabyte corpus components cross it. The remaining geometry (page, memory, main-log segment) comes from
        /// <c>TestUtils.lowMemory</c>, which the regeneration recipe mirrors on the generator command line.
        /// </summary>
        const string ObjectLogSegmentArg = "4m";

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            artifactsRoot = Environment.GetEnvironmentVariable(ArtifactsEnvVar);
        }

        [TearDown]
        public void TearDown()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            TestUtils.OnTearDown();
        }

        /// <summary>Copy the supplied artifacts into this test's directory so the upgrade mutates a throwaway copy, never the source.</summary>
        void RequireArtifacts()
        {
            if (string.IsNullOrEmpty(artifactsRoot) || !Directory.Exists(artifactsRoot))
                Assert.Ignore(SkipReason);

            var dest = new DirectoryInfo(TestUtils.MethodTestDir).FullName;
            _ = Directory.CreateDirectory(dest);
            foreach (var dir in Directory.GetDirectories(artifactsRoot, "*", SearchOption.AllDirectories))
                _ = Directory.CreateDirectory(dir.Replace(artifactsRoot, dest));
            foreach (var file in Directory.GetFiles(artifactsRoot, "*", SearchOption.AllDirectories))
                File.Copy(file, file.Replace(artifactsRoot, dest), overwrite: true);
        }

        /// <summary>
        /// A genuinely downlevel (cv7) checkpoint produced by the generator branch converts cleanly, leaves no record in the old
        /// encoding, and reads back byte-for-byte against the shared corpus.
        /// </summary>
        [Test]
        [Category("GarnetServer")]
        public void GeneratedV7StoreUpgradesAndReadsBack()
        {
            RequireArtifacts();

            using (var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, objectLogSegmentSize: ObjectLogSegmentArg, tryRecover: true, upgrade: true))
            {
                ClassicAssert.IsTrue(server.IsUpgradeRun);
                server.RunUpgrade();
            }

            // A record left downlevel does not fail recovery; it decodes as garbage on a later read. This is the check that catches it.
            var stillDownlevel = V7CheckpointFixture.CountDownlevelRecordsOnMainLog(new DirectoryInfo(TestUtils.MethodTestDir).FullName);
            ClassicAssert.AreEqual(0, stillDownlevel, "records were left in the downlevel encoding after the upgrade");

            using var recovered = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, objectLogSegmentSize: ObjectLogSegmentArg, tryRecover: true);
            recovered.Start();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            V7Corpus.Verify(redis.GetDatabase(0), V7Corpus.Build(V7Corpus.Profile.Full));
        }

        /// <summary>
        /// A genuinely downlevel checkpoint whose objects were evicted to the object-log device CANNOT be recovered in place
        /// without <c>--upgrade</c>: up-converting the downlevel object log needs the separate upgrade device that only an
        /// <c>--upgrade</c> run configures, so recovery refuses. With <c>FailOnRecoveryError</c> on, the refusal surfaces as a
        /// <see cref="TsavoriteException"/>; with the default off, the same refusal is swallowed and the server comes up with an
        /// empty store.
        /// </summary>
        /// <remarks>
        /// This corrects an earlier premise that cv7 data was "readable without --upgrade." That holds only for records still
        /// resident on the main log; once the object bytes live on the object-log device, <c>AllocatorBase.VerifyUpgradeCapability</c>
        /// rejects recovery unless an upgrade device is present, because the downlevel object log stores above-cutoff components
        /// headerless and cannot be decoded in place by a release that no longer carries the per-record downlevel selector. The
        /// default-off branch is a silent-data-loss footgun of the same family as the SILENT-EMPTY note above: an operator who
        /// restarts the new binary over a cv7 data directory without <c>--upgrade</c> gets an empty store and no error. Whether to
        /// make this refusal fatal regardless of <c>FailOnRecoveryError</c> (as the geometry-mismatch path already is) is a product
        /// decision left to the maintainers; this test pins the current behavior so a change is deliberate.
        /// </remarks>
        [Test]
        [Category("GarnetServer")]
        public void GeneratedV7StoreRejectsRecoveryWithoutUpgrade()
        {
            RequireArtifacts();

            // With FailOnRecoveryError on, recovery refuses rather than silently dropping the dataset.
            using (var strict = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, objectLogSegmentSize: ObjectLogSegmentArg, tryRecover: true, failOnRecoveryError: true))
            {
                var ex = Assert.Throws<TsavoriteException>(() => strict.Start(),
                    "recovering a downlevel object-log checkpoint without --upgrade must refuse, not come up empty");
                StringAssert.Contains("UpgradeObjectLogDevice", ex.Message);
            }

            // Under the default (FailOnRecoveryError off) the same refusal is swallowed, so the store recovers empty. Pinning this
            // documents the silent-data-loss footgun; a product change to fail loudly by default would update this assertion.
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            RequireArtifacts();
            using var recovered = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, objectLogSegmentSize: ObjectLogSegmentArg, tryRecover: true);
            recovered.Start();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            ClassicAssert.AreEqual(0L, (long)redis.GetDatabase(0).Execute("DBSIZE"),
                "without --upgrade and with FailOnRecoveryError off, an un-upgraded object-log store recovers empty");
        }

        /// <summary>
        /// The corpus and its byte-exact verifier, exercised end to end against the CURRENT binary: populate, checkpoint, recover,
        /// and verify. This needs no cv7 artifact and so runs in CI, proving the corpus plumbing, the object-log chunking, and a
        /// multi-megabyte component crossing a 4 MB object-log segment for both checkpoint types. It does NOT exercise the cv7
        /// decode; that is what <see cref="GeneratedV7StoreUpgradesAndReadsBack"/> and
        /// <see cref="GeneratedV7StoreRejectsRecoveryWithoutUpgrade"/> add once artifacts are supplied.
        /// </summary>
        [Test]
        [Category("GarnetServer")]
        public void GeneratedV7CorpusRoundTripsOnCurrentBinary([Values(false, true)] bool useFoldOver)
        {
            var corpus = V7Corpus.Build(V7Corpus.Profile.Smoke);

            using (var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, objectLogSegmentSize: ObjectLogSegmentArg, useFoldOverCheckpoints: useFoldOver))
            {
                server.Start();
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
                V7Corpus.Populate(redis.GetDatabase(0), corpus);
                _ = redis.GetDatabase(0).Execute("SAVE");
            }

            using (var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, objectLogSegmentSize: ObjectLogSegmentArg, useFoldOverCheckpoints: useFoldOver, tryRecover: true))
            {
                server.Start();
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                V7Corpus.Verify(redis.GetDatabase(0), corpus);
            }
        }

        /// <summary>
        /// Populate a RUNNING cv7 generator server with the shared corpus and SAVE, producing the artifacts the generated tests
        /// consume. Explicit because it needs an out-of-process downlevel server; see the regeneration recipe in the remarks on
        /// <see cref="GeneratedV7UpgradeTests"/>.
        /// </summary>
        [Test]
        [Explicit]
        [Category("GarnetServer")]
        public void GenerateV7Corpus()
        {
            var endpoint = Environment.GetEnvironmentVariable(GeneratorEndpointEnvVar);
            if (string.IsNullOrEmpty(endpoint))
                Assert.Ignore($"Set {GeneratorEndpointEnvVar} to the host:port of a running cv7 generator server; see the recipe on {nameof(GeneratedV7UpgradeTests)}.");

            var config = ConfigurationOptions.Parse(endpoint);
            config.AllowAdmin = true;
            using var redis = ConnectionMultiplexer.Connect(config);
            var db = redis.GetDatabase(0);
            V7Corpus.Populate(db, V7Corpus.Build(V7Corpus.Profile.Full));
            _ = db.Execute("SAVE");
        }
    }
}