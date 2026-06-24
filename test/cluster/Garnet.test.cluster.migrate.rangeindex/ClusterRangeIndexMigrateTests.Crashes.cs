// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
#if DEBUG
using Garnet.common;
#endif
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Cluster-migration tests that currently crash the test host via a native bf-tree panic
    /// (mini_page_op / circular_buffer assertion). They are split into this partial-class file
    /// and marked [Explicit] so they do not run in CI until the underlying RangeIndex
    /// crash/recovery issues are fixed, at which point they become passing regression tests.
    /// </summary>
    public partial class ClusterRangeIndexMigrateTests
    {
        /// <summary>
        /// Client writes RI.SET continuously while migration happens, following MOVED redirects.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        [Explicit("Reproduces a BfTree native crash (mini_page_op/circular_buffer panic) during cluster migration; crashes the test host. Tracked for RangeIndex crash/recovery.")]
        public async Task ClusterMigrateRangeIndexWhileModifyingAsync()
        {
            const int shards = 2;

            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(0);
            var primary1 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            var riKey = FindKeyOnNode(nameof(ClusterMigrateRangeIndexWhileModifyingAsync), primary0Id, slots);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            // Create RI key
            var createResult = (string)context.clusterTestUtils.Execute(
                primary0, "RI.CREATE",
                [riKey, "DISK", "CACHESIZE", "65536", "MINRECORD", "8"],
                flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual("OK", createResult);

            // Start background writer
            using var cts = new CancellationTokenSource();
            var written = new ConcurrentBag<(string Field, string Value)>();

            var writeTask = Task.Run(async () =>
            {
                await Task.Yield();

                using var con = ConnectionMultiplexer.Connect(context.clusterTestUtils.GetRedisConfig(context.endpoints));
                var db = con.GetDatabase();
                var ix = 0;

                while (!cts.IsCancellationRequested)
                {
                    var field = $"field_{ix}";
                    var value = $"value_{ix}";

                    try
                    {
                        var result = (string)db.Execute("RI.SET", [new RedisKey(riKey), field, value]);
                        if (result == "OK")
                            written.Add((field, value));
                    }
                    catch (Exception exc) when (
                        exc is RedisTimeoutException
                        || exc is RedisConnectionException
                        || (exc is RedisServerException rse && (
                            rse.Message.StartsWith("MOVED ")
                            || rse.Message.StartsWith("Key has MOVED to "))))
                    {
                        continue;
                    }

                    ix++;
                }
            });

            await Task.Delay(1_000).ConfigureAwait(false);

            var countPreMigration = written.Count;
            ClassicAssert.IsTrue(countPreMigration > 0, "Should have some writes before migration");

            // Migrate
            using (var migrateToken = new CancellationTokenSource())
            {
                migrateToken.CancelAfter(30_000);

                context.clusterTestUtils.MigrateSlots(primary0, primary1, [slot]);
                context.clusterTestUtils.WaitForMigrationCleanup(0, cancellationToken: migrateToken.Token);
                context.clusterTestUtils.WaitForMigrationCleanup(1, cancellationToken: migrateToken.Token);
            }

            WaitForSlotOwnership(primary0, primary1, slot);

            // Wait for more writes after migration
            var countPostMigration = written.Count;
            await Task.Delay(2_000).ConfigureAwait(false);
            var countAfterPause = written.Count;

            ClassicAssert.IsTrue(countAfterPause > countPostMigration, "Writes should resume after migration");

            cts.Cancel();
            await writeTask.ConfigureAwait(false);

            // Write directly to the target after migration completes and verify readback
            for (var i = 0; i < 5; i++)
            {
                var field = $"verify_field_{i}";
                var value = $"verify_value_{i}";
                var setRes = (string)context.clusterTestUtils.Execute(
                    primary1, "RI.SET", [riKey, field, value],
                    flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual("OK", setRes, $"RI.SET {field} should succeed on target after migration");

                var getRes = (string)context.clusterTestUtils.Execute(
                    primary1, "RI.GET", [riKey, field],
                    flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual(value, getRes, $"RI.GET {field} should return correct value on target");
            }
        }

        /// <summary>
        /// Large RangeIndex migration that generates enough data to span multiple chunks
        /// even at the default 256 KB chunk size. Verifies all data survives.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        [TestCase(1024)]          // 1 KB chunks — many chunks for large tree
        [TestCase(256 * 1024)]    // default — still multiple chunks with enough data
        [Explicit("Reproduces a BfTree native crash (mini_page_op/circular_buffer panic) during cluster migration; crashes the test host. Tracked for RangeIndex crash/recovery.")]
        public void ClusterMigrateRangeIndexLargeTree(int chunkSize)
        {
            const int shards = 2;
            const int fieldCount = 500;

            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(0);
            var primary1 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            var riKey = FindKeyOnNode($"{nameof(ClusterMigrateRangeIndexLargeTree)}_{chunkSize}", primary0Id, slots);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            // Create RI with large data — 500 fields × ~1 KB values ≈ 500 KB of data
            var rand = new Random(42);
            var fields = new List<(string Field, string Value)>();
            for (var i = 0; i < fieldCount; i++)
            {
                var valueBytes = new byte[512];
                rand.NextBytes(valueBytes);
                var value = Convert.ToBase64String(valueBytes);
                fields.Add(($"field_{i:D5}", value));
            }

            // Use a larger max record size to accommodate the ~700-byte base64 values
            var createResult = (string)context.clusterTestUtils.Execute(
                primary0, "RI.CREATE",
                [riKey, "DISK", "CACHESIZE", "65536", "MINRECORD", "8", "MAXRECORD", "1024"],
                flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual("OK", createResult, "RI.CREATE should succeed");

            foreach (var (field, value) in fields)
            {
                var setResult = (string)context.clusterTestUtils.Execute(
                    primary0, "RI.SET", [riKey, field, value],
                    flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual("OK", setResult, $"RI.SET should succeed for {field}");
            }

            // Verify a sample before migration
            VerifyFieldsOnEndpoint(primary0, riKey, fields.Take(10));

            // Migrate
            context.clusterTestUtils.MigrateSlots(primary0, primary1, [slot]);
            context.clusterTestUtils.WaitForMigrationCleanup(0);
            context.clusterTestUtils.WaitForMigrationCleanup(1);

            WaitForSlotOwnership(primary0, primary1, slot);

            // Verify ALL fields on target — every single one must survive
            VerifyFieldsOnEndpoint(primary1, riKey, fields);

            // Verify source returns MOVED
            var movedResult = (string)context.clusterTestUtils.Execute(
                primary0, "RI.GET", [riKey, "field_00000"],
                flags: CommandFlags.NoRedirect);
            ClassicAssert.IsTrue(movedResult.StartsWith("Key has MOVED to "),
                $"Expected MOVED from source, got: {movedResult}");
        }

        /// <summary>
        /// Test concurrent RI.SET writes and RI.GET reads simultaneously during migration.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        [Explicit("Reproduces a BfTree native crash (mini_page_op/circular_buffer panic) during cluster migration; crashes the test host. Tracked for RangeIndex crash/recovery.")]
        public async Task ClusterMigrateRangeIndexWhileReadingAndWritingAsync()
        {
            const int shards = 2;

            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(0);
            var primary1 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            var riKey = FindKeyOnNode(nameof(ClusterMigrateRangeIndexWhileReadingAndWritingAsync), primary0Id, slots);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            // Create RI key and seed with initial data
            var initialFields = Enumerable.Range(0, 50).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            CreateRangeIndexWithFields(primary0, riKey, initialFields);

            using var cts = new CancellationTokenSource();
            var written = new ConcurrentBag<(string Field, string Value)>();
            var readErrors = new ConcurrentBag<string>();

            // Background writer
            var writeTask = Task.Run(async () =>
            {
                await Task.Yield();

                using var con = ConnectionMultiplexer.Connect(context.clusterTestUtils.GetRedisConfig(context.endpoints));
                var db = con.GetDatabase();
                var ix = 1000;

                while (!cts.IsCancellationRequested)
                {
                    var field = $"w_field_{ix}";
                    var value = $"w_value_{ix}";

                    try
                    {
                        var result = (string)db.Execute("RI.SET", [new RedisKey(riKey), field, value]);
                        if (result == "OK")
                            written.Add((field, value));
                    }
                    catch (Exception exc) when (
                        exc is RedisTimeoutException
                        || exc is RedisConnectionException
                        || (exc is RedisServerException rse && (
                            rse.Message.StartsWith("MOVED ")
                            || rse.Message.StartsWith("Key has MOVED to "))))
                    {
                        continue;
                    }

                    ix++;
                }
            });

            // Background reader (reads initial fields)
            var readTask = Task.Run(async () =>
            {
                await Task.Yield();

                using var con = ConnectionMultiplexer.Connect(context.clusterTestUtils.GetRedisConfig(context.endpoints));
                var db = con.GetDatabase();

                while (!cts.IsCancellationRequested)
                {
                    var ix = Random.Shared.Next(initialFields.Count);
                    var (field, expectedValue) = initialFields[ix];

                    try
                    {
                        var result = (string)db.Execute("RI.GET", [new RedisKey(riKey), field]);
                        if (result != null && result != expectedValue)
                            readErrors.Add($"RI.GET {field}: expected '{expectedValue}', got '{result}'");
                    }
                    catch (Exception exc) when (
                        exc is RedisTimeoutException
                        || exc is RedisConnectionException
                        || (exc is RedisServerException rse && (
                            rse.Message.StartsWith("MOVED ")
                            || rse.Message.StartsWith("Key has MOVED to "))))
                    {
                        // Expected during migration
                    }
                }
            });

            await Task.Delay(1_000).ConfigureAwait(false);
            ClassicAssert.IsTrue(written.Count > 0, "Should have some writes before migration");

            // Migrate
            using (var migrateToken = new CancellationTokenSource())
            {
                migrateToken.CancelAfter(30_000);
                context.clusterTestUtils.MigrateSlots(primary0, primary1, [slot]);
                context.clusterTestUtils.WaitForMigrationCleanup(0, cancellationToken: migrateToken.Token);
                context.clusterTestUtils.WaitForMigrationCleanup(1, cancellationToken: migrateToken.Token);
            }

            WaitForSlotOwnership(primary0, primary1, slot);

            // Let reads + writes continue post-migration
            await Task.Delay(2_000).ConfigureAwait(false);

            cts.Cancel();
            await Task.WhenAll(writeTask, readTask).ConfigureAwait(false);

            ClassicAssert.IsEmpty(readErrors, $"Read value mismatches: {string.Join("; ", readErrors.Take(5))}");

            // Verify initial fields on target
            VerifyFieldsOnEndpoint(primary1, riKey, initialFields);

            // Verify we can still write on target after migration
            for (var i = 0; i < 5; i++)
            {
                var field = $"post_migrate_{i}";
                var value = $"post_value_{i}";
                var setRes = (string)context.clusterTestUtils.Execute(
                    primary1, "RI.SET", [riKey, field, value],
                    flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual("OK", setRes);

                var getRes = (string)context.clusterTestUtils.Execute(
                    primary1, "RI.GET", [riKey, field],
                    flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual(value, getRes);
            }
        }

#if DEBUG
        /// <summary>
        /// Slow-transmit test: pause before each transmit to widen the race window,
        /// while concurrent readers and writers are active.
        /// Verifies no deadlocks and data integrity.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        [Explicit("Reproduces a BfTree native crash (mini_page_op/circular_buffer panic) during cluster migration; crashes the test host. Tracked for RangeIndex crash/recovery.")]
        public async Task ClusterMigrateRangeIndexSlowTransmitWithConcurrentOpsAsync()
        {
            const int shards = 2;

            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(0);
            var primary1 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            var riKey = FindKeyOnNode(nameof(ClusterMigrateRangeIndexSlowTransmitWithConcurrentOpsAsync), primary0Id, slots);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            // Create RI key with data
            var fields = Enumerable.Range(0, 50).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            CreateRangeIndexWithFields(primary0, riKey, fields);

            using var cts = new CancellationTokenSource();
            var written = new ConcurrentBag<(string Field, string Value)>();
            var readErrors = new ConcurrentBag<string>();

            // Background writer
            var writeTask = Task.Run(async () =>
            {
                await Task.Yield();

                using var con = ConnectionMultiplexer.Connect(context.clusterTestUtils.GetRedisConfig(context.endpoints));
                var db = con.GetDatabase();
                var ix = 5000;

                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        var field = $"slow_w_{ix}";
                        var value = $"slow_v_{ix}";
                        var result = (string)db.Execute("RI.SET", [new RedisKey(riKey), field, value]);
                        if (result == "OK")
                            written.Add((field, value));
                    }
                    catch (Exception exc) when (
                        exc is RedisTimeoutException
                        || exc is RedisConnectionException
                        || (exc is RedisServerException rse && (
                            rse.Message.StartsWith("MOVED ")
                            || rse.Message.StartsWith("Key has MOVED to "))))
                    {
                        continue;
                    }

                    ix++;
                }
            });

            // Background reader
            var readTask = Task.Run(async () =>
            {
                await Task.Yield();

                using var con = ConnectionMultiplexer.Connect(context.clusterTestUtils.GetRedisConfig(context.endpoints));
                var db = con.GetDatabase();

                while (!cts.IsCancellationRequested)
                {
                    var ix = Random.Shared.Next(fields.Count);
                    var (field, expectedValue) = fields[ix];

                    try
                    {
                        var result = (string)db.Execute("RI.GET", [new RedisKey(riKey), field]);
                        if (result != null && result != expectedValue)
                            readErrors.Add($"RI.GET {field}: expected '{expectedValue}', got '{result}'");
                    }
                    catch (Exception exc) when (
                        exc is RedisTimeoutException
                        || exc is RedisConnectionException
                        || (exc is RedisServerException rse && (
                            rse.Message.StartsWith("MOVED ")
                            || rse.Message.StartsWith("Key has MOVED to "))))
                    {
                        // Expected during migration
                    }
                }
            });

            // Let readers + writers start up
            await Task.Delay(500).ConfigureAwait(false);

            // Arm pause hook to slow down the transmit phase
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.RangeIndex_Migration_After_Transmitting);

            try
            {
                var migrateTask = Task.Run(async () =>
                {
                    context.clusterTestUtils.MigrateSlots(primary0, primary1, [slot]);
                    await Task.CompletedTask;
                });

                // Wait for migration to hit the pause point
                var deadline = Stopwatch.GetTimestamp();
                while (ExceptionInjectionHelper.IsEnabled(ExceptionInjectionType.RangeIndex_Migration_After_Transmitting)
                       && Stopwatch.GetElapsedTime(deadline) < TimeSpan.FromSeconds(15))
                {
                    await Task.Delay(100).ConfigureAwait(false);
                }

                // Paused in TRANSMITTING — let concurrent ops run for a bit
                await Task.Delay(2_000).ConfigureAwait(false);

                // Resume
                ExceptionInjectionHelper.EnableException(ExceptionInjectionType.RangeIndex_Migration_After_Transmitting);

                await migrateTask.WaitAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false);
                context.clusterTestUtils.WaitForMigrationCleanup(logger: context.logger);

                WaitForSlotOwnership(primary0, primary1, slot);

                // Let ops run post-migration
                await Task.Delay(1_000).ConfigureAwait(false);

                cts.Cancel();
                await Task.WhenAll(writeTask, readTask).ConfigureAwait(false);

                ClassicAssert.IsEmpty(readErrors, $"Read mismatches: {string.Join("; ", readErrors.Take(5))}");

                // Verify initial fields on target
                VerifyFieldsOnEndpoint(primary1, riKey, fields);
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(ExceptionInjectionType.RangeIndex_Migration_After_Transmitting);
            }
        }
#endif
    }
}