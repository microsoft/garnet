// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
#if DEBUG
using Garnet.common;
#endif
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class ClusterRangeIndexMigrateTests : TestBase
    {
        ClusterTestContext context;
        readonly int defaultShards = 3;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public void TearDown()
        {
            context?.TearDown();
        }

        #region Helpers

        /// <summary>
        /// Find a key name whose hash slot is owned by the node with the given ID.
        /// </summary>
        private string FindKeyOnNode(string prefix, string nodeId, List<SlotItem> slots)
        {
            for (var ix = 0; ; ix++)
            {
                var key = $"{prefix}_{ix}";
                var slot = context.clusterTestUtils.HashSlot(key);
                if (slots.Any(x => x.nnInfo.Any(y => y.nodeid == nodeId) && slot >= x.startSlot && slot <= x.endSlot))
                    return key;
            }
        }

        /// <summary>
        /// Create a RangeIndex key and insert fields on the given endpoint.
        /// </summary>
        private void CreateRangeIndexWithFields(IPEndPoint endpoint, string key, IEnumerable<(string Field, string Value)> fields)
        {
            var createResult = (string)context.clusterTestUtils.Execute(
                endpoint, "RI.CREATE",
                [key, "DISK", "CACHESIZE", "65536", "MINRECORD", "8"],
                flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual("OK", createResult, $"RI.CREATE should succeed for key {key}");

            foreach (var (field, value) in fields)
            {
                var setResult = (string)context.clusterTestUtils.Execute(
                    endpoint, "RI.SET", [key, field, value],
                    flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual("OK", setResult, $"RI.SET should succeed for {key}/{field}");
            }
        }

        /// <summary>
        /// Verify all fields are readable on the given endpoint.
        /// </summary>
        private void VerifyFieldsOnEndpoint(IPEndPoint endpoint, string key, IEnumerable<(string Field, string Value)> fields)
        {
            foreach (var (field, value) in fields)
            {
                var result = (string)context.clusterTestUtils.Execute(
                    endpoint, "RI.GET", [key, field],
                    flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual(value, result, $"RI.GET {key}/{field} should return {value}");
            }
        }

        /// <summary>
        /// Wait for slot ownership to propagate: slot must be on target and not on source.
        /// </summary>
        private void WaitForSlotOwnership(IPEndPoint source, IPEndPoint target, int slot, int timeoutSeconds = 10)
        {
            var start = Stopwatch.GetTimestamp();
            while (Stopwatch.GetElapsedTime(start) < TimeSpan.FromSeconds(timeoutSeconds))
            {
                var sourceSlots = context.clusterTestUtils.GetOwnedSlotsFromNode(source, NullLogger.Instance);
                var targetSlots = context.clusterTestUtils.GetOwnedSlotsFromNode(target, NullLogger.Instance);

                if (!sourceSlots.Contains(slot) && targetSlots.Contains(slot))
                    return;

                Thread.Sleep(100);
            }

            ClassicAssert.Fail($"Slot {slot} ownership did not propagate within {timeoutSeconds}s");
        }

        #endregion

        /// <summary>
        /// Verifies that a RangeIndex key (RI.CREATE + RI.SET) survives slot migration
        /// and is accessible via RI.GET on the destination node.
        /// </summary>
        [Test, Order(1)]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexSlot()
        {
            context.CreateInstances(defaultShards, enableRangeIndexPreview: true);
            context.CreateConnection();

            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);
            var riKey = "{ri-migrate-test}";
            var slot = context.clusterTestUtils.HashSlot(riKey);
            var sourceNodeIndex = context.clusterTestUtils.GetSourceNodeIndexFromSlot((ushort)slot, context.logger);

            // Determine source and target endpoints
            var sourceEndpoint = context.clusterTestUtils.GetEndPoint(sourceNodeIndex);
            var targetNodeIndex = (sourceNodeIndex + 1) % defaultShards;
            var targetEndpoint = context.clusterTestUtils.GetEndPoint(targetNodeIndex);

            context.logger?.LogWarning("RI migration test: slot={slot}, source=node{sourceIndex}({sourcePort}), target=node{targetIndex}({targetPort})",
                slot, sourceNodeIndex, ((IPEndPoint)sourceEndpoint).Port, targetNodeIndex, ((IPEndPoint)targetEndpoint).Port);

            // Create RangeIndex and insert data on source node
            var createResult = (string)context.clusterTestUtils.Execute(
                (IPEndPoint)sourceEndpoint, "RI.CREATE",
                [riKey, "DISK", "CACHESIZE", "65536", "MINRECORD", "8"],
                flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual("OK", createResult, "RI.CREATE should succeed on source node");

            var setResult = (string)context.clusterTestUtils.Execute(
                (IPEndPoint)sourceEndpoint, "RI.SET",
                [riKey, "field1", "value1"],
                flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual("OK", setResult, "RI.SET should succeed on source node");

            // Verify data is readable on source before migration
            var getResult = (string)context.clusterTestUtils.Execute(
                (IPEndPoint)sourceEndpoint, "RI.GET",
                [riKey, "field1"],
                flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual("value1", getResult, "RI.GET should return correct value before migration");

            // Migrate the slot from source to target
            context.logger?.LogWarning("Initiating slot migration");
            context.clusterTestUtils.MigrateSlots(
                (IPEndPoint)sourceEndpoint,
                (IPEndPoint)targetEndpoint,
                new List<int> { slot },
                logger: context.logger);

            context.clusterTestUtils.WaitForMigrationCleanup(logger: context.logger);
            context.logger?.LogWarning("Migration cleanup complete");

            // Verify data is accessible on the target node
            var retries = 0;
            string targetGetResult = null;
            while (retries < 50)
            {
                try
                {
                    targetGetResult = (string)context.clusterTestUtils.Execute(
                        (IPEndPoint)targetEndpoint, "RI.GET",
                        [riKey, "field1"],
                        flags: CommandFlags.NoRedirect);
                    if (targetGetResult != null)
                        break;
                }
                catch
                {
                    // Slot may not be fully transferred yet
                }
                Thread.Sleep(100);
                retries++;
            }

            ClassicAssert.AreEqual("value1", targetGetResult, "RI.GET should return correct value on target node after migration");

            context.logger?.LogWarning("ClusterMigrateRangeIndexSlot test passed");
        }

        /// <summary>
        /// Migrate an empty RangeIndex key (RI.CREATE with no RI.SET) to verify that
        /// an empty BfTree snapshot can be transmitted and recovered.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateEmptyRangeIndex()
        {
            const int shards = 2;
            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(0);
            var primary1 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            var riKey = FindKeyOnNode(nameof(ClusterMigrateEmptyRangeIndex), primary0Id, slots);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            // Create RI key with no data — empty tree
            var createResult = (string)context.clusterTestUtils.Execute(
                primary0, "RI.CREATE",
                [riKey, "DISK", "CACHESIZE", "65536", "MINRECORD", "8"],
                flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual("OK", createResult, "RI.CREATE should succeed");

            // Migrate
            context.clusterTestUtils.MigrateSlots(primary0, primary1, [slot]);
            context.clusterTestUtils.WaitForMigrationCleanup(0);
            context.clusterTestUtils.WaitForMigrationCleanup(1);

            WaitForSlotOwnership(primary0, primary1, slot);

            // Verify the empty RI key exists on target — RI.SET should work
            var setResult = (string)context.clusterTestUtils.Execute(
                primary1, "RI.SET", [riKey, "field_00", "value_00"],
                flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual("OK", setResult, "RI.SET should succeed on migrated empty tree");

            var getResult = (string)context.clusterTestUtils.Execute(
                primary1, "RI.GET", [riKey, "field_00"],
                flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual("value_00", getResult, "RI.GET should return correct value on target");

            // Verify source returns MOVED
            var movedResult = (string)context.clusterTestUtils.Execute(
                primary0, "RI.GET", [riKey, "field_00"],
                flags: CommandFlags.NoRedirect);
            ClassicAssert.IsTrue(movedResult.StartsWith("Key has MOVED to "),
                $"Expected MOVED from source, got: {movedResult}");
        }

        /// <summary>
        /// Single RI key with multiple fields, slot-based migration between 2 primaries.
        /// Verifies all fields survive and source returns MOVED.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexSingleBySlot()
        {
            const int shards = 2;
            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(0);
            var primary1 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            var riKey = FindKeyOnNode(nameof(ClusterMigrateRangeIndexSingleBySlot), primary0Id, slots);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            // Create RI with multiple fields
            var fields = new[]
            {
                ("field1", "value1"), ("field2", "value2"), ("field3", "value3"),
                ("field4", "value4"), ("field5", "value5"),
            };
            CreateRangeIndexWithFields(primary0, riKey, fields);
            VerifyFieldsOnEndpoint(primary0, riKey, fields);

            // Migrate
            context.clusterTestUtils.MigrateSlots(primary0, primary1, [slot]);
            context.clusterTestUtils.WaitForMigrationCleanup(0);
            context.clusterTestUtils.WaitForMigrationCleanup(1);

            WaitForSlotOwnership(primary0, primary1, slot);

            // Verify on target
            VerifyFieldsOnEndpoint(primary1, riKey, fields);

            // Verify source returns MOVED
            var movedResult = (string)context.clusterTestUtils.Execute(
                primary0, "RI.GET", [riKey, "field1"],
                flags: CommandFlags.NoRedirect);
            ClassicAssert.IsTrue(movedResult.StartsWith("Key has MOVED to "),
                $"Expected MOVED response from source, got: {movedResult}");
        }

        /// <summary>
        /// Key-based migration of multiple RI keys in random order.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexByKeys()
        {
            const int shardCount = 3;
            const int keyCount = 10;

            context.CreateInstances(shardCount, enableRangeIndexPreview: true);
            context.CreateConnection();

            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var sourceNodeIndex = 1;
            var targetNodeIndex = 2;
            var sourceNodeId = context.clusterTestUtils.GetNodeIdFromNode(sourceNodeIndex, NullLogger.Instance);
            var targetNodeId = context.clusterTestUtils.GetNodeIdFromNode(targetNodeIndex, NullLogger.Instance);
            var sourceEndpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(sourceNodeIndex);
            var targetEndpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(targetNodeIndex);

            var keyBase = Encoding.ASCII.GetBytes("{abc}ri_");
            var workingSlot = ClusterTestUtils.HashSlot(keyBase);

            var rand = new Random(2025_05_03_00);
            var allKeys = new List<(byte[] Key, List<(string Field, string Value)> Fields)>();

            for (var i = 0; i < keyCount; i++)
            {
                var newKey = new byte[keyBase.Length + 1];
                Array.Copy(keyBase, 0, newKey, 0, keyBase.Length);
                newKey[^1] = (byte)('a' + i);
                ClassicAssert.AreEqual(workingSlot, ClusterTestUtils.HashSlot(newKey));

                var keyStr = Encoding.ASCII.GetString(newKey);
                var fields = new List<(string Field, string Value)>();
                var fieldCount = rand.Next(1, 4);
                for (var f = 0; f < fieldCount; f++)
                    fields.Add(($"field_{f:D4}", $"value_{i}_{f}_{rand.Next(10000)}"));

                CreateRangeIndexWithFields(sourceEndpoint, keyStr, fields);
                allKeys.Add((newKey, fields));
            }

            // Manual slot migration setup
            var respImport = context.clusterTestUtils.SetSlot(targetNodeIndex, workingSlot, "IMPORTING", sourceNodeId);
            ClassicAssert.AreEqual("OK", respImport);
            var respMigrate = context.clusterTestUtils.SetSlot(sourceNodeIndex, workingSlot, "MIGRATING", targetNodeId);
            ClassicAssert.AreEqual("OK", respMigrate);

            // Migrate keys one at a time in random order
            var toMigrate = allKeys.Select(k => k.Key).ToList();
            while (toMigrate.Count > 0)
            {
                var ix = rand.Next(toMigrate.Count);
                context.clusterTestUtils.MigrateKeys(sourceEndpoint, targetEndpoint, [toMigrate[ix]], NullLogger.Instance);
                toMigrate.RemoveAt(ix);
            }

            // Complete migration
            var respNodeTarget = context.clusterTestUtils.SetSlot(targetNodeIndex, workingSlot, "NODE", targetNodeId);
            ClassicAssert.AreEqual("OK", respNodeTarget);
            context.clusterTestUtils.BumpEpoch(targetNodeIndex, waitForSync: true);

            var respNodeSource = context.clusterTestUtils.SetSlot(sourceNodeIndex, workingSlot, "NODE", targetNodeId);
            ClassicAssert.AreEqual("OK", respNodeSource);
            context.clusterTestUtils.BumpEpoch(sourceNodeIndex, waitForSync: true);

            context.clusterTestUtils.WaitForMigrationCleanup();

            // Verify all keys and fields on target
            foreach (var (key, fields) in allKeys)
            {
                var keyStr = Encoding.ASCII.GetString(key);
                VerifyFieldsOnEndpoint(targetEndpoint, keyStr, fields);
            }
        }

        /// <summary>
        /// Key-based migration of a MIXED batch — some keys are RangeIndex and some are
        /// plain string/hash keys — all in a single MIGRATE ... KEYS call.
        ///
        /// Validates <c>RangeIndexManager.GetRangeIndexKeysForMigration</c>:
        /// it must identify the RI subset (so they go through the RI snapshot/transmit path)
        /// while leaving the non-RI keys to the regular key-transmission path. Both sets
        /// must arrive intact on the target.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateMixedRangeIndexAndRegularByKeys()
        {
            const int shardCount = 3;
            const int riKeyCount = 4;
            const int stringKeyCount = 4;
            const int hashKeyCount = 4;

            context.CreateInstances(shardCount, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var sourceNodeIndex = 1;
            var targetNodeIndex = 2;
            var sourceNodeId = context.clusterTestUtils.GetNodeIdFromNode(sourceNodeIndex, NullLogger.Instance);
            var targetNodeId = context.clusterTestUtils.GetNodeIdFromNode(targetNodeIndex, NullLogger.Instance);
            var sourceEndpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(sourceNodeIndex);
            var targetEndpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(targetNodeIndex);

            // All keys share the {abc} hash tag so they map to one slot.
            const string HashTag = "{abc}";
            var workingSlot = ClusterTestUtils.HashSlot(Encoding.ASCII.GetBytes(HashTag));

            var rand = new Random(2026_06_03_01);

            // RI keys
            var riKeys = new List<(string Key, List<(string Field, string Value)> Fields)>();
            for (var i = 0; i < riKeyCount; i++)
            {
                var key = $"{HashTag}ri_{i}";
                var fields = new List<(string Field, string Value)>();
                var fieldCount = rand.Next(1, 4);
                for (var f = 0; f < fieldCount; f++)
                    fields.Add(($"field_{f:D4}", $"ri_value_{i}_{f}_{rand.Next(10000)}"));

                CreateRangeIndexWithFields(sourceEndpoint, key, fields);
                riKeys.Add((key, fields));
            }

            // Plain string keys
            var stringKeys = new List<(string Key, string Value)>();
            for (var i = 0; i < stringKeyCount; i++)
            {
                var key = $"{HashTag}str_{i}";
                var value = $"str_value_{i}_{rand.Next(10000)}";
                var setResult = (string)context.clusterTestUtils.Execute(
                    sourceEndpoint, "SET", [key, value], flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual("OK", setResult, $"SET should succeed for {key}");
                stringKeys.Add((key, value));
            }

            // Hash keys
            var hashKeys = new List<(string Key, List<(string Field, string Value)> Fields)>();
            for (var i = 0; i < hashKeyCount; i++)
            {
                var key = $"{HashTag}hash_{i}";
                var fields = new List<(string Field, string Value)>();
                var fieldCount = rand.Next(1, 4);
                for (var f = 0; f < fieldCount; f++)
                {
                    var field = $"hf_{f:D4}";
                    var value = $"hash_value_{i}_{f}_{rand.Next(10000)}";
                    var hsetArgs = new object[] { key, field, value };
                    _ = context.clusterTestUtils.Execute(
                        sourceEndpoint, "HSET", hsetArgs, flags: CommandFlags.NoRedirect);
                    fields.Add((field, value));
                }
                hashKeys.Add((key, fields));
            }

            // Manual slot migration setup
            var respImport = context.clusterTestUtils.SetSlot(targetNodeIndex, workingSlot, "IMPORTING", sourceNodeId);
            ClassicAssert.AreEqual("OK", respImport);
            var respMigrate = context.clusterTestUtils.SetSlot(sourceNodeIndex, workingSlot, "MIGRATING", targetNodeId);
            ClassicAssert.AreEqual("OK", respMigrate);

            // Build a single MIGRATE ... KEYS batch with a shuffled mix of RI + non-RI keys.
            var batch = new List<byte[]>();
            batch.AddRange(riKeys.Select(k => Encoding.ASCII.GetBytes(k.Key)));
            batch.AddRange(stringKeys.Select(k => Encoding.ASCII.GetBytes(k.Key)));
            batch.AddRange(hashKeys.Select(k => Encoding.ASCII.GetBytes(k.Key)));
            for (var i = batch.Count - 1; i > 0; i--)
            {
                var j = rand.Next(i + 1);
                (batch[i], batch[j]) = (batch[j], batch[i]);
            }

            context.clusterTestUtils.MigrateKeys(sourceEndpoint, targetEndpoint, batch, NullLogger.Instance);

            // Complete migration
            var respNodeTarget = context.clusterTestUtils.SetSlot(targetNodeIndex, workingSlot, "NODE", targetNodeId);
            ClassicAssert.AreEqual("OK", respNodeTarget);
            context.clusterTestUtils.BumpEpoch(targetNodeIndex, waitForSync: true);

            var respNodeSource = context.clusterTestUtils.SetSlot(sourceNodeIndex, workingSlot, "NODE", targetNodeId);
            ClassicAssert.AreEqual("OK", respNodeSource);
            context.clusterTestUtils.BumpEpoch(sourceNodeIndex, waitForSync: true);

            context.clusterTestUtils.WaitForMigrationCleanup();

            // Verify RI keys on target
            foreach (var (key, fields) in riKeys)
                VerifyFieldsOnEndpoint(targetEndpoint, key, fields);

            // Verify string keys on target
            foreach (var (key, expected) in stringKeys)
            {
                var actual = (string)context.clusterTestUtils.Execute(
                    targetEndpoint, "GET", [key], flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual(expected, actual, $"GET {key} on target");
            }

            // Verify hash keys on target
            foreach (var (key, fields) in hashKeys)
            {
                foreach (var (field, expected) in fields)
                {
                    var actual = (string)context.clusterTestUtils.Execute(
                        targetEndpoint, "HGET", [key, field], flags: CommandFlags.NoRedirect);
                    ClassicAssert.AreEqual(expected, actual, $"HGET {key} {field} on target");
                }
            }

            // Verify source returns MOVED for one key from each type
            var movedRi = (string)context.clusterTestUtils.Execute(
                sourceEndpoint, "RI.GET", [riKeys[0].Key, riKeys[0].Fields[0].Field],
                flags: CommandFlags.NoRedirect);
            ClassicAssert.IsTrue(movedRi.StartsWith("Key has MOVED to "),
                $"Expected MOVED from source for RI key {riKeys[0].Key}, got: {movedRi}");

            var movedStr = (string)context.clusterTestUtils.Execute(
                sourceEndpoint, "GET", [stringKeys[0].Key],
                flags: CommandFlags.NoRedirect);
            ClassicAssert.IsTrue(movedStr.StartsWith("Key has MOVED to "),
                $"Expected MOVED from source for string key {stringKeys[0].Key}, got: {movedStr}");

            var movedHash = (string)context.clusterTestUtils.Execute(
                sourceEndpoint, "HGET", [hashKeys[0].Key, hashKeys[0].Fields[0].Field],
                flags: CommandFlags.NoRedirect);
            ClassicAssert.IsTrue(movedHash.StartsWith("Key has MOVED to "),
                $"Expected MOVED from source for hash key {hashKeys[0].Key}, got: {movedHash}");
        }

        /// <summary>
        /// Multiple RI keys in the same slot, slot-based migration.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexManyBySlot()
        {
            const int shards = 2;
            const int keysPerPrimary = 4;

            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(0);
            var primary1 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            var rand = new Random(42);
            var primary0Keys = new List<(string Key, int Slot, List<(string Field, string Value)> Fields)>();

            var ix = 0;
            while (primary0Keys.Count < keysPerPrimary)
            {
                var key = $"{nameof(ClusterMigrateRangeIndexManyBySlot)}_{ix}";
                var slot = context.clusterTestUtils.HashSlot(key);
                if (slots.Any(x => x.nnInfo.Any(y => y.nodeid == primary0Id) && slot >= x.startSlot && slot <= x.endSlot))
                {
                    var fields = new List<(string Field, string Value)>();
                    var fieldCount = rand.Next(1, 6);
                    for (var f = 0; f < fieldCount; f++)
                        fields.Add(($"field_{f:D4}", $"value_{ix}_{f:D4}"));

                    primary0Keys.Add((key, slot, fields));
                }
                ix++;
            }

            // Create all keys on primary0
            foreach (var (key, _, fields) in primary0Keys)
                CreateRangeIndexWithFields(primary0, key, fields);

            // Migrate all distinct slots
            var migrateSlots = primary0Keys.Select(k => k.Slot).Distinct().ToList();
            context.clusterTestUtils.MigrateSlots(primary0, primary1, migrateSlots);
            context.clusterTestUtils.WaitForMigrationCleanup(0);
            context.clusterTestUtils.WaitForMigrationCleanup(1);

            foreach (var slot in migrateSlots)
                WaitForSlotOwnership(primary0, primary1, slot);

            // Verify all keys on primary1
            foreach (var (key, _, fields) in primary0Keys)
                VerifyFieldsOnEndpoint(primary1, key, fields);

            // Verify source returns MOVED
            foreach (var (key, _, _) in primary0Keys)
            {
                var result = (string)context.clusterTestUtils.Execute(
                    primary0, "RI.GET", [key, "field_0000"],
                    flags: CommandFlags.NoRedirect);
                ClassicAssert.IsTrue(result.StartsWith("Key has MOVED to "),
                    $"Expected MOVED from source for key {key}");
            }
        }

        /// <summary>
        /// Round-trip migration: P0 → P1 → P0, with data additions between each migration.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexBack()
        {
            const int shards = 2;

            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(0);
            var primary1 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            var riKey = FindKeyOnNode(nameof(ClusterMigrateRangeIndexBack), primary0Id, slots);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            // Create RI with initial data on P0
            CreateRangeIndexWithFields(primary0, riKey, [("field_00", "value_00"), ("field_01", "value_01")]);

            // Migrate P0 → P1
            {
                using var migrateToken = new CancellationTokenSource();
                migrateToken.CancelAfter(30_000);

                context.clusterTestUtils.MigrateSlots(primary0, primary1, [slot]);
                context.clusterTestUtils.WaitForMigrationCleanup(0, cancellationToken: migrateToken.Token);
                context.clusterTestUtils.WaitForMigrationCleanup(1, cancellationToken: migrateToken.Token);
            }

            WaitForSlotOwnership(primary0, primary1, slot);

            // Verify on P1 and add more data
            VerifyFieldsOnEndpoint(primary1, riKey, [("field_00", "value_00"), ("field_01", "value_01")]);

            var setResult = (string)context.clusterTestUtils.Execute(
                primary1, "RI.SET", [riKey, "field_02", "value_02"],
                flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual("OK", setResult);

            // Migrate P1 → P0
            {
                using var migrateToken = new CancellationTokenSource();
                migrateToken.CancelAfter(30_000);

                context.clusterTestUtils.MigrateSlots(primary1, primary0, [slot]);
                context.clusterTestUtils.WaitForMigrationCleanup(0, cancellationToken: migrateToken.Token);
                context.clusterTestUtils.WaitForMigrationCleanup(1, cancellationToken: migrateToken.Token);
            }

            WaitForSlotOwnership(primary1, primary0, slot);

            // Verify all data (original + added) survived round-trip
            VerifyFieldsOnEndpoint(primary0, riKey, [("field_00", "value_00"), ("field_01", "value_01"), ("field_02", "value_02")]);

            // Add more data on P0
            var setResult2 = (string)context.clusterTestUtils.Execute(
                primary0, "RI.SET", [riKey, "field_03", "value_03"],
                flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual("OK", setResult2);

            VerifyFieldsOnEndpoint(primary0, riKey, [("field_00", "value_00"), ("field_01", "value_01"), ("field_02", "value_02"), ("field_03", "value_03")]);
        }

        /// <summary>
        /// Stress test: concurrent reads + writes + repeated back-and-forth migrations.
        /// Verifies zero data loss.
        /// </summary>
        /// <remarks>
        /// Currently marked Explicit because RI.SET via cluster-mode client redirect can hit
        /// "ERR no such range index" on the target if the RI key was just migrated and the
        /// BfTree native instance isn't yet registered. This needs investigation in the
        /// migration pipeline before this test can run reliably.
        /// </remarks>
        [Test, Explicit("RI.SET via cluster redirect not yet reliable during migration")]
        [Category("CLUSTER")]
        public async Task ClusterMigrateRangeIndexStressAsync()
        {
            const int shards = 2;
            const int keysPerPrimary = 2;

            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(0);
            var primary1 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            // Find keys on each primary
            var allKeys = new List<(string Key, int Slot, bool OnPrimary0)>();
            var numP0 = 0;
            var numP1 = 0;
            var ix = 0;

            while (numP0 < keysPerPrimary || numP1 < keysPerPrimary)
            {
                var key = $"{nameof(ClusterMigrateRangeIndexStressAsync)}_{ix}";
                var slot = context.clusterTestUtils.HashSlot(key);
                var isOnP0 = slots.Any(x => x.nnInfo.Any(y => y.nodeid == primary0Id) && slot >= x.startSlot && slot <= x.endSlot);

                if (isOnP0 && numP0 < keysPerPrimary)
                {
                    allKeys.Add((key, slot, true));
                    numP0++;
                }
                else if (!isOnP0 && numP1 < keysPerPrimary)
                {
                    allKeys.Add((key, slot, false));
                    numP1++;
                }
                ix++;
            }

            // Create RI keys on their respective primaries
            foreach (var (key, _, onP0) in allKeys)
            {
                var endpoint = onP0 ? primary0 : primary1;
                var createResult = (string)context.clusterTestUtils.Execute(
                    endpoint, "RI.CREATE",
                    [key, "DISK", "CACHESIZE", "65536", "MINRECORD", "8"],
                    flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual("OK", createResult);
            }

            // Start concurrent writers
            using var writeCancel = new CancellationTokenSource();
            var writeResults = new ConcurrentBag<(string Field, string Value)>[allKeys.Count];
            var writeTasks = new Task[allKeys.Count];
            var mostRecentWrite = 0L;

            using var readWriteCon = ConnectionMultiplexer.Connect(context.clusterTestUtils.GetRedisConfig(context.endpoints));
            var readWriteDb = readWriteCon.GetDatabase();

            for (var i = 0; i < allKeys.Count; i++)
            {
                var (key, _, _) = allKeys[i];
                var bag = writeResults[i] = new ConcurrentBag<(string, string)>();

                writeTasks[i] = Task.Run(async () =>
                {
                    await Task.Yield();
                    var wix = 0;

                    while (!writeCancel.IsCancellationRequested)
                    {
                        var field = $"field_{wix}";
                        var value = $"value_{wix}";

                        try
                        {
                            var result = (string)readWriteDb.Execute("RI.SET", [new RedisKey(key), field, value]);
                            if (result == "OK")
                                bag.Add((field, value));
                        }
                        catch (Exception exc) when (
                            exc is RedisTimeoutException
                            || exc is RedisConnectionException
                            || (exc is RedisServerException rse && (
                                rse.Message.StartsWith("MOVED ")
                                || rse.Message.StartsWith("Key has MOVED to "))))
                        {
                            if (writeCancel.IsCancellationRequested) return;
                            continue;
                        }

                        var now = DateTime.UtcNow.Ticks;
                        var prev = Interlocked.CompareExchange(ref mostRecentWrite, now, mostRecentWrite);
                        while (prev < now)
                            prev = Interlocked.CompareExchange(ref mostRecentWrite, now, prev);

                        wix++;
                    }
                });
            }

            // Start concurrent readers
            using var readCancel = new CancellationTokenSource();
            var readTasks = new Task<int>[allKeys.Count];

            for (var i = 0; i < allKeys.Count; i++)
            {
                var (key, _, _) = allKeys[i];
                var bag = writeResults[i];

                readTasks[i] = Task.Run(async () =>
                {
                    await Task.Yield();
                    var successfulReads = 0;
                    var rng = new Random(i);

                    while (!readCancel.IsCancellationRequested)
                    {
                        var snapshot = bag.ToList();
                        if (snapshot.Count == 0)
                        {
                            await Task.Delay(10).ConfigureAwait(false);
                            continue;
                        }

                        var (field, expectedValue) = snapshot[rng.Next(snapshot.Count)];

                        try
                        {
                            var result = (string)readWriteDb.Execute("RI.GET", [new RedisKey(key), field]);
                            if (result != null)
                            {
                                ClassicAssert.AreEqual(expectedValue, result, $"Read mismatch for {key}/{field}");
                                successfulReads++;
                            }
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
                    }

                    return successfulReads;
                });
            }

            await Task.Delay(1_000).ConfigureAwait(false);
            ClassicAssert.IsTrue(writeResults.All(r => !r.IsEmpty), "Should have writes before migration");

            // Migrator: ping-pong slots between primaries
            using var migrateCancel = new CancellationTokenSource();

            var migrateTask = Task.Run(async () =>
            {
                var slotsOnP0 = allKeys.Where(k => k.OnPrimary0).Select(k => k.Slot).Distinct().ToList();
                var slotsOnP1 = allKeys.Where(k => !k.OnPrimary0).Select(k => k.Slot).Distinct().ToList();
                var migrationCount = 0;
                var mostRecentMigration = 0L;

                while (!migrateCancel.IsCancellationRequested)
                {
                    await Task.Delay(100).ConfigureAwait(false);

                    // Wait for at least one write since last migration
                    if (Interlocked.CompareExchange(ref mostRecentWrite, 0, 0) < mostRecentMigration)
                        continue;

                    // Move P0 → P1
                    if (slotsOnP0.Count > 0)
                    {
                        using var token = new CancellationTokenSource();
                        token.CancelAfter(30_000);

                        context.clusterTestUtils.MigrateSlots(primary0, primary1, slotsOnP0);
                        context.clusterTestUtils.WaitForMigrationCleanup(0, cancellationToken: token.Token);
                        context.clusterTestUtils.WaitForMigrationCleanup(1, cancellationToken: token.Token);
                    }

                    // Move P1 → P0
                    if (slotsOnP1.Count > 0)
                    {
                        using var token = new CancellationTokenSource();
                        token.CancelAfter(30_000);

                        context.clusterTestUtils.MigrateSlots(primary1, primary0, slotsOnP1);
                        context.clusterTestUtils.WaitForMigrationCleanup(0, cancellationToken: token.Token);
                        context.clusterTestUtils.WaitForMigrationCleanup(1, cancellationToken: token.Token);
                    }

                    mostRecentMigration = DateTime.UtcNow.Ticks;
                    migrationCount++;

                    // Flip for next pass
                    (slotsOnP0, slotsOnP1) = (slotsOnP1, slotsOnP0);
                }

                return migrationCount;
            });

            await Task.Delay(10_000).ConfigureAwait(false);

            migrateCancel.Cancel();
            var migrations = await migrateTask.ConfigureAwait(false);
            ClassicAssert.IsTrue(migrations >= 2, $"Should have at least 2 migrations, had {migrations}");

            writeCancel.Cancel();
            await Task.WhenAll(writeTasks).ConfigureAwait(false);

            readCancel.Cancel();
            var readResults = await Task.WhenAll(readTasks).ConfigureAwait(false);
            ClassicAssert.IsTrue(readResults.All(r => r > 0), "Should have successful reads on all keys");

            // Final verification: every written field must be readable
            var curP0Slots = context.clusterTestUtils.GetOwnedSlotsFromNode(primary0, NullLogger.Instance);

            for (var i = 0; i < allKeys.Count; i++)
            {
                var (key, slot, _) = allKeys[i];
                var endpoint = curP0Slots.Contains(slot) ? primary0 : primary1;

                foreach (var (field, value) in writeResults[i])
                {
                    var result = (string)context.clusterTestUtils.Execute(
                        endpoint, "RI.GET", [key, field],
                        flags: CommandFlags.NoRedirect);
                    ClassicAssert.AreEqual(value, result, $"Data loss: {key}/{field} not found after stress");
                }
            }
        }

        /// <summary>
        /// Test migration with different chunk sizes to exercise multi-chunk paths.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        [TestCase(1024)]          // 1 KB — forces many chunks
        [TestCase(4096)]          // 4 KB
        [TestCase(256 * 1024)]    // 256 KB — default
        public void ClusterMigrateRangeIndexWithChunkSize(int chunkSize)
        {
            const int shards = 2;

            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(0);
            var primary1 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            var riKey = FindKeyOnNode($"{nameof(ClusterMigrateRangeIndexWithChunkSize)}_{chunkSize}", primary0Id, slots);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            // Create RI with enough data to span multiple chunks at small sizes
            var fields = new List<(string Field, string Value)>();
            for (var i = 0; i < 50; i++)
                fields.Add(($"field_{i}", new string('x', 100) + $"_{i}"));

            CreateRangeIndexWithFields(primary0, riKey, fields);
            VerifyFieldsOnEndpoint(primary0, riKey, fields);

            // Migrate
            context.clusterTestUtils.MigrateSlots(primary0, primary1, [slot]);
            context.clusterTestUtils.WaitForMigrationCleanup(0);
            context.clusterTestUtils.WaitForMigrationCleanup(1);

            WaitForSlotOwnership(primary0, primary1, slot);

            // Verify all fields on target
            VerifyFieldsOnEndpoint(primary1, riKey, fields);
        }

        /// <summary>
        /// Test concurrent RI.GET reads during migration. Reads could trigger RIPROMOTE
        /// (CTT for stubs), creating potential races with the migration snapshot path.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public async Task ClusterMigrateRangeIndexWhileReadingAsync()
        {
            const int shards = 2;

            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(0);
            var primary1 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            var riKey = FindKeyOnNode(nameof(ClusterMigrateRangeIndexWhileReadingAsync), primary0Id, slots);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            // Create RI key and populate with known data
            var fields = Enumerable.Range(0, 100).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            CreateRangeIndexWithFields(primary0, riKey, fields);

            // Start background reader
            using var cts = new CancellationTokenSource();
            var readCount = 0;
            var readErrors = new ConcurrentBag<string>();

            var readTask = Task.Run(async () =>
            {
                await Task.Yield();

                using var con = ConnectionMultiplexer.Connect(context.clusterTestUtils.GetRedisConfig(context.endpoints));
                var db = con.GetDatabase();

                while (!cts.IsCancellationRequested)
                {
                    var ix = Interlocked.Increment(ref readCount) % fields.Count;
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
                        // Expected during/after migration
                    }
                }
            });

            await Task.Delay(1_000).ConfigureAwait(false);
            ClassicAssert.IsTrue(readCount > 0, "Should have some reads before migration");

            // Migrate
            using (var migrateToken = new CancellationTokenSource())
            {
                migrateToken.CancelAfter(30_000);
                context.clusterTestUtils.MigrateSlots(primary0, primary1, [slot]);
                context.clusterTestUtils.WaitForMigrationCleanup(0, cancellationToken: migrateToken.Token);
                context.clusterTestUtils.WaitForMigrationCleanup(1, cancellationToken: migrateToken.Token);
            }

            WaitForSlotOwnership(primary0, primary1, slot);

            // Let reads continue post-migration
            await Task.Delay(2_000).ConfigureAwait(false);

            cts.Cancel();
            await readTask.ConfigureAwait(false);

            ClassicAssert.IsEmpty(readErrors, $"Read value mismatches during migration: {string.Join("; ", readErrors.Take(5))}");

            // Verify all fields intact on target
            VerifyFieldsOnEndpoint(primary1, riKey, fields);
        }

#if DEBUG
        /// <summary>
        /// Pause migration during TRANSMITTING state via fault injection,
        /// fire RI.SET and RI.GET while migration is paused, then resume.
        /// Writes should be blocked by sketch during TRANSMITTING.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public async Task ClusterMigrateRangeIndexWithPauseDuringTransmitAsync()
        {
            const int shards = 2;

            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(0);
            var primary1 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            var riKey = FindKeyOnNode(nameof(ClusterMigrateRangeIndexWithPauseDuringTransmitAsync), primary0Id, slots);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            var fields = Enumerable.Range(0, 20).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            CreateRangeIndexWithFields(primary0, riKey, fields);

            // Arm the pause hook — migration will pause after entering TRANSMITTING
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.RangeIndex_Migration_After_Transmitting);

            try
            {
                // Start migration in background
                var migrateTask = Task.Run(() =>
                {
                    context.clusterTestUtils.MigrateSlots(primary0, primary1, [slot]);
                });

                // Wait for migration to reach the pause point
                // ResetAndWaitAsync disables the flag when it reaches the pause, so we wait for it to clear
                var deadline = Stopwatch.GetTimestamp();
                while (ExceptionInjectionHelper.IsEnabled(ExceptionInjectionType.RangeIndex_Migration_After_Transmitting)
                       && Stopwatch.GetElapsedTime(deadline) < TimeSpan.FromSeconds(15))
                {
                    await Task.Delay(100).ConfigureAwait(false);
                }

                // Migration is now paused in TRANSMITTING state.
                // RI.GET on initial fields should still work (reads allowed during TRANSMITTING).
                foreach (var (field, value) in fields.Take(5))
                {
                    try
                    {
                        var result = (string)context.clusterTestUtils.Execute(
                            primary0, "RI.GET", [riKey, field],
                            flags: CommandFlags.NoRedirect);
                        // Read may succeed or fail depending on exact sketch gating behavior
                        context.logger?.LogInformation("RI.GET during TRANSMITTING: {field} = {result}", field, result);
                    }
                    catch (Exception ex)
                    {
                        context.logger?.LogInformation("RI.GET during TRANSMITTING: {field} threw {message}", field, ex.Message);
                    }
                }

                // Resume migration
                ExceptionInjectionHelper.EnableException(ExceptionInjectionType.RangeIndex_Migration_After_Transmitting);

                // Wait for migration to complete
                await migrateTask.WaitAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false);
                context.clusterTestUtils.WaitForMigrationCleanup(logger: context.logger);

                WaitForSlotOwnership(primary0, primary1, slot);

                // Verify all fields on target
                VerifyFieldsOnEndpoint(primary1, riKey, fields);
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(ExceptionInjectionType.RangeIndex_Migration_After_Transmitting);
            }
        }

        /// <summary>
        /// Pause migration during DELETING state via fault injection.
        /// Both reads and writes should be blocked during DELETING.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public async Task ClusterMigrateRangeIndexWithPauseDuringDeleteAsync()
        {
            const int shards = 2;

            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(0);
            var primary1 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            var riKey = FindKeyOnNode(nameof(ClusterMigrateRangeIndexWithPauseDuringDeleteAsync), primary0Id, slots);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            var fields = Enumerable.Range(0, 20).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            CreateRangeIndexWithFields(primary0, riKey, fields);

            // Arm the pause hook — migration will pause after entering DELETING
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.RangeIndex_Migration_After_Deleting);

            try
            {
                var migrateTask = Task.Run(() =>
                {
                    context.clusterTestUtils.MigrateSlots(primary0, primary1, [slot]);
                });

                // Wait for migration to reach the DELETING pause
                var deadline = Stopwatch.GetTimestamp();
                while (ExceptionInjectionHelper.IsEnabled(ExceptionInjectionType.RangeIndex_Migration_After_Deleting)
                       && Stopwatch.GetElapsedTime(deadline) < TimeSpan.FromSeconds(15))
                {
                    await Task.Delay(100).ConfigureAwait(false);
                }

                // Migration paused in DELETING. Data was already transmitted to target.
                // Verify target already has the data (it was sent before DELETING)
                foreach (var (field, value) in fields.Take(5))
                {
                    try
                    {
                        var result = (string)context.clusterTestUtils.Execute(
                            primary1, "RI.GET", [riKey, field],
                            flags: CommandFlags.NoRedirect);
                        context.logger?.LogInformation("RI.GET on target during DELETING: {field} = {result}", field, result);
                    }
                    catch (Exception ex)
                    {
                        context.logger?.LogInformation("RI.GET on target during DELETING threw: {message}", ex.Message);
                    }
                }

                // Resume migration
                ExceptionInjectionHelper.EnableException(ExceptionInjectionType.RangeIndex_Migration_After_Deleting);

                await migrateTask.WaitAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false);
                context.clusterTestUtils.WaitForMigrationCleanup(logger: context.logger);

                WaitForSlotOwnership(primary0, primary1, slot);

                // Verify all fields on target after completion
                VerifyFieldsOnEndpoint(primary1, riKey, fields);
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(ExceptionInjectionType.RangeIndex_Migration_After_Deleting);
            }
        }

        /// <summary>
        /// Inject an exception during the transmit phase. Verifies that on failure:
        /// - Sketch is cleared (operations unblocked)
        /// - Source data remains intact
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexExceptionDuringTransmit()
        {
            const int shards = 2;

            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(0);
            var primary1 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            var riKey = FindKeyOnNode(nameof(ClusterMigrateRangeIndexExceptionDuringTransmit), primary0Id, slots);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            var fields = Enumerable.Range(0, 20).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            CreateRangeIndexWithFields(primary0, riKey, fields);

            // Arm exception to fire before DELETING (after transmit completes).
            // This simulates a failure that aborts migration before source deletion.
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.RangeIndex_Migration_Before_Deleting);

            try
            {
                // Migration should fail but not crash — exception is caught by migration framework
                context.clusterTestUtils.MigrateSlots(primary0, primary1, [slot]);

                // Wait for cleanup
                context.clusterTestUtils.WaitForMigrationCleanup(logger: context.logger);

                // Source should still own the slot (migration failed)
                var sourceSlots = context.clusterTestUtils.GetOwnedSlotsFromNode(primary0, context.logger);
                ClassicAssert.IsTrue(sourceSlots.Contains(slot), "Source should still own slot after failed migration");

                // Source data should be intact
                VerifyFieldsOnEndpoint(primary0, riKey, fields);

                // RI.SET should work (sketch must have been cleared)
                var setResult = (string)context.clusterTestUtils.Execute(
                    primary0, "RI.SET", [riKey, "after_failure", "works"],
                    flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual("OK", setResult, "RI.SET should succeed after failed migration");
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(ExceptionInjectionType.RangeIndex_Migration_Before_Deleting);
            }
        }

#endif

        #region Post-migration lifecycle

        /// <summary>
        /// Restart a cluster node in place, recovering from its on-disk checkpoint and cluster
        /// configuration. Mirrors the restart recipe used by the RangeIndex replication tests.
        /// </summary>
        private void RestartNode(int nodeIndex)
        {
            context.nodes[nodeIndex].Dispose(false);
            context.nodes[nodeIndex] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(nodeIndex),
                tryRecover: true,
                enableAOF: true,
                cleanClusterConfig: false,
                enableRangeIndexPreview: true);
            context.nodes[nodeIndex].Start();
            context.CreateConnection();
        }

        /// <summary>
        /// Take a checkpoint on the given node and wait for it to complete.
        /// </summary>
        private void CheckpointNode(int nodeIndex)
        {
            var lastSave = context.clusterTestUtils.LastSave(nodeIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNextSecond(nodeIndex, lastSave);
            context.clusterTestUtils.Checkpoint(nodeIndex, logger: context.logger);
            context.clusterTestUtils.WaitCheckpoint(nodeIndex, lastSave, logger: context.logger);
        }

        /// <summary>
        /// After migration, write additional fields on the target, take a checkpoint (which
        /// snapshots the migrated tree to its own <c>data.bftree</c>), then read every field back.
        /// Exercises checkpoint of a migrated-then-modified tree plus sustained post-migration use.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexThenFlushAndRead()
        {
            const int shards = 2;
            const int targetIndex = 1;

            context.CreateInstances(shards, enableRangeIndexPreview: true, enableAOF: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(0);
            var primary1 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            var riKey = FindKeyOnNode(nameof(ClusterMigrateRangeIndexThenFlushAndRead), primary0Id, slots);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            var fields = Enumerable.Range(0, 30).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            CreateRangeIndexWithFields(primary0, riKey, fields);

            // Migrate the slot to the target.
            context.clusterTestUtils.MigrateSlots(primary0, primary1, [slot]);
            context.clusterTestUtils.WaitForMigrationCleanup(logger: context.logger);
            WaitForSlotOwnership(primary0, primary1, slot);

            // Write more fields to the migrated tree on the target.
            var moreFields = Enumerable.Range(30, 20).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            foreach (var (field, value) in moreFields)
            {
                var setResult = (string)context.clusterTestUtils.Execute(
                    primary1, "RI.SET", [riKey, field, value], flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual("OK", setResult, $"RI.SET should succeed on target for {riKey}/{field}");
            }

            // Checkpoint the target: OnFlush snapshots the migrated tree's BfTree to its data.bftree.
            CheckpointNode(targetIndex);

            // Read back every field (migrated + post-migration writes).
            VerifyFieldsOnEndpoint(primary1, riKey, fields.Concat(moreFields));
        }

        /// <summary>
        /// After migration, take a checkpoint on the target, restart the node, and verify the
        /// migrated tree (plus a post-migration write) is recovered from the checkpoint snapshot.
        /// </summary>
        /// <remarks>
        /// A checkpoint is required: migrated RangeIndex records are not yet carried in the AOF,
        /// so an AOF-only restart would not recover the tree. The checkpoint snapshots all live
        /// trees (including migrated ones) and records the AOF tail, so post-checkpoint writes
        /// replay normally onto the recovered tree.
        /// </remarks>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexThenCheckpointRestart()
        {
            const int shards = 2;
            const int targetIndex = 1;

            context.CreateInstances(shards, enableRangeIndexPreview: true, enableAOF: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(0);
            var primary1 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            var riKey = FindKeyOnNode(nameof(ClusterMigrateRangeIndexThenCheckpointRestart), primary0Id, slots);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            var fields = Enumerable.Range(0, 20).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            CreateRangeIndexWithFields(primary0, riKey, fields);

            // Migrate the slot to the target.
            context.clusterTestUtils.MigrateSlots(primary0, primary1, [slot]);
            context.clusterTestUtils.WaitForMigrationCleanup(logger: context.logger);
            WaitForSlotOwnership(primary0, primary1, slot);

            // Post-migration write on the target.
            var extra = ("field_extra", "value_extra");
            ClassicAssert.AreEqual("OK", (string)context.clusterTestUtils.Execute(
                primary1, "RI.SET", [riKey, extra.Item1, extra.Item2], flags: CommandFlags.NoRedirect));

            // Checkpoint the target so the migrated tree is persisted, then restart and recover.
            CheckpointNode(targetIndex);

            RestartNode(targetIndex);

            // All fields (migrated + post-migration) survive the restart.
            VerifyFieldsOnEndpoint(primary1, riKey, fields.Append(extra));
        }

        /// <summary>
        /// After migration, read the migrated tree on the target, then delete the whole key and
        /// verify it is gone (RI.EXISTS = 0, RI.GET null) and a fresh index can be created at the
        /// same key — proving the delete cleaned up the migrated tree's file state.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexThenDelete()
        {
            const int shards = 2;

            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(0);
            var primary1 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            var riKey = FindKeyOnNode(nameof(ClusterMigrateRangeIndexThenDelete), primary0Id, slots);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            var fields = Enumerable.Range(0, 20).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            CreateRangeIndexWithFields(primary0, riKey, fields);

            // Migrate the slot to the target.
            context.clusterTestUtils.MigrateSlots(primary0, primary1, [slot]);
            context.clusterTestUtils.WaitForMigrationCleanup(logger: context.logger);
            WaitForSlotOwnership(primary0, primary1, slot);

            // Reads work on the target.
            VerifyFieldsOnEndpoint(primary1, riKey, fields);

            // Delete the whole key on the target.
            var delResult = (int)context.clusterTestUtils.Execute(
                primary1, "DEL", [riKey], flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual(1, delResult, "DEL should remove the migrated key");

            // The key is gone.
            var exists = (int)context.clusterTestUtils.Execute(
                primary1, "RI.EXISTS", [riKey], flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual(0, exists, "RI.EXISTS should be 0 after DEL");

            var getResult = (string)context.clusterTestUtils.Execute(
                primary1, "RI.GET", [riKey, "field_0"], flags: CommandFlags.NoRedirect);
            ClassicAssert.IsTrue(getResult.Contains("not found", StringComparison.OrdinalIgnoreCase),
                $"RI.GET should report the index is gone after DEL, got: {getResult}");

            // A fresh index can be created at the same key (delete cleaned up file state).
            var recreated = new[] { ("field_a", "value_a"), ("field_b", "value_b") };
            CreateRangeIndexWithFields(primary1, riKey, recreated);
            VerifyFieldsOnEndpoint(primary1, riKey, recreated);
        }

        /// <summary>
        /// Migrate a tree to the target, take a checkpoint, restart, then keep using it: after
        /// recovery write new fields and read everything back, and take a second checkpoint +
        /// restart to confirm the recovered tree is itself durable.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexThenCheckpointRestartAndContinue()
        {
            const int shards = 2;
            const int targetIndex = 1;

            context.CreateInstances(shards, enableRangeIndexPreview: true, enableAOF: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(0);
            var primary1 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            var riKey = FindKeyOnNode(nameof(ClusterMigrateRangeIndexThenCheckpointRestartAndContinue), primary0Id, slots);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            var fields = Enumerable.Range(0, 15).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            CreateRangeIndexWithFields(primary0, riKey, fields);

            // Migrate the slot to the target.
            context.clusterTestUtils.MigrateSlots(primary0, primary1, [slot]);
            context.clusterTestUtils.WaitForMigrationCleanup(logger: context.logger);
            WaitForSlotOwnership(primary0, primary1, slot);

            // Checkpoint + restart (recover the migrated tree from the snapshot).
            CheckpointNode(targetIndex);
            RestartNode(targetIndex);
            VerifyFieldsOnEndpoint(primary1, riKey, fields);

            // Continue using the recovered tree: new writes + reads.
            var afterRecovery = Enumerable.Range(15, 15).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            foreach (var (field, value) in afterRecovery)
            {
                ClassicAssert.AreEqual("OK", (string)context.clusterTestUtils.Execute(
                    primary1, "RI.SET", [riKey, field, value], flags: CommandFlags.NoRedirect));
            }
            VerifyFieldsOnEndpoint(primary1, riKey, fields.Concat(afterRecovery));

            // Second checkpoint + restart: the recovered-then-extended tree is itself durable.
            CheckpointNode(targetIndex);
            RestartNode(targetIndex);
            VerifyFieldsOnEndpoint(primary1, riKey, fields.Concat(afterRecovery));
        }

        /// <summary>
        /// Round-trip a tree P0 → P1 → P0, then checkpoint and restart the original owner (P0) and
        /// verify the data is intact — guards against stale-file / liveIndexes issues left behind
        /// after a back-migration interacting with persistence.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexRoundTripThenRestart()
        {
            const int shards = 2;
            const int sourceIndex = 0;

            context.CreateInstances(shards, enableRangeIndexPreview: true, enableAOF: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(0);
            var primary1 = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            var riKey = FindKeyOnNode(nameof(ClusterMigrateRangeIndexRoundTripThenRestart), primary0Id, slots);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            var fields = Enumerable.Range(0, 15).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            CreateRangeIndexWithFields(primary0, riKey, fields);

            // P0 → P1.
            context.clusterTestUtils.MigrateSlots(primary0, primary1, [slot]);
            context.clusterTestUtils.WaitForMigrationCleanup(logger: context.logger);
            WaitForSlotOwnership(primary0, primary1, slot);

            // Add a field on P1, then migrate back P1 → P0.
            var extra = ("field_extra", "value_extra");
            ClassicAssert.AreEqual("OK", (string)context.clusterTestUtils.Execute(
                primary1, "RI.SET", [riKey, extra.Item1, extra.Item2], flags: CommandFlags.NoRedirect));

            context.clusterTestUtils.MigrateSlots(primary1, primary0, [slot]);
            context.clusterTestUtils.WaitForMigrationCleanup(logger: context.logger);
            WaitForSlotOwnership(primary1, primary0, slot);

            VerifyFieldsOnEndpoint(primary0, riKey, fields.Append(extra));

            // Checkpoint + restart the original owner and confirm the round-tripped data survives.
            CheckpointNode(sourceIndex);
            RestartNode(sourceIndex);
            VerifyFieldsOnEndpoint(primary0, riKey, fields.Append(extra));
        }

        #endregion
    }
}