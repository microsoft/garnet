// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Linq;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Tests for the <c>--upgrade</c> run that up-converts a downlevel object log and renames it in as the live one.
    /// The conversion itself is covered by the Tsavorite recovery tests, which can synthesize a downlevel checkpoint;
    /// these cover the host-side driver and its guards against damaging a store that needs no conversion.
    /// </summary>
    [TestFixture]
    public class ObjectLogUpgradeTests : TestBase
    {
        const int NumKeys = 3000;
        const int ValueLength = 500;

        static string ValueFor(int key) => new((char)('a' + (key % 26)), ValueLength);

        [SetUp]
        public void Setup() => TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

        [TearDown]
        public void TearDown()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        static void PopulateAndCheckpoint()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            for (var key = 0; key < NumKeys; key++)
                db.HashSet($"hash:{key}", [new HashEntry("field", ValueFor(key))]);
            _ = db.Execute("SAVE");
        }

        static void VerifyContents()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, tryRecover: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            for (var key = 0; key < NumKeys; key++)
            {
                var value = db.HashGet($"hash:{key}", "field");
                ClassicAssert.AreEqual(ValueFor(key), (string)value, $"hash:{key} did not survive");
            }
        }

        [Test]
        [Category("GarnetServer")]
        public void UpgradeOnCurrentFormatStoreLeavesObjectLogIntact()
        {
            // An --upgrade run against a store that needs no conversion must NOT rename anything: the upgrade device holds no
            // data, so swapping it in would replace a good object log with an empty one.
            PopulateAndCheckpoint();

            var storeDir = ObjectLogUpgradeSwap.GetStoreDirectory(new DirectoryInfo(TestUtils.MethodTestDir).FullName);
            var objectLogBefore = Directory.GetFiles(storeDir, GarnetServerOptions.ObjectLogFileName + ".*").Order().ToArray();
            ClassicAssert.IsNotEmpty(objectLogBefore, "the fixture should have written object-log segments");
            var bytesBefore = objectLogBefore.Select(File.ReadAllBytes).ToArray();

            using (var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, tryRecover: true, upgrade: true))
            {
                ClassicAssert.IsTrue(server.IsUpgradeRun);
                server.RunUpgrade();
            }

            ClassicAssert.IsFalse(ObjectLogUpgradeSwap.HasPendingSwap(new DirectoryInfo(TestUtils.MethodTestDir).FullName), "no swap should have been journaled");
            ClassicAssert.IsEmpty(Directory.GetFiles(storeDir, GarnetServerOptions.ObjectLogFileName + "_pre_upgrade_*"), "nothing should have been retired");

            var objectLogAfter = Directory.GetFiles(storeDir, GarnetServerOptions.ObjectLogFileName + ".*").Order().ToArray();
            CollectionAssert.AreEqual(objectLogBefore, objectLogAfter, "the live object-log segments should be untouched");
            for (var i = 0; i < objectLogAfter.Length; i++)
            {
                // Closing the store can append, but every byte that was already durable must still be there: a swap would have
                // replaced the file wholesale with the (empty) upgrade device.
                var after = File.ReadAllBytes(objectLogAfter[i]);
                ClassicAssert.GreaterOrEqual(after.Length, bytesBefore[i].Length, $"segment {objectLogAfter[i]} shrank");
                CollectionAssert.AreEqual(bytesBefore[i], after.Take(bytesBefore[i].Length).ToArray(), $"segment {objectLogAfter[i]} was rewritten");
            }

            VerifyContents();
        }

        [Test]
        [Category("GarnetServer")]
        public void RunUpgradeRequiresTheUpgradeOption()
        {
            // Program.Main chooses RunUpgrade over Start from IsUpgradeRun; calling it on an ordinary server would recover and
            // then close the store instead of serving, so it is rejected.
            PopulateAndCheckpoint();

            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, tryRecover: true);
            ClassicAssert.IsFalse(server.IsUpgradeRun);
            var ex = Assert.Throws<GarnetException>(server.RunUpgrade);
            ClassicAssert.IsTrue(ex.Message.Contains("--upgrade"), $"unexpected message: {ex.Message}");
        }
    }
}