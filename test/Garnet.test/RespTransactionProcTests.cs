// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespTransactionProcTests
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void TransactionProcTest1()
        {
            // Register sample custom command (SETIFPM = "set if prefix match")
            server.Register.NewTransactionProc("READWRITETX", () => new ReadWriteTxn(), new RespCommandsInfo { Arity = 4 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string readkey = "readkey";
            string value = "foovalue0";
            db.StringSet(readkey, value);

            string writekey1 = "writekey1";
            string writekey2 = "writekey2";

            var result = db.Execute("READWRITETX", readkey, writekey1, writekey2);

            ClassicAssert.AreEqual("SUCCESS", (string)result);

            // Read keys to verify transaction succeeded
            string retValue = db.StringGet(writekey1);
            ClassicAssert.IsNotNull(retValue);
            ClassicAssert.AreEqual(value, retValue);

            retValue = db.StringGet(writekey2);
            ClassicAssert.AreEqual(value, retValue);
        }

        [Test]
        public void TransactionProcTest2()
        {
            // Register sample custom command (SETIFPM = "set if prefix match")
            var numParams = 3;
            var id = server.Register.NewTransactionProc("READWRITETX", () => new ReadWriteTxn(), new RespCommandsInfo { Arity = numParams + 1 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Check RUNTXP without id
            var e = Assert.Throws<RedisServerException>(() => db.Execute("RUNTXP"));
            var expectedErrorMessage = string.Format(CmdStrings.GenericErrWrongNumArgs, nameof(RespCommand.RUNTXP));
            ClassicAssert.AreEqual(expectedErrorMessage, e.Message);

            string readkey = "readkey";
            string value = "foovalue0";
            db.StringSet(readkey, value);

            string writekey1 = "writekey1";
            string writekey2 = "writekey2";

            // Check RUNTXP with insufficient parameters
            e = Assert.Throws<RedisServerException>(() => db.Execute("RUNTXP", id, readkey));
            expectedErrorMessage = string.Format(CmdStrings.GenericErrWrongNumArgsTxn, id, numParams, 1);
            ClassicAssert.AreEqual(expectedErrorMessage, e.Message);

            var result = db.Execute("RUNTXP", id, readkey, writekey1, writekey2);
            ClassicAssert.AreEqual("SUCCESS", (string)result);

            // Read keys to verify transaction succeeded
            string retValue = db.StringGet(writekey1);
            ClassicAssert.IsNotNull(retValue);
            ClassicAssert.AreEqual(value, retValue);

            retValue = db.StringGet(writekey2);
            ClassicAssert.AreEqual(value, retValue);
        }

        [Test]
        public void TransactionProcSampleUpdateTest()
        {
            server.Register.NewTransactionProc("SAMPLEUPDATETX", () => new SampleUpdateTxn(), new RespCommandsInfo { Arity = 9 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string stringKey = "stringKey";
            string stringValue = "stringValue";

            string sortedSet1key = "sortedSet1key";
            string value1 = "value100";
            double score = 100;

            string sortedSetSecondkey = "sortedSetkey2";
            string secondValue = "value200";
            double score2 = 200;

            var result = db.Execute("SAMPLEUPDATETX", stringKey, stringValue,
                sortedSet1key, value1, score, sortedSetSecondkey, secondValue, score2);

            ClassicAssert.AreEqual("SUCCESS", (string)result);

            long size = db.SortedSetLength(sortedSet1key);

            ClassicAssert.AreEqual(1, size);

            SortedSetEntry? retEntry = db.SortedSetPop(sortedSet1key);
            ClassicAssert.NotNull(retEntry);
            ClassicAssert.AreEqual(value1, (string)retEntry?.Element);
            ClassicAssert.AreEqual(score, retEntry?.Score);
            ClassicAssert.AreEqual(0, db.SortedSetLength(sortedSet1key));
        }

        [Test]
        public void TransactionProcSampleDeleteTest()
        {
            server.Register.NewTransactionProc("SAMPLEDELETETX", () => new SampleDeleteTxn(), new RespCommandsInfo { Arity = 6 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string mainStoreKey = "key1";
            db.StringSet(mainStoreKey, "value1");


            string sortedSet1key = "sortedSet1key";
            string value = "value100";
            double score = 100;

            db.SortedSetAdd(sortedSet1key, [new SortedSetEntry(value, score)]);

            string sortedSetSecondkey = "sortedSetkey2";
            string secondValue = "value200";

            var result = db.Execute("SAMPLEDELETETX", mainStoreKey,
                sortedSet1key, value, sortedSetSecondkey, secondValue);

            ClassicAssert.AreEqual("SUCCESS", (string)result);

            long size = db.SortedSetLength(sortedSet1key);

            ClassicAssert.AreEqual(0, size);

            SortedSetEntry? retEntry = db.SortedSetPop(sortedSetSecondkey);
            ClassicAssert.IsNull(retEntry);
            string retValue = db.StringGet(mainStoreKey);

            ClassicAssert.IsNull(retValue);
        }

        [Test]
        public void TransactionWriteExpiryProcTest()
        {
            server.Register.NewTransactionProc("WRITEWITHEXPIRYTX", () => new WriteWithExpiryTxn(), new RespCommandsInfo { Arity = 4 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "key1";
            string value = "value1";

            var result = db.Execute("WRITEWITHEXPIRYTX", key, value, 5);

            ClassicAssert.AreEqual("SUCCESS", (string)result);

            Thread.Sleep(10);
            // Read keys to verify transaction succeeded
            string retValue = db.StringGet(key);
            ClassicAssert.IsNull(retValue);

            result = db.Execute("WRITEWITHEXPIRYTX", key, value, 1000);
            Thread.Sleep(10);
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(value, retValue);
        }

        [Test]
        public void TransactionObjectExpiryProcTest()
        {
            server.Register.NewTransactionProc("OBJECTEXPIRYTX", () => new ObjectExpiryTxn(), new RespCommandsInfo { Arity = 3 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "key1";
            string value = "value1";

            db.SortedSetAdd(key, value, 100);
            long size = db.SortedSetLength(key);

            ClassicAssert.AreEqual(1, size);
            var result = db.Execute("OBJECTEXPIRYTX", key, 1000);

            ClassicAssert.AreEqual("SUCCESS", (string)result);

            Thread.Sleep(15);
            size = db.SortedSetLength(key);

            ClassicAssert.AreEqual(1, size);

            Thread.Sleep(1000);
            size = db.SortedSetLength(key);

            ClassicAssert.AreEqual(0, size);

            var retEntry = db.SortedSetPop(key);

            ClassicAssert.IsNull(retEntry);
        }

        [Test]
        public void TransactionSortedSetRemoveProcTest()
        {
            server.Register.NewTransactionProc("SORTEDSETREMOVETX", () => new SortedSetRemoveTxn(), new RespCommandsInfo { Arity = 3 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string sortedSetKey = "sortedSetkey";
            double score = 100;
            string value = "value100";

            db.SortedSetAdd(sortedSetKey, [new SortedSetEntry(value, score)]);

            var result = db.Execute("SORTEDSETREMOVETX", sortedSetKey, value);

            ClassicAssert.AreEqual("SUCCESS", (string)result);

            ClassicAssert.AreEqual(0, db.SortedSetLength(sortedSetKey));

            SortedSetEntry? retEntry = db.SortedSetPop(sortedSetKey);
            ClassicAssert.IsNull(retEntry);
        }

        [Test]
        public void TransactionDeleteProcTest()
        {
            server.Register.NewTransactionProc("DELETETX", () => new DeleteTxn(), new RespCommandsInfo { Arity = 2 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "key1";
            string value = "value1";

            db.StringSet(key, value);

            var result = db.Execute("DELETETX", key);

            ClassicAssert.AreEqual("SUCCESS", (string)result);

            var retValue = db.StringGet(key);
            ClassicAssert.IsFalse(retValue.HasValue);
        }

        [Test]
        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void TransactionObjectsOperTest(RedisProtocol protocol)
        {
            server.Register.NewTransactionProc("SORTEDSETPROC", () => new TestProcedureSortedSets(), new RespCommandsInfo { Arity = 25 });
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: protocol));
            var db = redis.GetDatabase(0);

            string ssA = "ssA";

            // Sending zadd pairs
            var result = db.Execute("SORTEDSETPROC", ssA, 1, "item1", 2, "item2", 3, "item3", 4, "item4", 5, "item5", 6, "item6", 7, "item7", 8, "item8", 9, "item9", 10, "item10", "1", "9", "*em*");
            ClassicAssert.AreEqual("SUCCESS", (string)result);

            // Read keys to verify transaction succeeded
            long len = db.SortedSetLength("ssA");
            ClassicAssert.AreEqual(1, len);

        }

        [Test]
        public void TransactionListsOperTest()
        {
            server.Register.NewTransactionProc("LISTPROC", () => new TestProcedureLists(), new RespCommandsInfo { Arity = 14 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string lstA = "listA";
            string lstB = "listB";
            string lstC = "listC";

            var result = db.Execute("LISTPROC", lstA, lstB, lstC, "item1", "item2", "item3", "item4", "item5", "item6", "item7", "item8", "item9", "item10");

            ClassicAssert.AreEqual("SUCCESS", (string)result);

            // Read keys to verify transaction succeeded
            long lenListA = db.ListLength("listA");
            ClassicAssert.AreEqual(8, lenListA);

            long lenListB = db.ListLength("listB");
            ClassicAssert.AreEqual(3, lenListB);
        }

        [Test]
        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void TransactionSetProcTest(RedisProtocol protocol)
        {
            server.Register.NewTransactionProc("SETPROC", () => new TestProcedureSet(), new RespCommandsInfo { Arity = 13 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: protocol));
            var db = redis.GetDatabase(0);

            string setA = "setA";

            var result = db.Execute("SETPROC", setA, "item1", "item2", "item3", "item4", "item5", "item6", "item7", "item8", "item9", "item10", "item3");

            ClassicAssert.AreEqual("SUCCESS", (string)result);

            // Read keys to verify transaction succeeded
            long lenSetA = db.SetLength("setA");
            ClassicAssert.AreEqual(lenSetA, 2);
        }


        [Test]
        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void TransactionHashProcTest(RedisProtocol protocol)
        {
            server.Register.NewTransactionProc("HASHPROC", () => new TestProcedureHash(), new RespCommandsInfo { Arity = 15 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: protocol));
            var db = redis.GetDatabase(0);

            string mh = "myHash";
            var result = db.Execute("HASHPROC", mh, "field1", "foo", "field2", "faa", "field3", "fii", "field4", "fee", "field5", "foo", "age", "25", "field1");

            ClassicAssert.AreEqual("SUCCESS", (string)result);

            // Read keys to verify transaction succeeded
            long lenhash = db.HashLength("myHash");
            ClassicAssert.AreEqual(lenhash, 5);
        }

        [Test]
        public void TransactionProcFinalizeTest()
        {
            server.Register.NewTransactionProc("GETTWOKEYSNOTXN", () => new GetTwoKeysNoTxn(), new RespCommandsInfo { Arity = 3 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string readkey1 = "readkey1";
            string value1 = "foovalue1";
            db.StringSet(readkey1, value1);

            string readkey2 = "readkey2";
            string value2 = "foovalue2";
            db.StringSet(readkey2, value2);

            var result = db.Execute("GETTWOKEYSNOTXN", readkey1, readkey2);

            ClassicAssert.AreEqual(value1, ((string[])result)[0]);
            ClassicAssert.AreEqual(value2, ((string[])result)[1]);
        }

        [Test]
        public void TransactionProcMSetPxTest()
        {
            server.Register.NewTransactionProc("MSETPX", () => new MSetPxTxn());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            const int NumKeys = 5;

            var args = new string[1 + 2 * NumKeys];

            // Set expiry to 2 seconds
            args[0] = "2000";

            // Set key-value pairs
            for (int i = 0; i < NumKeys; i++)
            {
                args[2 * i + 1] = $"key{i}";
                args[2 * i + 2] = $"value{i}";
            }

            // Execute transaction
            var result = db.Execute("MSETPX", args);

            // Verify transaction succeeded
            ClassicAssert.AreEqual("OK", (string)result);

            // Read keys to verify transaction succeeded
            for (int i = 0; i < NumKeys; i++)
            {
                string key = $"key{i}";
                string value = $"value{i}";
                string retValue = db.StringGet(key);
                ClassicAssert.AreEqual(value, retValue);
            }

            // Wait for keys to expire
            Thread.Sleep(2100);

            // Verify that keys have expired
            for (int i = 0; i < NumKeys; i++)
            {
                string key = $"key{i}";
                string retValue = db.StringGet(key);
                ClassicAssert.IsNull(retValue);
            }
        }

        [Test]
        public void TransactionProcMGetIfPMTest()
        {
            server.Register.NewTransactionProc("MSETPX", () => new MSetPxTxn());
            server.Register.NewTransactionProc("MGETIFPM", () => new MGetIfPM());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            const int NumKeys = 15;
            const string prefix = "value1";

            var args1 = new string[1 + 2 * NumKeys];

            // Set expiry to 600 seconds
            args1[0] = "600000";

            // Set key-value pairs
            for (int i = 0; i < NumKeys; i++)
            {
                args1[2 * i + 1] = $"key{i}";
                args1[2 * i + 2] = $"value{i}";
            }

            // Execute transaction
            var result1 = (string)db.Execute("MSETPX", args1);

            // Verify transaction succeeded
            ClassicAssert.AreEqual("OK", result1);

            // Read keys to verify transaction succeeded
            for (int i = 0; i < NumKeys; i++)
            {
                string key = $"key{i}";
                string value = $"value{i}";
                string retValue = db.StringGet(key);
                ClassicAssert.AreEqual(value, retValue);
            }

            var args2 = new string[1 + NumKeys];

            // Set prefix
            args2[0] = prefix;

            // Set keys
            for (int i = 0; i < NumKeys; i++)
            {
                args2[i + 1] = $"key{i}";
            }

            // Execute transaction
            var result2 = (string[])db.Execute("MGETIFPM", args2);

            // Verify results
            int expectedCount = NumKeys - 9; // only values with specified prefix
            ClassicAssert.AreEqual(2 * expectedCount, result2.Length);
            // Verify that keys have the correct prefix
            for (int i = 0; i < expectedCount; i++)
            {
                ClassicAssert.AreEqual(prefix, result2[2 * i + 1].Substring(0, prefix.Length));
            }
        }
    }
}