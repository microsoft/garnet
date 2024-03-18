// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using NUnit.Framework;
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
            server.Register.NewTransactionProc("READWRITETX", 3, () => new ReadWriteTxn());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string readkey = "readkey";
            string value = "foovalue0";
            db.StringSet(readkey, value);

            string writekey1 = "writekey1";
            string writekey2 = "writekey2";

            var result = db.Execute("READWRITETX", readkey, writekey1, writekey2);

            Assert.AreEqual("SUCCESS", (string)result);

            // Read keys to verify transaction succeeded
            string retValue = db.StringGet(writekey1);
            Assert.IsNotNull(retValue);
            Assert.AreEqual(value, retValue);

            retValue = db.StringGet(writekey2);
            Assert.AreEqual(value, retValue);
        }

        [Test]
        public void TransactionProcTest2()
        {
            // Register sample custom command (SETIFPM = "set if prefix match")
            int id = server.Register.NewTransactionProc("READWRITETX", 3, () => new ReadWriteTxn());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string readkey = "readkey";
            string value = "foovalue0";
            db.StringSet(readkey, value);

            string writekey1 = "writekey1";
            string writekey2 = "writekey2";

            var result = db.Execute("RUNTXP", id, readkey, writekey1, writekey2);

            Assert.AreEqual("SUCCESS", (string)result);

            // Read keys to verify transaction succeeded
            string retValue = db.StringGet(writekey1);
            Assert.IsNotNull(retValue);
            Assert.AreEqual(value, retValue);

            retValue = db.StringGet(writekey2);
            Assert.AreEqual(value, retValue);
        }

        [Test]
        public void TransactionProcSampleUpdateTest()
        {
            server.Register.NewTransactionProc("SAMPLEUPDATETX", 8, () => new SampleUpdateTxn());

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

            Assert.AreEqual("SUCCESS", (string)result);

            long size = db.SortedSetLength(sortedSet1key);

            Assert.AreEqual(1, size);

            SortedSetEntry? retEntry = db.SortedSetPop(sortedSet1key);
            Assert.NotNull(retEntry);
            Assert.AreEqual(value1, (string)retEntry?.Element);
            Assert.AreEqual(score, retEntry?.Score);
            Assert.AreEqual(0, db.SortedSetLength(sortedSet1key));
        }

        [Test]
        public void TransactionProcSampleDeleteTest()
        {
            server.Register.NewTransactionProc("SAMPLEDELETETX", 5, () => new SampleDeleteTxn());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string mainStoreKey = "key1";
            db.StringSet(mainStoreKey, "value1");


            string sortedSet1key = "sortedSet1key";
            string value = "value100";
            double score = 100;

            db.SortedSetAdd(sortedSet1key, new SortedSetEntry[] { new SortedSetEntry(value, score) });

            string sortedSetSecondkey = "sortedSetkey2";
            string secondValue = "value200";

            var result = db.Execute("SAMPLEDELETETX", mainStoreKey,
                sortedSet1key, value, sortedSetSecondkey, secondValue);

            Assert.AreEqual("SUCCESS", (string)result);

            long size = db.SortedSetLength(sortedSet1key);

            Assert.AreEqual(0, size);

            SortedSetEntry? retEntry = db.SortedSetPop(sortedSetSecondkey);
            Assert.IsNull(retEntry);
            string retValue = db.StringGet(mainStoreKey);

            Assert.IsNull(retValue);
        }

        [Test]
        public void TransactionWriteExpiryProcTest()
        {
            server.Register.NewTransactionProc("WRITEWITHEXPIRYTX", 3, () => new WriteWithExpiryTxn());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "key1";
            string value = "value1";

            var result = db.Execute("WRITEWITHEXPIRYTX", key, value, 5);

            Assert.AreEqual("SUCCESS", (string)result);

            Thread.Sleep(10);
            // Read keys to verify transaction succeeded
            string retValue = db.StringGet(key);
            Assert.IsNull(retValue);

            result = db.Execute("WRITEWITHEXPIRYTX", key, value, 1000);
            Thread.Sleep(10);
            retValue = db.StringGet(key);
            Assert.AreEqual(value, retValue);
        }

        [Test]
        public void TransactionObjectExpiryProcTest()
        {
            server.Register.NewTransactionProc("OBJECTEXPIRYTX", 2, () => new ObjectExpiryTxn());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "key1";
            string value = "value1";

            db.SortedSetAdd(key, value, 100);
            long size = db.SortedSetLength(key);

            Assert.AreEqual(1, size);
            var result = db.Execute("OBJECTEXPIRYTX", key, 1000);

            Assert.AreEqual("SUCCESS", (string)result);

            Thread.Sleep(15);
            size = db.SortedSetLength(key);

            Assert.AreEqual(1, size);

            Thread.Sleep(1000);
            size = db.SortedSetLength(key);

            Assert.AreEqual(0, size);

            SortedSetEntry? retEntry = db.SortedSetPop(key);

            Assert.IsNull(retEntry);
        }

        [Test]
        public void TransactionSortedSetRemoveProcTest()
        {
            server.Register.NewTransactionProc("SORTEDSETREMOVETX", 2, () => new SortedSetRemoveTxn());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string sortedSetKey = "sortedSetkey";
            double score = 100;
            string value = "value100";

            db.SortedSetAdd(sortedSetKey, new SortedSetEntry[] { new SortedSetEntry(value, score) });

            var result = db.Execute("SORTEDSETREMOVETX", sortedSetKey, value);

            Assert.AreEqual("SUCCESS", (string)result);

            Assert.AreEqual(0, db.SortedSetLength(sortedSetKey));

            SortedSetEntry? retEntry = db.SortedSetPop(sortedSetKey);
            Assert.IsNull(retEntry);
        }

        [Test]
        public void TransactionDeleteProcTest()
        {
            server.Register.NewTransactionProc("DELETETX", 1, () => new DeleteTxn());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "key1";
            string value = "value1";

            db.StringSet(key, value);

            var result = db.Execute("DELETETX", key);

            Assert.AreEqual("SUCCESS", (string)result);

            var retValue = db.StringGet(key);
            Assert.IsFalse(retValue.HasValue);
        }

        [Test]
        public void TransactionObjectsOperTest()
        {
            server.Register.NewTransactionProc("SORTEDSETPROC", 24, () => new TestProcedureSortedSets());
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string ssA = "ssA";

            // Sending zadd pairs
            var result = db.Execute("SORTEDSETPROC", ssA, 1, "item1", 2, "item2", 3, "item3", 4, "item4", 5, "item5", 6, "item6", 7, "item7", 8, "item8", 9, "item9", 10, "item10", "1", "9", "*em*");
            Assert.AreEqual("SUCCESS", (string)result);

            // Read keys to verify transaction succeeded
            long len = db.SortedSetLength("ssA");
            Assert.AreEqual(5, len);

        }

        [Test]
        public void TransactionListsOperTest()
        {
            server.Register.NewTransactionProc("LISTPROC", 12, () => new TestProcedureLists());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string lstA = "listA";
            string lstB = "listB";

            var result = db.Execute("LISTPROC", lstA, lstB, "item1", "item2", "item3", "item4", "item5", "item6", "item7", "item8", "item9", "item10");

            Assert.AreEqual("SUCCESS", (string)result);

            // Read keys to verify transaction succeeded
            long lenListA = db.ListLength("listA");
            Assert.AreEqual(8, lenListA);

            long lenListB = db.ListLength("listB");
            Assert.AreEqual(3, lenListB);
        }

        [Test]
        public void TransactionSetProcTest()
        {
            server.Register.NewTransactionProc("SETPROC", 12, () => new TestProcedureSet());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string setA = "setA";

            var result = db.Execute("SETPROC", setA, "item1", "item2", "item3", "item4", "item5", "item6", "item7", "item8", "item9", "item10", "item3");

            Assert.AreEqual("SUCCESS", (string)result);

            // Read keys to verify transaction succeeded
            long lenSetA = db.SetLength("setA");
            Assert.AreEqual(lenSetA, 2);
        }


        [Test]
        public void TransactionHashProcTest()
        {
            server.Register.NewTransactionProc("HASHPROC", 14, () => new TestProcedureHash());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string mh = "myHash";
            var result = db.Execute("HASHPROC", mh, "field1", "foo", "field2", "faa", "field3", "fii", "field4", "fee", "field5", "foo", "age", "25", "field1");

            Assert.AreEqual("SUCCESS", (string)result);

            // Read keys to verify transaction succeeded
            long lenhash = db.HashLength("myHash");
            Assert.AreEqual(lenhash, 5);
        }

        [Test]
        public void TransactionProcFinalizeTest()
        {
            server.Register.NewTransactionProc("GETTWOKEYSNOTXN", 2, () => new GetTwoKeysNoTxn());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string readkey1 = "readkey1";
            string value1 = "foovalue1";
            db.StringSet(readkey1, value1);

            string readkey2 = "readkey2";
            string value2 = "foovalue2";
            db.StringSet(readkey2, value2);

            var result = db.Execute("GETTWOKEYSNOTXN", readkey1, readkey2);

            Assert.AreEqual(value1, ((string[])result)[0]);
            Assert.AreEqual(value2, ((string[])result)[1]);
        }
    }
}