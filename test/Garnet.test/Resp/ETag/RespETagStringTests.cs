// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.Resp.ETag
{
    [AllureNUnit]
    [TestFixture]
    public class RespETagStringTests : AllureTestBase
    {
        private GarnetServer server;

        [SetUp]
        public void Setup()
        {
            new Random(674386);
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            var useReviv = false;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is RevivificationMode revivMode)
                {
                    useReviv = revivMode == RevivificationMode.UseReviv;
                    continue;
                }
            }

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: false, useReviv: useReviv);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        #region ETAG SET Happy Paths

        [Test]
        public void SetReturnsEtagForNewData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var results = (RedisResult[])db.ExecWithEtag("SET", "rizz", "buzz");
            CheckEtagAndNullValue(results, 1);
        }

        [Test]
        public void SetIfMatchReturnsNewValueAndEtagWhenEtagMatches()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "florida";
            var results = (RedisResult[])db.ExecWithEtag("SET", key, "one");
            var expectedEtag = 1;
            CheckEtagAndNullValue(results, expectedEtag);

            var incorrectEtag = 1738;
            results = (RedisResult[])db.ExecIfMatch(incorrectEtag, "SET", key, "nextone");
            CheckEtagAndValue(results, expectedEtag, "one");

            // set a bigger val
            results = (RedisResult[])db.ExecIfMatch(expectedEtag, "SET", key, "nextone");
            expectedEtag++;
            CheckEtagAndNullValue(results, expectedEtag);

            // set a bigger val
            results = (RedisResult[])db.ExecIfMatch(expectedEtag, "SET", key, "nextnextone");
            expectedEtag++;
            CheckEtagAndNullValue(results, expectedEtag);

            // ETAGMISMATCH again
            results = (RedisResult[])db.ExecIfMatch(incorrectEtag, "SET", key, "lastOne");
            CheckEtagAndValue(results, expectedEtag, "nextnextone");

            // set a smaller val
            results = (RedisResult[])db.ExecIfMatch(expectedEtag, "SET", key, "lastOne");
            expectedEtag++;
            CheckEtagAndNullValue(results, expectedEtag);

            // ETAGMISMATCH on data that never had an etag
            db.KeyDelete(key);
            db.StringSet(key, "one");
            results = (RedisResult[])db.ExecIfMatch(incorrectEtag, "SET", key, "lastOne");
            CheckEtagAndValue(results, 0, "one");
        }

        [Test]
        public void SetIfMatchWorksWithExpiration()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "florida";
            // Scenario: Key existed before and had no expiration
            var results = (RedisResult[])db.ExecWithEtag("SET", key, "one");
            var expectedEtag = 1;
            CheckEtagAndNullValue(results, expectedEtag);

            // expiration added
            results = (RedisResult[])db.ExecIfMatch(1, "SET", key, "nextone", "EX", 100);
            expectedEtag++;
            CheckEtagAndNullValue(results, expectedEtag);

            // confirm expiration added -> TTL should exist
            var ttl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(ttl.HasValue);

            results = (RedisResult[])db.ExecIfMatch(expectedEtag, "SET", key, "nextoneeexpretained");
            expectedEtag++;
            CheckEtagAndNullValue(results, expectedEtag);

            // TTL should be retained 
            ttl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(ttl.HasValue);

            db.KeyDelete(key); // cleanup

            // Scenario: Key existed before and had expiration
            results = (RedisResult[])db.ExecWithEtag("SET", key, "one", "PX", 100000);
            expectedEtag = 1;
            CheckEtagAndNullValue(results, expectedEtag);

            // confirm expiration added -> TTL should exist
            ttl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(ttl.HasValue);

            // change value and retain expiration
            results = (RedisResult[])db.ExecIfMatch(1, "SET", key, "nextone");
            expectedEtag++;
            CheckEtagAndNullValue(results, expectedEtag);

            // TTL should be retained
            ttl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(ttl.HasValue);

            // change value and change expiration
            results = (RedisResult[])db.ExecIfMatch(2, "SET", key, "nextoneeexpretained", "EX", 100);
            expectedEtag++;
            CheckEtagAndNullValue(results, expectedEtag);

            db.KeyDelete(key); // cleanup

            // Scenario: SET without etag and existing expiration when sent with setifmatch will add etag and retain the expiration too
            db.Execute("SET", key, "one", "EX", 100000);

            // when no etag then count 0 as it's existing etag
            results = (RedisResult[])db.ExecIfMatch(0, "SET", key, "nextone");
            expectedEtag = 1;
            CheckEtagAndNullValue(results, expectedEtag);

            // confirm expiration retained -> TTL should exist
            ttl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(ttl.HasValue);

            // confirm has etag now
            results = (RedisResult[])db.ExecWithEtag("GET", key);
            CheckEtagAndValue(results, expectedEtag, "nextone");

            db.KeyDelete(key); // cleanup

            // Scenario: SET without etag and without expiration when sent with setifmatch will add etag and retain the expiration too
            // copy update
            db.Execute("SET", key, "one");
            // when no etag then count 0 as it's existing etag
            results = (RedisResult[])db.ExecIfMatch(0, "SET", key, "nextone", "EX", 10000);
            CheckEtagAndNullValue(results, expectedEtag);

            // confirm expiration retained -> TTL should exist
            ttl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(ttl.HasValue);

            // confirm has etag now
            results = (RedisResult[])db.ExecWithEtag("GET", key);
            CheckEtagAndValue(results, expectedEtag, "nextone");

            // same length update
            db.Execute("SET", key, "one");
            // when no etag then count 0 as it's existing etag
            results = (RedisResult[])db.ExecIfMatch(0, "SET", key, "two", "EX", 10000);
            CheckEtagAndNullValue(results, expectedEtag);

            // confirm expiration retained -> TTL should exist
            ttl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(ttl.HasValue);

            // confirm has etag now
            results = (RedisResult[])db.ExecWithEtag("GET", key);
            CheckEtagAndValue(results, expectedEtag, "two");

            db.KeyDelete(key); // cleanup

            // Scenario: smaller length update
            db.Execute("SET", key, "oneofusoneofus");
            // when no etag then count 0 as it's existing etag
            results = (RedisResult[])db.ExecIfMatch(0, "SET", key, "i", "EX", 10000);
            CheckEtagAndNullValue(results, expectedEtag);

            // confirm expiration retained -> TTL should exist
            ttl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(ttl.HasValue);

            // Scenario: smaller length update (IPU) of a key with existing etag should increment the ETAG and retain the expiration
            db.Execute("SET", key, "oneofusoneofus", "EX", 10000);
            // when no etag then count 0 as it's existing etag
            results = (RedisResult[])db.ExecIfMatch(0, "SET", key, "i");
            CheckEtagAndNullValue(results, expectedEtag);

            // confirm expiration retained -> TTL should exist
            ttl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(ttl.HasValue);
        }

        #endregion

        #region ETAG GET Happy Paths

        [Test]
        public void GetWithEtagReturnsValAndEtagForKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "florida";
            // Data that does not exist returns nil
            var results = (RedisResult[])db.ExecWithEtag("GET", key);
            CheckEtagAndNullValue(results, 0);

            // insert data
            var val = "hkhalid";
            results = (RedisResult[])db.ExecWithEtag("SET", key, val);
            CheckEtagAndNullValue(results, 1);

            GetAndCheckEtagAndValue(db, key, 1, val);
        }

        [Test]
        public void GetIfNotMatchReturnsDataWhenEtagDoesNotMatch()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "florida";
            // GetIfNotMatch on non-existing data will return null
            var results = (RedisResult[])db.ExecIfNotMatch(0, "GET", key);
            CheckEtagAndNullValue(results, 0);

            // insert data 
            var val = "maximus";
            var _ = db.ExecWithEtag("SET", key, val);

            results = (RedisResult[])db.ExecIfNotMatch(1, "GET", key);
            CheckEtagAndNullValue(results, 1);

            results = (RedisResult[])db.ExecIfNotMatch(2, "GET", key);
            CheckEtagAndValue(results, 1, val);
        }

        [Test]
        public void SetWithEtagWorksWithExpiration()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Scenario: set withetag with expiration on non existing key 
            var key1 = "key1";
            var results = (RedisResult[])db.ExecWithEtag("SET", key1, "value1", "EX", 10);
            CheckEtagAndNullValue(results, 1);
            db.KeyDelete(key1); // Cleanup

            // Scenario: set with etag with expiration NX with existing key
            var key2 = "key2";
            db.ExecWithEtag("SET", key2, "value2");
            results = (RedisResult[])db.ExecWithEtag("SET", key2, "value3", "NX", "EX", 10);
            CheckEtagAndNullValue(results, 1);
            db.KeyDelete(key2); // Cleanup

            // Scenario: set with etag with expiration NX with non-existent key
            var key3 = "key3";
            results = (RedisResult[])db.ExecWithEtag("SET", key3, "value4", "NX", "EX", 10);
            CheckEtagAndNullValue(results, 1);
            db.KeyDelete(key3); // Cleanup

            // Scenario: set with etag with expiration XX
            var key4 = "key4";
            db.ExecWithEtag("SET", key4, "value5");
            results = (RedisResult[])db.ExecWithEtag("SET", key4, "value6", "XX", "EX", 10);
            CheckEtagAndNullValue(results, 2);
            db.KeyDelete(key4); // Cleanup

            // Scenario: set with etag with expiration on existing data with etag
            var key5 = "key5";
            db.ExecWithEtag("SET", key5, "value7");
            results = (RedisResult[])db.ExecWithEtag("SET", key5, "value8", "EX", 10);
            CheckEtagAndNullValue(results, 2);
            db.KeyDelete(key5); // Cleanup

            // Scenario: set with etag with expiration on existing data without etag
            var key6 = "key6";
            db.StringSet(key6, "value9");
            results = (RedisResult[])db.ExecWithEtag("SET", key6, "value10", "EX", 10);
            CheckEtagAndNullValue(results, 1);
            db.KeyDelete(key6); // Cleanup

            // Scenario: set with keepttl on key with etag and expiration should retain metadata and 
            var key7 = "key7";
            db.ExecWithEtag("SET", key7, "value11", "EX", 10);
            results = (RedisResult[])db.ExecWithEtag("SET", key7, "value12", "KEEPTTL");
            CheckEtagAndNullValue(results, 2);
        }

        [Test]
        public void SetIfGreaterWorksWithInitialETag()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "meow-key";
            var value = "m";

            var results = (RedisResult[])db.ExecWithEtag("SET", key, value);
            CheckEtagAndNullValue(results, 1);

            // not greater etag sent so we expect a higher etag returned
            results = (RedisResult[])db.ExecIfGreater(0, "SET", key, "diggity");
            CheckEtagAndValue(results, 1, value);

            // greater etag sent so we expect the same etag returned
            var newValue = "meow";
            results = (RedisResult[])db.ExecIfGreater(2, "SET", key, newValue);
            CheckEtagAndNullValue(results, 2);

            // shrink value size and send greater etag
            newValue = "m";
            results = (RedisResult[])db.ExecIfGreater(5, "SET", key, newValue);
            CheckEtagAndNullValue(results, 5);
        }

        [Test]
        public void SetIfGreaterWorksWithoutInitialETag()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "meow-key";
            var value = "m";

            var result = db.Execute("SET", key, value);
            ClassicAssert.AreEqual("OK", result.ToString());

            // not greater etag sent so we expect the actual etag returned
            var results = (RedisResult[])db.ExecIfGreater(0, "SET", key, "check");
            CheckEtagAndValue(results, 0, value);

            // greater etag sent so we expect the same etag returned
            var newValue = "meow";
            results = (RedisResult[])db.ExecIfGreater(2, "SET", key, newValue);
            CheckEtagAndNullValue(results, 2);

            // shrink value size and send greater etag
            newValue = "m";
            results = (RedisResult[])db.ExecIfGreater(5, "SET", key, newValue);
            CheckEtagAndNullValue(results, 5);
        }

        #endregion

        #region ETAG DEL Happy Paths

        [Test]
        public void DelIfGreaterOnAnAlreadyExistingKeyWithEtagWorks()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "meow-key";
            var value = "m";

            var res = (RedisResult[])db.ExecWithEtag("SET", key, value);
            CheckEtagAndNullValue(res, 1);

            // does not delete when called with lesser or equal etag
            res = (RedisResult[])db.ExecIfGreater(0, "DEL", key);
            CheckEtagAndNullValue(res, 1);

            var actualValue = db.StringGet(key);
            ClassicAssert.AreEqual(value, actualValue.ToString());

            // Deletes when called with higher etag
            res = (RedisResult[])db.ExecIfGreater(2, "DEL", key);
            CheckEtagAndValue(res, 1, 1);

            actualValue = db.StringGet(key);
            ClassicAssert.IsTrue(actualValue.IsNull);
        }

        [Test]
        public void DelIfGreaterOnAnAlreadyExistingKeyWithoutEtagWorks()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "meow-key";
            var value = "m";

            var result = db.StringSet(key, value);
            ClassicAssert.IsTrue(result);

            // does not delete when called with lesser or equal etag
            var res = (RedisResult[])db.ExecIfGreater(0, "DEL", key);
            CheckEtagAndNullValue(res, 0);

            var returnedval = db.StringGet(key);
            ClassicAssert.AreEqual(value, returnedval.ToString());

            // Deletes when called with higher etag
            res = (RedisResult[])db.ExecIfGreater(2, "DEL", key);
            CheckEtagAndValue(res, 0, 1);

            returnedval = db.StringGet(key);
            ClassicAssert.IsTrue(returnedval.IsNull);
        }

        [Test]
        public void DelIfGreaterOnAnAlreadyExistingKeyWithEtagRCUWorks()
        {
            // get rid of the server we create at setup
            server.Dispose();

            // create a low memory server so we can get to the RCU state faster
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: false, lowMemory: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var garnetServer = redis.GetServer(TestUtils.EndPoint);

            var key = "rcuplease";
            var value = "havepatiencercushallbedonethisvalueisunnecssarilylongsoicanmakesureRCUdoesnotAllocateThismuch,anythinglesserthanthisisgoodenough";

            var results = (RedisResult[])db.ExecWithEtag("SET", key, value);
            CheckEtagAndNullValue(results, 1);

            var info = TestUtils.GetStoreAddressInfo(garnetServer);

            // now push the above key back all the way to stable region
            var prevTailAddr = info.TailAddress;
            MakeReadOnly(prevTailAddr, garnetServer, db);

            info = TestUtils.GetStoreAddressInfo(garnetServer);

            // The first record inserted (key0) is now read-only
            ClassicAssert.IsTrue(info.ReadOnlyAddress >= prevTailAddr);

            var tailAddressBeforeNonDeletingReq = info.TailAddress;
            // does not delete when called with lesser or equal etag
            results = (RedisResult[])db.ExecIfGreater(0, "DEL", key);
            CheckEtagAndNullValue(results, 1);

            var returnedval = db.StringGet(key);
            ClassicAssert.AreEqual(value, returnedval.ToString());

            info = TestUtils.GetStoreAddressInfo(garnetServer);
            var lastTailAddr = info.TailAddress;

            // non deleting req adds nothing to hlog
            ClassicAssert.AreEqual(tailAddressBeforeNonDeletingReq, lastTailAddr);

            // Deletes when called with higher etag
            // Moved by 32 bytes...
            results = (RedisResult[])db.ExecIfGreater(2, "DEL", key);
            CheckEtagAndValue(results, 1, 1);

            info = TestUtils.GetStoreAddressInfo(garnetServer);
            // check that deletion has happened
            var newTailAddr = info.TailAddress;

            // tombstoned size?
            ClassicAssert.IsTrue(newTailAddr - lastTailAddr < value.Length);

            returnedval = db.StringGet(key);
            ClassicAssert.IsTrue(returnedval.IsNull);
        }

        [Test]
        public void DelIfGreaterOnAnAlreadyExistingKeyWithoutEtagRCUWorks()
        {
            // get rid of the server created by setup and instead use a low mem server
            server.Dispose();

            // create a low memory server so we can get to the RCU state faster
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: false, lowMemory: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var garnetServer = redis.GetServer(TestUtils.EndPoint);

            var key = "rcuplease";
            var value = "havepatiencercushallbedonethisvalueisneedlesslylongsoIcantestnorecordwasaddedtohlogofthissize";

            var result = db.StringSet(key, value);
            ClassicAssert.IsTrue(result);

            var info = TestUtils.GetStoreAddressInfo(garnetServer);

            // now move this key all the way to stable region
            var prevTailAddr = info.TailAddress;
            MakeReadOnly(prevTailAddr, garnetServer, db);

            info = TestUtils.GetStoreAddressInfo(garnetServer);

            // The first record inserted (key0) is now read-only
            ClassicAssert.IsTrue(info.ReadOnlyAddress >= prevTailAddr);

            var nonDeletingReqTailAddr = info.TailAddress;

            // does not delete when called with lesser or equal etag
            var res = (RedisResult[])db.ExecIfGreater(0, "DEL", key);
            CheckEtagAndNullValue(res, 0);

            var returnedval = db.StringGet(key);
            ClassicAssert.AreEqual(value, returnedval.ToString());

            info = TestUtils.GetStoreAddressInfo(garnetServer);
            var lastTailAddr = info.TailAddress;

            // nothing added to hlog by last DELIFGREATER
            ClassicAssert.AreEqual(nonDeletingReqTailAddr, lastTailAddr);

            // Deletes when called with higher etag
            res = (RedisResult[])db.ExecIfGreater(2, "DEL", key);
            CheckEtagAndValue(res, 0, 1);

            info = TestUtils.GetStoreAddressInfo(garnetServer);
            // check that deletion has happened
            var newTailAddr = info.TailAddress;

            // tombstoned size?
            ClassicAssert.IsTrue(newTailAddr - lastTailAddr < value.Length);

            returnedval = db.StringGet(key);
            ClassicAssert.IsTrue(returnedval.IsNull);
        }

        private void MakeReadOnly(long untilAddress, IServer server, IDatabase db)
        {
            var i = 1000;
            var info = TestUtils.GetStoreAddressInfo(server);

            // Add keys so that the first record enters the read-only region
            // Each record is 40 bytes here, because they do not have expirations
            while (info.ReadOnlyAddress < untilAddress)
            {
                var key = $"key{i++:00000}";
                _ = db.StringSet(key, key);
                info = TestUtils.GetStoreAddressInfo(server);
            }
        }

        #endregion

        #region Edgecases

        [Test]
        [TestCase("m", "mo", null)] // RCU with no existing exp on noetag key
        [TestCase("mexicanmochawithdoubleespresso", "c", null)] // IPU with no existing exp on noetag key
        [TestCase("m", "mo", 30)] // RCU with existing exp on noetag key
        [TestCase("mexicanmochawithdoubleespresso", "c", 30)] // IPU with existing exp on noetag key
        public void SetIfGreaterWhenExpIsSentForExistingNonEtagKey(string initialValue, string newValue, double? exp)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "meow-key";

            if (exp != null)
                db.StringSet(key, initialValue, TimeSpan.FromSeconds(exp.Value));
            else
                db.StringSet(key, initialValue);

            var results = (RedisResult[])db.ExecIfGreater(5, "SET", key, newValue, "EX", 90);
            CheckEtagAndNullValue(results, 5);

            var res = db.StringGetWithExpiry(key);
            ClassicAssert.AreEqual(newValue, res.Value.ToString());
            ClassicAssert.IsTrue(res.Expiry.HasValue);
        }

        [Test]
        [TestCase("m", "mo", null)] // RCU with no existing exp on noetag key
        [TestCase("mexicanmochawithdoubleespresso", "c", null)] // IPU with no existing exp on noetag key
        [TestCase("m", "mo", 30)] // RCU with existing exp on noetag key
        [TestCase("mexicanmochawithdoubleespresso", "c", 30)] // IPU with existing exp on noetag key
        public void SetIfMatchWhenExpIsSentForExistingNonEtagKey(string initialValue, string newValue, int? exp)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "meow-key";

            if (exp != null)
                db.StringSet(key, initialValue, TimeSpan.FromSeconds(exp.Value));
            else
                db.StringSet(key, initialValue);

            var results = (RedisResult[])db.ExecIfMatch(0, "SET", key, newValue, "EX", 90);
            CheckEtagAndNullValue(results, 1);

            var res = db.StringGetWithExpiry(key);
            ClassicAssert.AreEqual(newValue, res.Value.ToString());
            ClassicAssert.IsTrue(res.Expiry.HasValue);
        }


        [Test]
        public void SetIfMatchSetsKeyValueOnNonExistingKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var result = (RedisResult[])db.ExecIfMatch(1, "SET", "key", "valueanother", "EX", 3);
            ClassicAssert.IsTrue(result[0].IsNull);
            ClassicAssert.AreEqual(2, (long)result[1]);
        }


        [Test]
        public void SetIfGreaterSetsKeyValueOnNonExistingKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var results = (RedisResult[])db.ExecIfGreater(1, "SET", "key", "valueanother", "EX", 3);
            CheckEtagAndNullValue(results, 1);
        }

        [Test]
        public void SetOnAlreadyExistingSetDataOverridesItWithInitialEtag()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var results = (RedisResult[])db.ExecWithEtag("SET", "rizz", "buzz");
            var expectedEtag = 1;
            CheckEtagAndNullValue(results, expectedEtag);

            // update to value to update the etag
            results = (RedisResult[])db.ExecIfMatch(expectedEtag, "SET", "rizz", "fixx");
            expectedEtag++;
            CheckEtagAndNullValue(results, expectedEtag);

            // inplace update
            results = (RedisResult[])db.ExecWithEtag("SET", "rizz", "meow");
            expectedEtag++;
            CheckEtagAndNullValue(results, expectedEtag);

            // update to value to update the etag
            results = (RedisResult[])db.ExecIfMatch(expectedEtag, "SET", "rizz", "fooo");
            expectedEtag++;
            CheckEtagAndNullValue(results, expectedEtag);

            // Copy update
            results = (RedisResult[])db.ExecWithEtag("SET", "rizz", "oneofus");
            expectedEtag++;
            CheckEtagAndNullValue(results, expectedEtag);

            // now we should do a GET with etag and see the etag as 0
            var result = db.Execute("SET", ["rizz", "oneofus"]);
            ClassicAssert.AreEqual("OK", result.ToString());

            GetAndCheckEtagAndValue(db, "rizz", 0, "oneofus");
        }

        [Test]
        public void SetWithEtagOnAlreadyExistingSetDataOverridesItButUpdatesEtag()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var results = (RedisResult[])db.ExecWithEtag("SET", "rizz", "buzz");
            var expectedEtag = 1;
            CheckEtagAndNullValue(results, expectedEtag);

            // update to value to update the etag
            results = (RedisResult[])db.ExecIfMatch(expectedEtag, "SET", "rizz", "fixx");
            expectedEtag++;
            CheckEtagAndNullValue(results, expectedEtag);

            // inplace update
            results = (RedisResult[])db.ExecWithEtag("SET", "rizz", "meow");
            expectedEtag++;
            CheckEtagAndNullValue(results, expectedEtag);

            // update to value to update the etag
            results = (RedisResult[])db.ExecIfMatch(expectedEtag, "SET", "rizz", "fooo");
            expectedEtag++;
            CheckEtagAndNullValue(results, expectedEtag);

            // Copy update
            results = (RedisResult[])db.ExecWithEtag("SET", "rizz", "oneofus");
            expectedEtag++;
            CheckEtagAndNullValue(results, expectedEtag);
        }

        [Test]
        public void SetWithEtagOnAlreadyExistingNonEtagDataOverridesItToInitialEtag()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            ClassicAssert.IsTrue(db.StringSet("rizz", "used"));

            // inplace update
            var results = (RedisResult[])db.ExecWithEtag("SET", "rizz", "buzz");
            CheckEtagAndNullValue(results, 1);

            db.KeyDelete("rizz");

            ClassicAssert.IsTrue(db.StringSet("rizz", "my"));

            // Copy update
            results = (RedisResult[])db.ExecWithEtag("SET", "rizz", "some");
            CheckEtagAndNullValue(results, 1);
        }

        [Test]
        public void DelIfGreaterOnNonExistingKeyWorks()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var res = (RedisResult[])db.ExecIfGreater(10, "DEL", "nonexistingkey");
            CheckEtagAndNullValue(res, 0);
        }

        #endregion

        #region ETAG Apis with non-etag data

        [Test]
        public void SetOnAlreadyExistingNonEtagDataOverridesIt()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            ClassicAssert.IsTrue(db.StringSet("rizz", "used"));

            // inplace update
            var results = (RedisResult[])db.ExecWithEtag("SET", "rizz", "buzz");
            CheckEtagAndNullValue(results, 1);

            results = (RedisResult[])db.ExecWithEtag("SET", "rizz", "buzz");
            CheckEtagAndNullValue(results, 2);

            db.KeyDelete("rizz");

            ClassicAssert.IsTrue(db.StringSet("rizz", "my"));

            // Copy update
            results = (RedisResult[])db.ExecWithEtag("SET", "rizz", "some");
            CheckEtagAndNullValue(results, 1);
        }

        [Test]
        public void SetIfMatchOnNonEtagDataReturnsNewEtagAndNoValue()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var _ = db.StringSet("h", "k");

            var res = (RedisResult[])db.ExecIfMatch(0, "SET", "h", "t");
            ClassicAssert.IsTrue(res[0].IsNull);
            ClassicAssert.AreEqual("1", res[1].ToString());
        }

        [Test]
        public void GetIfNotMatchOnNonEtagDataReturnsNilForEtagAndCorrectData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var _ = db.StringSet("h", "k");

            var res = (RedisResult[])db.ExecIfNotMatch(1, "GET", "h");

            ClassicAssert.AreEqual("k", res[0].ToString());
            ClassicAssert.AreEqual("0", res[1].ToString());
        }

        [Test]
        public void GetWithEtagOnNonEtagDataReturns0ForEtagAndCorrectData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var _ = db.StringSet("h", "k");

            var res = (RedisResult[])db.ExecWithEtag("GET", "h");
            ClassicAssert.AreEqual("k", res[0].ToString());
            ClassicAssert.AreEqual("0", res[1].ToString());
        }

        #endregion

        #region Backwards Compatability Testing

        [Test]
        public void SingleEtagSetGet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var origValue = "abcdefg";
            db.ExecWithEtag("SET", "mykey", origValue);

            string retValue = db.StringGet("mykey");

            ClassicAssert.AreEqual(origValue, retValue);
        }

        [Test]
        public async Task SingleUnicodeEtagSetGetGarnetClient()
        {
            using var db = TestUtils.GetGarnetClient();
            db.Connect();

            var origValue = "笑い男";
            await db.ExecuteForStringArrayResultAsync("EXECWITHETAG", ["SET", "mykey", origValue])
                .ConfigureAwait(false);

            var retValue = await db.StringGetAsync("mykey").ConfigureAwait(false);

            ClassicAssert.AreEqual(origValue, retValue);
        }

        [Test]
        public async Task LargeEtagSetGet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const int length = (1 << 19) + 100;
            var value = new byte[length];

            for (var i = 0; i < length; i++)
                value[i] = (byte)((byte)'a' + ((byte)i % 26));

            var results = (RedisResult[])await db.ExecWithEtagAsync("SET", "mykey", value).ConfigureAwait(false);
            CheckEtagAndNullValue(results, 1);

            // Backwards compatibility of data set with etag and plain GET call
            var actualValue = (byte[])await db.StringGetAsync("mykey").ConfigureAwait(false);

            ClassicAssert.IsTrue(new ReadOnlySpan<byte>(value).SequenceEqual(new ReadOnlySpan<byte>(actualValue)));
        }

        [Test]
        public void SetExpiryForEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var origValue = "abcdefghij";

            // set with etag
            var results = (RedisResult[])db.ExecWithEtag("SET", "mykey", origValue, "EX", 2);
            CheckEtagAndNullValue(results, 1);

            string retValue = db.StringGet("mykey");
            ClassicAssert.AreEqual(origValue, retValue, "Get() before expiration");

            var actualDbSize = db.Execute("DBSIZE");
            ClassicAssert.AreEqual(1, (ulong)actualDbSize, "DBSIZE before expiration");

            var actualKeys = db.Execute("KEYS", ["*"]);
            ClassicAssert.AreEqual(1, ((RedisResult[])actualKeys).Length, "KEYS before expiration");

            var actualScan = db.Execute("SCAN", "0");
            ClassicAssert.AreEqual(1, ((RedisValue[])((RedisResult[])actualScan!)[1]).Length, "SCAN before expiration");

            Thread.Sleep(2500);

            retValue = db.StringGet("mykey");
            ClassicAssert.AreEqual(null, retValue, "Get() after expiration");

            actualDbSize = db.Execute("DBSIZE");
            ClassicAssert.AreEqual(0, (ulong)actualDbSize, "DBSIZE after expiration");

            actualKeys = db.Execute("KEYS", ["*"]);
            ClassicAssert.AreEqual(0, ((RedisResult[])actualKeys).Length, "KEYS after expiration");

            actualScan = db.Execute("SCAN", "0");
            ClassicAssert.AreEqual(0, ((RedisValue[])((RedisResult[])actualScan!)[1]).Length, "SCAN after expiration");
        }

        [Test]
        public void SetExpiryHighPrecisionForEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var origValue = "abcdeghijklmno";
            // set with etag
            var results = (RedisResult[])db.ExecWithEtag("SET", "mykey", origValue, "PX", 1900);
            CheckEtagAndNullValue(results, 1);

            string retValue = db.StringGet("mykey");
            ClassicAssert.AreEqual(origValue, retValue);

            Thread.Sleep(1000);
            retValue = db.StringGet("mykey");
            ClassicAssert.AreEqual(origValue, retValue);

            Thread.Sleep(2000);
            retValue = db.StringGet("mykey");
            ClassicAssert.AreEqual(null, retValue);
        }

        [Test]
        public void SetExpiryIncrForEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Key storing integer
            var nVal = -100000;
            var strKey = "key1";
            db.ExecWithEtag("SET", strKey, nVal);
            db.KeyExpire(strKey, TimeSpan.FromSeconds(5));

            string res1 = db.StringGet(strKey);

            var n = db.StringIncrement(strKey);

            // This should increase the ETAG internally so we have a check for that here
            var checkEtag = long.Parse(db.ExecWithEtag("GET", strKey)[1].ToString());
            ClassicAssert.AreEqual(2, checkEtag);

            string res = db.StringGet(strKey);
            var nRetVal = Convert.ToInt64(res);
            ClassicAssert.AreEqual(n, nRetVal);
            ClassicAssert.AreEqual(-99999, nRetVal);

            n = db.StringIncrement(strKey);

            // This should increase the ETAG internally so we have a check for that here
            checkEtag = long.Parse(db.ExecWithEtag("GET", strKey)[1].ToString());
            ClassicAssert.AreEqual(3, checkEtag);

            nRetVal = Convert.ToInt64(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);
            ClassicAssert.AreEqual(-99998, nRetVal);

            var res69 = db.KeyTimeToLive(strKey);

            Thread.Sleep(5000);

            // Expired key, restart increment,after exp this is treated as new record 
            n = db.StringIncrement(strKey);
            ClassicAssert.AreEqual(1, n);

            nRetVal = Convert.ToInt64(db.StringGet(strKey));
            ClassicAssert.AreEqual(1, nRetVal);

            var etagGet = (RedisResult[])db.ExecWithEtag("GET", strKey);
            // Etag will show up as 0 since the previous one had expired
            ClassicAssert.AreEqual(1, Convert.ToInt64(etagGet[0]));
            ClassicAssert.AreEqual("0", etagGet[1].ToString());
        }

        [Test]
        public void IncrDecrChangeDigitsWithExpiry()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var strKey = "key1";

            db.ExecWithEtag("SET", strKey, 9);

            var checkEtag = long.Parse(db.ExecWithEtag("GET", strKey)[1].ToString());
            ClassicAssert.AreEqual(1, checkEtag);

            db.KeyExpire(strKey, TimeSpan.FromSeconds(5));

            var n = db.StringIncrement(strKey);
            var nRetVal = Convert.ToInt64(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);
            ClassicAssert.AreEqual(10, nRetVal);

            checkEtag = long.Parse(db.ExecWithEtag("GET", strKey)[1].ToString());
            ClassicAssert.AreEqual(2, checkEtag);

            n = db.StringDecrement(strKey);
            nRetVal = Convert.ToInt64(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);
            ClassicAssert.AreEqual(9, nRetVal);

            checkEtag = long.Parse(db.ExecWithEtag("GET", strKey)[1].ToString());
            ClassicAssert.AreEqual(3, checkEtag);

            Thread.Sleep(TimeSpan.FromSeconds(5));

            var res = (string)db.StringGet(strKey);
            ClassicAssert.IsNull(res);
        }

        [Test]
        public void StringSetOnAnExistingEtagDataOverrides()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var strKey = "mykey";
            db.ExecWithEtag("SET", strKey, 9);

            var checkEtag = long.Parse(db.ExecWithEtag("GET", strKey)[1].ToString());
            ClassicAssert.AreEqual(1, checkEtag);

            // Unless the SET was called with WITHETAG a call to set will override the SET to a new
            // value altogether, this will make it lose it's etag capability. This is a limitation for Etags
            // because plain sets are upserts (blind updates), and currently we cannot increase the latency in
            // the common path for set to check beyong Readonly address for the existence of a record with ETag.
            // This means that sets are complete upserts and clients need to use setifmatch, or set with WITHETAG
            // if they want each consequent set to maintain the key value pair's etag property.
            ClassicAssert.IsTrue(db.StringSet(strKey, "ciaociao"));

            var retVal = db.StringGet(strKey).ToString();
            ClassicAssert.AreEqual("ciaociao", retVal);

            var res = (RedisResult[])db.ExecWithEtag("GET", strKey);
            ClassicAssert.AreEqual("ciaociao", res[0].ToString());
            ClassicAssert.AreEqual("0", res[1].ToString());
        }

        [Test]
        public void StringSetOnAnExistingEtagDataUpdatesEtagIfEtagRetain()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var strKey = "mykey";
            db.ExecWithEtag("SET", strKey, 9);

            var checkEtag = (long)db.ExecWithEtag("GET", strKey)[1];
            ClassicAssert.AreEqual(1, checkEtag);

            // Unless you explicitly call SET with WITHETAG option you will lose the etag on the previous key-value pair
            db.ExecWithEtag("SET", strKey, "ciaociao");

            var retVal = db.StringGet(strKey).ToString();
            ClassicAssert.AreEqual("ciaociao", retVal);

            var res = (RedisResult[])db.ExecWithEtag("GET", strKey);
            ClassicAssert.AreEqual(2, (long)res[1]);

            // on subsequent upserts we are still increasing the etag transparently
            db.ExecWithEtag("SET", strKey, "ciaociaociao");

            retVal = db.StringGet(strKey).ToString();
            ClassicAssert.AreEqual("ciaociaociao", retVal);

            res = (RedisResult[])db.ExecWithEtag("GET", strKey);
            ClassicAssert.AreEqual("ciaociaociao", res[0].ToString());
            ClassicAssert.AreEqual(3, (long)res[1]);
        }

        [Test]
        public void LockTakeReleaseOnAValueInitiallySet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "lock-key";
            var value = "lock-value";

            var results = (RedisResult[])db.ExecWithEtag("SET", key, value);
            CheckEtagAndNullValue(results, 1);

            var success = db.LockTake(key, value, TimeSpan.FromSeconds(100));
            ClassicAssert.IsFalse(success);

            success = db.LockRelease(key, value);
            ClassicAssert.IsTrue(success);

            success = db.LockRelease(key, value);
            ClassicAssert.IsFalse(success);

            success = db.LockTake(key, value, TimeSpan.FromSeconds(100));
            ClassicAssert.IsTrue(success);

            success = db.LockRelease(key, value);
            ClassicAssert.IsTrue(success);

            // Test auto-lock-release
            success = db.LockTake(key, value, TimeSpan.FromSeconds(1));
            ClassicAssert.IsTrue(success);

            Thread.Sleep(2000);
            success = db.LockTake(key, value, TimeSpan.FromSeconds(1));
            ClassicAssert.IsTrue(success);

            success = db.LockRelease(key, value);
            ClassicAssert.IsTrue(success);
        }

        [Test]
        [TestCase("key1", 1000)]
        [TestCase("key1", 0)]
        public void SingleDecrForEtagSetData(string strKey, int nVal)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Key storing integer
            var results = (RedisResult[])db.ExecWithEtag("SET", strKey, nVal);
            CheckEtagAndNullValue(results, 1);

            var n = db.StringDecrement(strKey);
            ClassicAssert.AreEqual(nVal - 1, n);

            GetAndCheckEtagAndValue(db, strKey, 2, n.ToString());
        }

        [Test]
        [TestCase(-1000, 100)]
        [TestCase(-1000, -9000)]
        [TestCase(-10000, 9000)]
        [TestCase(9000, 10000)]
        public void SingleDecrByForEtagSetData(long nVal, long nDecr)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            // Key storing integer val
            var strKey = "key1";
            var results = (RedisResult[])db.ExecWithEtag("SET", strKey, nVal);
            CheckEtagAndNullValue(results, 1);

            var n = db.StringDecrement(strKey, nDecr);

            GetAndCheckEtagAndValue(db, strKey, 2, n.ToString());
        }

        [Test]
        [TestCase(RespCommand.INCR)]
        [TestCase(RespCommand.DECR)]
        [TestCase(RespCommand.INCRBY)]
        [TestCase(RespCommand.DECRBY)]
        public void SimpleIncrementInvalidValueForEtagSetdata(RespCommand cmd)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            string[] values = ["", "7 3", "02+(34", "笑い男", "01", "-01", "7ab"];

            for (var i = 0; i < values.Length; i++)
            {
                var key = $"key{i}";
                var exception = false;
                var results = (RedisResult[])db.ExecWithEtag("SET", key, values[i]);
                CheckEtagAndNullValue(results, 1);

                try
                {
                    _ = cmd switch
                    {
                        RespCommand.INCR => db.StringIncrement(key),
                        RespCommand.DECR => db.StringDecrement(key),
                        RespCommand.INCRBY => db.StringIncrement(key, 10L),
                        RespCommand.DECRBY => db.StringDecrement(key, 10L),
                        _ => throw new Exception($"Command {cmd} not supported!"),
                    };
                }
                catch (Exception ex)
                {
                    exception = true;
                    var msg = ex.Message;
                    ClassicAssert.AreEqual("ERR value is not an integer or out of range.", msg);
                }
                ClassicAssert.IsTrue(exception);
            }
        }

        [Test]
        [TestCase(RespCommand.INCR)]
        [TestCase(RespCommand.DECR)]
        [TestCase(RespCommand.INCRBY)]
        [TestCase(RespCommand.DECRBY)]
        public void SimpleIncrementOverflowForEtagSetData(RespCommand cmd)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var exception = false;

            var key = "test";

            try
            {
                switch (cmd)
                {
                    case RespCommand.INCR:
                        _ = db.ExecWithEtag("SET", key, long.MaxValue);
                        _ = db.StringIncrement(key);
                        break;
                    case RespCommand.DECR:
                        _ = db.ExecWithEtag("SET", key, long.MinValue);
                        _ = db.StringDecrement(key);
                        break;
                    case RespCommand.INCRBY:
                        _ = db.ExecWithEtag("SET", key, 0);
                        _ = db.Execute("INCRBY", [key, ulong.MaxValue.ToString()]);
                        break;
                    case RespCommand.DECRBY:
                        _ = db.ExecWithEtag("SET", key, 0);
                        _ = db.Execute("DECRBY", [key, ulong.MaxValue.ToString()]);
                        break;
                }
            }
            catch (Exception ex)
            {
                exception = true;
                var msg = ex.Message;
                ClassicAssert.AreEqual("ERR value is not an integer or out of range.", msg);
            }
            ClassicAssert.IsTrue(exception);
        }

        [Test]
        [TestCase(0, 12.6)]
        [TestCase(12.6, 0)]
        [TestCase(10, 10)]
        [TestCase(910151, 0.23659)]
        [TestCase(663.12336412, 12342.3)]
        [TestCase(10, -110)]
        [TestCase(110, -110.234)]
        [TestCase(-2110.95255555, -110.234)]
        [TestCase(-2110.95255555, 100000.526654512219412)]
        [TestCase(double.MaxValue, double.MinValue)]
        public void SimpleIncrementByFloatForEtagSetData(double initialValue, double incrByValue)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key1";
            db.ExecWithEtag("SET", key, initialValue);

            var expectedResult = initialValue + incrByValue;

            var actualResultStr = (string)db.Execute("INCRBYFLOAT", [key, incrByValue]);
            var actualResultRawStr = db.StringGet(key);

            var actualResult = double.Parse(actualResultStr, CultureInfo.InvariantCulture);
            var actualResultRaw = double.Parse((string)actualResultRawStr, CultureInfo.InvariantCulture);

            Assert.That(actualResult, Is.EqualTo(expectedResult).Within(1.0 / Math.Pow(10, 15)));
            Assert.That(actualResult, Is.EqualTo(actualResultRaw).Within(1.0 / Math.Pow(10, 15)));

            var res = (RedisResult[])db.ExecWithEtag("GET", key);
            var value = double.Parse(res[0].ToString(), CultureInfo.InvariantCulture);
            var etag = (long)res[1];
            Assert.That(value, Is.EqualTo(actualResultRaw).Within(1.0 / Math.Pow(10, 15)));
            ClassicAssert.AreEqual(2, etag);
        }

        [Test]
        public void SingleDeleteForEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Key storing integer
            var nVal = 100;
            var strKey = "key1";
            db.ExecWithEtag("SET", strKey, nVal);
            db.KeyDelete(strKey);
            var retVal = Convert.ToBoolean(db.StringGet(strKey));
            ClassicAssert.AreEqual(retVal, false);
        }

        [Test]
        public void SingleDeleteWithObjectStoreDisabledForEtagSetData()
        {
            TearDown();

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disableObjects: true);
            server.Start();

            var key = "delKey";
            var value = "1234";
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.ExecWithEtag("SET", key, value);

            var resp = (string)db.StringGet(key);
            ClassicAssert.AreEqual(resp, value);

            var respDel = db.KeyDelete(key);
            ClassicAssert.IsTrue(respDel);

            respDel = db.KeyDelete(key);
            ClassicAssert.IsFalse(respDel);
        }

        [Test]
        public void SingleDeleteWithObjectStoreDisable_LTMForEtagSetData()
        {
            TearDown();

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, disableObjects: true);
            server.Start();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var keyCount = 5;
            var valLen = 256;
            var keyLen = 8;

            List<Tuple<string, string>> data = [];
            for (var i = 0; i < keyCount; i++)
            {
                data.Add(new Tuple<string, string>(TestUtils.GetRandomString(keyLen), TestUtils.GetRandomString(valLen)));
                var pair = data.Last();
                db.ExecWithEtag("SET", pair.Item1, pair.Item2);
            }


            for (var i = 0; i < keyCount; i++)
            {
                var pair = data[i];

                var resp = (string)db.StringGet(pair.Item1);
                ClassicAssert.AreEqual(resp, pair.Item2);

                var respDel = db.KeyDelete(pair.Item1);
                resp = (string)db.StringGet(pair.Item1);
                ClassicAssert.IsNull(resp);

                respDel = db.KeyDelete(pair.Item2);
                ClassicAssert.IsFalse(respDel);
            }
        }

        [Test]
        public void MultiKeyDeleteForEtagSetData([Values] bool withoutObjectStore)
        {
            if (withoutObjectStore)
            {
                TearDown();
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disableObjects: true);
                server.Start();
            }

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var keyCount = 10;
            var valLen = 16;
            var keyLen = 8;

            List<Tuple<string, string>> data = [];
            for (var i = 0; i < keyCount; i++)
            {
                data.Add(new Tuple<string, string>(TestUtils.GetRandomString(keyLen), TestUtils.GetRandomString(valLen)));
                var pair = data.Last();
                db.ExecWithEtag("SET", pair.Item1, pair.Item2);
            }

            var keys = data.Select(x => (RedisKey)x.Item1).ToArray();
            var keysDeleted = db.KeyDeleteAsync(keys);
            keysDeleted.Wait();
            ClassicAssert.AreEqual(keysDeleted.Result, 10);

            var keysDel = db.KeyDelete(keys);
            ClassicAssert.AreEqual(keysDel, 0);
        }

        [Test]
        public void MultiKeyUnlinkForEtagSetData([Values] bool withoutObjectStore)
        {
            if (withoutObjectStore)
            {
                TearDown();
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disableObjects: true);
                server.Start();
            }

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var keyCount = 10;
            var valLen = 16;
            var keyLen = 8;

            List<Tuple<string, string>> data = [];
            for (var i = 0; i < keyCount; i++)
            {
                data.Add(new Tuple<string, string>(TestUtils.GetRandomString(keyLen), TestUtils.GetRandomString(valLen)));
                var pair = data.Last();
                db.ExecWithEtag("SET", pair.Item1, pair.Item2);
            }

            var keys = data.Select(x => (object)x.Item1).ToArray();
            var keysDeleted = (string)db.Execute("unlink", keys);
            ClassicAssert.AreEqual(10, int.Parse(keysDeleted));

            keysDeleted = (string)db.Execute("unlink", keys);
            ClassicAssert.AreEqual(0, int.Parse(keysDeleted));
        }

        [Test]
        public void SingleExistsForEtagSetData([Values] bool withoutObjectStore)
        {
            if (withoutObjectStore)
            {
                TearDown();
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disableObjects: true);
                server.Start();
            }
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Key storing integer
            var nVal = 100;
            var strKey = "key1";
            ClassicAssert.IsFalse(db.KeyExists(strKey));

            db.ExecWithEtag("SET", strKey, nVal);

            var fExists = db.KeyExists("key1", CommandFlags.None);
            ClassicAssert.AreEqual(fExists, true);

            fExists = db.KeyExists("key2", CommandFlags.None);
            ClassicAssert.AreEqual(fExists, false);
        }


        [Test]
        public void MultipleExistsKeysAndObjectsAndEtagData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var count = db.ListLeftPush("listKey", ["a", "b", "c", "d"]);
            ClassicAssert.AreEqual(4, count);

            var zaddItems = db.SortedSetAdd("zset:test", [new SortedSetEntry("a", 1), new SortedSetEntry("b", 2)]);
            ClassicAssert.AreEqual(2, zaddItems);

            db.StringSet("foo", "bar");

            db.ExecWithEtag("SET", "rizz", "bar");

            var exists = db.KeyExists(["key", "listKey", "zset:test", "foo", "rizz"]);
            ClassicAssert.AreEqual(4, exists);
        }

        #region RENAME


        [Test]
        public void RenameEtagTests()
        {
            // old key had etag => new key zero'd etag when made without withetag (new key did not exists)
            // old key had etag => new key zero'd etag when made without withetag (new key exists without etag)
            // old key had etag => new key has updated etag when made with withetag (new key exists withetag)
            // old key not have etag => new key made with updated etag when made withetag (new key did exist withetag)
            // old key had etag and, new key has initial etag when made with withetag (new key did not exists)
            // old key not have etag and, new key made with initial etag when made withetag (new key did not exist)
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var origValue = "test1";
            var oldKey = "key1";
            var newKey = "key2";

            // Scenario: old key had etag and => new key zero'd etag when made without withetag (new key did not exists)

            db.ExecWithEtag("SET", oldKey, origValue);
            GetAndCheckEtagAndValue(db, oldKey, 1, origValue);

            db.KeyRename(oldKey, newKey);

            ClassicAssert.IsTrue(db.StringGet(oldKey).IsNull);
            GetAndCheckEtagAndValue(db, newKey, 0, origValue);
            // old key has been deleted, and new key exists without etag at this point

            // Scenario: old key had etag => new key zero'd etag when made without withetag (new key exists without etag)
            db.ExecWithEtag("SET", oldKey, origValue);

            db.KeyRename(oldKey, newKey);

            ClassicAssert.IsTrue(db.StringGet(oldKey).IsNull);
            GetAndCheckEtagAndValue(db, newKey, 0, origValue);
            db.KeyDelete(newKey);

            // Scenario: old key had etag => new key has updated etag when made with withetag (new key exists withetag)
            // setup new key with updated etag
            db.ExecWithEtag("SET", newKey, origValue + "delta");
            GetAndCheckEtagAndValue(db, newKey, 1, origValue + "delta");

            db.ExecIfMatch(1, "SET", newKey, origValue); // updates etag to 2
            GetAndCheckEtagAndValue(db, newKey, 2, origValue);

            // old key with etag
            db.ExecWithEtag("SET", oldKey, origValue);
            GetAndCheckEtagAndValue(db, oldKey, 1, origValue);

            db.ExecWithEtag("RENAME", oldKey, newKey); // should update etag to 3

            ClassicAssert.IsTrue(db.StringGet(oldKey).IsNull);
            GetAndCheckEtagAndValue(db, newKey, 3, origValue);
            // at this point new key exists with etag, old key does not exist at all

            // Scenario: old key not have etag => new key made with updated etag when made withetag (new key did exist withetag)
            db.StringSet(oldKey, origValue);

            db.ExecWithEtag("RENAME", oldKey, newKey);

            ClassicAssert.IsTrue(db.StringGet(oldKey).IsNull);
            GetAndCheckEtagAndValue(db, newKey, 4, origValue);
            db.KeyDelete(newKey);

            // Scenario: old key had etag => new key has initial etag when made with withetag (new key did not exists)
            db.ExecWithEtag("SET", oldKey, origValue);

            db.ExecWithEtag("RENAME", oldKey, newKey);

            ClassicAssert.IsTrue(db.StringGet(oldKey).IsNull);
            GetAndCheckEtagAndValue(db, newKey, 1, origValue);
            db.KeyDelete(newKey);

            // Scenario: old key not have etag => new key made with initial etag when made withetag (new key did not exist)
            db.StringSet(oldKey, origValue);

            db.ExecWithEtag("RENAME", oldKey, newKey);

            ClassicAssert.IsTrue(db.StringGet(oldKey).IsNull);
            GetAndCheckEtagAndValue(db, newKey, 1, origValue);
            db.KeyDelete(newKey);
        }

        private void GetAndCheckEtagAndValue(IDatabase db, string key, long expectedEtag, string expectedValue)
        {
            var results = (RedisResult[])db.ExecWithEtag("GET", key);
            CheckEtagAndValue(results, expectedEtag, expectedValue);
        }

        private void CheckEtagAndValue(RedisResult[] results, long expectedEtag, long expectedValue)
        {
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(expectedValue, (long)results[0]);
            ClassicAssert.AreEqual(expectedEtag, (long)results[1]);
        }

        private void CheckEtagAndValue(RedisResult[] results, long expectedEtag, string expectedValue)
        {
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(expectedValue, (string)results[0]);
            ClassicAssert.AreEqual(expectedEtag, (long)results[1]);
        }

        private void CheckEtagAndNullValue(RedisResult[] results, long expectedEtag) =>
            CheckEtagAndValue(results, expectedEtag, expectedValue: null);

        #endregion

        [Test]
        public void PersistTTLTestForEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "expireKey";
            var val = "expireValue";
            var expire = 2;

            var ttl = db.Execute("TTL", key);
            ClassicAssert.AreEqual(-2, (long)ttl);

            db.ExecWithEtag("SET", key, val);
            ttl = db.Execute("TTL", key);
            ClassicAssert.AreEqual(-1, (long)ttl);

            db.KeyExpire(key, TimeSpan.FromSeconds(expire));

            var res = (RedisResult[])db.ExecWithEtag("GET", key);
            ClassicAssert.AreEqual(val, res[0].ToString());
            ClassicAssert.AreEqual(1, long.Parse(res[1].ToString()));

            var time = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(time.Value.TotalSeconds > 0);

            db.KeyExpire(key, TimeSpan.FromSeconds(expire));
            res = (RedisResult[])db.ExecWithEtag("GET", key);
            ClassicAssert.AreEqual(val, res[0].ToString());
            ClassicAssert.AreEqual(1, long.Parse(res[1].ToString()));

            db.KeyPersist(key);
            res = (RedisResult[])db.ExecWithEtag("GET", key);
            // unchanged etag
            ClassicAssert.AreEqual(val, res[0].ToString());
            ClassicAssert.AreEqual(1, long.Parse(res[1].ToString()));

            Thread.Sleep((expire + 1) * 1000);

            var _val = db.StringGet(key);
            ClassicAssert.AreEqual(val, _val.ToString());

            time = db.KeyTimeToLive(key);
            ClassicAssert.IsNull(time);

            res = (RedisResult[])db.ExecWithEtag("GET", key);
            // the tag was persisted along with data from persist despite previous TTL
            ClassicAssert.AreEqual(val, res[0].ToString());
            ClassicAssert.AreEqual(1, long.Parse(res[1].ToString()));
        }

        [Test]
        public void PersistTestForEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var expire = 100;
            var keyA = "keyA";
            db.ExecWithEtag("SET", keyA, keyA);

            var response = db.KeyPersist(keyA);
            ClassicAssert.IsFalse(response);

            db.KeyExpire(keyA, TimeSpan.FromSeconds(expire));
            var time = db.KeyTimeToLive(keyA);
            ClassicAssert.IsTrue(time.Value.TotalSeconds > 0);

            response = db.KeyPersist(keyA);
            ClassicAssert.IsTrue(response);

            time = db.KeyTimeToLive(keyA);
            ClassicAssert.IsTrue(time == null);

            var value = db.StringGet(keyA);
            ClassicAssert.AreEqual(value, keyA);

            var res = (RedisResult[])db.ExecWithEtag("GET", keyA);
            ClassicAssert.AreEqual(keyA, res[0].ToString());
            ClassicAssert.AreEqual(1, long.Parse(res[1].ToString()));

            var noKey = "noKey";
            response = db.KeyPersist(noKey);
            ClassicAssert.IsFalse(response);
        }

        [Test]
        [TestCase("EXPIRE")]
        [TestCase("PEXPIRE")]
        public void KeyExpireStringTestForEtagSetData(string command)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "keyA";
            db.ExecWithEtag("SET", key, key);

            var value = db.StringGet(key);
            ClassicAssert.AreEqual(key, (string)value);

            if (command.Equals("EXPIRE"))
                db.KeyExpire(key, TimeSpan.FromSeconds(1));
            else
                db.Execute(command, [key, 1000]);

            Thread.Sleep(1500);

            value = db.StringGet(key);
            ClassicAssert.AreEqual(null, (string)value);
        }

        [Test]
        [TestCase("EXPIRE")]
        [TestCase("PEXPIRE")]
        public void KeyExpireOptionsTestForEtagSetData(string command)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "keyA";
            object[] args = [key, 1000, ""];
            db.ExecWithEtag("SET", key, key);

            args[2] = "XX";// XX -- Set expiry only when the key has an existing expiry
            var resp = (bool)db.Execute($"{command}", args);
            ClassicAssert.IsFalse(resp);//XX return false no existing expiry

            args[2] = "NX";// NX -- Set expiry only when the key has no expiry
            resp = (bool)db.Execute($"{command}", args);
            ClassicAssert.IsTrue(resp);// NX return true no existing expiry

            args[2] = "NX";// NX -- Set expiry only when the key has no expiry
            resp = (bool)db.Execute($"{command}", args);
            ClassicAssert.IsFalse(resp);// NX return false existing expiry

            args[1] = 50;
            args[2] = "XX";// XX -- Set expiry only when the key has an existing expiry
            resp = (bool)db.Execute($"{command}", args);
            ClassicAssert.IsTrue(resp);// XX return true existing expiry
            var time = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(time.Value.TotalSeconds <= (double)((int)args[1]) && time.Value.TotalSeconds > 0);

            args[1] = 1;
            args[2] = "GT";// GT -- Set expiry only when the new expiry is greater than current one
            resp = (bool)db.Execute($"{command}", args);
            ClassicAssert.IsFalse(resp); // GT return false new expiry < current expiry

            args[1] = 1000;
            args[2] = "GT";// GT -- Set expiry only when the new expiry is greater than current one
            resp = (bool)db.Execute($"{command}", args);
            ClassicAssert.IsTrue(resp); // GT return true new expiry > current expiry
            time = db.KeyTimeToLive(key);

            if (command.Equals("EXPIRE"))
                ClassicAssert.IsTrue(time.Value.TotalSeconds > 500);
            else
                ClassicAssert.IsTrue(time.Value.TotalMilliseconds > 500);

            args[1] = 2000;
            args[2] = "LT";// LT -- Set expiry only when the new expiry is less than current one
            resp = (bool)db.Execute($"{command}", args);
            ClassicAssert.IsFalse(resp); // LT return false new expiry > current expiry

            args[1] = 15;
            args[2] = "LT";// LT -- Set expiry only when the new expiry is less than current one
            resp = (bool)db.Execute($"{command}", args);
            ClassicAssert.IsTrue(resp); // LT return true new expiry < current expiry
            time = db.KeyTimeToLive(key);

            if (command.Equals("EXPIRE"))
                ClassicAssert.IsTrue(time.Value.TotalSeconds <= (double)((int)args[1]) && time.Value.TotalSeconds > 0);
            else
                ClassicAssert.IsTrue(time.Value.TotalMilliseconds <= (double)((int)args[1]) && time.Value.TotalMilliseconds > 0);
        }

        [Test]
        public void MainObjectKeyForEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var server = redis.GetServers()[0];
            var db = redis.GetDatabase(0);

            const string key = "test:1";

            var results = (RedisResult[])db.ExecWithEtag("SET", key, "v1");
            CheckEtagAndNullValue(results, 1);

            // Do SetAdd using the same key, expected error
            Assert.Throws<RedisServerException>(() => db.SetAdd(key, "v2"),
                Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE));

            // One key "test:1" with a string value is expected
            var keys = server.Keys(db.Database, key).ToList();
            ClassicAssert.AreEqual(1, keys.Count);
            ClassicAssert.AreEqual(key, (string)keys[0]);
            var value = db.StringGet(key);
            ClassicAssert.AreEqual("v1", (string)value);

            // do ListRightPush using the same key, expected error
            Assert.Throws<RedisServerException>(() => db.ListRightPush(key, "v3"), Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE));
        }

        [Test]
        public void GetSliceTestForEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "rangeKey";
            var value = "0123456789";

            var resp = (string)db.StringGetRange(key, 2, 10);
            ClassicAssert.AreEqual(string.Empty, resp);

            var results = (RedisResult[])db.ExecWithEtag("SET", key, value);
            CheckEtagAndNullValue(results, 1);

            //0,0
            resp = (string)db.StringGetRange(key, 0, 0);
            ClassicAssert.AreEqual("0", resp);

            //actual value
            resp = (string)db.StringGetRange(key, 0, -1);
            ClassicAssert.AreEqual(value, resp);

            #region testA
            //s[2,len] s < e & e = len
            resp = (string)db.StringGetRange(key, 2, 10);
            ClassicAssert.AreEqual(value.Substring(2), resp);

            //s[2,len] s < e & e = len - 1
            resp = (string)db.StringGetRange(key, 2, 9);
            ClassicAssert.AreEqual(value.Substring(2), resp);

            //s[2,len] s < e < len
            resp = (string)db.StringGetRange(key, 2, 5);
            ClassicAssert.AreEqual(value.Substring(2, 4), resp);

            //s[2,len] s < len < e
            resp = (string)db.StringGetRange(key, 2, 15);
            ClassicAssert.AreEqual(value.Substring(2), resp);

            //s[4,len] e < s < len
            resp = (string)db.StringGetRange(key, 4, 2);
            ClassicAssert.AreEqual("", resp);

            //s[4,len] e < 0 < s < len
            resp = (string)db.StringGetRange(key, 4, -2);
            ClassicAssert.AreEqual(value.Substring(4, 5), resp);

            //s[4,len] e < -len < 0 < s < len
            resp = (string)db.StringGetRange(key, 4, -12);
            ClassicAssert.AreEqual("", resp);
            #endregion

            #region testB
            //-len < s < 0 < len < e
            resp = (string)db.StringGetRange(key, -4, 15);
            ClassicAssert.AreEqual(value.Substring(6, 4), resp);

            //-len < s < 0 < e < len where len + s > e
            resp = (string)db.StringGetRange(key, -4, 5);
            ClassicAssert.AreEqual("", resp);

            //-len < s < 0 < e < len where len + s < e
            resp = (string)db.StringGetRange(key, -4, 8);
            ClassicAssert.AreEqual(value.Substring(value.Length - 4, 2), resp);

            //-len < s < e < 0
            resp = (string)db.StringGetRange(key, -4, -1);
            ClassicAssert.AreEqual(value.Substring(value.Length - 4, 4), resp);

            //-len < e < s < 0
            resp = (string)db.StringGetRange(key, -4, -7);
            ClassicAssert.AreEqual("", resp);
            #endregion

            //range start > end > len
            resp = (string)db.StringGetRange(key, 17, 13);
            ClassicAssert.AreEqual("", resp);

            //range 0 > start > end
            resp = (string)db.StringGetRange(key, -1, -4);
            ClassicAssert.AreEqual("", resp);

            //equal offsets
            resp = db.StringGetRange(key, 4, 4);
            ClassicAssert.AreEqual("4", resp);

            //equal offsets
            resp = db.StringGetRange(key, -4, -4);
            ClassicAssert.AreEqual("6", resp);

            //equal offsets
            resp = db.StringGetRange(key, -100, -100);
            ClassicAssert.AreEqual("0", resp);

            //equal offsets
            resp = db.StringGetRange(key, -101, -101);
            ClassicAssert.AreEqual("9", resp);

            //start larger than end
            resp = db.StringGetRange(key, -1, -3);
            ClassicAssert.AreEqual("", resp);

            //2,-1 -> 2 9
            var negend = -1;
            resp = db.StringGetRange(key, 2, negend);
            ClassicAssert.AreEqual(value.Substring(2, 8), resp);

            //2,-3 -> 2 7
            negend = -3;
            resp = db.StringGetRange(key, 2, negend);
            ClassicAssert.AreEqual(value.Substring(2, 6), resp);

            //-5,-3 -> 5,7
            var negstart = -5;
            resp = db.StringGetRange(key, negstart, negend);
            ClassicAssert.AreEqual(value.Substring(5, 3), resp);
        }

        [Test]
        public void SetRangeTestForEtagSetData([Values] RevivificationMode revivificationModeUsedBySetupOnly)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "setRangeKey";
            var value = "0123456789";
            var newValue = "ABCDE";

            db.ExecWithEtag("SET", key, value);

            var resp = db.StringGet(key);
            ClassicAssert.AreEqual("0123456789", resp.ToString());

            // new key, length 10, offset 5 -> 15 ("\0\0\0\0\00123456789")
            resp = db.StringSetRange(key, 5, value);
            ClassicAssert.AreEqual("15", resp.ToString());
            resp = db.StringGet(key);
            ClassicAssert.AreEqual("012340123456789", resp.ToString());

            // should update the etag internally
            var updatedEtagRes = db.ExecWithEtag("GET", key);
            ClassicAssert.AreEqual(2, long.Parse(updatedEtagRes[1].ToString()));

            ClassicAssert.IsTrue(db.KeyDelete(key));

            // new key, length 10, offset -1 -> RedisServerException ("ERR offset is out of range")
            var ex = Assert.Throws<RedisServerException>(() => db.StringSetRange(key, -1, value));
            ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_OFFSETOUTOFRANGE), ex.Message);

            // existing key, length 10, offset 0, value length 5 -> 10 ("ABCDE56789")
            db.ExecWithEtag("SET", key, value);

            resp = db.StringSetRange(key, 0, newValue);
            ClassicAssert.AreEqual("10", resp.ToString());
            resp = db.StringGet(key);
            ClassicAssert.AreEqual("ABCDE56789", resp.ToString());

            // should update the etag internally
            updatedEtagRes = db.ExecWithEtag("GET", key);
            ClassicAssert.AreEqual(2, long.Parse(updatedEtagRes[1].ToString()));

            ClassicAssert.IsTrue(db.KeyDelete(key));

            // key, length 10, offset 5, value length 5 -> 10 ("01234ABCDE")
            db.ExecWithEtag("SET", key, value);

            resp = db.StringSetRange(key, 5, newValue);
            ClassicAssert.AreEqual("10", resp.ToString());

            updatedEtagRes = db.ExecWithEtag("GET", key);
            ClassicAssert.AreEqual(2, long.Parse(updatedEtagRes[1].ToString()));

            resp = db.StringGet(key);
            ClassicAssert.AreEqual("01234ABCDE", resp.ToString());
            ClassicAssert.IsTrue(db.KeyDelete(key));

            // existing key, length 10, offset 10, value length 5 -> 15 ("0123456789ABCDE")
            db.ExecWithEtag("SET", key, value);
            resp = db.StringSetRange(key, 10, newValue);
            ClassicAssert.AreEqual("15", resp.ToString());
            resp = db.StringGet(key);
            ClassicAssert.AreEqual("0123456789ABCDE", resp.ToString());
            ClassicAssert.IsTrue(db.KeyDelete(key));

            // existing key, length 10, offset 15, value length 5 -> 20 ("0123456789\0\0\0\0\0ABCDE")
            db.ExecWithEtag("SET", key, value);

            resp = db.StringSetRange(key, 15, newValue);
            ClassicAssert.AreEqual("20", resp.ToString());
            resp = db.StringGet(key);
            ClassicAssert.AreEqual("0123456789\0\0\0\0\0ABCDE", resp.ToString());
            ClassicAssert.IsTrue(db.KeyDelete(key));

            // existing key, length 10, offset -1, value length 5 -> RedisServerException ("ERR offset is out of range")
            db.ExecWithEtag("SET", key, value);

            ex = Assert.Throws<RedisServerException>(() => db.StringSetRange(key, -1, newValue));
            ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_OFFSETOUTOFRANGE), ex.Message);
        }

        [Test]
        public void KeepTtlTestForDataInitiallySet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var expire = 3;
            var keyA = "keyA";
            var keyB = "keyB";
            db.Execute("SET", [keyA, keyA]);
            db.Execute("SET", [keyB, keyB]);

            db.KeyExpire(keyA, TimeSpan.FromSeconds(expire));
            db.KeyExpire(keyB, TimeSpan.FromSeconds(expire));

            db.StringSet(keyA, keyA, null, keepTtl: true);
            var time = db.KeyTimeToLive(keyA);
            ClassicAssert.IsTrue(time.Value.Ticks > 0);

            db.StringSet(keyB, keyB, null, keepTtl: false);
            time = db.KeyTimeToLive(keyB);
            ClassicAssert.IsTrue(time == null);

            Thread.Sleep(expire * 1000 + 100);

            string value = db.StringGet(keyB);
            ClassicAssert.AreEqual(keyB, value);

            value = db.StringGet(keyA);
            ClassicAssert.AreEqual(null, value);
        }

        [Test]
        public void StrlenTestOnEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.ExecWithEtag("SET", "mykey", "foo bar");

            ClassicAssert.AreEqual(7, db.StringLength("mykey"));
            ClassicAssert.AreEqual(0, db.StringLength("nokey"));

            var etagToCheck = db.ExecWithEtag("GET", "mykey");
            ClassicAssert.AreEqual(1, long.Parse(etagToCheck[1].ToString()));
        }

        [Test]
        public void TTLTestMillisecondsForEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "myKey";
            var val = "myKeyValue";
            var expireTimeInMilliseconds = 3000;

            var pttl = db.Execute("PTTL", key);
            ClassicAssert.AreEqual(-2, (long)pttl);

            db.ExecWithEtag("SET", key, val);

            pttl = db.Execute("PTTL", key);
            ClassicAssert.AreEqual(-1, (long)pttl);

            db.KeyExpire(key, TimeSpan.FromMilliseconds(expireTimeInMilliseconds));

            //check TTL of the key in milliseconds
            pttl = db.Execute("PTTL", key);

            ClassicAssert.IsTrue(long.TryParse(pttl.ToString(), out var pttlInMs));
            ClassicAssert.IsTrue(pttlInMs > 0);

            db.KeyPersist(key);
            Thread.Sleep(expireTimeInMilliseconds);

            var _val = db.StringGet(key);
            ClassicAssert.AreEqual(val, _val.ToString());

            var ttl = db.KeyTimeToLive(key);
            ClassicAssert.IsNull(ttl);

            // nothing should have affected the etag in the above commands
            var etagToCheck = long.Parse(((RedisResult[])db.ExecWithEtag("GET", key))[1].ToString());
            ClassicAssert.AreEqual(1, etagToCheck);
        }

        [Test]
        public void GetDelTestForEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "myKey";
            var val = "myKeyValue";

            // Key Setup
            db.ExecWithEtag("SET", key, val);

            var retval = db.StringGet(key);
            ClassicAssert.AreEqual(val, retval.ToString());

            retval = db.StringGetDelete(key);
            ClassicAssert.AreEqual(val, retval.ToString());

            // Try retrieving already deleted key
            retval = db.StringGetDelete(key);
            ClassicAssert.AreEqual(string.Empty, retval.ToString());

            // Try retrieving & deleting non-existent key
            retval = db.StringGetDelete("nonExistentKey");
            ClassicAssert.AreEqual(string.Empty, retval.ToString());

            // Key setup with metadata
            key = "myKeyWithMetadata";
            val = "myValueWithMetadata";

            db.ExecWithEtag("SET", key, val);
            db.KeyExpire(key, TimeSpan.FromSeconds(10000));

            retval = db.StringGet(key);
            ClassicAssert.AreEqual(val, retval.ToString());

            retval = db.StringGetDelete(key);
            ClassicAssert.AreEqual(val, retval.ToString());

            // Try retrieving already deleted key with metadata
            retval = db.StringGetDelete(key);
            ClassicAssert.AreEqual(string.Empty, retval.ToString());
        }

        [Test]
        public void AppendTestForEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "myKey";
            var val = "myKeyValue";
            var val2 = "myKeyValue2";

            db.ExecWithEtag("SET", key, val);

            var len = db.StringAppend(key, val2);
            ClassicAssert.AreEqual(val.Length + val2.Length, len);

            GetAndCheckEtagAndValue(db, key, 2, val + val2);

            db.KeyDelete(key);

            // Test appending an empty string
            db.ExecWithEtag("SET", key, val);

            var len1 = db.StringAppend(key, "");
            ClassicAssert.AreEqual(val.Length, len1);

            // we appended nothing so etag remains 1
            GetAndCheckEtagAndValue(db, key, 1, val);

            // Test appending to a non-existent key
            var nonExistentKey = "nonExistentKey";
            var len2 = db.StringAppend(nonExistentKey, val2);
            ClassicAssert.AreEqual(val2.Length, len2);

            var _val = db.StringGet(nonExistentKey);
            ClassicAssert.AreEqual(val2, _val.ToString());

            db.KeyDelete(key);

            // Test appending to a key with a large value
            var largeVal = new string('a', 1000000);
            db.StringSet(key, largeVal);
            db.ExecWithEtag("SET", key, largeVal);
            var len3 = db.StringAppend(key, val2);
            ClassicAssert.AreEqual(largeVal.Length + val2.Length, len3);

            GetAndCheckEtagAndValue(db, key, 2, largeVal + val2);

            // Test appending to a key with metadata
            var keyWithMetadata = "keyWithMetadata";
            db.ExecWithEtag("SET", keyWithMetadata, val);
            db.KeyExpire(keyWithMetadata, TimeSpan.FromSeconds(10000));
            var time = db.KeyTimeToLive(keyWithMetadata);
            ClassicAssert.Less(0, time!.Value.TotalSeconds);

            GetAndCheckEtagAndValue(db, keyWithMetadata, 1, val);

            var len4 = db.StringAppend(keyWithMetadata, val2);
            ClassicAssert.AreEqual(val.Length + val2.Length, len4);

            time = db.KeyTimeToLive(keyWithMetadata);
            ClassicAssert.Less(0, time!.Value.TotalSeconds);

            GetAndCheckEtagAndValue(db, keyWithMetadata, 2, val + val2);
        }

        [Test]
        public void SetBitOperationsOnEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "miki";
            // 64 BIT BITMAP
            var initialBitmap = new byte[8];
            var bitMapAsStr = Encoding.UTF8.GetString(initialBitmap); ;

            db.ExecWithEtag("SET", key, bitMapAsStr);

            var setbits = db.StringBitCount(key);
            ClassicAssert.AreEqual(0, setbits);

            var etagToCheck = long.Parse(((RedisResult[])db.ExecWithEtag("GET", key))[1].ToString());
            ClassicAssert.AreEqual(1, etagToCheck);

            // set all 64 bits one by one 
            long expectedBitCount = 0;
            long expectedEtag = 1;
            for (var i = 0; i < 64; i++)
            {
                // SET the ith bit in the bitmap 
                var originalValAtBit = db.StringSetBit(key, i, true);
                ClassicAssert.IsFalse(originalValAtBit);

                expectedBitCount++;
                expectedEtag++;

                var currentBitVal = db.StringGetBit(key, i);
                ClassicAssert.IsTrue(currentBitVal);

                setbits = db.StringBitCount(key);
                ClassicAssert.AreEqual(expectedBitCount, setbits);

                // Use BitPosition to find the first set bit
                var firstSetBitPosition = db.StringBitPosition(key, true);
                ClassicAssert.AreEqual(0, firstSetBitPosition); // As we are setting bits in order, first set bit should be 0

                // find the first unset bit
                var firstUnsetBitPos = db.StringBitPosition(key, false);
                long firstUnsetBitPosExpected = i == 63 ? -1 : i + 1;
                ClassicAssert.AreEqual(firstUnsetBitPosExpected, firstUnsetBitPos); // As we are setting bits in order, first unset bit should be 1 ahead


                // with each bit set that we do, we are increasing the etag as well by 1
                etagToCheck = long.Parse(((RedisResult[])db.ExecWithEtag("GET", key))[1].ToString());
                ClassicAssert.AreEqual(expectedEtag, etagToCheck);
            }

            // unset all 64 bits one by one in reverse order
            for (var i = 63; i > -1; i--)
            {
                var originalValAtBit = db.StringSetBit(key, i, false);
                ClassicAssert.IsTrue(originalValAtBit);

                expectedEtag++;
                expectedBitCount--;

                var currentBitVal = db.StringGetBit(key, i);
                ClassicAssert.IsFalse(currentBitVal);

                setbits = db.StringBitCount(key);
                ClassicAssert.AreEqual(expectedBitCount, setbits);

                // find the first set bit
                var firstSetBit = db.StringBitPosition(key, true);
                long expectedSetBit = i == 0 ? -1 : 0;
                ClassicAssert.AreEqual(expectedSetBit, firstSetBit);

                // Use BitPosition to find the first unset bit
                var firstUnsetBitPosition = db.StringBitPosition(key, false);
                ClassicAssert.AreEqual(i, firstUnsetBitPosition); // After unsetting, the first unset bit should be i

                etagToCheck = long.Parse(((RedisResult[])db.ExecWithEtag("GET", key))[1].ToString());
                ClassicAssert.AreEqual(expectedEtag, etagToCheck);
            }
        }

        [Test]
        public void BitFieldSetGetOnEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mewo";

            // Arrange - Set an 8-bit unsigned value at offset 0
            db.ExecWithEtag("SET", key, Encoding.UTF8.GetString(new byte[1])); // Initialize key with an empty byte

            // Act - Set value to 127 (binary: 01111111)
            db.Execute("BITFIELD", key, "SET", "u8", "0", "127");

            var etagToCheck = long.Parse(((RedisResult[])db.ExecWithEtag("GET", key))[1].ToString());
            ClassicAssert.AreEqual(2, etagToCheck);

            // Get value back
            var getResult = (RedisResult[])db.Execute("BITFIELD", key, "GET", "u8", "0");

            // Assert
            ClassicAssert.AreEqual(127, (long)getResult[0]); // Ensure the value set was retrieved correctly
        }

        [Test]
        public void BitFieldIncrementWithWrapOverflowOnEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mewo";

            // Arrange - Set an 8-bit unsigned value at offset 0
            db.ExecWithEtag("SET", key, Encoding.UTF8.GetString(new byte[1])); // Initialize key with an empty byte

            // Act - Set initial value to 255 and try to increment by 1
            db.Execute("BITFIELD", key, "SET", "u8", "0", "255");
            var etagToCheck = long.Parse(((RedisResult[])db.ExecWithEtag("GET", key))[1].ToString());
            ClassicAssert.AreEqual(2, etagToCheck);

            var incrResult = db.Execute("BITFIELD", key, "INCRBY", "u8", "0", "1");

            etagToCheck = long.Parse(((RedisResult[])db.ExecWithEtag("GET", key))[1].ToString());
            ClassicAssert.AreEqual(3, etagToCheck);

            // Assert
            ClassicAssert.AreEqual(0, (long)incrResult); // Should wrap around and return 0
        }

        [Test]
        public void BitFieldIncrementWithSaturateOverflowOnEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mewo";

            // Arrange - Set an 8-bit unsigned value at offset 0
            db.ExecWithEtag("SET", key, Encoding.UTF8.GetString(new byte[1])); // Initialize key with an empty byte

            // Act - Set initial value to 250 and try to increment by 10 with saturate overflow
            var bitfieldRes = db.Execute("BITFIELD", key, "SET", "u8", "0", "250");
            ClassicAssert.AreEqual(0, (long)bitfieldRes);

            var result = (RedisResult[])db.ExecWithEtag("GET", key);
            var etagToCheck = long.Parse(result[1].ToString());
            ClassicAssert.AreEqual(2, etagToCheck);

            var incrResult = db.Execute("BITFIELD", key, "OVERFLOW", "SAT", "INCRBY", "u8", "0", "10");

            etagToCheck = long.Parse(((RedisResult[])db.ExecWithEtag("GET", key))[1].ToString());
            ClassicAssert.AreEqual(3, etagToCheck);

            // Assert
            ClassicAssert.AreEqual(255, (long)incrResult); // Should saturate at the max value of 255 for u8
        }

        [Test]
        public void HyperLogLogCommandsShouldReturnWrongTypeErrorForEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mewo";
            var key2 = "dude";

            _ = db.ExecWithEtag("SET", key, "mars");
            _ = db.ExecWithEtag("SET", key2, "marsrover");

            // TODO: This is RedisServerException in the InPlaceUpdater call, but GetRMWModifiedFieldInfo currently throws RedisConnectionException.
            // This can be different in CIs vs. locally.
            Assert.That(() => db.Execute("PFADD", [key, "woohoo"]),
                    Throws.TypeOf<RedisServerException>().With.Message.EndsWith(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE_HLL))
                    .Or.TypeOf<RedisConnectionException>());

            Assert.That(() => db.Execute("PFMERGE", [key, key2]),
                    Throws.TypeOf<RedisServerException>().With.Message.EndsWith(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE_HLL))
                    .Or.TypeOf<RedisConnectionException>());
        }

        [Test]
        public void SetWithEtagOnANewUpsertWillCreateKeyValueWithoutEtag()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mickey";
            var val = "mouse";

            // a new upsert on a non-existing key will retain the "nil" etag
            db.ExecWithEtag("SET", key, val).ToString();

            var res = (RedisResult[])db.ExecWithEtag("GET", key);
            var value = res[0].ToString();
            var etag = res[1];

            ClassicAssert.AreEqual("1", etag.ToString());
            ClassicAssert.AreEqual(val, value);

            var newval = "clubhouse";

            // a new upsert on an existing key will reset the etag on the key
            db.Execute("SET", [key, newval]).ToString();
            res = (RedisResult[])db.ExecWithEtag("GET", key);
            value = res[0].ToString();
            etag = res[1];

            ClassicAssert.AreEqual("0", etag.ToString());
            ClassicAssert.AreEqual(newval, value);
        }

        #endregion
    }
}