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

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class RespETagTests : AllureTestBase
    {
        private GarnetServer server;
        private Random r;

        [SetUp]
        public void Setup()
        {
            r = new Random(674386);
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
        public void SETReturnsEtagForNewData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);
            RedisResult res = db.Execute("SETWITHETAG", ["rizz", "buzz"]);
            long etag = long.Parse(res.ToString());
            ClassicAssert.AreEqual(1, etag);
        }

        [Test]
        public void SetWithEtagClearsTTLWhenNoExpiryProvided()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            // Set key with ETag and expiration
            var etag = long.Parse(db.Execute("SETWITHETAG", "mykey", "val1", "EX", "100").ToString());
            ClassicAssert.AreEqual(1, etag);

            // Confirm TTL exists
            var ttl = db.KeyTimeToLive("mykey");
            ClassicAssert.IsTrue(ttl.HasValue);

            // SETWITHETAG without EX/PX should clear the expiration
            etag = long.Parse(db.Execute("SETWITHETAG", "mykey", "val2").ToString());
            ClassicAssert.AreEqual(2, etag);

            // TTL should be cleared
            ttl = db.KeyTimeToLive("mykey");
            ClassicAssert.IsFalse(ttl.HasValue);
        }

        [Test]
        public void SetIfMatchReturnsNewValueAndEtagWhenEtagMatches()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            var key = "florida";
            RedisResult res = db.Execute("SETWITHETAG", [key, "one"]);
            long initalEtag = long.Parse(res.ToString());
            ClassicAssert.AreEqual(1, initalEtag);

            var incorrectEtag = 1738;
            RedisResult[] etagMismatchMsg = (RedisResult[])db.Execute("SETIFMATCH", [key, "nextone", incorrectEtag]);
            ClassicAssert.AreEqual("1", etagMismatchMsg[0].ToString());
            ClassicAssert.AreEqual("one", etagMismatchMsg[1].ToString());

            // set a bigger val
            RedisResult[] setIfMatchRes = (RedisResult[])db.Execute("SETIFMATCH", [key, "nextone", initalEtag]);

            long nextEtag = long.Parse(setIfMatchRes[0].ToString());
            var value = setIfMatchRes[1];

            ClassicAssert.AreEqual(2, nextEtag);
            ClassicAssert.IsTrue(value.IsNull);

            // set a bigger val
            setIfMatchRes = (RedisResult[])db.Execute("SETIFMATCH", [key, "nextnextone", nextEtag]);
            nextEtag = long.Parse(setIfMatchRes[0].ToString());
            value = setIfMatchRes[1];

            ClassicAssert.AreEqual(3, nextEtag);
            ClassicAssert.IsTrue(value.IsNull);

            // ETAGMISMATCH again
            res = db.Execute("SETIFMATCH", [key, "lastOne", incorrectEtag]);
            ClassicAssert.AreEqual(nextEtag.ToString(), res[0].ToString());
            ClassicAssert.AreEqual("nextnextone", res[1].ToString());

            // set a smaller val
            setIfMatchRes = (RedisResult[])db.Execute("SETIFMATCH", [key, "lastOne", nextEtag]);
            nextEtag = long.Parse(setIfMatchRes[0].ToString());
            value = setIfMatchRes[1];

            ClassicAssert.AreEqual(4, nextEtag);
            ClassicAssert.IsTrue(value.IsNull);

            // ETAGMISMATCH on data that never had an etag
            db.KeyDelete(key);
            db.StringSet(key, "one");
            res = db.Execute("SETIFMATCH", [key, "lastOne", incorrectEtag]);
            ClassicAssert.AreEqual("0", res[0].ToString());
            ClassicAssert.AreEqual("one", res[1].ToString());
        }

        [Test]
        public void SetIfMatchWorksWithExpiration()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            var key = "florida";
            // Scenario: Key existed before and had no expiration
            RedisResult res = db.Execute("SETWITHETAG", key, "one");
            long initalEtag = long.Parse(res.ToString());
            ClassicAssert.AreEqual(1, initalEtag);

            // expiration added
            long updatedEtagRes = long.Parse(db.Execute("SETIFMATCH", key, "nextone", 1, "EX", 100)[0].ToString());
            ClassicAssert.AreEqual(2, updatedEtagRes);

            // confirm expiration added -> TTL should exist
            var ttl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(ttl.HasValue);

            updatedEtagRes = long.Parse(db.Execute("SETIFMATCH", key, "nextoneeexpretained", updatedEtagRes)[0].ToString());
            ClassicAssert.AreEqual(3, updatedEtagRes);

            // TTL should be retained 
            ttl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(ttl.HasValue);

            db.KeyDelete(key); // cleanup

            // Scenario: Key existed before and had expiration
            res = db.Execute("SETWITHETAG", key, "one", "PX", 100000);

            // confirm expiration added -> TTL should exist
            ttl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(ttl.HasValue);

            // change value and retain expiration
            updatedEtagRes = long.Parse(db.Execute("SETIFMATCH", key, "nextone", 1)[0].ToString());
            ClassicAssert.AreEqual(2, updatedEtagRes);

            // TTL should be retained
            ttl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(ttl.HasValue);

            // change value and change expiration
            updatedEtagRes = long.Parse(db.Execute("SETIFMATCH", key, "nextoneeexpretained", 2, "EX", 100)[0].ToString());
            ClassicAssert.AreEqual(3, updatedEtagRes);

            db.KeyDelete(key); // cleanup
        }

        #endregion

        #region ETAG GET Happy Paths

        [Test]
        public void GetWithEtagReturnsValAndEtagForKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            var key = "florida";
            // Data that does not exist returns nil
            RedisResult nonExistingData = db.Execute("GETWITHETAG", [key]);
            ClassicAssert.IsTrue(nonExistingData.IsNull);

            // insert data
            var initEtag = db.Execute("SETWITHETAG", key, "hkhalid");
            ClassicAssert.AreEqual(1, long.Parse(initEtag.ToString()));

            RedisResult[] res = (RedisResult[])db.Execute("GETWITHETAG", [key]);
            long etag = long.Parse(res[0].ToString());
            string value = res[1].ToString();

            ClassicAssert.AreEqual(1, etag);
            ClassicAssert.AreEqual("hkhalid", value);
        }

        [Test]
        public void GetIfNotMatchReturnsDataWhenEtagDoesNotMatch()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            var key = "florida";
            // GetIfNotMatch on non-existing data will return null
            RedisResult nonExistingData = db.Execute("GETIFNOTMATCH", [key, 0]);
            ClassicAssert.IsTrue(nonExistingData.IsNull);

            // insert data 
            var _ = db.Execute("SETWITHETAG", key, "maximus");

            RedisResult[] noDataOnMatch = (RedisResult[])db.Execute("GETIFNOTMATCH", key, 1);
            ClassicAssert.AreEqual("1", noDataOnMatch[0].ToString());
            ClassicAssert.IsTrue(noDataOnMatch[1].IsNull);

            RedisResult[] res = (RedisResult[])db.Execute("GETIFNOTMATCH", [key, 2]);
            long etag = long.Parse(res[0].ToString());
            string value = res[1].ToString();

            ClassicAssert.AreEqual(1, etag);
            ClassicAssert.AreEqual("maximus", value);
        }
        [Test]
        public void SetIfGreaterWorksWithInitialETag()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            var key = "meow-key";
            var value = "m";

            RedisResult res = db.Execute("SETWITHETAG", key, value);
            ClassicAssert.AreEqual(1, (long)res);

            // not greater etag sent so we expect a higher etag returned
            RedisResult[] arrRes = (RedisResult[])db.Execute("SETIFGREATER", key, "diggity", 0);
            ClassicAssert.AreEqual(1, (long)arrRes[0]);
            ClassicAssert.AreEqual(value, arrRes[1].ToString());

            // greater etag sent so we expect the same etag returned
            var newValue = "meow";
            arrRes = (RedisResult[])db.Execute("SETIFGREATER", key, newValue, 2);
            ClassicAssert.AreEqual(2, (long)arrRes[0]);
            ClassicAssert.IsTrue(arrRes[1].IsNull);

            // shrink value size and send greater etag
            newValue = "m";
            arrRes = (RedisResult[])db.Execute("SETIFGREATER", key, newValue, 5);
            ClassicAssert.AreEqual(5, (long)arrRes[0]);
            ClassicAssert.IsTrue(arrRes[1].IsNull);
        }

        [Test]
        public void SetIfGreaterWorksWithoutInitialETag()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            var key = "meow-key";
            var value = "m";

            RedisResult res = db.Execute("SET", key, value);
            ClassicAssert.AreEqual("OK", res.ToString());

            // not greater etag sent so we expect the actual etag returned
            RedisResult[] arrRes = (RedisResult[])db.Execute("SETIFGREATER", key, "check", 0);
            ClassicAssert.AreEqual(0, (long)arrRes[0]);
            ClassicAssert.AreEqual(value, arrRes[1].ToString());

            // greater etag sent so we expect the same etag returned
            var newValue = "meow";
            arrRes = (RedisResult[])db.Execute("SETIFGREATER", key, newValue, 2);
            ClassicAssert.AreEqual(2, (long)arrRes[0]);
            ClassicAssert.IsTrue(arrRes[1].IsNull);

            // shrink value size and send greater etag
            newValue = "m";
            arrRes = (RedisResult[])db.Execute("SETIFGREATER", key, newValue, 5);
            ClassicAssert.AreEqual(5, (long)arrRes[0]);
            ClassicAssert.IsTrue(arrRes[1].IsNull);
        }

        #endregion

        #region ETAG DEL Happy Paths

        [Test]
        public void DelIfGreaterOnAnAlreadyExistingKeyWithEtagWorks()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            var key = "meow-key";
            var value = "m";

            RedisResult res = db.Execute("SETWITHETAG", key, value);
            ClassicAssert.AreEqual(1, (long)res);

            // does not delete when called with lesser or equal etag
            res = db.Execute("DELIFGREATER", key, 0);
            ClassicAssert.AreEqual(0, (long)res);

            RedisValue returnedval = db.StringGet(key);
            ClassicAssert.AreEqual(value, returnedval.ToString());

            // Deletes when called with higher etag
            res = db.Execute("DELIFGREATER", key, 2);
            ClassicAssert.AreEqual(1, (long)res);

            returnedval = db.StringGet(key);
            ClassicAssert.IsTrue(returnedval.IsNull);
        }

        [Test]
        public void DelIfGreaterOnAnAlreadyExistingKeyWithoutEtagWorks()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            var key = "meow-key";
            var value = "m";

            bool result = db.StringSet(key, value);
            ClassicAssert.IsTrue(result);

            // does not delete when called with lesser or equal etag
            RedisResult res = db.Execute("DELIFGREATER", key, 0);
            ClassicAssert.AreEqual(0, (long)res);

            RedisValue returnedval = db.StringGet(key);
            ClassicAssert.AreEqual(value, returnedval.ToString());

            // Deletes when called with higher etag
            res = db.Execute("DELIFGREATER", key, 2);
            ClassicAssert.AreEqual(1, (long)res);

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

            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            IDatabase db = redis.GetDatabase(0);
            IServer garnetServer = redis.GetServer(TestUtils.EndPoint);

            string key = "rcuplease";
            string value = "havepatiencercushallbedonethisvalueisunnecssarilylongsoicanmakesureRCUdoesnotAllocateThismuch,anythinglesserthanthisisgoodenough";

            RedisResult res = db.Execute("SETWITHETAG", key, value);
            ClassicAssert.AreEqual(1, (long)res);

            StoreAddressInfo info = TestUtils.GetStoreAddressInfo(garnetServer);

            // now push the above key back all the way to stable region
            long prevTailAddr = info.TailAddress;
            MakeReadOnly(prevTailAddr, garnetServer, db);

            info = TestUtils.GetStoreAddressInfo(garnetServer);

            // The first record inserted (key0) is now read-only
            ClassicAssert.IsTrue(info.ReadOnlyAddress >= prevTailAddr);

            long tailAddressBeforeNonDeletingReq = info.TailAddress;
            // does not delete when called with lesser or equal etag
            res = db.Execute("DELIFGREATER", key, 0);
            ClassicAssert.AreEqual(0, (long)res);

            RedisValue returnedval = db.StringGet(key);
            ClassicAssert.AreEqual(value, returnedval.ToString());

            info = TestUtils.GetStoreAddressInfo(garnetServer);
            long lastTailAddr = info.TailAddress;

            // non deleting req adds nothing to hlog
            ClassicAssert.AreEqual(tailAddressBeforeNonDeletingReq, lastTailAddr);

            // Deletes when called with higher etag
            // Moved by 32 bytes...
            res = db.Execute("DELIFGREATER", key, 2);
            ClassicAssert.AreEqual(1, (long)res);

            info = TestUtils.GetStoreAddressInfo(garnetServer);
            // check that deletion has happened
            long newTailAddr = info.TailAddress;

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

            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            IDatabase db = redis.GetDatabase(0);
            IServer garnetServer = redis.GetServer(TestUtils.EndPoint);

            string key = "rcuplease";
            string value = "havepatiencercushallbedonethisvalueisneedlesslylongsoIcantestnorecordwasaddedtohlogofthissize";

            bool result = db.StringSet(key, value);
            ClassicAssert.IsTrue(result);

            StoreAddressInfo info = TestUtils.GetStoreAddressInfo(garnetServer);

            // now move this key all the way to stable region
            long prevTailAddr = info.TailAddress;
            MakeReadOnly(prevTailAddr, garnetServer, db);

            info = TestUtils.GetStoreAddressInfo(garnetServer);

            // The first record inserted (key0) is now read-only
            ClassicAssert.IsTrue(info.ReadOnlyAddress >= prevTailAddr);

            long nonDeletingReqTailAddr = info.TailAddress;

            // does not delete when called with lesser or equal etag
            RedisResult res = db.Execute("DELIFGREATER", key, 0);
            ClassicAssert.AreEqual(0, (long)res);

            RedisValue returnedval = db.StringGet(key);
            ClassicAssert.AreEqual(value, returnedval.ToString());

            info = TestUtils.GetStoreAddressInfo(garnetServer);
            long lastTailAddr = info.TailAddress;

            // nothing added to hlog by last DELIFGREATER
            ClassicAssert.AreEqual(nonDeletingReqTailAddr, lastTailAddr);

            // Deletes when called with higher etag
            res = db.Execute("DELIFGREATER", key, 2);
            ClassicAssert.AreEqual(1, (long)res);

            info = TestUtils.GetStoreAddressInfo(garnetServer);
            // check that deletion has happened
            long newTailAddr = info.TailAddress;

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
            IDatabase db = redis.GetDatabase(0);
            var key = "meow-key";

            if (exp != null)
                db.StringSet(key, initialValue, TimeSpan.FromSeconds(exp.Value));
            else
                db.StringSet(key, initialValue);

            RedisResult[] arrRes = (RedisResult[])db.Execute("SETIFGREATER", key, newValue, 5, "EX", 90);

            ClassicAssert.AreEqual(5, (long)arrRes[0]);
            ClassicAssert.IsTrue(arrRes[1].IsNull);

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
            IDatabase db = redis.GetDatabase(0);
            var key = "meow-key";

            if (exp != null)
                db.StringSet(key, initialValue, TimeSpan.FromSeconds(exp.Value));
            else
                db.StringSet(key, initialValue);

            RedisResult[] arrRes = (RedisResult[])db.Execute("SETIFMATCH", key, newValue, 0, "EX", 90);

            ClassicAssert.AreEqual(1, (long)arrRes[0]);
            ClassicAssert.IsTrue(arrRes[1].IsNull);

            var res = db.StringGetWithExpiry(key);
            ClassicAssert.AreEqual(newValue, res.Value.ToString());
            ClassicAssert.IsTrue(res.Expiry.HasValue);
        }


        [Test]
        public void SetIfMatchSetsKeyValueOnNonExistingKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            RedisResult[] result = (RedisResult[])db.Execute("SETIFMATCH", "key", "valueanother", 1, "EX", 3);
            ClassicAssert.AreEqual(2, (long)result[0]);
            ClassicAssert.IsTrue(result[1].IsNull);
        }


        [Test]
        public void SetIfGreaterSetsKeyValueOnNonExistingKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            RedisResult[] result = (RedisResult[])db.Execute("SETIFGREATER", "key", "valueanother", 1, "EX", 3);
            ClassicAssert.AreEqual(1, (long)result[0]);
            ClassicAssert.IsTrue(result[1].IsNull);
        }

        [Test]
        public void SETOnAlreadyExistingSETDataOverridesItWithInitialEtag()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            RedisResult res = db.Execute("SETWITHETAG", "rizz", "buzz");
            long etag = (long)res;
            ClassicAssert.AreEqual(1, etag);

            // update to value to update the etag
            RedisResult[] updateRes = (RedisResult[])db.Execute("SETIFMATCH", ["rizz", "fixx", etag.ToString()]);
            etag = (long)updateRes[0];
            ClassicAssert.AreEqual(2, etag);
            ClassicAssert.IsTrue(updateRes[1].IsNull);

            // inplace update
            res = db.Execute("SETWITHETAG", "rizz", "meow");
            etag = (long)res;
            ClassicAssert.AreEqual(3, etag);

            // update to value to update the etag
            updateRes = (RedisResult[])db.Execute("SETIFMATCH", ["rizz", "fooo", etag.ToString()]);
            etag = (long)updateRes[0];
            ClassicAssert.AreEqual(4, etag);
            ClassicAssert.IsTrue(updateRes[1].IsNull);

            // Copy update
            res = db.Execute("SETWITHETAG", ["rizz", "oneofus"]);
            etag = (long)res;
            ClassicAssert.AreEqual(5, etag);
        }

        [Test]
        public void SETWITHETAGOnAlreadyExistingSETDataOverridesItButUpdatesEtag()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            RedisResult res = db.Execute("SETWITHETAG", ["rizz", "buzz"]);
            long etag = (long)res;
            ClassicAssert.AreEqual(1, etag);

            // update to value to update the etag
            RedisResult[] updateRes = (RedisResult[])db.Execute("SETIFMATCH", ["rizz", "fixx", etag.ToString()]);
            etag = (long)updateRes[0];
            ClassicAssert.AreEqual(2, etag);
            ClassicAssert.IsTrue(updateRes[1].IsNull);

            // inplace update
            res = db.Execute("SETWITHETAG", ["rizz", "meow"]);
            etag = (long)res;
            ClassicAssert.AreEqual(3, etag);

            // update to value to update the etag
            updateRes = (RedisResult[])db.Execute("SETIFMATCH", ["rizz", "fooo", etag.ToString()]);
            etag = (long)updateRes[0];
            ClassicAssert.AreEqual(4, etag);
            ClassicAssert.IsTrue(updateRes[1].IsNull);

            // Copy update
            res = db.Execute("SETWITHETAG", ["rizz", "oneofus"]);
            etag = (long)res;
            ClassicAssert.AreEqual(5, etag);
        }

        [Test]
        public void SETWITHETAGOnAlreadyExistingNonEtagDataOverridesItToInitialEtag()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            ClassicAssert.IsTrue(db.StringSet("rizz", "used"));

            // inplace update
            RedisResult res = db.Execute("SETWITHETAG", ["rizz", "buzz"]);
            long etag = long.Parse(res.ToString());
            ClassicAssert.AreEqual(1, etag);

            db.KeyDelete("rizz");

            ClassicAssert.IsTrue(db.StringSet("rizz", "my"));

            // Copy update
            res = db.Execute("SETWITHETAG", ["rizz", "some"]);
            etag = long.Parse(res.ToString());
            ClassicAssert.AreEqual(1, etag);
        }

        [Test]
        public void DelIfGreaterOnNonExistingKeyWorks()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            RedisResult res = db.Execute("DELIFGREATER", "nonexistingkey", 10);
            ClassicAssert.AreEqual(0, (long)res);
        }

        #endregion

        #region ETAG Apis with non-etag data

        [Test]
        public void SETOnAlreadyExistingNonEtagDataOverridesIt()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            ClassicAssert.IsTrue(db.StringSet("rizz", "used"));

            // inplace update
            RedisResult res = db.Execute("SETWITHETAG", ["rizz", "buzz"]);
            long etag = long.Parse(res.ToString());
            ClassicAssert.AreEqual(1, etag);

            res = db.Execute("SETWITHETAG", ["rizz", "buzz"]);
            etag = long.Parse(res.ToString());
            ClassicAssert.AreEqual(2, etag);

            db.KeyDelete("rizz");

            ClassicAssert.IsTrue(db.StringSet("rizz", "my"));

            // Copy update
            res = db.Execute("SETWITHETAG", ["rizz", "some"]);
            etag = long.Parse(res.ToString());
            ClassicAssert.AreEqual(1, etag);
        }

        [Test]
        public void SetIfMatchOnNonEtagDataReturnsNewEtagAndNoValue()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            var _ = db.StringSet("h", "k");

            var res = (RedisResult[])db.Execute("SETIFMATCH", ["h", "t", "0"]);
            ClassicAssert.AreEqual("1", res[0].ToString());
            ClassicAssert.IsTrue(res[1].IsNull);
        }

        [Test]
        public void SetIfMatchReturnsNewEtagButNoValueWhenUsingNoGet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            var _ = db.StringSet("h", "k");

            var res = (RedisResult[])db.Execute("SETIFMATCH", "h", "t", "0", "NOGET");
            ClassicAssert.AreEqual("1", res[0].ToString());
            ClassicAssert.IsTrue(res[1].IsNull);

            // ETag mismatch
            res = (RedisResult[])db.Execute("SETIFMATCH", "h", "t", "2", "NOGET");
            ClassicAssert.AreEqual("1", res[0].ToString());
            ClassicAssert.IsTrue(res[1].IsNull);
        }

        [Test]
        public void GetIfNotMatchOnNonEtagDataReturnsNilForEtagAndCorrectData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            var _ = db.StringSet("h", "k");

            var res = (RedisResult[])db.Execute("GETIFNOTMATCH", ["h", "1"]);

            ClassicAssert.AreEqual("0", res[0].ToString());
            ClassicAssert.AreEqual("k", res[1].ToString());
        }

        [Test]
        public void GetWithEtagOnNonEtagDataReturns0ForEtagAndCorrectData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            var _ = db.StringSet("h", "k");

            var res = (RedisResult[])db.Execute("GETWITHETAG", ["h"]);
            ClassicAssert.AreEqual("0", res[0].ToString());
            ClassicAssert.AreEqual("k", res[1].ToString());
        }

        #endregion

        #region Backwards Compatability Testing

        [Test]
        public void SingleEtagSetGet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string origValue = "abcdefg";
            db.Execute("SETWITHETAG", ["mykey", origValue]);

            string retValue = db.StringGet("mykey");

            ClassicAssert.AreEqual(origValue, retValue);
        }

        [Test]
        public async Task SingleUnicodeEtagSetGetGarnetClient()
        {
            using var db = TestUtils.GetGarnetClient();
            db.Connect();

            string origValue = "笑い男";
            await db.ExecuteForLongResultAsync("SETWITHETAG", ["mykey", origValue]).ConfigureAwait(false);

            string retValue = await db.StringGetAsync("mykey").ConfigureAwait(false);

            ClassicAssert.AreEqual(origValue, retValue);
        }

        [Test]
        public async Task LargeEtagSetGet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const int length = (1 << 19) + 100;
            var value = new byte[length];

            for (int i = 0; i < length; i++)
                value[i] = (byte)((byte)'a' + ((byte)i % 26));

            RedisResult res = await db.ExecuteAsync("SETWITHETAG", ["mykey", value]).ConfigureAwait(false);
            long initalEtag = long.Parse(res.ToString());
            ClassicAssert.AreEqual(1, initalEtag);

            // Backwards compatability of data set with etag and plain GET call
            var retvalue = (byte[])await db.StringGetAsync("mykey").ConfigureAwait(false);

            ClassicAssert.IsTrue(new ReadOnlySpan<byte>(value).SequenceEqual(new ReadOnlySpan<byte>(retvalue)));
        }

        [Test]
        public void SetExpiryForEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string origValue = "abcdefghij";

            // set with etag
            long initalEtag = long.Parse(db.Execute("SETWITHETAG", ["mykey", origValue, "EX", 2]).ToString());
            ClassicAssert.AreEqual(1, initalEtag);

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
        public void SetExpiryHighPrecisionForEtagSetDatat()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var origValue = "abcdeghijklmno";
            // set with etag
            long initalEtag = long.Parse(db.Execute("SETWITHETAG", ["mykey", origValue, "PX", 1900]).ToString());
            ClassicAssert.AreEqual(1, initalEtag);

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
        public void LockTakeReleaseOnAValueInitiallySET()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "lock-key";
            string value = "lock-value";

            var initalEtag = long.Parse(db.Execute("SETWITHETAG", [key, value]).ToString());
            ClassicAssert.AreEqual(1, initalEtag);

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
            var initalEtag = long.Parse(db.Execute("SETWITHETAG", [strKey, nVal]).ToString());
            ClassicAssert.AreEqual(1, initalEtag);

            long n = db.StringDecrement(strKey);
            ClassicAssert.AreEqual(nVal - 1, n);
            long nRetVal = Convert.ToInt64(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);
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
            var initalEtag = long.Parse(db.Execute("SETWITHETAG", [strKey, nVal]).ToString());
            ClassicAssert.AreEqual(1, initalEtag);

            long n = db.StringDecrement(strKey, nDecr);

            int nRetVal = Convert.ToInt32(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);
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
                var initalEtag = long.Parse(db.Execute("SETWITHETAG", [key, values[i]]).ToString());
                ClassicAssert.AreEqual(1, initalEtag);

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
                        _ = db.Execute("SETWITHETAG", [key, long.MaxValue.ToString()]);
                        _ = db.StringIncrement(key);
                        break;
                    case RespCommand.DECR:
                        _ = db.Execute("SETWITHETAG", [key, long.MinValue.ToString()]);
                        _ = db.StringDecrement(key);
                        break;
                    case RespCommand.INCRBY:
                        _ = db.Execute("SETWITHETAG", [key, 0]);
                        _ = db.Execute("INCRBY", [key, ulong.MaxValue.ToString()]);
                        break;
                    case RespCommand.DECRBY:
                        _ = db.Execute("SETWITHETAG", [key, 0]);
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
            db.Execute("SETWITHETAG", key, initialValue);

            var expectedResult = initialValue + incrByValue;

            var actualResultStr = (string)db.Execute("INCRBYFLOAT", [key, incrByValue]);
            var actualResultRawStr = db.StringGet(key);

            var actualResult = double.Parse(actualResultStr, CultureInfo.InvariantCulture);
            var actualResultRaw = double.Parse((string)actualResultRawStr, CultureInfo.InvariantCulture);

            Assert.That(actualResult, Is.EqualTo(expectedResult).Within(1.0 / Math.Pow(10, 15)));
            Assert.That(actualResult, Is.EqualTo(actualResultRaw).Within(1.0 / Math.Pow(10, 15)));
        }

        [Test]
        public void SingleDeleteForEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Key storing integer
            var nVal = 100;
            var strKey = "key1";
            db.Execute("SETWITHETAG", [strKey, nVal]);
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

            db.Execute("SETWITHETAG", [key, value]);

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

            int keyCount = 5;
            int valLen = 256;
            int keyLen = 8;

            List<Tuple<string, string>> data = [];
            for (int i = 0; i < keyCount; i++)
            {
                data.Add(new Tuple<string, string>(TestUtils.GetRandomString(keyLen), TestUtils.GetRandomString(valLen)));
                var pair = data.Last();
                db.Execute("SETWITHETAG", [pair.Item1, pair.Item2]);
            }


            for (int i = 0; i < keyCount; i++)
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

            int keyCount = 10;
            int valLen = 16;
            int keyLen = 8;

            List<Tuple<string, string>> data = [];
            for (int i = 0; i < keyCount; i++)
            {
                data.Add(new Tuple<string, string>(TestUtils.GetRandomString(keyLen), TestUtils.GetRandomString(valLen)));
                var pair = data.Last();
                db.Execute("SETWITHETAG", [pair.Item1, pair.Item2]);
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

            int keyCount = 10;
            int valLen = 16;
            int keyLen = 8;

            List<Tuple<string, string>> data = [];
            for (int i = 0; i < keyCount; i++)
            {
                data.Add(new Tuple<string, string>(TestUtils.GetRandomString(keyLen), TestUtils.GetRandomString(valLen)));
                var pair = data.Last();
                db.Execute("SETWITHETAG", [pair.Item1, pair.Item2]);
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

            db.Execute("SETWITHETAG", [strKey, nVal]);

            bool fExists = db.KeyExists("key1", CommandFlags.None);
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

            db.Execute("SETWITHETAG", ["rizz", "bar"]);

            var exists = db.KeyExists(["key", "listKey", "zset:test", "foo", "rizz"]);
            ClassicAssert.AreEqual(4, exists);
        }

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

            db.Execute("SETWITHETAG", [key, val]);
            ttl = db.Execute("TTL", key);
            ClassicAssert.AreEqual(-1, (long)ttl);

            db.KeyExpire(key, TimeSpan.FromSeconds(expire));

            var res = (RedisResult[])db.Execute("GETWITHETAG", [key]);
            ClassicAssert.AreEqual(1, long.Parse(res[0].ToString()));
            ClassicAssert.AreEqual(val, res[1].ToString());

            var time = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(time.Value.TotalSeconds > 0);

            db.KeyExpire(key, TimeSpan.FromSeconds(expire));
            res = (RedisResult[])db.Execute("GETWITHETAG", [key]);
            ClassicAssert.AreEqual(1, long.Parse(res[0].ToString()));
            ClassicAssert.AreEqual(val, res[1].ToString());

            db.KeyPersist(key);
            res = (RedisResult[])db.Execute("GETWITHETAG", [key]);
            // unchanged etag
            ClassicAssert.AreEqual(1, long.Parse(res[0].ToString()));
            ClassicAssert.AreEqual(val, res[1].ToString());

            Thread.Sleep((expire + 1) * 1000);

            var _val = db.StringGet(key);
            ClassicAssert.AreEqual(val, _val.ToString());

            time = db.KeyTimeToLive(key);
            ClassicAssert.IsNull(time);

            res = (RedisResult[])db.Execute("GETWITHETAG", [key]);
            // the tag was persisted along with data from persist despite previous TTL
            ClassicAssert.AreEqual(1, long.Parse(res[0].ToString()));
            ClassicAssert.AreEqual(val, res[1].ToString());
        }

        [Test]
        public void PersistTestForEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int expire = 100;
            var keyA = "keyA";
            db.Execute("SETWITHETAG", [keyA, keyA]);

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

            var res = (RedisResult[])db.Execute("GETWITHETAG", [keyA]);
            ClassicAssert.AreEqual(1, long.Parse(res[0].ToString()));
            ClassicAssert.AreEqual(keyA, res[1].ToString());

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
            db.Execute("SETWITHETAG", [key, key]);

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
            db.Execute("SETWITHETAG", [key, key]);

            args[2] = "XX";// XX -- Set expiry only when the key has an existing expiry
            bool resp = (bool)db.Execute($"{command}", args);
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

            ClassicAssert.AreEqual(1, long.Parse(db.Execute("SETWITHETAG", key, "v1").ToString()));

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

            string key = "rangeKey";
            string value = "0123456789";

            var resp = (string)db.StringGetRange(key, 2, 10);
            ClassicAssert.AreEqual(string.Empty, resp);

            ClassicAssert.AreEqual(1, long.Parse(db.Execute("SETWITHETAG", key, value).ToString()));

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
        public void KeepTtlTestForDataInitiallySET()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int expire = 3;
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

            db.Execute("SETWITHETAG", ["mykey", "foo bar"]);

            ClassicAssert.AreEqual(7, db.StringLength("mykey"));
            ClassicAssert.AreEqual(0, db.StringLength("nokey"));

            var etagToCheck = db.Execute("GETWITHETAG", "mykey");
            ClassicAssert.AreEqual(1, long.Parse(etagToCheck[0].ToString()));
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

            db.Execute("SETWITHETAG", [key, val]);

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
            long etagToCheck = long.Parse(((RedisResult[])db.Execute("GETWITHETAG", [key]))[0].ToString());
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
            db.Execute("SETWITHETAG", [key, val]);

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

            db.Execute("SETWITHETAG", [key, val]);
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
        public void HyperLogLogCommandsShouldReturnWrongTypeErrorForEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mewo";
            var key2 = "dude";

            _ = db.Execute("SETWITHETAG", [key, "mars"]);
            _ = db.Execute("SETWITHETAG", [key2, "marsrover"]);

            // TODO: This is RedisServerException in the InPlaceUpdater call, but GetRMWModifiedFieldInfo currently throws RedisConnectionException.
            // This can be different in CIs vs. locally.
            Assert.That(() => db.Execute("PFADD", [key, "woohoo"]),
                    Throws.TypeOf<RedisServerException>().With.Message.EndsWith(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE_HLL))
                    .Or.TypeOf<RedisConnectionException>());

            Assert.That(() => db.Execute("PFMERGE", [key, key2]),
                    Throws.TypeOf<RedisServerException>().With.Message.EndsWith(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE_HLL))
                    .Or.TypeOf<RedisConnectionException>());
        }
        #endregion
    }
}