
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespEtagTests
    {
        private GarnetServer server;
        private Random r;

        [SetUp]
        public void Setup()
        {
            r = new Random(674386);
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: false);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        #region ETAG SET Happy Paths

        [Test]
        public void SetWithEtagReturnsEtagForNewData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);
            RedisResult res = db.Execute("SETWITHETAG", ["rizz", "buzz"]);
            long etag = long.Parse(res.ToString());
            ClassicAssert.AreEqual(0, etag);
        }

        [Test]
        public void SetIfMatchReturnsNewValueAndEtagWhenEtagMatches()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            var key = "florida";
            RedisResult res = (RedisResult)db.Execute("SETWITHETAG", [key, "one"]);
            long initalEtag = long.Parse(res.ToString());
            ClassicAssert.AreEqual(0, initalEtag);

            // ETAGMISMATCH test
            var incorrectEtag = 1738;
            RedisResult etagMismatchMsg = db.Execute("SETIFMATCH", [key, "nextone", incorrectEtag]);
            ClassicAssert.AreEqual("ETAGMISMATCH", etagMismatchMsg.ToString());

            // set a bigger val
            RedisResult[] setIfMatchRes = (RedisResult[])db.Execute("SETIFMATCH", [key, "nextone", initalEtag]);

            long nextEtag = long.Parse(setIfMatchRes[0].ToString());
            string value = setIfMatchRes[1].ToString();

            ClassicAssert.AreEqual(1, nextEtag);
            ClassicAssert.AreEqual(value, "nextone");

            // set a bigger val
            setIfMatchRes = (RedisResult[])db.Execute("SETIFMATCH", [key, "nextnextone", nextEtag]);
            nextEtag = long.Parse(setIfMatchRes[0].ToString());
            value = setIfMatchRes[1].ToString();

            ClassicAssert.AreEqual(2, nextEtag);
            ClassicAssert.AreEqual(value, "nextnextone");

            // ETAGMISMATCH again
            etagMismatchMsg = db.Execute("SETIFMATCH", [key, "lastOne", incorrectEtag]);
            ClassicAssert.AreEqual("ETAGMISMATCH", etagMismatchMsg.ToString());

            // set a smaller val
            setIfMatchRes = (RedisResult[])db.Execute("SETIFMATCH", [key, "lastOne", nextEtag]);
            nextEtag = long.Parse(setIfMatchRes[0].ToString());
            value = setIfMatchRes[1].ToString();

            ClassicAssert.AreEqual(3, nextEtag);
            ClassicAssert.AreEqual(value, "lastOne");
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
            var initEtag = db.Execute("SETWITHETAG", [key, "hkhalid"]);
            ClassicAssert.AreEqual(0, long.Parse(initEtag.ToString()));

            RedisResult[] res = (RedisResult[])db.Execute("GETWITHETAG", [key]);
            long etag = long.Parse(res[0].ToString());
            string value = res[1].ToString();

            ClassicAssert.AreEqual(0, etag);
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
            var _ = db.Execute("SETWITHETAG", [key, "maximus"]);

            RedisResult noDataOnMatch = db.Execute("GETIFNOTMATCH", [key, 0]);

            ClassicAssert.AreEqual("NOTCHANGED", noDataOnMatch.ToString());

            RedisResult[] res = (RedisResult[])db.Execute("GETIFNOTMATCH", [key, 1]);
            long etag = long.Parse(res[0].ToString());
            string value = res[1].ToString();

            ClassicAssert.AreEqual(0, etag);
            ClassicAssert.AreEqual("maximus", value);
        }

        #endregion

        # region Edgecases

        [Test]
        public void SetWithEtagOnAlreadyExistingSetWithEtagDataOverridesItWithInitialEtag()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            RedisResult res = db.Execute("SETWITHETAG", ["rizz", "buzz"]);
            long etag = (long)res;
            ClassicAssert.AreEqual(0, etag);

            // update to value to update the etag
            RedisResult[] updateRes = (RedisResult[])db.Execute("SETIFMATCH", ["rizz", "fixx", etag.ToString()]);
            etag = (long)updateRes[0];
            ClassicAssert.AreEqual(1, etag);
            ClassicAssert.AreEqual("fixx", updateRes[1].ToString());

            // inplace update
            res = db.Execute("SETWITHETAG", ["rizz", "meow"]);
            etag = (long)res;
            ClassicAssert.AreEqual(0, etag);

            // update to value to update the etag
            updateRes = (RedisResult[])db.Execute("SETIFMATCH", ["rizz", "fooo", etag.ToString()]);
            etag = (long)updateRes[0];
            ClassicAssert.AreEqual(1, etag);
            ClassicAssert.AreEqual("fooo", updateRes[1].ToString());

            // Copy update
            res = db.Execute("SETWITHETAG", ["rizz", "oneofus"]);
            etag = (long)res;
            ClassicAssert.AreEqual(0, etag);
        }

        [Test]
        public void SetWithEtagWithRetainEtagOnAlreadyExistingSetWithEtagDataOverridesItButUpdatesEtag()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            RedisResult res = db.Execute("SETWITHETAG", ["rizz", "buzz", "RETAINETAG"]);
            long etag = (long)res;
            ClassicAssert.AreEqual(0, etag);

            // update to value to update the etag
            RedisResult[] updateRes = (RedisResult[])db.Execute("SETIFMATCH", ["rizz", "fixx", etag.ToString()]);
            etag = (long)updateRes[0];
            ClassicAssert.AreEqual(1, etag);
            ClassicAssert.AreEqual("fixx", updateRes[1].ToString());

            // inplace update
            res = db.Execute("SETWITHETAG", ["rizz", "meow", "RETAINETAG"]);
            etag = (long)res;
            ClassicAssert.AreEqual(2, etag);

            // update to value to update the etag
            updateRes = (RedisResult[])db.Execute("SETIFMATCH", ["rizz", "fooo", etag.ToString()]);
            etag = (long)updateRes[0];
            ClassicAssert.AreEqual(3, etag);
            ClassicAssert.AreEqual("fooo", updateRes[1].ToString());

            // Copy update
            res = db.Execute("SETWITHETAG", ["rizz", "oneofus", "RETAINETAG"]);
            etag = (long)res;
            ClassicAssert.AreEqual(4, etag);
        }

        [Test]
        public void SetWithEtagWithRetainEtagOnAlreadyExistingNonEtagDataOverridesItToInitialEtag()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            ClassicAssert.IsTrue(db.StringSet("rizz", "used"));

            // inplace update
            RedisResult res = db.Execute("SETWITHETAG", ["rizz", "buzz", "RETAINETAG"]);
            long etag = long.Parse(res.ToString());
            ClassicAssert.AreEqual(0, etag);

            db.KeyDelete("rizz");

            ClassicAssert.IsTrue(db.StringSet("rizz", "my"));

            // Copy update
            res = db.Execute("SETWITHETAG", ["rizz", "some", "RETAINETAG"]);
            etag = long.Parse(res.ToString());
            ClassicAssert.AreEqual(0, etag);
        }

        #endregion

        #region ETAG Apis with non-etag data

        [Test]
        public void SetWithEtagOnAlreadyExistingNonEtagDataOverridesIt()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            ClassicAssert.IsTrue(db.StringSet("rizz", "used"));

            // inplace update
            RedisResult res = db.Execute("SETWITHETAG", ["rizz", "buzz"]);
            long etag = long.Parse(res.ToString());
            ClassicAssert.AreEqual(0, etag);

            db.KeyDelete("rizz");

            ClassicAssert.IsTrue(db.StringSet("rizz", "my"));

            // Copy update
            res = db.Execute("SETWITHETAG", ["rizz", "some"]);
            etag = long.Parse(res.ToString());
            ClassicAssert.AreEqual(0, etag);
        }

        [Test]
        public void SetWithEtagWithRetainEtagOnAlreadyExistingNonEtagDataOverridesIt()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            ClassicAssert.IsTrue(db.StringSet("rizz", "used"));

            // inplace update
            RedisResult res = db.Execute("SETWITHETAG", ["rizz", "buzz"]);
            long etag = long.Parse(res.ToString());
            ClassicAssert.AreEqual(0, etag);

            db.KeyDelete("rizz");

            ClassicAssert.IsTrue(db.StringSet("rizz", "my"));

            // Copy update
            res = db.Execute("SETWITHETAG", ["rizz", "some"]);
            etag = long.Parse(res.ToString());
            ClassicAssert.AreEqual(0, etag);
        }


        [Test]
        public void SetIfMatchOnNonEtagDataReturnsEtagMismatch()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            var _ = db.StringSet("h", "k");

            var res = db.Execute("SETIFMATCH", ["h", "t", "0"]);
            ClassicAssert.AreEqual("ETAGMISMATCH", res.ToString());
        }

        [Test]
        public void GetIfNotMatchOnNonEtagDataReturnsNilForEtagAndCorrectData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            var _ = db.StringSet("h", "k");

            var res = (RedisResult[])db.Execute("GETIFNOTMATCH", ["h", "1"]);

            ClassicAssert.IsTrue(res[0].IsNull);
            ClassicAssert.AreEqual("k", res[1].ToString());
        }

        [Test]
        public void GetWithEtagOnNonEtagDataReturnsNilForEtagAndCorrectData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            var _ = db.StringSet("h", "k");

            var res = (RedisResult[])db.Execute("GETWITHETAG", ["h"]);
            ClassicAssert.IsTrue(res[0].IsNull);
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
            await db.ExecuteForLongResultAsync("SETWITHETAG", ["mykey", origValue]);

            string retValue = await db.StringGetAsync("mykey");

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

            RedisResult res = await db.ExecuteAsync("SETWITHETAG", ["mykey", value]);
            long initalEtag = long.Parse(res.ToString());
            ClassicAssert.AreEqual(0, initalEtag);

            // Backwards compatability of data set with etag and plain GET call
            var retvalue = (byte[])await db.StringGetAsync("mykey");

            ClassicAssert.IsTrue(new ReadOnlySpan<byte>(value).SequenceEqual(new ReadOnlySpan<byte>(retvalue)));
        }

        [Test]
        public void SetExpiryForEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string origValue = "abcdefghij";

            // set with etag
            long initalEtag = long.Parse(db.Execute("SETWITHETAG", ["mykey", origValue]).ToString());
            ClassicAssert.AreEqual(0, initalEtag);

            // Expire the key in few seconds from now
            ClassicAssert.IsTrue(
                db.KeyExpire("mykey", TimeSpan.FromSeconds(2))
            );

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
            long initalEtag = long.Parse(db.Execute("SETWITHETAG", ["mykey", origValue]).ToString());
            ClassicAssert.AreEqual(0, initalEtag);

            // Expire the key in few seconds from now
            db.KeyExpire("mykey", TimeSpan.FromSeconds(1.9));

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
        public void SetGetWithRetainEtagForEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "mykey";
            string origValue = "abcdefghijklmnopqrst";

            // Initial set
            var _ = db.Execute("SETWITHETAG", [key, origValue]);
            string retValue = db.StringGet(key);
            ClassicAssert.AreEqual(origValue, retValue);

            // Smaller new value without expiration
            string newValue1 = "abcdefghijklmnopqrs";

            retValue = db.Execute("SET", [key, newValue1, "GET", "RETAINETAG"]).ToString();

            ClassicAssert.AreEqual(origValue, retValue);

            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue1, retValue);

            // This should increase the ETAG internally so we have a check for that here
            long checkEtag = long.Parse(db.Execute("GETWITHETAG", [key])[0].ToString());
            ClassicAssert.AreEqual(1, checkEtag);

            // Smaller new value with KeepTtl
            string newValue2 = "abcdefghijklmnopqr";
            retValue = db.Execute("SET", [key, newValue2, "GET", "RETAINETAG"]).ToString();

            // This should increase the ETAG internally so we have a check for that here
            checkEtag = long.Parse(db.Execute("GETWITHETAG", [key])[0].ToString());
            ClassicAssert.AreEqual(2, checkEtag);

            ClassicAssert.AreEqual(newValue1, retValue);
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue2, retValue);
            var expiry = db.KeyTimeToLive(key);
            ClassicAssert.IsNull(expiry);

            // Smaller new value with expiration
            string newValue3 = "01234";
            retValue = db.Execute("SET", [key, newValue3, "EX", "10", "GET", "RETAINETAG"]).ToString();
            ClassicAssert.AreEqual(newValue2, retValue);

            // This should increase the ETAG internally so we have a check for that here
            checkEtag = long.Parse(db.Execute("GETWITHETAG", [key])[0].ToString());
            ClassicAssert.AreEqual(3, checkEtag);

            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue3, retValue);
            expiry = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(expiry.Value.TotalSeconds > 0);

            // Larger new value with expiration
            string newValue4 = "abcdefghijklmnopqrstabcdefghijklmnopqrst";
            retValue = db.Execute("SET", [key, newValue4, "EX", "100", "GET", "RETAINETAG"]).ToString();
            ClassicAssert.AreEqual(newValue3, retValue);

            // This should increase the ETAG internally so we have a check for that here
            checkEtag = long.Parse(db.Execute("GETWITHETAG", [key])[0].ToString());
            ClassicAssert.AreEqual(4, checkEtag);

            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue4, retValue);
            expiry = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(expiry.Value.TotalSeconds > 0);

            // Smaller new value without expiration
            string newValue5 = "0123401234";
            retValue = db.Execute("SET", [key, newValue5, "GET", "RETAINETAG"]).ToString();
            ClassicAssert.AreEqual(newValue4, retValue);
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue5, retValue);
            expiry = db.KeyTimeToLive(key);
            ClassicAssert.IsNull(expiry);

            // This should increase the ETAG internally so we have a check for that here
            checkEtag = long.Parse(db.Execute("GETWITHETAG", [key])[0].ToString());
            ClassicAssert.AreEqual(5, checkEtag);

            // Larger new value without expiration
            string newValue6 = "abcdefghijklmnopqrstabcdefghijklmnopqrst";
            retValue = db.Execute("SET", [key, newValue6, "GET", "RETAINETAG"]).ToString();
            ClassicAssert.AreEqual(newValue5, retValue);
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue6, retValue);
            expiry = db.KeyTimeToLive(key);
            ClassicAssert.IsNull(expiry);

            // This should increase the ETAG internally so we have a check for that here
            checkEtag = long.Parse(db.Execute("GETWITHETAG", [key])[0].ToString());
            ClassicAssert.AreEqual(6, checkEtag);
        }

        [Test]
        public void SetExpiryIncrForEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Key storing integer
            var nVal = -100000;
            var strKey = "key1";
            db.Execute("SETWITHETAG", [strKey, nVal]);
            db.KeyExpire(strKey, TimeSpan.FromSeconds(5));

            string res1 = db.StringGet(strKey);

            long n = db.StringIncrement(strKey);

            // This should increase the ETAG internally so we have a check for that here
            var checkEtag = long.Parse(db.Execute("GETWITHETAG", [strKey])[0].ToString());
            ClassicAssert.AreEqual(1, checkEtag);

            string res = db.StringGet(strKey);
            long nRetVal = Convert.ToInt64(res);
            ClassicAssert.AreEqual(n, nRetVal);
            ClassicAssert.AreEqual(-99999, nRetVal);

            n = db.StringIncrement(strKey);

            // This should increase the ETAG internally so we have a check for that here
            checkEtag = long.Parse(db.Execute("GETWITHETAG", [strKey])[0].ToString());
            ClassicAssert.AreEqual(2, checkEtag);

            nRetVal = Convert.ToInt64(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);
            ClassicAssert.AreEqual(-99998, nRetVal);

            var res69 = db.KeyTimeToLive(strKey);

            Thread.Sleep(5000);

            // Expired key, restart increment,after exp this is treated as new record
            // without etag
            n = db.StringIncrement(strKey);
            ClassicAssert.AreEqual(1, n);

            nRetVal = Convert.ToInt64(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);
            ClassicAssert.AreEqual(1, nRetVal);

            var etagGet = (RedisResult[])db.Execute("GETWITHETAG", [strKey]);
            ClassicAssert.IsTrue(etagGet[0].IsNull);
            ClassicAssert.AreEqual(1, Convert.ToInt64(etagGet[1]));
        }

        [Test]
        public void IncrDecrChangeDigitsWithExpiry()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var strKey = "key1";

            db.Execute("SETWITHETAG", [strKey, 9]);

            long checkEtag = long.Parse(db.Execute("GETWITHETAG", [strKey])[0].ToString());
            ClassicAssert.AreEqual(0, checkEtag);

            db.KeyExpire(strKey, TimeSpan.FromSeconds(5));

            long n = db.StringIncrement(strKey);
            long nRetVal = Convert.ToInt64(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);
            ClassicAssert.AreEqual(10, nRetVal);

            checkEtag = long.Parse(db.Execute("GETWITHETAG", [strKey])[0].ToString());
            ClassicAssert.AreEqual(1, checkEtag);

            n = db.StringDecrement(strKey);
            nRetVal = Convert.ToInt64(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);
            ClassicAssert.AreEqual(9, nRetVal);

            checkEtag = long.Parse(db.Execute("GETWITHETAG", [strKey])[0].ToString());
            ClassicAssert.AreEqual(2, checkEtag);

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
            db.Execute("SETWITHETAG", [strKey, 9]);

            long checkEtag = long.Parse(db.Execute("GETWITHETAG", [strKey])[0].ToString());
            ClassicAssert.AreEqual(0, checkEtag);

            // Unless the SET was called with RETAINETAG a call to set will override the setwithetag to a new
            // value altogether, this will make it lose it's etag capability. This is a limitation for Etags
            // because plain sets are upserts (blind updates), and currently we cannot increase the latency in
            // the common path for set to check beyong Readonly address for the existence of a record with ETag.
            // This means that sets are complete upserts and clients need to use setifmatch, or set with RETAINETAG
            // if they want each consequent set to maintain the key value pair's etag property.
            ClassicAssert.IsTrue(db.StringSet(strKey, "ciaociao"));

            string retVal = db.StringGet(strKey).ToString();
            ClassicAssert.AreEqual("ciaociao", retVal);

            var res = (RedisResult[])db.Execute("GETWITHETAG", [strKey]);
            ClassicAssert.IsTrue(res[0].IsNull);
            ClassicAssert.AreEqual("ciaociao", res[1].ToString());
        }

        [Test]
        public void StringSetOnAnExistingEtagDataUpdatesEtagIfEtagRetain()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var strKey = "mykey";
            db.Execute("SETWITHETAG", [strKey, 9]);

            long checkEtag = (long)db.Execute("GETWITHETAG", [strKey])[0];
            ClassicAssert.AreEqual(0, checkEtag);

            // Unless you explicitly call SET with RETAINETAG option you will lose the etag on the previous key-value pair
            db.Execute("SET", [strKey, "ciaociao", "RETAINETAG"]);

            string retVal = db.StringGet(strKey).ToString();
            ClassicAssert.AreEqual("ciaociao", retVal);

            var res = (RedisResult[])db.Execute("GETWITHETAG", strKey);
            ClassicAssert.AreEqual(1, (long)res[0]);

            // on subsequent upserts we are still increasing the etag transparently
            db.Execute("SET", [strKey, "ciaociaociao", "RETAINETAG"]);

            retVal = db.StringGet(strKey).ToString();
            ClassicAssert.AreEqual("ciaociaociao", retVal);

            res = (RedisResult[])db.Execute("GETWITHETAG", strKey);
            ClassicAssert.AreEqual(2, (long)res[0]);
            ClassicAssert.AreEqual("ciaociaociao", res[1].ToString());
        }

        [Test]
        public void LockTakeReleaseOnAValueInitiallySetWithEtag()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "lock-key";
            string value = "lock-value";

            var initalEtag = long.Parse(db.Execute("SETWITHETAG", [key, value]).ToString());
            ClassicAssert.AreEqual(0, initalEtag);

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
            ClassicAssert.AreEqual(0, initalEtag);

            long n = db.StringDecrement(strKey);
            ClassicAssert.AreEqual(nVal - 1, n);
            long nRetVal = Convert.ToInt64(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);

            long checkEtag = long.Parse(db.Execute("GETWITHETAG", [strKey])[0].ToString());
            ClassicAssert.AreEqual(1, checkEtag);
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
            ClassicAssert.AreEqual(0, initalEtag);

            long n = db.StringDecrement(strKey, nDecr);

            int nRetVal = Convert.ToInt32(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);

            long checkEtag = long.Parse(db.Execute("GETWITHETAG", [strKey])[0].ToString());
            ClassicAssert.AreEqual(1, checkEtag);
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
                ClassicAssert.AreEqual(0, initalEtag);

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
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, DisableObjects: true);
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

        private string GetRandomString(int len)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, len)
                .Select(s => s[r.Next(s.Length)]).ToArray());
        }

        [Test]
        public void SingleDeleteWithObjectStoreDisable_LTMForEtagSetData()
        {
            TearDown();

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, DisableObjects: true);
            server.Start();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int keyCount = 5;
            int valLen = 256;
            int keyLen = 8;

            List<Tuple<string, string>> data = [];
            for (int i = 0; i < keyCount; i++)
            {
                data.Add(new Tuple<string, string>(GetRandomString(keyLen), GetRandomString(valLen)));
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
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, DisableObjects: true);
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
                data.Add(new Tuple<string, string>(GetRandomString(keyLen), GetRandomString(valLen)));
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
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, DisableObjects: true);
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
                data.Add(new Tuple<string, string>(GetRandomString(keyLen), GetRandomString(valLen)));
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
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, DisableObjects: true);
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
        public void SingleRenameEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string origValue = "test1";
            long etag = long.Parse(db.Execute("SETWITHETAG", ["key1", origValue]).ToString());
            ClassicAssert.AreEqual(0, etag);

            db.KeyRename("key1", "key2");
            string retValue = db.StringGet("key2");

            ClassicAssert.AreEqual(origValue, retValue);

            // other key now gives no result
            ClassicAssert.AreEqual("", db.Execute("GETWITHETAG", ["key1"]).ToString());

            // new Key value pair created with older value, the etag is reset here back to 0
            var res = (RedisResult[])db.Execute("GETWITHETAG", ["key2"]);
            ClassicAssert.AreEqual("0", res[0].ToString());
            ClassicAssert.AreEqual(origValue, res[1].ToString());

            origValue = db.StringGet("key1");
            ClassicAssert.AreEqual(null, origValue);
        }

        [Test]
        public void SingleRenameEtagShouldRetainEtagOfNewKeyIfExistsWithEtag()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string existingNewKey = "key2";
            string existingVal = "foo";
            long etag = (long)db.Execute("SETWITHETAG", [existingNewKey, existingVal]);
            ClassicAssert.AreEqual(0, etag);
            RedisResult[] updateRes = (RedisResult[])db.Execute("SETIFMATCH", [existingNewKey, "updated", etag.ToString()]);
            long updatedEtag = (long)updateRes[0];

            string origValue = "test1";
            etag = long.Parse(db.Execute("SETWITHETAG", ["key1", origValue]).ToString());
            ClassicAssert.AreEqual(0, etag);

            db.KeyRename("key1", existingNewKey);
            string retValue = db.StringGet(existingNewKey);
            ClassicAssert.AreEqual(origValue, retValue);

            // new Key value pair created with older value, the etag is reusing the existingnewkey etag
            var res = (RedisResult[])db.Execute("GETWITHETAG", [existingNewKey]);
            ClassicAssert.AreEqual(updatedEtag + 1, (long)res[0]);
            ClassicAssert.AreEqual(origValue, res[1].ToString());

            origValue = db.StringGet("key1");
            ClassicAssert.AreEqual(null, origValue);
        }

        [Test]
        public void SingleRenameKeyEdgeCaseEtagSetData([Values] bool withoutObjectStore)
        {
            if (withoutObjectStore)
            {
                TearDown();
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, DisableObjects: true);
                server.Start();
            }
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            //1. Key rename does not exist
            try
            {
                var res = db.KeyRename("key1", "key2");
            }
            catch (Exception ex)
            {
                ClassicAssert.AreEqual("ERR no such key", ex.Message);
            }

            //2. Key rename oldKey.Equals(newKey)
            string origValue = "test1";
            db.Execute("SETWITHETAG", ["key1", origValue]);

            bool renameRes = db.KeyRename("key1", "key1");
            ClassicAssert.IsTrue(renameRes);
            string retValue = db.StringGet("key1");
            ClassicAssert.AreEqual(origValue, retValue);
        }

        [Test]
        public void SingleRenameShouldNotAddEtagEvenIfExistingKeyHadEtagButNotTheOriginal()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string existingNewKey = "key2";
            string existingVal = "foo";
            long etag = (long)db.Execute("SETWITHETAG", [existingNewKey, existingVal]);
            ClassicAssert.AreEqual(0, etag);
            RedisResult[] updateRes = (RedisResult[])db.Execute("SETIFMATCH", [existingNewKey, "updated", etag.ToString()]);
            long updatedEtag = (long)updateRes[0];

            string origValue = "test1";
            ClassicAssert.IsTrue(db.StringSet("key1", origValue));

            db.KeyRename("key1", existingNewKey);
            string retValue = db.StringGet(existingNewKey);
            ClassicAssert.AreEqual(origValue, retValue);

            // new Key value pair created with older value, the etag is reusing the existingnewkey etag
            var res = (RedisResult[])db.Execute("GETWITHETAG", [existingNewKey]);
            ClassicAssert.IsTrue(res[0].IsNull);
            ClassicAssert.AreEqual(origValue, res[1].ToString());

            origValue = db.StringGet("key1");
            ClassicAssert.AreEqual(null, origValue);
        }

        [Test]
        public void SingleRenameShouldAddEtagIfOldKeyHadEtagButNotExistingNewkey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string existingNewKey = "key2";
            string existingVal = "foo";

            ClassicAssert.IsTrue(db.StringSet(existingNewKey, existingVal));

            string origValue = "test1";
            long etag = (long)db.Execute("SETWITHETAG", ["key1", origValue]);
            ClassicAssert.AreEqual(0, etag);

            db.KeyRename("key1", existingNewKey);
            string retValue = db.StringGet(existingNewKey);
            ClassicAssert.AreEqual(origValue, retValue);

            // new Key value pair created with older value
            var res = (RedisResult[])db.Execute("GETWITHETAG", [existingNewKey]);
            ClassicAssert.AreEqual(0, (long)res[0]);
            ClassicAssert.AreEqual(origValue, res[1].ToString());

            origValue = db.StringGet("key1");
            ClassicAssert.AreEqual(null, origValue);
        }

        [Test]
        public void SingleRenameShouldAddEtagAndMetadataIfOldKeyHadEtagAndMetadata()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string origKey = "key1";
            string origValue = "test1";
            long etag = (long)db.Execute("SETWITHETAG", ["key1", origValue]);
            ClassicAssert.AreEqual(0, etag);

            ClassicAssert.IsTrue(db.KeyExpire(origKey, TimeSpan.FromSeconds(10)));

            string newKey = "key2";
            db.KeyRename(origKey, newKey);

            string retValue = db.StringGet(newKey);
            ClassicAssert.AreEqual(origValue, retValue);

            // new Key value pair created with older value
            var res = (RedisResult[])db.Execute("GETWITHETAG", [newKey]);
            ClassicAssert.AreEqual(0, (long)res[0]);
            ClassicAssert.AreEqual(origValue, res[1].ToString());

            // check that the ttl is not empty on new key because it inherited it from prev key
            TimeSpan? ttl = db.KeyTimeToLive(newKey);
            ClassicAssert.IsNotNull(ttl);

            origValue = db.StringGet(origKey);
            ClassicAssert.AreEqual(null, origValue);
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
            ClassicAssert.AreEqual(-2, (int)ttl);

            db.Execute("SETWITHETAG", [key, val]);
            ttl = db.Execute("TTL", key);
            ClassicAssert.AreEqual(-1, (int)ttl);

            db.KeyExpire(key, TimeSpan.FromSeconds(expire));

            var res = (RedisResult[])db.Execute("GETWITHETAG", [key]);
            ClassicAssert.AreEqual(0, long.Parse(res[0].ToString()));
            ClassicAssert.AreEqual(val, res[1].ToString());

            var time = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(time.Value.TotalSeconds > 0);

            db.KeyExpire(key, TimeSpan.FromSeconds(expire));
            res = (RedisResult[])db.Execute("GETWITHETAG", [key]);
            ClassicAssert.AreEqual(0, long.Parse(res[0].ToString()));
            ClassicAssert.AreEqual(val, res[1].ToString());

            db.KeyPersist(key);
            res = (RedisResult[])db.Execute("GETWITHETAG", [key]);
            ClassicAssert.AreEqual(0, long.Parse(res[0].ToString()));
            ClassicAssert.AreEqual(val, res[1].ToString());

            Thread.Sleep((expire + 1) * 1000);

            var _val = db.StringGet(key);
            ClassicAssert.AreEqual(val, _val.ToString());

            time = db.KeyTimeToLive(key);
            ClassicAssert.IsNull(time);

            res = (RedisResult[])db.Execute("GETWITHETAG", [key]);
            ClassicAssert.AreEqual(0, long.Parse(res[0].ToString()));
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
            ClassicAssert.AreEqual(0, long.Parse(res[0].ToString()));
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

            // Do SETIWTHETAG
            ClassicAssert.AreEqual(0, long.Parse(db.Execute("SETWITHETAG", [key, "v1"]).ToString()));

            // Do SetAdd using the same key
            ClassicAssert.IsTrue(db.SetAdd(key, "v2"));

            // Two keys "test:1" - this is expected as of now
            // because Garnet has a separate main and object store
            var keys = server.Keys(db.Database, key).ToList();
            ClassicAssert.AreEqual(2, keys.Count);
            ClassicAssert.AreEqual(key, (string)keys[0]);
            ClassicAssert.AreEqual(key, (string)keys[1]);

            // do ListRightPush using the same key, expected error
            var ex = Assert.Throws<RedisServerException>(() => db.ListRightPush(key, "v3"));
            var expectedError = Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE);
            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(expectedError, ex.Message);
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

            ClassicAssert.AreEqual(0, long.Parse(db.Execute("SETWITHETAG", [key, value]).ToString()));

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
        public void SetRangeTestForEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "setRangeKey";
            string value = "0123456789";
            string newValue = "ABCDE";

            db.Execute("SETWITHETAG", [key, value]);

            var resp = db.StringGet(key);
            ClassicAssert.AreEqual("0123456789", resp.ToString());

            // new key, length 10, offset 5 -> 15 ("\0\0\0\0\00123456789")
            resp = db.StringSetRange(key, 5, value);
            ClassicAssert.AreEqual("15", resp.ToString());
            resp = db.StringGet(key);
            ClassicAssert.AreEqual("012340123456789", resp.ToString());

            // should update the etag internally
            var updatedEtagRes = db.Execute("GETWITHETAG", key);
            ClassicAssert.AreEqual(1, long.Parse(updatedEtagRes[0].ToString()));

            ClassicAssert.IsTrue(db.KeyDelete(key));

            // new key, length 10, offset -1 -> RedisServerException ("ERR offset is out of range")
            try
            {
                db.StringSetRange(key, -1, value);
                Assert.Fail();
            }
            catch (RedisServerException ex)
            {
                ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_OFFSETOUTOFRANGE), ex.Message);
            }

            // existing key, length 10, offset 0, value length 5 -> 10 ("ABCDE56789")
            db.Execute("SETWITHETAG", [key, value]);

            resp = db.StringSetRange(key, 0, newValue);
            ClassicAssert.AreEqual("10", resp.ToString());
            resp = db.StringGet(key);
            ClassicAssert.AreEqual("ABCDE56789", resp.ToString());

            // should update the etag internally
            updatedEtagRes = db.Execute("GETWITHETAG", key);
            ClassicAssert.AreEqual(1, long.Parse(updatedEtagRes[0].ToString()));

            ClassicAssert.IsTrue(db.KeyDelete(key));

            // key, length 10, offset 5, value length 5 -> 10 ("01234ABCDE")
            db.Execute("SETWITHETAG", [key, value]);

            resp = db.StringSetRange(key, 5, newValue);
            ClassicAssert.AreEqual("10", resp.ToString());

            updatedEtagRes = db.Execute("GETWITHETAG", key);
            ClassicAssert.AreEqual(1, long.Parse(updatedEtagRes[0].ToString()));

            resp = db.StringGet(key);
            ClassicAssert.AreEqual("01234ABCDE", resp.ToString());
            ClassicAssert.IsTrue(db.KeyDelete(key));

            // existing key, length 10, offset 10, value length 5 -> 15 ("0123456789ABCDE")
            db.Execute("SETWITHETAG", [key, value]);
            resp = db.StringSetRange(key, 10, newValue);
            ClassicAssert.AreEqual("15", resp.ToString());
            resp = db.StringGet(key);
            ClassicAssert.AreEqual("0123456789ABCDE", resp.ToString());
            ClassicAssert.IsTrue(db.KeyDelete(key));

            // existing key, length 10, offset 15, value length 5 -> 20 ("0123456789\0\0\0\0\0ABCDE")
            db.Execute("SETWITHETAG", [key, value]);

            resp = db.StringSetRange(key, 15, newValue);
            ClassicAssert.AreEqual("20", resp.ToString());
            resp = db.StringGet(key);
            ClassicAssert.AreEqual("0123456789\0\0\0\0\0ABCDE", resp.ToString());
            ClassicAssert.IsTrue(db.KeyDelete(key));

            // existing key, length 10, offset -1, value length 5 -> RedisServerException ("ERR offset is out of range")
            db.Execute("SETWITHETAG", [key, value]);
            try
            {
                db.StringSetRange(key, -1, newValue);
                Assert.Fail();
            }
            catch (RedisServerException ex)
            {
                ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_OFFSETOUTOFRANGE), ex.Message);
            }
        }

        [Test]
        public void KeepTtlTestForDataInitiallySetWithEtag()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int expire = 3;
            var keyA = "keyA";
            var keyB = "keyB";
            db.Execute("SETWITHETAG", [keyA, keyA]);
            db.Execute("SETWITHETAG", [keyB, keyB]);

            db.KeyExpire(keyA, TimeSpan.FromSeconds(expire));
            db.KeyExpire(keyB, TimeSpan.FromSeconds(expire));

            db.StringSet(keyA, keyA, keepTtl: true);
            var time = db.KeyTimeToLive(keyA);
            ClassicAssert.IsTrue(time.Value.Ticks > 0);

            db.StringSet(keyB, keyB, keepTtl: false);
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
            ClassicAssert.AreEqual(0, long.Parse(etagToCheck[0].ToString()));
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
            ClassicAssert.AreEqual(-2, (int)pttl);

            db.Execute("SETWITHETAG", [key, val]);

            pttl = db.Execute("PTTL", key);
            ClassicAssert.AreEqual(-1, (int)pttl);

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
            ClassicAssert.AreEqual(0, etagToCheck);
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
        public void AppendTestForEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "myKey";
            var val = "myKeyValue";
            var val2 = "myKeyValue2";

            db.Execute("SETWITHETAG", [key, val]);

            var len = db.StringAppend(key, val2);
            ClassicAssert.AreEqual(val.Length + val2.Length, len);

            var _val = db.StringGet(key);
            ClassicAssert.AreEqual(val + val2, _val.ToString());

            long etagToCheck = long.Parse(((RedisResult[])db.Execute("GETWITHETAG", [key]))[0].ToString());
            ClassicAssert.AreEqual(1, etagToCheck);

            db.KeyDelete(key);

            // Test appending an empty string
            db.Execute("SETWITHETAG", [key, val]);

            var len1 = db.StringAppend(key, "");
            ClassicAssert.AreEqual(val.Length, len1);

            _val = db.StringGet(key);
            ClassicAssert.AreEqual(val, _val.ToString());

            etagToCheck = long.Parse(((RedisResult[])db.Execute("GETWITHETAG", [key]))[0].ToString());
            // we appended nothing so this remains 0
            ClassicAssert.AreEqual(0, etagToCheck);

            // Test appending to a non-existent key
            var nonExistentKey = "nonExistentKey";
            var len2 = db.StringAppend(nonExistentKey, val2);
            ClassicAssert.AreEqual(val2.Length, len2);

            _val = db.StringGet(nonExistentKey);
            ClassicAssert.AreEqual(val2, _val.ToString());

            db.KeyDelete(key);

            // Test appending to a key with a large value
            var largeVal = new string('a', 1000000);
            db.Execute("SETWITHETAG", [key, largeVal]);
            var len3 = db.StringAppend(key, val2);
            ClassicAssert.AreEqual(largeVal.Length + val2.Length, len3);

            etagToCheck = long.Parse(((RedisResult[])db.Execute("GETWITHETAG", [key]))[0].ToString());
            ClassicAssert.AreEqual(1, etagToCheck);

            // Test appending to a key with metadata
            var keyWithMetadata = "keyWithMetadata";
            db.Execute("SETWITHETAG", [keyWithMetadata, val]);
            db.KeyExpire(keyWithMetadata, TimeSpan.FromSeconds(10000));
            etagToCheck = long.Parse(((RedisResult[])db.Execute("GETWITHETAG", [keyWithMetadata]))[0].ToString());
            ClassicAssert.AreEqual(0, etagToCheck);

            var len4 = db.StringAppend(keyWithMetadata, val2);
            ClassicAssert.AreEqual(val.Length + val2.Length, len4);

            _val = db.StringGet(keyWithMetadata);
            ClassicAssert.AreEqual(val + val2, _val.ToString());

            var time = db.KeyTimeToLive(keyWithMetadata);
            ClassicAssert.IsTrue(time.Value.TotalSeconds > 0);

            etagToCheck = long.Parse(((RedisResult[])db.Execute("GETWITHETAG", [keyWithMetadata]))[0].ToString());
            ClassicAssert.AreEqual(1, etagToCheck);
        }

        [Test]
        public void SetBitOperationsOnEtagSetData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "miki";
            // 64 BIT BITMAP
            Byte[] initialBitmap = new byte[8];
            string bitMapAsStr = Encoding.UTF8.GetString(initialBitmap); ;

            db.Execute("SETWITHETAG", [key, bitMapAsStr]);

            long setbits = db.StringBitCount(key);
            ClassicAssert.AreEqual(0, setbits);

            long etagToCheck = long.Parse(((RedisResult[])db.Execute("GETWITHETAG", [key]))[0].ToString());
            ClassicAssert.AreEqual(0, etagToCheck);

            // set all 64 bits one by one 
            var expectedBitCount = 0;
            for (int i = 0; i < 64; i++)
            {
                // SET the ith bit in the bitmap 
                bool originalValAtBit = db.StringSetBit(key, i, true);
                ClassicAssert.IsFalse(originalValAtBit);

                expectedBitCount++;

                bool currentBitVal = db.StringGetBit(key, i);
                ClassicAssert.IsTrue(currentBitVal);

                setbits = db.StringBitCount(key);
                ClassicAssert.AreEqual(expectedBitCount, setbits);

                // with each bit set that we do, we are increasing the etag as well by 1
                etagToCheck = long.Parse(((RedisResult[])db.Execute("GETWITHETAG", [key]))[0].ToString());
                ClassicAssert.AreEqual(expectedBitCount, etagToCheck);
            }

            var expectedEtag = expectedBitCount;
            // unset all 64 bits one by one in reverse order
            for (int i = 63; i > -1; i--)
            {
                bool originalValAtBit = db.StringSetBit(key, i, false);
                ClassicAssert.IsTrue(originalValAtBit);

                expectedEtag++;
                expectedBitCount--;

                bool currentBitVal = db.StringGetBit(key, i);
                ClassicAssert.IsFalse(currentBitVal);

                setbits = db.StringBitCount(key);
                ClassicAssert.AreEqual(expectedBitCount, setbits);

                etagToCheck = long.Parse(((RedisResult[])db.Execute("GETWITHETAG", [key]))[0].ToString());
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
            db.Execute("SETWITHETAG", [key, Encoding.UTF8.GetString(new byte[1])]); // Initialize key with an empty byte

            // Act - Set value to 127 (binary: 01111111)
            db.Execute("BITFIELD", key, "SET", "u8", "0", "127");

            long etagToCheck = long.Parse(((RedisResult[])db.Execute("GETWITHETAG", [key]))[0].ToString());
            ClassicAssert.AreEqual(1, etagToCheck);

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
            db.Execute("SETWITHETAG", [key, Encoding.UTF8.GetString(new byte[1])]); // Initialize key with an empty byte

            // Act - Set initial value to 255 and try to increment by 1
            db.Execute("BITFIELD", key, "SET", "u8", "0", "255");
            long etagToCheck = long.Parse(((RedisResult[])db.Execute("GETWITHETAG", [key]))[0].ToString());
            ClassicAssert.AreEqual(1, etagToCheck);

            var incrResult = db.Execute("BITFIELD", key, "INCRBY", "u8", "0", "1");

            etagToCheck = long.Parse(((RedisResult[])db.Execute("GETWITHETAG", [key]))[0].ToString());
            ClassicAssert.AreEqual(2, etagToCheck);

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
            db.Execute("SETWITHETAG", [key, Encoding.UTF8.GetString(new byte[1])]); // Initialize key with an empty byte

            // Act - Set initial value to 250 and try to increment by 10 with saturate overflow
            db.Execute("BITFIELD", key, "SET", "u8", "0", "250");

            long etagToCheck = long.Parse(((RedisResult[])db.Execute("GETWITHETAG", [key]))[0].ToString());
            ClassicAssert.AreEqual(1, etagToCheck);

            var incrResult = db.Execute("BITFIELD", key, "OVERFLOW", "SAT", "INCRBY", "u8", "0", "10");

            etagToCheck = long.Parse(((RedisResult[])db.Execute("GETWITHETAG", [key]))[0].ToString());
            ClassicAssert.AreEqual(2, etagToCheck);

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

            db.Execute("SETWITHETAG", [key, "mars"]);
            db.Execute("SETWITHETAG", [key2, "marsrover"]);

            RedisServerException ex = Assert.Throws<RedisServerException>(() => db.Execute("PFADD", [key, "woohoo"]));

            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE_HLL), ex.Message);

            ex = Assert.Throws<RedisServerException>(() => db.Execute("PFMERGE", [key, key2]));

            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE_HLL), ex.Message);
        }

        [Test]
        public void SetWithRetainEtagOnANewUpsertWillCreateKeyValueWithoutEtag()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "mickey";
            string val = "mouse";

            // a new upsert on a non-existing key will retain the "nil" etag
            db.Execute("SET", [key, val, "RETAINETAG"]).ToString();

            RedisResult[] res = (RedisResult[])db.Execute("GETWITHETAG", [key]);
            RedisResult etag = res[0];
            string value = res[1].ToString();

            ClassicAssert.IsTrue(etag.IsNull);
            ClassicAssert.AreEqual(val, value);

            string newval = "clubhouse";

            // a new upsert on an existing key will retain the "nil" etag from the prev
            db.Execute("SET", [key, newval, "RETAINETAG"]).ToString();
            res = (RedisResult[])db.Execute("GETWITHETAG", [key]);
            etag = res[0];
            value = res[1].ToString();

            ClassicAssert.IsTrue(etag.IsNull);
            ClassicAssert.AreEqual(newval, value);
        }

        #endregion
    }
}