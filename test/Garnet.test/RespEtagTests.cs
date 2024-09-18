
using System.Text;
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

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
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

            RedisResult[] setIfMatchRes = (RedisResult[])db.Execute("SETIFMATCH", [key, "nextone", initalEtag]);

            long nextEtag = long.Parse(setIfMatchRes[0].ToString());
            string value = setIfMatchRes[1].ToString();

            ClassicAssert.AreEqual(1, nextEtag);
            ClassicAssert.AreEqual(value, "nextone");

            setIfMatchRes = (RedisResult[])db.Execute("SETIFMATCH", [key, "nextnextone", nextEtag]);
            nextEtag = long.Parse(setIfMatchRes[0].ToString());
            value = setIfMatchRes[1].ToString();

            ClassicAssert.AreEqual(2, nextEtag);
            ClassicAssert.AreEqual(value, "nextnextone");

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
            var _ = db.Execute("SETWITHETAG", [key, "hkhalid"]);

            RedisResult[] res = (RedisResult[])db.Execute("GETWITHETAG", [key]);
            long etag= long.Parse(res[0].ToString());
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
            long etag= long.Parse(res[0].ToString());
            string value = res[1].ToString();

            ClassicAssert.AreEqual(0, etag);
            ClassicAssert.AreEqual("maximus", value);
        }

        #endregion

        #region ETAG Apis with non-etag data

        // ETAG Apis with non-Etag data just tests that in all scenarios we always return wrong data type response
        [Test]
        public void SetIfMatchOnNonEtagDataReturnsWrongType()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            var _ = db.StringSet("h", "k");

            RedisServerException ex = Assert.Throws<RedisServerException>(() => db.Execute("SETIFMATCH", ["h", "t", "0"]));

            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE), ex.Message);

            ex = Assert.Throws<RedisServerException>(() => db.Execute("SETIFMATCH", ["h", "t", "1"]));

            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE), ex.Message);
        }

        [Test]
        public void GetIfNotMatchOnNonEtagDataReturnsWrongType()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            var _ = db.StringSet("h", "k");

            RedisServerException ex = Assert.Throws<RedisServerException>(() => db.Execute("GETIFNOTMATCH", ["h", "0"]));

            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE), ex.Message);

            ex = Assert.Throws<RedisServerException>(() => db.Execute("GETIFNOTMATCH", ["h", "1"]));

            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE), ex.Message);
        }

        #endregion
    }
}