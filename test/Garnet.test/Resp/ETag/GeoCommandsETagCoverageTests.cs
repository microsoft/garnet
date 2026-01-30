// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.Resp.ETag
{
    [TestFixture]
    public class GeoCommandsETagCoverageTests : EtagCoverageTestsBase
    {
        static readonly RedisKey[] GeoKeys = [KeyWithEtag, "geoKey2", "geoKey3"];

        static readonly GeoEntry[][] GeoData =
        [
            [new GeoEntry(2.0853, 34.7818, "Tel Aviv"), new GeoEntry(32.7940, 34.9896, "Haifa")],
            [new GeoEntry(7.9838, 23.7275, "Athens"), new GeoEntry(40.6401, 22.9444, "Thessaloniki")],
            [new GeoEntry(13.404954, 52.520008, "Berlin")]
        ];

        [Test]
        public async Task GeoAddETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { GeoKeys[0], GeoData[1][0].Longitude, GeoData[1][0].Latitude, GeoData[1][0].Member };
            await CheckCommandsAsync(RespCommand.GEOADD, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task GeoRadiusETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { GeoKeys[1], GeoData[1][0].Longitude, GeoData[1][0].Latitude, 1000, "KM", "STORE", GeoKeys[0] };
            await CheckCommandsAsync(RespCommand.GEORADIUS, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task GeoRadiusByMemberETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { GeoKeys[1], GeoData[1][0].Member, 1000, "KM", "STORE", GeoKeys[0] };
            await CheckCommandsAsync(RespCommand.GEORADIUSBYMEMBER, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task GeoSearchStoreETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { GeoKeys[0], GeoKeys[1], "FROMMEMBER", GeoData[1][0].Member, "BYRADIUS", 1000, "KM" };
            await CheckCommandsAsync(RespCommand.GEOSEARCHSTORE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        public override void DataSetUp()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.KeyDelete(GeoKeys);

            var geoAddCmdArgs = new object[] { "GEOADD", GeoKeys[0] }.Union(GeoData[0]
                .SelectMany(e => new[] { e.Longitude.ToString(), e.Latitude.ToString(), e.Member.ToString() })).ToArray();
            var results = (string[])db.Execute("EXECWITHETAG", geoAddCmdArgs);

            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(GeoData[0].Length, long.Parse(results[0]!));
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1

            var result = db.GeoAdd(GeoKeys[1], GeoData[1]);
            ClassicAssert.AreEqual(GeoData[1].Length, result);

            result = db.GeoAdd(GeoKeys[2], GeoData[2]);
            ClassicAssert.AreEqual(GeoData[2].Length, result);
        }
    }
}
