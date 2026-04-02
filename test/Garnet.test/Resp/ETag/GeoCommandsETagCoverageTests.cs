// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
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
    public class GeoCommandsETagCoverageTests : ETagCoverageTestsBase
    {
        const double DoubleTolerance = 1e-6;

        static readonly RedisKey[] GeoKeys = [KeysWithETag[0], "geoKey2", "geoKey3"];

        static readonly GeoEntry[][] GeoData =
        [
            [new GeoEntry(2.0853, 34.7818, "Tel Aviv"), new GeoEntry(32.7940, 34.9896, "Haifa")],
            [new GeoEntry(7.9838, 23.7275, "Athens"), new GeoEntry(40.6401, 22.9444, "Thessaloniki")],
            [new GeoEntry(13.404954, 52.520008, "Berlin")]
        ];

        [Test]
        public async Task GeoAddETagTestAsync([Values(true, false)] bool nxKey)
        {
            var cmdArgs = new object[] { GeoKeys[0], GeoData[1][0].Longitude, GeoData[1][0].Latitude, GeoData[1][0].Member };
            await CheckCommandAsync(RespCommand.GEOADD, cmdArgs, VerifyResult, nxKey: nxKey);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task GeoDistETagTestAsync()
        {
            var cmdArgs = new object[] { GeoKeys[0], GeoData[0][0].Member, GeoData[0][1].Member, "KM" };
            await CheckCommandAsync(RespCommand.GEODIST, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2790.730346476901, (double)result, DoubleTolerance);
            }
        }

        [Test]
        public async Task GeoHashETagTestAsync()
        {
            var cmdArgs = new object[] { GeoKeys[0], GeoData[0][0].Member, GeoData[0][1].Member };
            await CheckCommandAsync(RespCommand.GEOHASH, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, result.Length);
                ClassicAssert.AreEqual("sn1mz7y6xj0", (string)result[0]);
                ClassicAssert.AreEqual("swpr41xv5w0", (string)result[1]);
            }
        }

        [Test]
        public async Task GeoPosETagTestAsync()
        {
            var cmdArgs = new object[] { GeoKeys[0], GeoData[0][0].Member };
            await CheckCommandAsync(RespCommand.GEOPOS, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, result.Length);
                var lonLat = result[0];
                ClassicAssert.AreEqual(2, lonLat.Length);
                ClassicAssert.AreEqual(2.085302174091339, (double)lonLat[0], DoubleTolerance);
                ClassicAssert.AreEqual(34.781798869371414, (double)lonLat[1], DoubleTolerance);
            }
        }

        [Test]
        public async Task GeoRadiusETagTestAsync()
        {
            var cmdArgs = new object[] { GeoKeys[1], GeoData[1][0].Longitude, GeoData[1][0].Latitude, 1000, "KM", "STORE", GeoKeys[0] };
            await CheckCommandAsync(RespCommand.GEORADIUS, cmdArgs, VerifyResult);

            void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task GeoRadiusROETagTestAsync()
        {
            var cmdArgs = new object[] { GeoKeys[0], GeoData[0][0].Longitude, GeoData[0][0].Latitude, 1000, "KM" };
            await CheckCommandAsync(RespCommand.GEORADIUS_RO, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, result.Length);
                ClassicAssert.AreEqual(GeoData[0][0].Member, (string)result[0]);
            }
        }

        [Test]
        public async Task GeoRadiusByMemberETagTestAsync()
        {
            var cmdArgs = new object[] { GeoKeys[1], GeoData[1][0].Member, 1000, "KM", "STORE", GeoKeys[0] };
            await CheckCommandAsync(RespCommand.GEORADIUSBYMEMBER, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task GeoRadiusByMemberROETagTestAsync()
        {
            var cmdArgs = new object[] { GeoKeys[0], GeoData[0][0].Member, 1000, "KM" };
            await CheckCommandAsync(RespCommand.GEORADIUSBYMEMBER_RO, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, result.Length);
                ClassicAssert.AreEqual(GeoData[0][0].Member, (string)result[0]);
            }
        }

        [Test]
        public async Task GeoSearchETagTestAsync()
        {
            var cmdArgs = new object[] { GeoKeys[0], "FROMMEMBER", GeoData[0][0].Member, "BYRADIUS", 1000, "KM" };
            await CheckCommandAsync(RespCommand.GEOSEARCH, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, result.Length);
                ClassicAssert.AreEqual(GeoData[0][0].Member, (string)result[0]);
            }
        }

        [Test]
        public async Task GeoSearchStoreETagTestAsync()
        {
            var cmdArgs = new object[] { GeoKeys[0], GeoKeys[1], "FROMMEMBER", GeoData[1][0].Member, "BYRADIUS", 1000, "KM" };
            await CheckCommandAsync(RespCommand.GEOSEARCHSTORE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        public override void DataSetUp(bool nxKey = false)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.KeyDelete(GeoKeys);
            if (nxKey) return;

            var geoAddCmdArgs = new object[] { GeoKeys[0] }.Concat(GeoData[0]
                .SelectMany(e => new[] { e.Longitude.ToString(), e.Latitude.ToString(), e.Member.ToString() })).ToArray();
            var results = (string[])db.ExecWithETag("GEOADD", geoAddCmdArgs);

            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(GeoData[0].Length, long.Parse(results[0]!));
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // ETag 1

            var result = db.GeoAdd(GeoKeys[1], GeoData[1]);
            ClassicAssert.AreEqual(GeoData[1].Length, result);

            result = db.GeoAdd(GeoKeys[2], GeoData[2]);
            ClassicAssert.AreEqual(GeoData[2].Length, result);
        }
    }
}