// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.vectorset
{
    public class VectorSetShallowCopyTests : TestBase
    {
        private const string DefaultAOFMemorySize = "2g";  // Very large because CI boxes have low IOPS, so try and flush to disk veeeeeery rarely

        private GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = CreateGarnetServer(tryRecover: false);

            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        public async Task ReadSimpleCopyAsync()
        {
            const string CopyVectorSetKey = nameof(ReadSimpleCopyAsync);
            const int ProbeSearchCount = 100;

            await using var redis = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig()).ConfigureAwait(false);
            var db = redis.GetDatabase();

            var baseSet = await MakeRandomVectorSetAsync(db).ConfigureAwait(false);
            var copySetRes = (string)await db.ExecuteAsync("XVSCOPY", baseSet.Name, CopyVectorSetKey).ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", copySetRes);

            // Scan shallow copy to ensure all raw values are found
            foreach (var (elem, vals, attr) in VectorSetValuesForSeed(baseSet.Seed))
            {
                using var embRes = await db.VectorSetGetApproximateVectorAsync(CopyVectorSetKey, elem).ConfigureAwait(false);
                var attrRes = await db.VectorSetGetAttributesJsonAsync(CopyVectorSetKey, elem).ConfigureAwait(false);

                ClassicAssert.IsTrue(embRes.Span.SequenceEqual(vals));
                if (attr == null)
                {
                    ClassicAssert.IsNull(attrRes);
                }
                else
                {
                    ClassicAssert.AreEqual(attr, attrRes);
                }
            }

            // Perform some (repatably) random searches and ensure we get _identical_ results
            var rand = new Random(baseSet.Seed);

            for (var i = 0; i < ProbeSearchCount; i++)
            {
                var searchVec = new float[baseSet.Dimensions];
                for (var j = 0; j < searchVec.Length; j++)
                {
                    searchVec[j] = rand.Next(256);
                }

                var searchReq = VectorSetSimilaritySearchRequest.ByVector(searchVec);
                searchReq.WithScores = true;
                searchReq.WithAttributes = true;

                using var baseSearchRes = await db.VectorSetSimilaritySearchAsync(baseSet.Name, searchReq).ConfigureAwait(false);
                using var copySearchRes = await db.VectorSetSimilaritySearchAsync(CopyVectorSetKey, searchReq).ConfigureAwait(false);

                ClassicAssert.AreEqual(baseSearchRes.Length, copySearchRes.Length);

                for (var j = 0; j < baseSearchRes.Length; j++)
                {
                    var baseRes = baseSearchRes.Span[j];
                    var copyRes = copySearchRes.Span[j];

                    ClassicAssert.AreEqual(baseRes.Member, copyRes.Member);
                    ClassicAssert.AreEqual(baseRes.Score, copyRes.Score);
                    ClassicAssert.AreEqual(baseRes.AttributesJson, copyRes.AttributesJson);
                }
            }
        }

        /// <summary>
        /// Create a (repeatbly) random Vector Set to test with.
        /// </summary>
        private async Task<(string Name, int Seed, int Dimensions)> MakeRandomVectorSetAsync(IDatabase db, [CallerLineNumber] int line = -1, [CallerMemberName] string caller = null)
        {
            ClassicAssert.IsNotNull(caller);

            var seed = 2026_11_12_00 + line;

            var key = $"{caller}_root_{seed}";

            int? dims = null;

            foreach (var (elem, vals, attr) in VectorSetValuesForSeed(seed))
            {
                dims ??= vals.Length;

                var res = await db.VectorSetAddAsync(key, VectorSetAddRequest.Member(elem, vals, attr)).ConfigureAwait(false);
                ClassicAssert.IsTrue(res);
            }

            return (key, seed, dims.Value);
        }

        private static IEnumerable<(string Element, float[] Values, string Attribute)> VectorSetValuesForSeed(int seed)
        {
            const int VectorSetSize = 100;

            var rand = new Random(seed);

            var dims = rand.Next(16) + 1;

            for (var i = 0; i < VectorSetSize; i++)
            {
                var elem = $"elem_{i}";
                var attr = rand.Next(2) == 0 ? $"{{\"id\":{i}}}" : null;
                var vals = new float[dims];
                for (var j = 0; j < vals.Length; j++)
                {
                    vals[j] = rand.Next(256);
                }

                yield return (elem, vals, attr);
            }
        }

        /// <summary>
        /// Create a new GarnetServer instance with common parameters.
        /// </summary>
        private static GarnetServer CreateGarnetServer(bool tryRecover, bool enableVectorSetPreview = true)
        => TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, tryRecover: tryRecover, aofMemorySize: DefaultAOFMemorySize, enableVectorSetPreview: enableVectorSetPreview);
    }
}