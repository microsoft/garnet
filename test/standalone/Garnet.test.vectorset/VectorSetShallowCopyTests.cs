// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
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

        [Test]
        public async Task AddToCopyAsync()
        {
            const string CopyVectorSetKey = nameof(AddToCopyAsync);
            const string AddedElementKey = nameof(AddToCopyAsync) + "_Added";

            await using var redis = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig()).ConfigureAwait(false);
            var db = redis.GetDatabase();

            var baseSet = await MakeRandomVectorSetAsync(db).ConfigureAwait(false);
            var copySetRes = (string)await db.ExecuteAsync("XVSCOPY", baseSet.Name, CopyVectorSetKey).ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", copySetRes);

            var rand = new Random(baseSet.Seed);

            var values = new float[baseSet.Dimensions];
            for (var i = 0; i < values.Length; i++)
            {
                values[i] = rand.Next(256);
            }

            ClassicAssert.IsTrue(await db.VectorSetAddAsync(CopyVectorSetKey, VectorSetAddRequest.Member(AddedElementKey, values, "{\"foo\":\"bar\"}")).ConfigureAwait(false));

            using var baseEmbRes = await db.VectorSetGetApproximateVectorAsync(baseSet.Name, AddedElementKey).ConfigureAwait(false);
            ClassicAssert.IsTrue(baseEmbRes.IsEmpty);

            using var copyEmbRes = await db.VectorSetGetApproximateVectorAsync(CopyVectorSetKey, AddedElementKey).ConfigureAwait(false);
            ClassicAssert.IsTrue(values.SequenceEqual(copyEmbRes.Span));
        }

        [Test]
        public async Task UpdateCopyAsync()
        {
            const string CopyVectorSetKey = nameof(UpdateCopyAsync);

            await using var redis = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig()).ConfigureAwait(false);
            var db = redis.GetDatabase();

            var baseSet = await MakeRandomVectorSetAsync(db).ConfigureAwait(false);
            var copySetRes = (string)await db.ExecuteAsync("XVSCOPY", baseSet.Name, CopyVectorSetKey).ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", copySetRes);

            // Mutate every vector in the copy
            foreach (var (elem, oldValues, oldAttributes) in VectorSetValuesForSeed(baseSet.Seed))
            {
                var newValues = oldValues.Select(static i => i + 1).ToArray();
                var newAttrs = $"{{\"{elem}\": \"{elem}\"}}";
                ClassicAssert.IsFalse(await db.VectorSetAddAsync(CopyVectorSetKey, VectorSetAddRequest.Member(elem, newValues, newAttrs)).ConfigureAwait(false));
            }

            // Scan both to ensure changes isolated
            foreach (var (elem, oldValues, oldAttributes) in VectorSetValuesForSeed(baseSet.Seed))
            {
                using var baseEmbRes = await db.VectorSetGetApproximateVectorAsync(baseSet.Name, elem).ConfigureAwait(false);
                var baseAttrRes = await db.VectorSetGetAttributesJsonAsync(baseSet.Name, elem).ConfigureAwait(false);

                ClassicAssert.IsTrue(baseEmbRes.Span.SequenceEqual(oldValues));
                if (oldAttributes == null)
                {
                    ClassicAssert.IsNull(baseAttrRes);
                }
                else
                {
                    ClassicAssert.AreEqual(oldAttributes, baseAttrRes);
                }

                var newValues = oldValues.Select(static i => i + 1).ToArray();
                var newAttrs = $"{{\"{elem}\": \"{elem}\"}}";

                using var copyEmbRes = await db.VectorSetGetApproximateVectorAsync(CopyVectorSetKey, elem).ConfigureAwait(false);
                var copyAttrRes = await db.VectorSetGetAttributesJsonAsync(CopyVectorSetKey, elem).ConfigureAwait(false);

                ClassicAssert.IsTrue(copyEmbRes.Span.SequenceEqual(newValues));
                ClassicAssert.AreEqual(newAttrs, copyAttrRes);
            }
        }

        [Test]
        public async Task DeleteFromCopyAsync()
        {
            const string CopyVectorSetKey = nameof(UpdateCopyAsync);

            await using var redis = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig()).ConfigureAwait(false);
            var db = redis.GetDatabase();

            var baseSet = await MakeRandomVectorSetAsync(db).ConfigureAwait(false);
            var copySetRes = (string)await db.ExecuteAsync("XVSCOPY", baseSet.Name, CopyVectorSetKey).ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", copySetRes);

            // Delete every second element, remove the attributes on anything we don't delete
            {
                var ix = 0;
                foreach (var (elem, oldValues, oldAttributes) in VectorSetValuesForSeed(baseSet.Seed))
                {
                    if ((ix % 2) == 0)
                    {
                        ClassicAssert.True(await db.VectorSetRemoveAsync(CopyVectorSetKey, elem).ConfigureAwait(false));
                    }
                    else
                    {
                        if (oldAttributes != null)
                        {
                            ClassicAssert.IsTrue(await db.VectorSetSetAttributesJsonAsync(CopyVectorSetKey, elem, "").ConfigureAwait(false));
                        }
                        else
                        {
                            ClassicAssert.IsFalse(await db.VectorSetSetAttributesJsonAsync(CopyVectorSetKey, elem, "").ConfigureAwait(false));
                        }
                    }

                    ix++;
                }
            }

            // Scan both to ensure changes isolated
            {
                var ix = 0;
                foreach (var (elem, oldValues, oldAttributes) in VectorSetValuesForSeed(baseSet.Seed))
                {
                    using var baseEmbRes = await db.VectorSetGetApproximateVectorAsync(baseSet.Name, elem).ConfigureAwait(false);
                    var baseAttrRes = await db.VectorSetGetAttributesJsonAsync(baseSet.Name, elem).ConfigureAwait(false);

                    using var copyEmbRes = await db.VectorSetGetApproximateVectorAsync(CopyVectorSetKey, elem).ConfigureAwait(false);
                    var copyAttrRes = await db.VectorSetGetAttributesJsonAsync(CopyVectorSetKey, elem).ConfigureAwait(false);

                    ClassicAssert.IsTrue(baseEmbRes.Span.SequenceEqual(oldValues));
                    if (oldAttributes == null)
                    {
                        ClassicAssert.IsNull(baseAttrRes);
                    }
                    else
                    {
                        ClassicAssert.AreEqual(oldAttributes, baseAttrRes);
                    }

                    if ((ix % 2) == 0)
                    {
                        ClassicAssert.IsFalse(await db.VectorSetContainsAsync(CopyVectorSetKey, elem).ConfigureAwait(false));
                        ClassicAssert.IsTrue(copyEmbRes.IsEmpty);
                        ClassicAssert.IsNull(copyAttrRes);
                    }
                    else
                    {
                        ClassicAssert.IsTrue(await db.VectorSetContainsAsync(CopyVectorSetKey, elem).ConfigureAwait(false));
                        ClassicAssert.IsTrue(copyEmbRes.Span.SequenceEqual(oldValues));
                        ClassicAssert.IsNull(copyAttrRes);
                    }

                    ix++;
                }
            }
        }

        [Test]
        public async Task DeleteRootVectorSetAsync()
        {
            const string CopyVectorSetKey = nameof(DeleteRootVectorSetAsync);

            await using var redis = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig()).ConfigureAwait(false);
            var db = redis.GetDatabase();

            var baseSet = await MakeRandomVectorSetAsync(db).ConfigureAwait(false);
            var copySetRes = (string)await db.ExecuteAsync("XVSCOPY", baseSet.Name, CopyVectorSetKey).ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", copySetRes);

            // Make an addition to copy
            ClassicAssert.IsTrue(await db.VectorSetAddAsync(CopyVectorSetKey, VectorSetAddRequest.Member("fizzbuzz", Enumerable.Repeat(3.0f, baseSet.Dimensions).ToArray(), "{\"id\":12345}")).ConfigureAwait(false));

            // Make a removal
            var toRemove = VectorSetValuesForSeed(baseSet.Seed).ElementAt(10).Element;
            ClassicAssert.IsTrue(await db.VectorSetRemoveAsync(CopyVectorSetKey, toRemove));

            // Make an update
            var toUpdate = VectorSetValuesForSeed(baseSet.Seed).ElementAt(15).Element;
            ClassicAssert.IsFalse(await db.VectorSetAddAsync(CopyVectorSetKey, VectorSetAddRequest.Member(toUpdate, Enumerable.Repeat(8.0f, baseSet.Dimensions).ToArray(), "{\"id\":789}")).ConfigureAwait(false));

            // Delete the base Vector Set
            ClassicAssert.IsTrue(await db.KeyDeleteAsync(baseSet.Name).ConfigureAwait(false));

            // Wait for cleanup
            server.Provider.StoreWrapper.DefaultDatabase.VectorManager.WaitForQuiescence();

            // Validate we copied rows over
            ClassicAssert.IsFalse(await db.KeyExistsAsync(baseSet.Name));
            foreach (var (elem, vals, attrs) in VectorSetValuesForSeed(baseSet.Seed))
            {
                if (elem.Equals(toRemove))
                {
                    ClassicAssert.IsFalse(await db.VectorSetContainsAsync(CopyVectorSetKey, elem).ConfigureAwait(false));
                    continue;
                }

                var expectedVals = vals;
                var expectedAttrs = attrs;

                if (elem.Equals(toUpdate))
                {
                    expectedVals = Enumerable.Repeat(8.0f, baseSet.Dimensions).ToArray();
                    expectedAttrs = "{\"id\":789}";
                }

                using var actualVals = await db.VectorSetGetApproximateVectorAsync(CopyVectorSetKey, elem).ConfigureAwait(false);
                var actualAttrs = await db.VectorSetGetAttributesJsonAsync(CopyVectorSetKey, elem).ConfigureAwait(false);

                ClassicAssert.IsTrue(actualVals.Span.SequenceEqual(expectedVals));

                if (expectedAttrs == null)
                {
                    ClassicAssert.IsNull(actualAttrs);
                }
                else
                {
                    ClassicAssert.AreEqual(expectedAttrs, actualAttrs);
                }
            }

            using var copyAdditionValues = await db.VectorSetGetApproximateVectorAsync(CopyVectorSetKey, "fizzbuzz").ConfigureAwait(false);
            var copyAdditionAttrs = await db.VectorSetGetAttributesJsonAsync(CopyVectorSetKey, "fizzbuzz").ConfigureAwait(false);

            ClassicAssert.IsTrue(copyAdditionValues.Span.SequenceEqual(Enumerable.Repeat(3.0f, baseSet.Dimensions).ToArray()));
            ClassicAssert.AreEqual("{\"id\":12345}", copyAdditionAttrs);
        }

        [Test]
        public void DetermineShallowCopyDataCopies()
        {
            // Make sure completely empty works
            {
                var empty = VectorManager.DetermineShallowCopyDataCopies([], []);
                ClassicAssert.IsEmpty(empty);
            }

            // Make sure no shallow copies works
            {
                var empty = VectorManager.DetermineShallowCopyDataCopies([], [8]);
                ClassicAssert.IsEmpty(empty);
            }

            // Simple case, 1 shallow copy and we're deleting it's source
            {
                ConcurrentDictionary<ulong, ulong> shallowCopyBackRefs = new();
                shallowCopyBackRefs[16] = 8;

                var singleCopy = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [8]);
                AssertSame(new() { [8] = [16] }, singleCopy);
            }

            // Deleting each layer of a longer chain of shallow copies
            {
                ConcurrentDictionary<ulong, ulong> shallowCopyBackRefs = new();
                shallowCopyBackRefs[16] = 8;
                shallowCopyBackRefs[24] = 16;

                var deleteBase = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [8]);
                AssertSame(new() { [8] = [16] }, deleteBase);

                var deleteMid = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [16]);
                AssertSame(new() { [16] = [24] }, deleteMid);

                var deleteTip = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [24]);
                ClassicAssert.IsEmpty(deleteTip);

                var deleteBaseAndMid = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [8, 16]);
                AssertSame(new() { [8] = [24], [16] = [24] }, deleteBaseAndMid);

                var deleteBaseAndTip = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [8, 24]);
                AssertSame(new() { [8] = [16] }, deleteBaseAndTip);

                var deleteMidAndTip = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [16, 24]);
                ClassicAssert.IsEmpty(deleteMidAndTip);

                var deleteAll = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [8, 16, 24]);
                ClassicAssert.IsEmpty(deleteAll);
            }

            // Delete base with two copies
            {
                ConcurrentDictionary<ulong, ulong> shallowCopyBackRefs = new();
                shallowCopyBackRefs[16] = 8;
                shallowCopyBackRefs[24] = 8;

                var deleteBase = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [8]);
                AssertSame(new() { [8] = [16, 24] }, deleteBase);

                var deleteTip0 = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [16]);
                ClassicAssert.IsEmpty(deleteTip0);

                var deleteTip1 = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [24]);
                ClassicAssert.IsEmpty(deleteTip1);

                var deleteAll = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [8, 16, 24]);
                ClassicAssert.IsEmpty(deleteAll);
            }

            // Delete each layer of longer chain with multiple copies of single base
            {
                ConcurrentDictionary<ulong, ulong> shallowCopyBackRefs = new();
                shallowCopyBackRefs[16] = 8;
                shallowCopyBackRefs[24] = 8;
                shallowCopyBackRefs[32] = 16;
                shallowCopyBackRefs[40] = 24;
                shallowCopyBackRefs[48] = 24;

                var deleteBase = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [8]);
                AssertSame(new() { [8] = [16, 24] }, deleteBase);

                var deleteMid0 = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [16]);
                AssertSame(new() { [16] = [32] }, deleteMid0);

                var deleteMid1 = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [24]);
                AssertSame(new() { [24] = [40, 48] }, deleteMid1);

                var deleteAllMid = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [16, 24]);
                AssertSame(new() { [16] = [32], [24] = [40, 48] }, deleteAllMid);

                var deleteBaseAndAllMid = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [8, 16, 24]);
                AssertSame(new() { [8] = [32, 40, 48], [16] = [32], [24] = [40, 48] }, deleteBaseAndAllMid);

                var deleteTip0 = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [32]);
                ClassicAssert.IsEmpty(deleteTip0);

                var deleteTip1 = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [40]);
                ClassicAssert.IsEmpty(deleteTip1);

                var deleteTip2 = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [48]);
                ClassicAssert.IsEmpty(deleteTip2);

                var deleteBaseAndAllTip = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [8, 32, 40, 48]);
                AssertSame(new() { [8] = [16, 24] }, deleteBaseAndAllTip);

                var deleteAllMidAndAllTip = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [24, 32, 40, 48]);
                ClassicAssert.IsEmpty(deleteAllMidAndAllTip);

                var deleteAll = VectorManager.DetermineShallowCopyDataCopies(shallowCopyBackRefs, [8, 16, 24, 32, 40, 48]);
                ClassicAssert.IsEmpty(deleteAll);
            }

            // Compare two dictionary maps for equality
            static void AssertSame(Dictionary<ulong, List<ulong>> expected, Dictionary<ulong, List<ulong>> actual)
            {
                ClassicAssert.AreEqual(expected.Count, actual.Count);
                foreach (var (key, expectedVal) in expected)
                {
                    ClassicAssert.IsTrue(actual.TryGetValue(key, out var actualVal));

                    ClassicAssert.AreEqual(expectedVal.Count, actualVal.Count);
                    ClassicAssert.AreEqual(actualVal.Count, actualVal.Distinct().Count());

                    foreach (var context in expectedVal)
                    {
                        ClassicAssert.Contains(context, actualVal);
                    }
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