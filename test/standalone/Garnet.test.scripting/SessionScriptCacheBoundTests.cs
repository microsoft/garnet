// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Reflection;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// The per-session script cache holds a whole Lua VM per entry, so it is capped and evicts the
    /// least recently used script.
    ///
    /// The instrument here is the cache's own entry count and key membership, deliberately not a
    /// memory measurement: the default memory management mode is Native, so the VM lives outside the
    /// managed heap and a forced compacting collection cannot observe <c>lua_close</c> at all.
    /// </summary>
    [TestFixture]
    public class SessionScriptCacheBoundTests : TestBase
    {
        const int CacheSize = 4;

        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableLua: true, luaScriptCacheSize: CacheSize);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// A distinct script whose body is unique per index, so each one gets its own digest.
        /// </summary>
        static string ScriptFor(int i) => $"return {i}";

        /// <summary>
        /// Reaches the one session behind the given connection. The tests run a single client, so
        /// there is exactly one RESP session to find.
        /// </summary>
        SessionScriptCache SoleSessionCache()
        {
            var serversField = typeof(GarnetServer).GetField("servers", BindingFlags.Instance | BindingFlags.NonPublic);
            ClassicAssert.IsNotNull(serversField, "GarnetServer should still hold its listeners in a 'servers' field");

            var caches = new List<SessionScriptCache>();
            foreach (var listener in (IGarnetServer[])serversField.GetValue(server))
            {
                foreach (var consumer in ((GarnetServerBase)listener).ActiveConsumers())
                {
                    if (consumer is not RespServerSession resp)
                        continue;

                    var cacheField = typeof(RespServerSession).GetField("sessionScriptCache", BindingFlags.Instance | BindingFlags.NonPublic);
                    ClassicAssert.IsNotNull(cacheField, "RespServerSession should still hold its per-session script cache in a 'sessionScriptCache' field");

                    if (cacheField.GetValue(resp) is SessionScriptCache cache)
                        caches.Add(cache);
                }
            }

            ClassicAssert.AreEqual(1, caches.Count, "expected exactly one live RESP session with a script cache");
            return caches[0];
        }

        byte[] DigestOf(SessionScriptCache cache, string script)
        {
            var digest = new byte[SessionScriptCache.SHA1Len];
            cache.GetScriptDigest(System.Text.Encoding.UTF8.GetBytes(script), digest);
            return digest;
        }

        /// <summary>
        /// Running more distinct scripts than the cache holds must not grow the cache past its cap.
        /// Without eviction every distinct script is retained for the life of the session, which is
        /// the ratchet this cap exists to remove.
        /// </summary>
        [Test]
        public void RunningMoreDistinctScriptsThanTheCapDoesNotGrowTheCache()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const int Distinct = CacheSize * 4;
            for (var i = 0; i < Distinct; i++)
                ClassicAssert.AreEqual(i, (int)db.ScriptEvaluate(ScriptFor(i)));

            var cache = SoleSessionCache();
            ClassicAssert.LessOrEqual(cache.CachedScriptCount, CacheSize,
                $"the session cached {cache.CachedScriptCount} scripts against a cap of {CacheSize}, so it is still unbounded");
        }

        /// <summary>
        /// The eviction has to be least-recently-used rather than arbitrary, so a script that keeps
        /// being used must outlive colder ones added after it.
        ///
        /// This cannot be asserted through the wire, because an evicted script still runs correctly
        /// by recompiling from the global cache -- so behaviour alone cannot tell an LRU apart from
        /// evicting whatever is convenient.
        /// </summary>
        [Test]
        public void ARepeatedlyUsedScriptOutlivesColderOnes()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var hot = ScriptFor(1000);
            ClassicAssert.AreEqual(1000, (int)db.ScriptEvaluate(hot));

            var cache = SoleSessionCache();
            var hotDigest = DigestOf(cache, hot);

            // Fill past the cap, touching the hot script before each newcomer so it is never the
            // coldest entry.
            for (var i = 0; i < CacheSize * 3; i++)
            {
                ClassicAssert.AreEqual(1000, (int)db.ScriptEvaluate(hot));
                ClassicAssert.AreEqual(i, (int)db.ScriptEvaluate(ScriptFor(i)));
            }

            ClassicAssert.IsTrue(cache.ContainsDigest(hotDigest),
                "the repeatedly used script was evicted while colder scripts added after it were kept, so eviction is not least-recently-used");
        }

        /// <summary>
        /// The one behavioural regression a cap can introduce: an EVALSHA whose script has been
        /// evicted from the session cache must still run, by recompiling from the global cache,
        /// rather than failing with NOSCRIPT.
        /// </summary>
        [Test]
        public void AnEvictedScriptStillRunsByDigest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var first = ScriptFor(4242);
            var loaded = db.ScriptEvaluate(first);
            ClassicAssert.AreEqual(4242, (int)loaded);

            var cache = SoleSessionCache();
            var firstDigest = DigestOf(cache, first);

            // Push it out with enough distinct scripts that it cannot still be resident.
            for (var i = 0; i < CacheSize * 3; i++)
                ClassicAssert.AreEqual(i, (int)db.ScriptEvaluate(ScriptFor(i)));

            ClassicAssert.IsFalse(cache.ContainsDigest(firstDigest),
                "the script under test was still cached, so this run never exercised the evicted path");

            // Re-running by digest must recompile transparently.
            var byDigest = db.ScriptEvaluate(System.Text.Encoding.UTF8.GetString(firstDigest), [], []);
            ClassicAssert.AreEqual(4242, (int)byDigest,
                "an evicted script failed to run by digest, so the cap turned a recompile into a client-visible error");
        }

        /// <summary>
        /// A script used repeatedly from the cache keeps returning the right answer -- i.e. eviction
        /// of its neighbours does not disturb the entries that remain.
        /// </summary>
        [Test]
        public void SurvivingEntriesStillReturnCorrectResults()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            for (var round = 0; round < 6; round++)
            {
                for (var i = 0; i < CacheSize * 2; i++)
                    ClassicAssert.AreEqual(i, (int)db.ScriptEvaluate(ScriptFor(i)),
                        $"script {i} returned the wrong result on round {round}");
            }
        }

        /// <summary>
        /// A cap of zero means no limit, which is the escape hatch if a deployment genuinely needs
        /// every script resident.
        /// </summary>
        [Test]
        public void AZeroCapRetainsEveryScript()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableLua: true, luaScriptCacheSize: 0);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const int Distinct = CacheSize * 4;
            for (var i = 0; i < Distinct; i++)
                ClassicAssert.AreEqual(i, (int)db.ScriptEvaluate(ScriptFor(i)));

            var cache = SoleSessionCache();
            ClassicAssert.AreEqual(Distinct, cache.CachedScriptCount,
                "a zero cap must disable eviction entirely");
        }
    }
}