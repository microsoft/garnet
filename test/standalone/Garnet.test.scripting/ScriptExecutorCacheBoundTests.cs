// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// A script executor holds a whole Lua VM per cached script, so its cache is capped and evicts the
    /// least recently used script.
    ///
    /// The instrument here is the cache's own entry count and key membership, deliberately not a
    /// memory measurement: the default memory management mode is Native, so the VM lives outside the
    /// managed heap and a forced compacting collection cannot observe <c>lua_close</c> at all.
    /// </summary>
    [TestFixture]
    public class ScriptExecutorCacheBoundTests : TestBase
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
        /// Runs <paramref name="read"/> against the one executor behind the server. The tests drive scripts
        /// one at a time, so the pool holds exactly one idle executor to rent.
        /// </summary>
        T Inspect<T>(Func<ScriptExecutor, T> read)
        {
            var storeWrapper = server.Provider.StoreWrapper;
            var pool = storeWrapper.scriptExecutorPool;
            ClassicAssert.AreEqual(1, pool.IdleCount, "expected exactly one pooled script executor");

            var executor = pool.Rent(null);
            try
            {
                return read(executor);
            }
            finally
            {
                pool.Return(executor);
            }
        }

        int CachedScriptCount() => Inspect(static executor => executor.CachedScriptCount);

        bool ContainsDigest(byte[] digest) => Inspect(executor => executor.ContainsDigest(digest));

        static byte[] DigestOf(string script)
        {
            var digest = new byte[ScriptExecutor.SHA1Len];
            ScriptExecutor.GetScriptDigest(System.Text.Encoding.UTF8.GetBytes(script), digest);
            return digest;
        }

        /// <summary>
        /// Executors are pooled across sessions, so scripts run by many connections one after another
        /// share a single executor (one Lua working set) rather than one per connection.
        /// </summary>
        [Test]
        public void SessionsShareAPooledExecutor()
        {
            for (var i = 0; i < 32; i++)
            {
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                ClassicAssert.AreEqual(i, (int)redis.GetDatabase(0).ScriptEvaluate(ScriptFor(i)));
            }

            ClassicAssert.AreEqual(1, server.Provider.StoreWrapper.scriptExecutorPool.IdleCount,
                "32 sessions ran scripts one after another but did not share a single executor");
        }

        /// <summary>
        /// Concurrent sessions each rent their own executor; none may observe another's script state.
        /// </summary>
        [Test]
        public void ConcurrentSessionsGetTheirOwnResults()
        {
            const int Sessions = 16;
            const int Iterations = 100;

            var wrong = new ConcurrentQueue<string>();
            Parallel.For(0, Sessions, new ParallelOptions { MaxDegreeOfParallelism = Sessions }, session =>
            {
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                var db = redis.GetDatabase(0);
                for (var i = 0; i < Iterations; i++)
                {
                    var arg = session * 1000 + i;
                    var result = (int)db.ScriptEvaluate("return tonumber(ARGV[1]) + 1", [], [arg]);
                    if (result != arg + 1)
                        wrong.Enqueue($"session {session} iteration {i}: expected {arg + 1}, got {result}");
                }
            });

            ClassicAssert.IsEmpty(wrong, string.Join(Environment.NewLine, wrong.Take(5)));

            var idle = server.Provider.StoreWrapper.scriptExecutorPool.IdleCount;
            ClassicAssert.That(idle, Is.InRange(1, Sessions));
        }

        /// <summary>
        /// Running more distinct scripts than the cache holds must not grow the cache past its cap.
        /// Without eviction every distinct script is retained for the life of the executor, which is
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

            var cached = CachedScriptCount();
            ClassicAssert.LessOrEqual(cached, CacheSize,
                $"the executor cached {cached} scripts against a cap of {CacheSize}, so it is still unbounded");
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

            var hotDigest = DigestOf(hot);

            // Fill past the cap, touching the hot script before each newcomer so it is never the
            // coldest entry.
            for (var i = 0; i < CacheSize * 3; i++)
            {
                ClassicAssert.AreEqual(1000, (int)db.ScriptEvaluate(hot));
                ClassicAssert.AreEqual(i, (int)db.ScriptEvaluate(ScriptFor(i)));
            }

            ClassicAssert.IsTrue(ContainsDigest(hotDigest),
                "the repeatedly used script was evicted while colder scripts added after it were kept, so eviction is not least-recently-used");
        }

        /// <summary>
        /// The one behavioural regression a cap can introduce: an EVALSHA whose script has been
        /// evicted from the executor's cache must still run, by recompiling from the global cache,
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

            var firstDigest = DigestOf(first);

            // Push it out with enough distinct scripts that it cannot still be resident.
            for (var i = 0; i < CacheSize * 3; i++)
                ClassicAssert.AreEqual(i, (int)db.ScriptEvaluate(ScriptFor(i)));

            ClassicAssert.IsFalse(ContainsDigest(firstDigest),
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

            ClassicAssert.AreEqual(Distinct, CachedScriptCount(),
                "a zero cap must disable eviction entirely");
        }
    }
}