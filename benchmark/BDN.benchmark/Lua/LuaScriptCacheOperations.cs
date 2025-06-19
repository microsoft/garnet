// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Embedded.server;
using Garnet.common;
using Garnet.server;
using Garnet.server.Auth;

namespace BDN.benchmark.Lua
{
    [MemoryDiagnoser]
    public class LuaScriptCacheOperations
    {
        /// <summary>
        /// Lua parameters
        /// </summary>
        [ParamsSource(nameof(LuaParamsProvider))]
        public LuaParams Params { get; set; }

        /// <summary>
        /// Lua parameters provider
        /// </summary>
        public IEnumerable<LuaParams> LuaParamsProvider()
        => [
            new(LuaMemoryManagementMode.Native, false),
            new(LuaMemoryManagementMode.Tracked, false),
            new(LuaMemoryManagementMode.Tracked, true),
            new(LuaMemoryManagementMode.Managed, false),
            new(LuaMemoryManagementMode.Managed, true),
        ];

        private EmbeddedRespServer server;
        private StoreWrapper storeWrapper;
        private SessionScriptCache sessionScriptCache;
        private RespServerSession session;

        private byte[] outerHitDigest;
        private byte[] innerHitDigest;
        private byte[] missDigest;

        [GlobalSetup]
        public void GlobalSetup()
        {
            var options = Params.CreateOptions();

            server = new EmbeddedRespServer(new GarnetServerOptions() { EnableLua = true, QuietMode = true, LuaOptions = options });
            storeWrapper = server.StoreWrapper;
            sessionScriptCache = new SessionScriptCache(storeWrapper, new GarnetNoAuthAuthenticator(), null);
            session = server.GetRespSession();

            outerHitDigest = GC.AllocateUninitializedArray<byte>(SessionScriptCache.SHA1Len, pinned: true);
            sessionScriptCache.GetScriptDigest("return 1"u8, outerHitDigest);
            if (!storeWrapper.storeScriptCache.TryAdd(new(outerHitDigest), new("return 1"u8.ToArray())))
            {
                throw new InvalidOperationException("Should have been able to load into global cache");
            }

            innerHitDigest = GC.AllocateUninitializedArray<byte>(SessionScriptCache.SHA1Len, pinned: true);
            sessionScriptCache.GetScriptDigest("return 1 + 1"u8, innerHitDigest);
            if (!storeWrapper.storeScriptCache.TryAdd(new(innerHitDigest), new("return 1 + 1"u8.ToArray())))
            {
                throw new InvalidOperationException("Should have been able to load into global cache");
            }

            missDigest = GC.AllocateUninitializedArray<byte>(SessionScriptCache.SHA1Len, pinned: true);
            sessionScriptCache.GetScriptDigest("foobar"u8, missDigest);
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            session?.Dispose();
            server?.Dispose();
        }

        [IterationSetup]
        public void IterationSetup()
        {
            // Force lookup to do work
            sessionScriptCache.Clear();

            // Make outer hit available for every iteration
            LuaScriptHandle scriptHandle = null;
            if (!sessionScriptCache.TryLoad(session, "return 1"u8, new(outerHitDigest), ref scriptHandle, out _, out _))
            {
                throw new InvalidOperationException("Should have been able to load");
            }
        }

        [Benchmark]
        public void LookupHit()
        {
            _ = sessionScriptCache.TryGetFromDigest(new(outerHitDigest), out _, out _);
        }

        [Benchmark]
        public void LookupMiss()
        {
            _ = sessionScriptCache.TryGetFromDigest(new(missDigest), out _, out _);
        }

        [Benchmark]
        public void LoadOuterHit()
        {
            // First if returns true
            //
            // This is the common case
            LoadScript(outerHitDigest);
        }

        [Benchmark]
        public void LoadInnerHit()
        {
            // First if returns false, second if returns true
            //
            // This is expected, but rare
            LoadScript(innerHitDigest);
        }

        [Benchmark]
        public void LoadMiss()
        {
            // First if returns false, second if returns false
            //
            // This is extremely unlikely, basically implies an error on the client
            LoadScript(missDigest);
        }

        [Benchmark]
        public void Digest()
        {
            Span<byte> digest = stackalloc byte[SessionScriptCache.SHA1Len];
            sessionScriptCache.GetScriptDigest("return 1 + redis.call('GET', KEYS[1])"u8, digest);
        }

        /// <summary>
        /// The moral equivalent to our cache load operation.
        /// </summary>
        private void LoadScript(Span<byte> digest)
        {
            AsciiUtils.ToLowerInPlace(digest);

            var digestKey = new ScriptHashKey(digest);

            if (!sessionScriptCache.TryGetFromDigest(digestKey, out var runner, out _))
            {
                if (storeWrapper.storeScriptCache.TryGetValue(digestKey, out var scriptHandle))
                {
                    LuaScriptHandle newScriptHandle = null;
                    if (!sessionScriptCache.TryLoad(session, scriptHandle.ScriptData.Span, digestKey, ref newScriptHandle, out runner, out _))
                    {
                        // TryLoad will have written an error out, it any

                        _ = storeWrapper.storeScriptCache.TryRemove(digestKey, out _);
                    }
                }
            }
        }
    }
}