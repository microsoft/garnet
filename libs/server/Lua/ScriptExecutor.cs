// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Garnet.common;
using Garnet.server.ACL;
using Garnet.server.Auth;
using Garnet.server.Lua;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Runs Lua scripts: a cache of compiled scripts (one Lua state each) plus the inner
    /// <see cref="RespServerSession"/> those scripts issue <c>redis.call</c> commands through.
    /// </summary>
    /// <remarks>
    /// Executors are shared by all sessions and rented from a <see cref="ScriptExecutorPool"/> for the
    /// duration of one EVAL, EVALSHA or SCRIPT LOAD. The Lua and storage working set therefore scales with
    /// the number of scripts running concurrently rather than with the number of connections. An executor
    /// is used by one thread at a time.
    /// </remarks>
    internal sealed class ScriptExecutor : IDisposable
    {
        // Important to keep the hash length to this value 
        // for compatibility
        internal const int SHA1Len = 40;
        readonly RespServerSession processor;
        readonly StoreWrapper storeWrapper;
        readonly ScratchBufferNetworkSender scratchBufferNetworkSender;
        readonly ILogger logger;
        readonly Dictionary<ScriptHashKey, CacheEntry> scriptCache = [];

        // Largest number of compiled scripts this executor retains, or 0 for no limit.
        readonly int scriptCacheSize;

        // Returns since the scratch buffers were last trimmed
        int returnsSinceTrim;

        // Monotonic recency stamp. Incremented on every cache hit and insert, so the entry
        // holding the smallest value is the least recently used one.
        long scriptCacheClock;

        /// <summary>
        /// One cached script: the executor's compiled runner, the global handle it was compiled
        /// from, and the recency stamp used to choose an eviction victim.
        /// </summary>
        struct CacheEntry
        {
            public LuaRunner Runner;
            public LuaScriptHandle Handle;
            public long LastUsed;
        }

        readonly LuaMemoryManagementMode memoryManagementMode;
        readonly int? memoryLimitBytes;
        readonly LuaTimeoutManager timeoutManager;
        readonly LuaLoggingMode logMode;
        readonly HashSet<string> allowedFunctions;

        LuaRunner timeoutRunningScript;
        LuaTimeoutManager.Registration timeoutRegistration;

        // Tracks in-flight script executions so disposal can wait for a running script to
        // finish before freeing the Lua state. The Lua allocators are single-thread-by-design,
        // so closing the state (lua_close) concurrently with an executing script corrupts the heap.
        readonly ActiveWorkerMonitor scriptRunMonitor = new();

        // Provides a unique value for script invocations
        //
        // It doesn't need to be globally unique, it just needs to be able
        // distiguish two different runs of some script on the same executor.
        //
        // It's OK if this wraps around, because ~4 billion invocations are unlikely
        // to race.
        uint timeoutRunningCookie;

        public ScriptExecutor(StoreWrapper storeWrapper, LuaTimeoutManager timeoutManager, ILogger logger = null)
        {
            this.storeWrapper = storeWrapper;
            this.timeoutManager = timeoutManager;
            this.logger = logger;

            scratchBufferNetworkSender = new ScratchBufferNetworkSender(storeWrapper.serverOptions.GetSessionScratchBufferMaxRetainedSize());
            // Pass storeWrapper.subscribeBroker so Lua scripts can use publish-side Pub/Sub
            // commands (e.g. redis.call('PUBLISH', ...)) consistently with network sessions.
            // SUBSCRIBE/PSUBSCRIBE remain blocked by the NoScript bitmap.
            //
            // The processor runs on behalf of whichever session rented the executor, which has already
            // passed authentication and the ACL check for the script command itself, so it is always
            // authenticated; the renting session's user handle supplies the per-command ACL checks.
            processor = new RespServerSession(0, scratchBufferNetworkSender, storeWrapper, storeWrapper.subscribeBroker, new GarnetNoAuthAuthenticator(), false);

            // There's some parsing involved in these, so save them off per-executor
            memoryManagementMode = storeWrapper.serverOptions.LuaOptions.MemoryManagementMode;
            memoryLimitBytes = storeWrapper.serverOptions.LuaOptions.GetMemoryLimitBytes();
            logMode = storeWrapper.serverOptions.LuaOptions.LogMode;
            allowedFunctions = storeWrapper.serverOptions.LuaOptions.AllowedFunctions;
            scriptCacheSize = storeWrapper.serverOptions.LuaOptions.ScriptCacheSize;
        }

        public void Dispose()
        {
            Clear();
            scratchBufferNetworkSender.Dispose();
            processor.Dispose();
        }

        /// <summary>
        /// Trims the script processor's scratch buffers.
        /// </summary>
        /// <remarks>
        /// The processor is a <see cref="RespServerSession"/> that never reads from a network, so it has no
        /// batch boundary of its own to drive the trim from, and Lua resets its buffer many times
        /// within a single script -- once per string while decoding JSON, for instance -- so those resets
        /// cannot drive it either. <see cref="OnReturned"/> calls this once every
        /// <see cref="RespServerSession.SessionTrimInterval"/> returns, outside any script execution. Both the
        /// buffer <c>redis.call</c> requests are built in and the one their replies are written into are covered.
        /// </remarks>
        internal void TrimScratchBuffer()
        {
            processor.scratchBufferBuilder.ResetAndTrimNow();
            scratchBufferNetworkSender.Trim();
        }

        internal RespServerSession Processor => processor;

        internal int ScratchBufferCapacityForTests => processor.scratchBufferBuilder.ScratchBufferCapacity;

        /// <summary>
        /// Called when the executor goes back to its pool, at which point no script is running.
        /// </summary>
        internal void OnReturned()
        {
            if (++returnsSinceTrim < RespServerSession.SessionTrimInterval)
                return;

            returnsSinceTrim = 0;
            TrimScratchBuffer();
        }

        /// <summary>
        /// Bind the executor to the user whose commands it is about to run.
        /// </summary>
        public void SetUserHandle(UserHandle userHandle)
        {
            processor.SetUserHandle(userHandle);
        }

        /// <summary>
        /// Indicate that at a script is about to run.
        /// 
        /// Enables timeouts, if they are configured.
        /// 
        /// Returns <c>false</c> if the executor is being disposed, in which case the caller
        /// must NOT execute the script (the Lua state may be freed concurrently).
        /// 
        /// Should always be paired with a call to <see cref="StopRunningScript"/> when it returns <c>true</c>.
        /// </summary>
        public bool StartRunningScript(LuaRunner script)
        {
            if (!scriptRunMonitor.TryEnter())
                return false;

            if (timeoutRegistration != null)
            {
                timeoutRegistration.SetCookie(++timeoutRunningCookie);
                timeoutRunningScript = script;
            }

            return true;
        }

        /// <summary>
        /// Indicate that a script has stopped running.
        /// 
        /// Should always be paired with a preceding call to <see cref="StartRunningScript"/> that returned <c>true</c>.
        /// </summary>
        public void StopRunningScript()
        {
            if (timeoutRegistration != null)
            {
                timeoutRegistration.SetCookie(0);
                timeoutRunningScript = null;
            }

            _ = scriptRunMonitor.Exit();
        }

        /// <summary>
        /// Request that the currently running script timeout.
        /// </summary>
        public void RequestTimeout(uint cookie)
        {
            if (cookie == timeoutRunningCookie)
            {
                // No race, request the timeout
                timeoutRunningScript.RequestTimeout();
            }
        }

        /// <summary>
        /// Try get script runner for given digest
        /// </summary>
        public bool TryGetFromDigest(ScriptHashKey digest, out LuaRunner scriptRunner, out LuaScriptHandle scriptHandle)
        {
            ref var entry = ref CollectionsMarshal.GetValueRefOrNullRef(scriptCache, digest);
            if (Unsafe.IsNullRef(ref entry))
            {
                scriptRunner = null;
                scriptHandle = null;
                return false;
            }

            scriptRunner = entry.Runner;
            scriptHandle = entry.Handle;

            // If the global cache has been invalidated, remove from the executor's cache
            if (scriptHandle.IsDisposed)
            {
                _ = scriptCache.Remove(digest);
                scriptRunner.Dispose();

                scriptRunner = null;
                scriptHandle = null;
                return false;
            }

            // Mark as most recently used, so eviction prefers colder entries.
            entry.LastUsed = ++scriptCacheClock;

            return true;
        }

        /// <summary>
        /// Compile Lua source text and load it into the executor's cache.
        /// 
        /// If necessary, <paramref name="digestOnHeap"/> will be set so the allocation can be reused.
        /// </summary>
        internal bool TryGetOrCreateRunnerFromSource(
            RespServerSession session,
            ReadOnlySpan<byte> source,
            ScriptHashKey digest,
            out LuaScriptHandle luaScriptHandle,
            out LuaRunner runner,
            out ScriptHashKey? digestOnHeap
        )
        {
            if (TryGetFromDigest(digest, out runner, out var existingLuaScriptHandle))
            {
                luaScriptHandle = existingLuaScriptHandle;
                digestOnHeap = null;
                return true;
            }

            luaScriptHandle = null;
            return TryCompileSourceAndCreateRunner(session, source, digest, ref luaScriptHandle, out runner, out digestOnHeap);
        }

        /// <summary>
        /// Load a script previously stored in the global cache.
        /// </summary>
        internal bool TryGetOrCreateRunnerFromCachedScript(RespServerSession session, ScriptHashKey digest, LuaScriptHandle cachedScriptHandle, out LuaRunner runner)
        {
            if (TryGetFromDigest(digest, out runner, out _))
                return true;

            return TryGetOrCreateRunnerFromGeneratedBytecode(session, cachedScriptHandle.Chunk, digest, ref cachedScriptHandle, out runner, out _);
        }

        private bool TryCompileSourceAndCreateRunner(RespServerSession session, ReadOnlySpan<byte> source, ScriptHashKey digest, ref LuaScriptHandle luaScriptHandle, out LuaRunner runner, out ScriptHashKey? digestOnHeap)
        {
            LuaScriptChunk generatedBytecode;
            string error;
            try
            {
                if (LuaRunner.TryCompileSource(source, out generatedBytecode, out error))
                    return TryGetOrCreateRunnerFromGeneratedBytecode(session, generatedBytecode, digest, ref luaScriptHandle, out runner, out digestOnHeap);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "During Lua script compilation, an unexpected exception");
                runner = null;
                digestOnHeap = null;
                luaScriptHandle = null;
                return false;
            }

            session.WriteLuaCompilationError(error);
            runner = null;
            digestOnHeap = null;
            return false;
        }

        /// <summary>
        /// Load internally compiled script bytecode into the cache.
        /// </summary>
        private bool TryGetOrCreateRunnerFromGeneratedBytecode(RespServerSession session, LuaScriptChunk generatedBytecode, ScriptHashKey digest, ref LuaScriptHandle luaScriptHandle, out LuaRunner runner, out ScriptHashKey? digestOnHeap)
        {
            if (TryGetFromDigest(digest, out runner, out var existingLuaScriptHandle))
            {
                luaScriptHandle = existingLuaScriptHandle;
                digestOnHeap = null;
                return true;
            }

            try
            {
                runner = new LuaRunner(memoryManagementMode, memoryLimitBytes, logMode, allowedFunctions, generatedBytecode, storeWrapper.serverOptions.LuaTransactionMode, processor, scratchBufferNetworkSender, storeWrapper.redisProtocolVersion, logger);

                // If compilation fails, an error is written out
                if (runner.CompileForSession(session))
                {
                    // Need to make sure the key is on the heap, so move it over
                    //
                    // There's an implicit assumption that all callers are using unmanaged memory.
                    // If that becomes untrue, there's an optimization opportunity to re-use the 
                    // managed memory here.
                    var into = GC.AllocateUninitializedArray<byte>(SHA1Len, pinned: true);
                    digest.CopyTo(into);

                    ScriptHashKey storeKeyDigest = new(into);
                    digestOnHeap = storeKeyDigest;

                    luaScriptHandle ??= new(generatedBytecode.Data);

                    EvictIfAtCapacity();

                    scriptCache.Add(storeKeyDigest, new CacheEntry { Runner = runner, Handle = luaScriptHandle, LastUsed = ++scriptCacheClock });

                    // On first script load, register for timeout notifications
                    //
                    // We don't do this for every executor because not every executor will run scripts
                    if (timeoutManager != null && timeoutRegistration == null)
                    {
                        timeoutRegistration = timeoutManager.RegisterForTimeout(this);
                    }
                }
                else
                {
                    runner.Dispose();

                    digestOnHeap = null;
                    return false;
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "During Lua script loading, an unexpected exception");

                digestOnHeap = null;
                luaScriptHandle = null;
                return false;
            }

            return true;
        }

        /// <summary>
        /// Number of compiled scripts currently held. For tests.
        /// </summary>
        internal int CachedScriptCount => scriptCache.Count;

        /// <summary>
        /// Whether the script with the given digest is currently held. For tests.
        /// </summary>
        internal bool ContainsDigest(ReadOnlySpan<byte> digest)
        => scriptCache.ContainsKey(new ScriptHashKey(digest));

        /// <summary>
        /// Make room for one more entry when the cache is at its configured capacity, by evicting
        /// the least recently used script.
        /// </summary>
        /// <remarks>
        /// Safe against a running script: every command that inserts into this cache (EVAL, EVALSHA
        /// and SCRIPT LOAD) is flagged NoScript, so no insert -- and therefore no eviction -- can be
        /// reached from inside an executing script. Disposing a runner closes its Lua state, which
        /// would corrupt the single-threaded allocator if it were still in use.
        ///
        /// Safe for correctness: this is an executor-local cache in front of the global script cache,
        /// so an EVALSHA that misses here recompiles from the global cache rather than failing. The
        /// only cost of evicting too eagerly is that recompilation.
        /// </remarks>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private void EvictIfAtCapacity()
        {
            if (scriptCacheSize <= 0 || scriptCache.Count < scriptCacheSize)
            {
                return;
            }

            var oldestStamp = long.MaxValue;
            ScriptHashKey oldestKey = default;
            var found = false;

            foreach (var candidate in scriptCache)
            {
                if (candidate.Value.LastUsed < oldestStamp)
                {
                    oldestStamp = candidate.Value.LastUsed;
                    oldestKey = candidate.Key;
                    found = true;
                }
            }

            if (found && scriptCache.Remove(oldestKey, out var evicted))
            {
                // Intentionally NOT disposing the script handle
                //
                // Removing from an executor's cache does not invalidate the global cache
                evicted.Runner.Dispose();
            }
        }

        /// <summary>
        /// Attempt to remove the script with the given hash from the cache.
        /// </summary>
        internal void Remove(ScriptHashKey key)
        {
            if (scriptCache.Remove(key, out var entry))
            {
                // Intentionally NOT disposing the script handle
                //
                // Removing from an executor's cache does not invalidate the global cache
                entry.Runner.Dispose();
            }
        }

        /// <summary>
        /// Clear the executor's script cache
        /// </summary>
        public void Clear()
        {
            // Prevent new script executions and block until any in-flight script has exited before
            // disposing runners (which frees the Lua state via lua_close). This avoids corrupting the
            // single-threaded Lua allocator by freeing it while a script is still executing.
            scriptRunMonitor.Dispose();

            timeoutRegistration?.Dispose();
            timeoutRegistration = null;

            foreach (var entry in scriptCache.Values)
            {
                // Intentionally NOT disposing the script handles
                //
                // Removing from an executor's cache does not invalidate the global cache
                entry.Runner.Dispose();
            }

            scriptCache.Clear();
        }

        /// <summary>
        /// Swap database sessions in processor session
        /// </summary>
        /// <param name="dbId1">First database ID</param>
        /// <param name="dbId2">Second database ID</param>
        /// <returns>True if successful</returns>
        internal bool TrySwapDatabaseSessions(int dbId1, int dbId2) =>
            processor.TrySwapDatabaseSessions(dbId1, dbId2);

        static ReadOnlySpan<byte> HEX_CHARS => "0123456789abcdef"u8;

        /// <summary>
        /// Writes the lowercase hex SHA-1 digest of <paramref name="source"/> into <paramref name="into"/>.
        /// </summary>
        public static void GetScriptDigest(ReadOnlySpan<byte> source, Span<byte> into)
        {
            Span<byte> sha1Bytes = stackalloc byte[SHA1Len / 2];
            GetScriptDigest(source, sha1Bytes, into);
        }

        public static void GetScriptDigest(ReadOnlySpan<byte> source, Span<byte> sha1Bytes, Span<byte> into)
        {
            Debug.Assert(sha1Bytes.Length >= SHA1Len / 2, "sha1Bytes must be large enough for the hash");
            Debug.Assert(into.Length >= SHA1Len, "into must be large enough for the hash hex bytes");

            _ = SHA1.HashData(source, sha1Bytes);

            for (var i = 0; i < SHA1Len / 2; i++)
            {
                into[i * 2] = HEX_CHARS[sha1Bytes[i] >> 4];
                into[(i * 2) + 1] = HEX_CHARS[sha1Bytes[i] & 0x0F];
            }
        }
    }
}