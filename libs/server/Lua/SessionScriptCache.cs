// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Cryptography;
using Garnet.server.ACL;
using Garnet.server.Auth;
using Garnet.server.Lua;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Cache of Lua scripts, per session
    /// </summary>
    internal sealed class SessionScriptCache : IDisposable
    {
        // Important to keep the hash length to this value 
        // for compatibility
        internal const int SHA1Len = 40;
        readonly RespServerSession processor;
        readonly StoreWrapper storeWrapper;
        readonly ScratchBufferNetworkSender scratchBufferNetworkSender;
        readonly ILogger logger;
        readonly Dictionary<ScriptHashKey, (LuaRunner Runner, LuaScriptHandle Handle)> scriptCache = [];
        readonly byte[] hash = new byte[SHA1Len / 2];

        readonly LuaMemoryManagementMode memoryManagementMode;
        readonly int? memoryLimitBytes;
        readonly LuaTimeoutManager timeoutManager;
        readonly LuaLoggingMode logMode;
        readonly HashSet<string> allowedFunctions;

        LuaRunner timeoutRunningScript;
        LuaTimeoutManager.Registration timeoutRegistration;

        // Provides a unique value for script invocations
        //
        // It doesn't need to be globally unique, it just needs to be able
        // distiguish two different runs of some script on the same session.
        //
        // It's OK if this wraps around, because ~4 billion invocations are unlikely
        // to race.
        uint timeoutRunningCookie;

        public SessionScriptCache(StoreWrapper storeWrapper, IGarnetAuthenticator authenticator, LuaTimeoutManager timeoutManager, ILogger logger = null)
        {
            this.storeWrapper = storeWrapper;
            this.timeoutManager = timeoutManager;
            this.logger = logger;

            scratchBufferNetworkSender = new ScratchBufferNetworkSender();
            processor = new RespServerSession(0, scratchBufferNetworkSender, storeWrapper, null, authenticator, false);

            // There's some parsing involved in these, so save them off per-session
            memoryManagementMode = storeWrapper.serverOptions.LuaOptions.MemoryManagementMode;
            memoryLimitBytes = storeWrapper.serverOptions.LuaOptions.GetMemoryLimitBytes();
            logMode = storeWrapper.serverOptions.LuaOptions.LogMode;
            allowedFunctions = storeWrapper.serverOptions.LuaOptions.AllowedFunctions;
        }

        public void Dispose()
        {
            Clear();
            scratchBufferNetworkSender.Dispose();
            processor.Dispose();
        }

        public void SetUserHandle(UserHandle userHandle)
        {
            processor.SetUserHandle(userHandle);
        }

        /// <summary>
        /// Indicate that at a script is about to run.
        /// 
        /// Enables timeouts, if they are configured.
        /// 
        /// Should always be paired with a call to <see cref="StopRunningScript"/>.
        /// </summary>
        public void StartRunningScript(LuaRunner script)
        {
            if (timeoutRegistration != null)
            {
                timeoutRegistration.SetCookie(++timeoutRunningCookie);
                timeoutRunningScript = script;
            }
        }

        /// <summary>
        /// Indicate that a script has stopped running.
        /// 
        /// Should always be paired with a call to <see cref="StartRunningScript"/>.
        /// </summary>
        public void StopRunningScript()
        {
            if (timeoutRegistration != null)
            {
                timeoutRegistration.SetCookie(0);
                timeoutRunningScript = null;
            }
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
            if (!scriptCache.TryGetValue(digest, out var loadedTuple))
            {
                scriptRunner = null;
                scriptHandle = null;
                return false;
            }

            (scriptRunner, scriptHandle) = loadedTuple;

            // If the global cache has been invalidated, remove from the session cache
            if (scriptHandle.IsDisposed)
            {
                _ = scriptCache.Remove(digest);
                scriptRunner.Dispose();

                scriptRunner = null;
                scriptHandle = null;
                return false;
            }

            return true;
        }

        /// <summary>
        /// Load script into the cache.
        /// 
        /// If necessary, <paramref name="digestOnHeap"/> will be set so the allocation can be reused.
        /// </summary>
        internal bool TryLoad(
            RespServerSession session,
            ReadOnlySpan<byte> source,
            ScriptHashKey digest,
            ref LuaScriptHandle luaScriptHandle,
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

            try
            {
                var compiledSource = LuaRunner.CompileSource(source);

                runner = new LuaRunner(memoryManagementMode, memoryLimitBytes, logMode, allowedFunctions, compiledSource, storeWrapper.serverOptions.LuaTransactionMode, processor, scratchBufferNetworkSender, storeWrapper.redisProtocolVersion, logger);

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

                    luaScriptHandle ??= new(compiledSource);
                    scriptCache.Add(storeKeyDigest, (runner, luaScriptHandle));

                    // On first script load, register for timeout notifications
                    //
                    // We don't do this for every session because not every session will run scripts
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
        /// Attempt to remove the script with the given hash from the cache.
        /// </summary>
        internal void Remove(ScriptHashKey key)
        {
            if (scriptCache.Remove(key, out var loadedTuple))
            {
                // Intentionally NOT disposing the script handle
                //
                // Removing from a session cache does not invalidate the global cache
                loadedTuple.Runner.Dispose();
            }
        }

        /// <summary>
        /// Clear the session script cache
        /// </summary>
        public void Clear()
        {
            timeoutRegistration?.Dispose();
            timeoutRegistration = null;

            foreach (var (runner, _) in scriptCache.Values)
            {
                // Intentionally NOT disposing the script handles
                //
                // Removing from a session cache does not invalidate the global cache
                runner.Dispose();
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

        public void GetScriptDigest(ReadOnlySpan<byte> source, Span<byte> into)
        => GetScriptDigest(source, hash, into);

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