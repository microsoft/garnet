// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Cryptography;
using Garnet.server.ACL;
using Garnet.server.Auth;
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
        readonly ScratchBufferNetworkSender scratchBufferNetworkSender;
        readonly StoreWrapper storeWrapper;
        readonly ILogger logger;
        readonly Dictionary<ScriptHashKey, LuaRunner> scriptCache = [];
        readonly byte[] hash = new byte[SHA1Len / 2];

        readonly LuaMemoryManagementMode memoryManagementMode;
        readonly int? memoryLimitBytes;

        public SessionScriptCache(StoreWrapper storeWrapper, IGarnetAuthenticator authenticator, ILogger logger = null)
        {
            this.storeWrapper = storeWrapper;
            this.logger = logger;

            scratchBufferNetworkSender = new ScratchBufferNetworkSender();
            processor = new RespServerSession(0, scratchBufferNetworkSender, storeWrapper, null, authenticator, false);

            // There's some parsing involved in these, so save them off per-session
            memoryManagementMode = storeWrapper.serverOptions.LuaOptions.MemoryManagementMode;
            memoryLimitBytes = storeWrapper.serverOptions.LuaOptions.GetMemoryLimitBytes();
        }

        public void Dispose()
        {
            Clear();
            scratchBufferNetworkSender.Dispose();
            processor.Dispose();
        }

        public void SetUser(User user)
        {
            processor.SetUser(user);
        }

        /// <summary>
        /// Try get script runner for given digest
        /// </summary>
        public bool TryGetFromDigest(ScriptHashKey digest, out LuaRunner scriptRunner)
        => scriptCache.TryGetValue(digest, out scriptRunner);

        /// <summary>
        /// Load script into the cache.
        /// 
        /// If necessary, <paramref name="digestOnHeap"/> will be set so the allocation can be reused.
        /// </summary>
        internal bool TryLoad(RespServerSession session, ReadOnlySpan<byte> source, ScriptHashKey digest, out LuaRunner runner, out ScriptHashKey? digestOnHeap, out string error)
        {
            error = null;

            if (scriptCache.TryGetValue(digest, out runner))
            {
                digestOnHeap = null;
                return true;
            }

            try
            {
                var sourceOnHeap = source.ToArray();

                runner = new LuaRunner(memoryManagementMode, memoryLimitBytes, sourceOnHeap, storeWrapper.serverOptions.LuaTransactionMode, processor, scratchBufferNetworkSender, logger);

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

                    _ = scriptCache.TryAdd(storeKeyDigest, runner);
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
                error = ex.Message;
                digestOnHeap = null;
                return false;
            }

            return true;
        }

        /// <summary>
        /// Clear the session script cache
        /// </summary>
        public void Clear()
        {
            foreach (var runner in scriptCache.Values)
            {
                runner.Dispose();
            }

            scriptCache.Clear();
        }

        static ReadOnlySpan<byte> HEX_CHARS => "0123456789abcdef"u8;

        public void GetScriptDigest(ReadOnlySpan<byte> source, Span<byte> into)
        {
            Debug.Assert(into.Length >= SHA1Len, "into must be large enough for the hash");

            _ = SHA1.HashData(source, new Span<byte>(hash));

            for (var i = 0; i < hash.Length; i++)
            {
                into[i * 2] = HEX_CHARS[hash[i] >> 4];
                into[i * 2 + 1] = HEX_CHARS[hash[i] & 0x0F];
            }
        }
    }
}