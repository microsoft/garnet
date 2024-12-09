// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Cryptography;
using Garnet.server.ACL;
using Garnet.server.Auth;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

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
        readonly Dictionary<SpanByteAndMemory, LuaRunner> scriptCache = new(SpanByteAndMemoryComparer.Instance);
        readonly byte[] hash = new byte[SHA1Len / 2];

        public SessionScriptCache(StoreWrapper storeWrapper, IGarnetAuthenticator authenticator, ILogger logger = null)
        {
            this.storeWrapper = storeWrapper;
            this.logger = logger;

            scratchBufferNetworkSender = new ScratchBufferNetworkSender();
            processor = new RespServerSession(0, scratchBufferNetworkSender, storeWrapper, null, authenticator, false);
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
        public bool TryGetFromDigest(SpanByteAndMemory digest, out LuaRunner scriptRunner)
        => scriptCache.TryGetValue(digest, out scriptRunner);

        /// <summary>
        /// Load script into the cache
        /// </summary>
        public bool TryLoad(byte[] source, out byte[] digest, out LuaRunner runner, out string error)
        {
            digest = new byte[SHA1Len];
            GetScriptDigest(source, digest);

            return TryLoad(source, new SpanByteAndMemory(new ScriptHashOwner(digest), digest.Length), out runner, out error);
        }

        internal bool TryLoad(byte[] source, SpanByteAndMemory digest, out LuaRunner runner, out string error)
        {
            error = null;

            if (scriptCache.TryGetValue(digest, out runner))
                return true;

            try
            {
                runner = new LuaRunner(source, storeWrapper.serverOptions.LuaTransactionMode, processor, scratchBufferNetworkSender, logger);
                runner.Compile();

                // need to make sure the key is on the heap, so move it over if needed
                var storeKeyDigest = digest;
                if (storeKeyDigest.IsSpanByte)
                {
                    var into = new byte[storeKeyDigest.Length];
                    storeKeyDigest.AsReadOnlySpan().CopyTo(into);

                    storeKeyDigest = new SpanByteAndMemory(new ScriptHashOwner(into), into.Length);
                }

                _ = scriptCache.TryAdd(storeKeyDigest, runner);
            }
            catch (Exception ex)
            {
                error = ex.Message;
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