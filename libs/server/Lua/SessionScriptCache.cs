// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Cache of Lua scripts, per session
    /// </summary>
    internal sealed unsafe class SessionScriptCache
    {
        // Important to keep the hash length to this value 
        // for compatibility
        const int SHA1Len = 40;
        readonly StoreWrapper storeWrapper;
        readonly ILogger logger;
        readonly Dictionary<byte[], LuaRunner> scriptCache = new(new ByteArrayComparer());
        readonly byte[] hash = new byte[SHA1Len / 2];

        public SessionScriptCache(StoreWrapper storeWrapper, ILogger logger = null)
        {
            this.storeWrapper = storeWrapper;
            this.logger = logger;
        }

        /// <summary>
        /// Try get script runner for given digest
        /// </summary>
        public bool TryGetFromDigest(ReadOnlySpan<byte> digest, out LuaRunner scriptRunner)
            => scriptCache.TryGetValue(digest.ToArray(), out scriptRunner);

        /// <summary>
        /// Load script into the cache
        /// </summary>
        public bool TryLoad(ReadOnlySpan<byte> source, out byte[] digest, out LuaRunner runner)
        {
            digest = GetScriptDigest(source);
            return TryLoad(source, digest, out runner);
        }

        internal bool TryLoad(ReadOnlySpan<byte> source, byte[] digest, out LuaRunner runner)
        {
            runner = null;

            if (scriptCache.TryGetValue(digest, out runner))
                return true;

            try
            {
                runner = new LuaRunner(source, storeWrapper, logger);
                runner.Compile();
                scriptCache.TryAdd(digest, runner);
            }
            catch
            {
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

        public byte[] GetScriptDigest(ReadOnlySpan<byte> source)
        {
            var digest = new byte[SHA1Len];
            SHA1.HashData(source, new Span<byte>(hash));
            for (int i = 0; i < 20; i++)
            {
                digest[i * 2] = HEX_CHARS[hash[i] >> 4];
                digest[i * 2 + 1] = HEX_CHARS[hash[i] & 0x0F];
            }
            return digest;
        }
    }
}