// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Security.Cryptography;

namespace Garnet.server.Scripting
{
    /// <summary>
    /// Cache of Lua scripts, per session
    /// </summary>
    internal unsafe class SessionScriptCache(RespServerSession respServerSession)
    {
        // Important to keep the hash length to this value 
        // for compatibility
        const int SHA1Len = 40;
        readonly RespServerSession respServerSession = respServerSession;
        readonly Dictionary<byte[], Runner> scriptCache = new(new ByteArrayComparer());
        byte[] hash = new byte[SHA1Len / 2];
        byte[] digest = new byte[SHA1Len];

        /// <summary>
        /// Try get script runner for given digest
        /// </summary>
        public bool TryGet(byte[] digest, out Runner scriptRunner)
            => scriptCache.TryGetValue(digest, out scriptRunner);

        /// <summary>
        /// Load script into the cache
        /// </summary>
        public bool TryLoad(byte[] source, out byte[] digest, out Runner runner)
        {
            digest = GetScriptDigest(source);
            return TryLoad(source, digest, out runner);
        }

        internal bool TryLoad(byte[] source, byte[] digest, out Runner runner)
        {
            runner = null;

            if (scriptCache.ContainsKey(digest))
                return false;

            runner = new Runner(source, respServerSession, true);
            runner.LoadScript(source, out var error);
            if (error == string.Empty)
            {
                return scriptCache.TryAdd(digest, runner);
            }
            return false;
        }

        /// <summary>
        /// Whether the cache contains a script with the given SHA1 hash
        /// </summary>
        public bool Contains(byte[] sha1) => scriptCache.ContainsKey(sha1);

        /// <summary>
        /// Count of scripts in the cache
        /// </summary>
        public int Count => scriptCache.Count;

        /// <summary>
        /// Clear the session script cache
        /// </summary>
        public void Clear() => scriptCache.Clear();

        public ScriptRunner GetRunner(byte[] sha1) => scriptCache.GetValueOrDefault(sha1);

        static ReadOnlySpan<byte> HEX_CHARS => "0123456789abcdef"u8;

        public byte[] GetScriptDigest(byte[] source)
        {
            SHA1.HashData(new ReadOnlySpan<byte>(source), new Span<byte>(hash));
            for (int i = 0; i < 20; i++)
            {
                digest[i * 2] = HEX_CHARS[hash[i] >> 4];
                digest[i * 2 + 1] = HEX_CHARS[hash[i] & 0x0F];
            }
            return digest;
        }
    }
}
