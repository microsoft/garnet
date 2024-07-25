// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;


namespace Garnet.server.Scripting
{
    /// <summary>
    /// internal class thread safe to manage the contents of the Scripts dictionary
    /// Script FLUSH: complete flush the scripts cache, removing all the scripts.
    /// Script EXISTS
    /// Script LOAD script
    /// </summary>
    internal unsafe class ScriptMemoryManager
    {
        // Important to keep the hash length to this value 
        // for compatibility
        private const int SHA1Len = 40;

        private Dictionary<byte[], ScriptRunner> scriptCache;

        internal ScriptMemoryManager()
        {
            // key = sha1
            // value = source and runner instance
            scriptCache = new Dictionary<byte[], ScriptRunner>(new ByteArrayComparer());
        }

        internal class ScriptRunner
        {
            internal byte[] source { get; }
            internal Runner runnerMachine { get; }

            public ScriptRunner(byte[] source, Runner runnerMachine)
            {
                this.source = source;
                this.runnerMachine = runnerMachine;
            }
        }

        // adds a new script source to the cache, if it is not already there 
        internal byte[] ScriptLoad(byte[] source, RespServerSession session, out string error)
        {
            byte[] sha1 = default;

            var scriptApi = new ScriptApi(session, true);
            var runner = new Runner(scriptApi);
            runner.LoadScript(source, out error);
            if (error == string.Empty)
            {
                sha1 = Encoding.ASCII.GetBytes(GetScriptHash(source));
                scriptCache.TryAdd(sha1, new ScriptRunner(source, runner));
            }
            return sha1;
        }

        internal bool ScriptExists(byte[] sha1) => scriptCache.ContainsKey(sha1);

        internal int ScriptCount() => scriptCache.Count;

        internal void FlushDictionary()
        {
            scriptCache.Clear();
        }

        internal ScriptRunner GetRunner(byte[] sha1) => scriptCache.GetValueOrDefault(sha1);

        public static string GetScriptHash(byte[] source)
        {
            byte[] hashBytes = SHA256.HashData(source);
            return Convert.ToHexString(hashBytes).ToLowerInvariant().Substring(0, SHA1Len);
        }
    }
}
