// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Security.Cryptography;
using System.Text;
using BDN.benchmark.Lua;
using BenchmarkDotNet.Attributes;
using Embedded.server;
using Garnet.server;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Benchmark for EVALSHA of scripts whose cost is dominated by <c>redis.call</c> of a single command
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class ScriptFastPathOperations
    {
        const int batchSize = ScriptOperations.batchSize;

        const string GetScript = "return redis.call('GET', KEYS[1])";
        const string SetScript = "return redis.call('SET', KEYS[1], ARGV[1])";
        const string HGetScript = "return redis.call('HGET', KEYS[1], ARGV[1])";
        const string HSetScript = "return redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])";
        const string ExpireScript = "return redis.call('EXPIRE', KEYS[1], ARGV[1])";
        const string HGet10Script = "local r; for i = 1, 10 do r = redis.call('HGET', KEYS[1], ARGV[1]) end return r";
        const string HSet10Script = "local r; for i = 1, 10 do r = redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]) end return r";

        Request get, set, hget, hset, expire, hget10, hset10;

        /// <summary>
        /// Lua parameters
        /// </summary>
        [ParamsSource(nameof(LuaParamsProvider))]
        public LuaParams Params { get; set; }

        /// <summary>
        /// Lua parameters provider
        /// </summary>
        public static IEnumerable<LuaParams> LuaParamsProvider()
        => [
            new(LuaMemoryManagementMode.Native, false),
            new(LuaMemoryManagementMode.Managed, true),
        ];

        EmbeddedRespServer server;
        RespServerSession session;

        /// <summary>
        /// Setup
        /// </summary>
        [GlobalSetup]
        public void GlobalSetup()
        {
            server = new EmbeddedRespServer(new GarnetServerOptions
            {
                QuietMode = true,
                EnableLua = true,
                LuaOptions = Params.CreateOptions(),
            });
            session = server.GetRespSession();

            Once(Resp("SET", "k", "value"));
            Once(Resp("HSET", "h", "f", "value"));
            Once(Resp("EXPIRE", "h", "1000000"));

            SetupScript(ref get, GetScript, "k");
            SetupScript(ref set, SetScript, "k", "value");
            SetupScript(ref hget, HGetScript, "h", "f");
            SetupScript(ref hset, HSetScript, "h", "f", "value");
            SetupScript(ref expire, ExpireScript, "h", "1000000");
            SetupScript(ref hget10, HGet10Script, "h", "f");
            SetupScript(ref hset10, HSet10Script, "h", "f", "value");
        }

        /// <summary>
        /// Cleanup
        /// </summary>
        [GlobalCleanup]
        public void GlobalCleanup()
        {
            session.Dispose();
            server.Dispose();
        }

        [Benchmark]
        public void Get() => Send(get);

        [Benchmark]
        public void Set() => Send(set);

        [Benchmark]
        public void HGet() => Send(hget);

        [Benchmark]
        public void HSet() => Send(hset);

        [Benchmark]
        public void Expire() => Send(expire);

        [Benchmark]
        public void HGet10() => Send(hget10);

        [Benchmark]
        public void HSet10() => Send(hset10);

        void Send(Request request) => ScriptOperations.Send(session, request);

        static string Resp(params string[] parts)
        => $"*{parts.Length}\r\n" + string.Concat(parts.Select(static p => $"${p.Length}\r\n{p}\r\n"));

        void Once(string command)
        {
            Request request = default;
            ScriptOperations.SetupOperation(ref request, command, 1);
            Send(request);
        }

        void SetupScript(ref Request request, string script, string key, params string[] args)
        {
            Once(Resp("SCRIPT", "LOAD", script));
            var sha = string.Join("", SHA1.HashData(Encoding.UTF8.GetBytes(script)).Select(static x => x.ToString("x2")));
            ScriptOperations.SetupOperation(ref request, Resp(["EVALSHA", sha, "1", key, .. args]), batchSize);
        }
    }
}