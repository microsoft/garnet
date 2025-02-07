// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Security.Cryptography;
using System.Text;
using BDN.benchmark.Operations;
using BenchmarkDotNet.Attributes;
using Embedded.server;
using Garnet.server;

namespace BDN.benchmark.Lua
{
    /// <summary>
    /// Benchmark the overhead of enabling timeouts on Lua scripting objects.
    /// </summary>
    public class LuaTimeouts
    {
        private const string Script = @"
local counter = 7
for i = 1, 5 do
    counter = counter + 1
end

return counter";

        /// <summary>
        /// Lua parameters
        /// </summary>
        [ParamsSource(nameof(LuaParamsProvider))]
        public LuaParams Params { get; set; }

        internal EmbeddedRespServer server;
        internal RespServerSession session;

        /// <summary>
        /// Lua parameters provider
        /// </summary>
        public IEnumerable<LuaParams> LuaParamsProvider()
        => [
            // We don't expect this to vary by allocator
            new(LuaMemoryManagementMode.Native, false, Timeout.InfiniteTimeSpan),
            new(LuaMemoryManagementMode.Native, false, TimeSpan.FromSeconds(1)),
            new(LuaMemoryManagementMode.Native, false, TimeSpan.FromMinutes(1)),
        ];

        private Request evalShaRequest;

        [GlobalSetup]
        public void GlobalSetup()
        {
            var opts = new GarnetServerOptions
            {
                QuietMode = true,
                EnableLua = true,
                LuaOptions = Params.CreateOptions(),
            };

            server = new EmbeddedRespServer(opts);

            session = server.GetRespSession();

            var scriptLoadStr = $"*3\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n${Script.Length}\r\n{Script}\r\n";
            Request scriptLoad = default;

            ScriptOperations.SetupOperation(ref scriptLoad, scriptLoadStr, batchSize: 1);
            ScriptOperations.Send(session, scriptLoad);

            var scriptHash = string.Join("", SHA1.HashData(Encoding.UTF8.GetBytes(Script)).Select(static x => x.ToString("x2")));

            var evalShaStr = $"*3\r\n$7\r\nEVALSHA\r\n$40\r\n{scriptHash}\r\n$1\r\n0\r\n";

            // Use a batchSize that gets us up around ~100ms
            ScriptOperations.SetupOperation(ref evalShaRequest, evalShaStr, batchSize: 500 * 1_000);
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            session.Dispose();
            server.Dispose();
        }

        [Benchmark]
        public void RunScript()
        {
            ScriptOperations.Send(session, evalShaRequest);
        }
    }
}