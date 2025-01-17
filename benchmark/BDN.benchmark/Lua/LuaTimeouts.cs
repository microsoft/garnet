// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using BenchmarkDotNet.Attributes;
using Garnet.server;

namespace BDN.benchmark.Lua
{
    /// <summary>
    /// Benchmark the overhead of enabling timeouts on Lua scripting objects.
    /// </summary>
    public class LuaTimeouts
    {
        const string Script = @"
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

        /// <summary>
        /// Lua parameters provider
        /// </summary>
        public IEnumerable<LuaParams> LuaParamsProvider()
        => [
            // We don't expect this to vary by allocator
            new(LuaMemoryManagementMode.Native, false)
        ];

        private LuaRunner withTimeout;
        private LuaRunner noTimeout;

        [GlobalSetup]
        public void GlobalSetup()
        {
            var opts = Params.CreateOptions();

            noTimeout = new LuaRunner(opts.MemoryManagementMode, opts.GetMemoryLimitBytes(), Timeout.InfiniteTimeSpan, Encoding.UTF8.GetBytes(Script));
            withTimeout = new LuaRunner(opts.MemoryManagementMode, opts.GetMemoryLimitBytes(), TimeSpan.FromMilliseconds(5), Encoding.UTF8.GetBytes(Script));

            noTimeout.CompileForRunner();
            withTimeout.CompileForRunner();
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            noTimeout.Dispose();
            withTimeout.Dispose();
        }

        [Benchmark(Baseline = true)]
        public void NoTimeout()
        {
            _ = noTimeout.RunForRunner();
        }

        [Benchmark]
        public void WithTimeout()
        {
            _ = withTimeout.RunForRunner();
        }

    }
}
