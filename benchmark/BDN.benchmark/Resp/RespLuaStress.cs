// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using NLua;

namespace BDN.benchmark.Resp
{
    [MemoryDiagnoser]
    public unsafe class RespLuaStress
    {
        Lua state;
        LuaFunction function2;

        public long callback(long arg1, long arg2) => arg1 + arg2;

        [GlobalSetup]
        public void GlobalSetup()
        {
            state = new Lua();

            state.RegisterFunction("callback", this, this.GetType().GetMethod("callback"));

            state.DoString(@"
                import = function () end
                redis = {}
                function redis.call(a,b)
                    return callback(a,b)
                end
                function load_sandboxed(source)
                    if (not source) then return nil end
                    return load(source)
                end
                function execute_fc(sb_code)
                    return pcall(sb_code);
                end
            ");

            using var func = (LuaFunction)state["load_sandboxed"];
            var res = func?.Call("return redis.call");
            using var function = res[0] as LuaFunction;
            function2 = function.Call()[0] as LuaFunction;
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            function2.Dispose();
            state.Dispose();
        }

        [Benchmark]
        public void BasicLua()
        {
            var res = function2.Call(1, 4);
        }
    }
}