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
        LuaFunction function;

        public string garnet_call(string arg1) => arg1;

        [GlobalSetup]
        public void GlobalSetup()
        {
            const string source = "return KEYS[1]";
            state = new Lua();

            state.RegisterFunction("garnet_call", this, this.GetType().GetMethod("garnet_call"));

            state["KEYS"] = new string[] { "key1", "key2" };
            state["ARGV"] = new string[] { "arg1", "arg2" };

            state.DoString(@"
                import = function () end
                redis = {}
                function redis.call(a)
                    return garnet_call(a)
                end
                function load_sandboxed(source)
                    if (not source) then return nil end
                    return load(source)
                end
            ");

            using var loader = (LuaFunction)state["load_sandboxed"];
            function = loader.Call(source)[0] as LuaFunction;
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            function.Dispose();
            state.Dispose();
        }

        [Benchmark]
        public void BasicLua()
        {
            var res = function.Call();
        }
    }
}