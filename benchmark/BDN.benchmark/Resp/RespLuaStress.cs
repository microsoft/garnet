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
        LuaFunction f1, f2, f3, f4;

        public string garnet_call(string arg1) => arg1;

        [GlobalSetup]
        public void GlobalSetup()
        {
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

            f1 = CreateFunction("return");
            f2 = CreateFunction("return 1 + 1");
            f3 = CreateFunction("return KEYS[1]");
            f4 = CreateFunction("return redis.call(KEYS[1])");
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            f1.Dispose();
            f2.Dispose();
            f3.Dispose();
            f4.Dispose();
            state.Dispose();
        }

        LuaFunction CreateFunction(string source)
        {
            using var loader = (LuaFunction)state["load_sandboxed"];
            return loader.Call(source)[0] as LuaFunction;
        }

        [Benchmark]
        public void BasicLuaStress1()
            => f1.Call();

        [Benchmark]
        public void BasicLuaStress2()
            => f2.Call();

        [Benchmark]
        public void BasicLuaStress3()
            => f3.Call();

        [Benchmark]
        public void BasicLuaStress4()  
            => f4.Call();
    }
}