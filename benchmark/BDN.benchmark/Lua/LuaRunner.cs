// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Lua
{
    [MemoryDiagnoser]
    public unsafe class LuaRunner
    {
        Garnet.server.LuaRunner r1, r2, r3, r4;
        readonly string[] keys = ["key1"];

        [GlobalSetup]
        public void GlobalSetup()
        {
            r1 = new Garnet.server.LuaRunner("return");
            r1.Compile();
            r2 = new Garnet.server.LuaRunner("return 1 + 1");
            r2.Compile();
            r3 = new Garnet.server.LuaRunner("return KEYS[1]");
            r3.Compile();
            r4 = new Garnet.server.LuaRunner("return redis.call(KEYS[1])");
            r4.Compile();
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            r1.Dispose();
            r2.Dispose();
            r3.Dispose();
            r4.Dispose();
        }

        [Benchmark]
        public void BasicLua1()
            => r1.Run();

        [Benchmark]
        public void BasicLua2()
            => r2.Run();

        [Benchmark]
        public void BasicLua3()
            => r3.Run(keys, null);

        [Benchmark]
        public void BasicLua4()
            => r4.Run(keys, null);
    }
}