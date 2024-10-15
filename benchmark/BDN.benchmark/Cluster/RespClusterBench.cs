// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Cluster
{
    [MemoryDiagnoser]
    public unsafe class RespClusterBench
    {
        protected bool enableCluster = true;
        ClusterContext cc;

        [GlobalSetup]
        public virtual void GlobalSetup()
        {
            cc = new ClusterContext();
            cc.SetupSingleInstance(enableCluster);
            cc.AddSlotRange([(0, 16383)]);
            cc.CreateGetSet();
            cc.CreateMGetMSet();
            cc.CreateCPBSET();

            // Warmup/Prepopulate stage
            cc.Consume(cc.singleGetSet[1].ptr, cc.singleGetSet[1].buffer.Length);
            // Warmup/Prepopulate stage
            cc.Consume(cc.singleMGetMSet[1].ptr, cc.singleMGetMSet[1].buffer.Length);
            // Warmup/Prepopulate stage
            cc.Consume(cc.singleCPBSET.ptr, cc.singleCPBSET.buffer.Length);
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            cc.Dispose();
        }

        [Benchmark]
        public void Get()
        {
            cc.Consume(cc.singleGetSet[0].ptr, cc.singleGetSet[0].buffer.Length);
        }

        [Benchmark]
        public void Set()
        {
            cc.Consume(cc.singleGetSet[1].ptr, cc.singleGetSet[1].buffer.Length);
        }

        [Benchmark]
        public void MGet()
        {
            cc.Consume(cc.singleMGetMSet[0].ptr, cc.singleMGetMSet[0].buffer.Length);
        }

        [Benchmark]
        public void MSet()
        {
            cc.Consume(cc.singleMGetMSet[1].ptr, cc.singleMGetMSet[1].buffer.Length);
        }

        [Benchmark]
        public void CPBSET()
        {
            cc.Consume(cc.singleCPBSET.ptr, cc.singleCPBSET.buffer.Length);
        }
    }
}