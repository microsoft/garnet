// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Cluster
{
    [MemoryDiagnoser]
    public unsafe class RespClusterBenchNoSlot : RespClusterBench
    {
        public new void GlobalSetup()
        {
            enableCluster = false;
            base.GlobalSetup();
        }

        public new void GlobalCleanup()
        {
            base.GlobalCleanup();
        }
    }
}
