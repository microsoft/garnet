// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Cluster
{
    [MemoryDiagnoser]
    public unsafe class RespClusterDisableSlotVerificationBench : RespClusterBench
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