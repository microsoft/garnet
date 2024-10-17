// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Cluster
{
    [MemoryDiagnoser]
    public unsafe class RespClusterDisableSlotVerificationBench : RespClusterBench
    {
        public override void GlobalSetup()
        {
            enableCluster = false;
            base.GlobalSetup();
        }
    }
}