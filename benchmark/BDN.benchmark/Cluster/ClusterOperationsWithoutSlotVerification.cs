// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Cluster
{
    [MemoryDiagnoser]
    public unsafe class ClusterOperationsWithoutSlotVerification : ClusterOperations
    {
        public override void GlobalSetup()
        {
            enableCluster = false;
            base.GlobalSetup();
        }
    }
}