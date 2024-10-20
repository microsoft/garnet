// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Operations
{
    [MemoryDiagnoser]
    public class RawStringOperationsWithACL : RawStringOperations
    {
        public override void GlobalSetup()
        {
            useACLs = true;
            base.GlobalSetup();
        }
    }
}