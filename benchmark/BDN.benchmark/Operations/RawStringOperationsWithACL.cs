// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Operations
{
    [MemoryDiagnoser]
    public class RawStringOperationsWithACL : RawStringOperations
    {
        [Params(false, true)]
        public bool UseACLs { get; set; }

        [GlobalSetup]
        public override void GlobalSetup()
        {
            useACLs = UseACLs;
            base.GlobalSetup();
        }
    }
}