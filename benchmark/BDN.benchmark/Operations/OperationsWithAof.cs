// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Operations
{
    [MemoryDiagnoser]
    public class RawStringOperationsWithAof : RawStringOperations
    {
        [Params(false, true)]
        public bool UseAof { get; set; }

        [GlobalSetup]
        public override void GlobalSetup()
        {
            useAof = UseAof;
            base.GlobalSetup();
        }
    }
}