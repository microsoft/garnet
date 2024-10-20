// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Operations
{
    [MemoryDiagnoser]
    public class RawStringOperationsWithAof : RawStringOperations
    {
        public override void GlobalSetup()
        {
            useAof = true;
            base.GlobalSetup();
        }
    }
}