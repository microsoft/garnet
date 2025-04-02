// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Embedded.server;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Benchmark for ObjectOperations
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class ObjectOperations : OperationsBase
    {
        static ReadOnlySpan<byte> LPUSHPOP => "*3\r\n$5\r\nLPUSH\r\n$1\r\nd\r\n$1\r\ne\r\n*2\r\n$4\r\nLPOP\r\n$1\r\nd\r\n"u8;
        Request lPushPop;

        public override void GlobalSetup()
        {
            base.GlobalSetup();

            SetupOperation(ref lPushPop, LPUSHPOP);

            // Pre-populate data
            SlowConsumeMessage("*3\r\n$5\r\nLPUSH\r\n$1\r\nd\r\n$1\r\nf\r\n"u8);
        }

        [Benchmark]
        public void LPushPop()
        {
            Send(lPushPop);
        }
    }
}