// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Embedded.server;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Benchmark for TxnOperations
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class TxnOperations : OperationsBase
    {
        static ReadOnlySpan<byte> MULTIEXEC => "*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$2\r\nx1\r\n$2\r\nv1\r\n*3\r\n$3\r\nSET\r\n$2\r\nx2\r\n$2\r\nv2\r\n*2\r\n$3\r\nGET\r\n$2\r\nx1\r\n*2\r\n$3\r\nGET\r\n$2\r\nx2\r\n*1\r\n$4\r\nEXEC\r\n"u8;
        Request multiExec;

        public override void GlobalSetup()
        {
            base.GlobalSetup();

            SetupOperation(ref multiExec, MULTIEXEC);

            // Pre-populate data
            SlowConsumeMessage("*3\r\n$3\r\nSET\r\n$2\r\nx1\r\n$2\r\nv1\r\n"u8);
            SlowConsumeMessage("*3\r\n$3\r\nSET\r\n$2\r\nx2\r\n$2\r\nv2\r\n"u8);
        }

        [Benchmark]
        public void MultiExec()
        {
            Send(multiExec);
        }
    }
}