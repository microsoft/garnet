// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Embedded.server;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Benchmark for ServerOperations
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class ServerOperations : OperationsBase
    {
        static ReadOnlySpan<byte> SELECTUNSELECT => "*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n"u8;
        Request selectUnselect;

        static ReadOnlySpan<byte> SWAPDB => "*3\r\n$6\r\nSWAPDB\r\n$1\r\n1\r\n$1\r\n0\r\n"u8;
        Request swapDb;

        public override void GlobalSetup()
        {
            base.GlobalSetup();

            SetupOperation(ref selectUnselect, SELECTUNSELECT);
            SetupOperation(ref swapDb, SWAPDB);

            // Pre-populate data in DB0
            SlowConsumeMessage("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\na\r\n"u8);
            SlowConsumeMessage("*3\r\n$5\r\nLPUSH\r\n$1\r\nd\r\n$1\r\nf\r\n"u8);
            SlowConsumeMessage("*3\r\n$4\r\nSADD\r\n$1\r\ne\r\n$1\r\nb\r\n"u8);

            // Pre-populate data in DB1
            SlowConsumeMessage("*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"u8);
            SlowConsumeMessage("*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\nb\r\n"u8);
            SlowConsumeMessage("*3\r\n$5\r\nLPUSH\r\n$1\r\nf\r\n$1\r\nh\r\n"u8);
            SlowConsumeMessage("*3\r\n$4\r\nSADD\r\n$1\r\ng\r\n$1\r\ni\r\n"u8);
        }

        [Benchmark]
        public void SelectUnselect()
        {
            Send(selectUnselect);
        }

        [Benchmark]
        public void SwapDb()
        {
            Send(swapDb);
        }
    }
}