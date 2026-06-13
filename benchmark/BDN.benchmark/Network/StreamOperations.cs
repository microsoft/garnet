// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Embedded.server;

namespace BDN.benchmark.Network
{
    /// <summary>
    /// Benchmark for BasicOperations
    /// </summary>
    [MemoryDiagnoser]
    public class StreamOperations : NetworkBase
    {
        static ReadOnlySpan<byte> XADD => "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$1\r\n*\r\n$5\r\nfield\r\n$5\r\nvalue\r\n"u8;
        Request xadd;

        public override void GlobalSetup()
        {
            base.GlobalSetup();
            SetupOperation(ref xadd, XADD);
        }

        [Benchmark]
        public async ValueTask InlineXAdd()
        {
            await Send(xadd);
        }
    }
}