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
    public class BasicOperations : NetworkBase
    {
        static ReadOnlySpan<byte> INLINE_PING => "PING\r\n"u8;
        Request ping;

        public override void GlobalSetup()
        {
            base.GlobalSetup();
            SetupOperation(ref ping, INLINE_PING);
        }

        [Benchmark]
        public async ValueTask InlinePing()
        {
            await Send(ping);
        }
    }
}