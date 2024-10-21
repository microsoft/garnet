// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Benchmark for BasicOperations
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class BasicOperations : OperationsBase
    {
        static ReadOnlySpan<byte> INLINE_PING => "PING\r\n"u8;
        byte[] pingRequestBuffer;
        byte* pingRequestBufferPointer;

        public override void GlobalSetup()
        {
            base.GlobalSetup();
            SetupOperation(ref pingRequestBuffer, ref pingRequestBufferPointer, INLINE_PING);
        }

        [Benchmark]
        public void InlinePing()
        {
            _ = session.TryConsumeMessages(pingRequestBufferPointer, pingRequestBuffer.Length);
        }
    }
}