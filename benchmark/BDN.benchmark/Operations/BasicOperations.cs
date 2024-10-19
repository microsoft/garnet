// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Operations
{
    [MemoryDiagnoser]
    public unsafe class BasicOperations : OperationsBase
    {
        static ReadOnlySpan<byte> INLINE_PING => "PING\r\n"u8;
        byte[] pingRequestBuffer;
        byte* pingRequestBufferPointer;

        [GlobalSetup]
        public void GlobalSetup()
        {
            Setup();
            SetupOperation(ref pingRequestBuffer, ref pingRequestBufferPointer, INLINE_PING);
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            Cleanup();
        }

        [Benchmark]
        public void InlinePing()
        {
            _ = session.TryConsumeMessages(pingRequestBufferPointer, pingRequestBuffer.Length);
        }
    }
}