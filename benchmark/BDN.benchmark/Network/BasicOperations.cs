// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Network
{
    /// <summary>
    /// Benchmark for BasicOperations
    /// </summary>
    [MemoryDiagnoser]
    public class BasicNetworkOperations : NetworkBase
    {
        static ReadOnlySpan<byte> INLINE_PING => "PING\r\n"u8;
        byte[] pingRequestBuffer;
        unsafe byte* pingRequestBufferPointer;

        public override void GlobalSetup()
        {
            base.GlobalSetup();
            unsafe
            {
                SetupOperation(ref pingRequestBuffer, ref pingRequestBufferPointer, INLINE_PING);
            }
        }

        [Benchmark]
        public async ValueTask InlinePing()
        {
            unsafe
            {
                PrepareBuffer(pingRequestBuffer, pingRequestBufferPointer);
            }
            await Send(pingRequestBuffer.Length);

        }
    }
}