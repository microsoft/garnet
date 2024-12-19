// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Embedded.server;
using Garnet.server;

namespace BDN.benchmark.Network
{
    /// <summary>
    /// Base class for network benchmarks
    /// </summary>
    public abstract unsafe class NetworkBase
    {
        /// <summary>
        /// Parameters
        /// </summary>
        [ParamsSource(nameof(NetworkParamsProvider))]
        public NetworkParams Params { get; set; }

        /// <summary>
        /// Operation parameters provider
        /// </summary>
        public IEnumerable<NetworkParams> NetworkParamsProvider()
        {
            yield return new(false);
        }

        /// <summary>
        /// Batch size per method invocation
        /// With a batchSize of 100, we have a convenient conversion of latency to throughput:
        ///   5 us = 20 Mops/sec
        ///  10 us = 10 Mops/sec
        ///  20 us =  5 Mops/sec
        ///  25 us =  4 Mops/sec
        /// 100 us =  1 Mops/sec
        /// </summary>
        const int batchSize = 100;
        EmbeddedRespServer server;
        EmbeddedNetworkHandler networkHandler;

        /// <summary>
        /// Setup
        /// </summary>
        [GlobalSetup]
        public virtual void GlobalSetup()
        {
            var opts = new GarnetServerOptions
            {
                QuietMode = true,
                DisablePubSub = true,
            };

            server = new EmbeddedRespServer(opts, null, new GarnetServerEmbedded());
            networkHandler = server.GetNetworkHandler();

            // Send a PING message to warm up the session
            SlowConsumeMessage("PING\r\n"u8);
        }

        /// <summary>
        /// Cleanup
        /// </summary>
        [GlobalCleanup]
        public virtual void GlobalCleanup()
        {
            networkHandler.Dispose();
            server.Dispose();
        }

        protected void Send(byte[] requestBuffer, byte* requestBufferPointer, int length)
        {
            networkHandler.Send(requestBuffer, requestBufferPointer, length);
        }

        protected void SetupOperation(ref byte[] requestBuffer, ref byte* requestBufferPointer, ReadOnlySpan<byte> operation)
        {
            requestBuffer = GC.AllocateArray<byte>(operation.Length * batchSize, pinned: true);
            requestBufferPointer = (byte*)Unsafe.AsPointer(ref requestBuffer[0]);
            for (int i = 0; i < batchSize; i++)
                operation.CopyTo(new Span<byte>(requestBuffer).Slice(i * operation.Length));
        }

        protected void SlowConsumeMessage(ReadOnlySpan<byte> message)
        {
            var buffer = GC.AllocateArray<byte>(message.Length, pinned: true);
            var bufferPointer = (byte*)Unsafe.AsPointer(ref buffer[0]);
            message.CopyTo(new Span<byte>(buffer));
            Send(buffer, bufferPointer, buffer.Length);
        }
    }
}