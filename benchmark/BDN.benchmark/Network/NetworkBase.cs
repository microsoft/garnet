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
        /// Batch size per method invocation - we use a batch size of 1 for network BDNs
        /// in order to stress the network layer.
        /// </summary>
        const int batchSize = 1;
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

        protected ValueTask Send(Request request) => networkHandler.Send(request);

        protected unsafe void SetupOperation(ref Request request, ReadOnlySpan<byte> operation, int batchSize = batchSize)
        {
            request.buffer = GC.AllocateArray<byte>(operation.Length * batchSize, pinned: true);
            request.bufferPtr = (byte*)Unsafe.AsPointer(ref request.buffer[0]);
            for (int i = 0; i < batchSize; i++)
                operation.CopyTo(new Span<byte>(request.buffer).Slice(i * operation.Length));
        }

        protected void SlowConsumeMessage(ReadOnlySpan<byte> message)
        {
            Request request = default;
            SetupOperation(ref request, message, 1);
            Send(request);
        }
    }
}