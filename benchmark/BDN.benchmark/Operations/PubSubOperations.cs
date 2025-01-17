// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Embedded.server;
using Garnet.server;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Benchmark for PubSubOperations
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class PubSubOperations : OperationsBase
    {
        static ReadOnlySpan<byte> PUBLISH => "*3\r\n$7\r\nPUBLISH\r\n$7\r\nchannel\r\n$7\r\nmessage\r\n"u8;
        Request publish;

        static ReadOnlySpan<byte> SUBSCRIBE => "*2\r\n$9\r\nSUBSCRIBE\r\n$7\r\nchannel\r\n"u8;
        Request subscribe;

        public override void GlobalSetup()
        {
            var opts = new GarnetServerOptions
            {
                QuietMode = true,
                DisablePubSub = false,
            };

            server = new EmbeddedRespServer(opts, null, new GarnetServerEmbedded());
            session = server.GetRespSession();
            subscribeSession = server.GetRespSession();

            SetupOperation(ref publish, PUBLISH);
            SetupOperation(ref subscribe, SUBSCRIBE);

            // Subscribe to secondary session
            _ = subscribeSession.TryConsumeMessages(subscribe.bufferPtr, subscribe.buffer.Length);

            // Warm up
            SlowConsumeMessage(new Span<byte>(publish.bufferPtr, publish.buffer.Length));
            SlowConsumeMessage(new Span<byte>(publish.bufferPtr, publish.buffer.Length));
            SlowConsumeMessage(new Span<byte>(publish.bufferPtr, publish.buffer.Length));
            SlowConsumeMessage(new Span<byte>(publish.bufferPtr, publish.buffer.Length));
            SlowConsumeMessage(new Span<byte>(publish.bufferPtr, publish.buffer.Length));
            SlowConsumeMessage(new Span<byte>(publish.bufferPtr, publish.buffer.Length));
        }

        [Benchmark]
        public void Publish()
        {
            Send(publish);
        }
    }
}