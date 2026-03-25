// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#nullable disable

using Garnet;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Embedded.server
{
    /// <summary>
    /// Non-generic interface for EmbeddedRespServer, allowing callers to interact
    /// without knowing the TServerOptions type parameter.
    /// </summary>
    internal interface IEmbeddedRespServer : IGarnetServerApp
    {
        StoreWrapper StoreWrapper { get; }
        RespServerSession GetRespSession();
        RespServerSession[] GetRespSessions(int count);
        EmbeddedNetworkHandler GetNetworkHandler();
    }

    /// <summary>
    /// Implements an embedded Garnet RESP server with JIT-optimized configuration.
    /// </summary>
    internal sealed class EmbeddedRespServer<TServerOptions> : GarnetServer<TServerOptions>, IEmbeddedRespServer
        where TServerOptions : struct, IGarnetServerOptions
    {
        readonly GarnetServerEmbedded garnetServerEmbedded;
        readonly SubscribeBroker subscribeBroker;

        /// <summary>
        /// Creates an EmbeddedRespServer instance
        /// </summary>
        public EmbeddedRespServer(GarnetServerOptions opts, ILoggerFactory loggerFactory = null, GarnetServerEmbedded server = null)
            : base(opts, loggerFactory, server == null ? null : [server])
        {
            this.garnetServerEmbedded = server;
            this.subscribeBroker = default(TServerOptions).DisablePubSub ? null :
                new SubscribeBroker(
                    null,
                    opts.PubSubPageSizeBytes(),
                    opts.SubscriberRefreshFrequencyMs,
                    pubSubEpoch,
                    true);
        }

        public new void Dispose() => base.Dispose();

        public StoreWrapper StoreWrapper => storeWrapper;

        public RespServerSession GetRespSession()
        {
            return new RespServerSession(0, new EmbeddedNetworkSender(), storeWrapper, subscribeBroker: subscribeBroker, null, true);
        }

        public RespServerSession[] GetRespSessions(int count)
        {
            var sessions = new RespServerSession[count];
            for (var i = 0; i < count; i++)
                sessions[i] = new RespServerSession(i, new EmbeddedNetworkSender(), storeWrapper, subscribeBroker: subscribeBroker, null, true);
            return sessions;
        }

        public EmbeddedNetworkHandler GetNetworkHandler()
        {
            return garnetServerEmbedded.CreateNetworkHandler();
        }
    }

    /// <summary>
    /// Factory that creates EmbeddedRespServer instances with JIT-optimized configuration.
    /// </summary>
    internal static class EmbeddedRespServerFactory
    {
        /// <summary>
        /// Creates an EmbeddedRespServer with a dynamically emitted JIT-optimized options struct.
        /// </summary>
        public static IEmbeddedRespServer CreateServer(GarnetServerOptions opts, ILoggerFactory loggerFactory = null, GarnetServerEmbedded server = null)
        {
            var structType = GarnetServerFactory.GetOrCreateOptionsStruct(opts);
            var serverType = typeof(EmbeddedRespServer<>).MakeGenericType(structType);
            return (IEmbeddedRespServer)Activator.CreateInstance(serverType, opts, loggerFactory, server);
        }
    }
}