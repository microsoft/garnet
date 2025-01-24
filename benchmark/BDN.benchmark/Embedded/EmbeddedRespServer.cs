// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tsavorite.core;

namespace Embedded.server
{
    /// <summary>
    /// Implements an embedded Garnet RESP server
    /// </summary>
    internal sealed class EmbeddedRespServer : GarnetServer
    {
        readonly StoreWrapper store;
        readonly GarnetServerEmbedded server;
        readonly SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>> subscriberBroker;
        
        public EmbeddedRespServer(
            IOptions<GarnetServerOptions> options,
            ILogger<GarnetServer> logger, 
            ILoggerFactory loggerFactory,
            GarnetServerEmbedded server,
            GarnetProvider provider,
            SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>> subscriberBroker,
            StoreWrapper store)
            : base(options, logger, loggerFactory, server, provider)
        {
            this.store = store;
            this.server = server;
            this.subscriberBroker = subscriberBroker;
        }

        /// <summary>
        /// Dispose server
        /// </summary>
        public new void Dispose() => base.Dispose();

        public StoreWrapper StoreWrapper => store;

        /// <summary>
        /// Return a direct RESP session to this server
        /// </summary>
        /// <returns>A new RESP server session</returns>
        internal RespServerSession GetRespSession()
        {
            return new RespServerSession(0, 
                new EmbeddedNetworkSender(), 
                store, 
                subscribeBroker: this.subscriberBroker, 
                null, 
                true);
        }

        internal EmbeddedNetworkHandler GetNetworkHandler()
        {
            return server.CreateNetworkHandler();
        }
    }
}