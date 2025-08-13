// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#nullable disable

using Garnet;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Embedded.server
{
    /// <summary>
    /// Implements an embedded Garnet RESP server
    /// </summary>
    internal sealed class EmbeddedRespServer : GarnetServer
    {
        readonly GarnetServerEmbedded garnetServerEmbedded;
        readonly SubscribeBroker subscribeBroker;

        /// <summary>
        /// Creates an EmbeddedRespServer instance
        /// </summary>
        /// <param name="opts">Server options to configure the base GarnetServer instance</param>
        /// <param name="loggerFactory">Logger factory to configure the base GarnetServer instance</param>
        /// <param name="server">Server network</param>
        public EmbeddedRespServer(GarnetServerOptions opts, ILoggerFactory loggerFactory = null, GarnetServerEmbedded server = null) : base(opts, loggerFactory, server == null ? null : [server])
        {
            this.garnetServerEmbedded = server;
            this.subscribeBroker = opts.DisablePubSub ? null :
                new SubscribeBroker(
                    null,
                    opts.PubSubPageSizeBytes(),
                    opts.SubscriberRefreshFrequencyMs,
                    true);
        }

        /// <summary>
        /// Dispose server
        /// </summary>
        public new void Dispose() => base.Dispose();

        public StoreWrapper StoreWrapper => storeWrapper;

        /// <summary>
        /// Return a direct RESP session to this server
        /// </summary>
        /// <returns>A new RESP server session</returns>
        internal RespServerSession GetRespSession()
        {
            return new RespServerSession(0, new EmbeddedNetworkSender(), storeWrapper, subscribeBroker: subscribeBroker, null, true);
        }

        internal RespServerSession[] GetRespSessions(int count)
        {
            var sessions = new RespServerSession[count];
            for (var i = 0; i < count; i++)
                sessions[i] = new RespServerSession(i, new EmbeddedNetworkSender(), storeWrapper, subscribeBroker: subscribeBroker, null, true);
            return sessions;
        }

        internal EmbeddedNetworkHandler GetNetworkHandler()
        {
            return garnetServerEmbedded.CreateNetworkHandler();
        }
    }
}