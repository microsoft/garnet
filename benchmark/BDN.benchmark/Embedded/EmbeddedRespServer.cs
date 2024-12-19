// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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

        /// <summary>
        /// Creates an EmbeddedRespServer instance
        /// </summary>
        /// <param name="opts">Server options to configure the base GarnetServer instance</param>
        /// <param name="loggerFactory">Logger factory to configure the base GarnetServer instance</param>
        /// <param name="server">Server network</param>
        public EmbeddedRespServer(GarnetServerOptions opts, ILoggerFactory loggerFactory = null, GarnetServerEmbedded server = null) : base(opts, loggerFactory, server)
        {
            this.garnetServerEmbedded = server;
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
            return new RespServerSession(0, new EmbeddedNetworkSender(), storeWrapper, null, null, true);
        }

        internal EmbeddedNetworkHandler GetNetworkHandler()
        {
            return garnetServerEmbedded.CreateNetworkHandler();
        }
    }
}