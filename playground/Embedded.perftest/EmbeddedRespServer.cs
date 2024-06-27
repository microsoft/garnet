// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Embedded.perftest
{
    /// <summary>
    /// Implements an embedded Garnet RESP server
    /// </summary>
    public sealed class EmbeddedRespServer : GarnetServer
    {

        /// <summary>
        /// Creates an EmbeddedRespServer instance
        /// </summary>
        /// <param name="opts">Server options to configure the base GarnetServer instance</param>
        /// <param name="loggerFactory">Logger factory to configure the base GarnetServer instance</param>
        public EmbeddedRespServer(GarnetServerOptions opts, ILoggerFactory loggerFactory = null) : base(opts, loggerFactory)
        {
            // Nothing...
        }

        /// <summary>
        /// Dispose server
        /// </summary>
        public new void Dispose() => base.Dispose();

        public StoreWrapper StoreWrapper => storeWrapper;

        /// <summary>
        /// Return a RESP session to this server
        /// </summary>
        /// <returns>A new RESP server session</returns>
        internal RespServerSession GetRespSession()
        {
            return new RespServerSession(new DummyNetworkSender(), storeWrapper, null, null);
        }
    }
}