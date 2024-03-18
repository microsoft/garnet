// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.networking;

namespace Garnet.server
{
    /// <summary>
    /// Interface to provides server-side session processing logic
    /// </summary>
    public interface ISessionProvider
    {
        /// <summary>
        /// Given messages of wire format type and a networkSender, returns a session that handles that wire format. If no provider is configured
        /// for the given wire format, an exception is thrown.
        /// </summary>
        /// <param name="wireFormat">Wire format</param>
        /// <param name="networkSender">Socket connection</param>
        /// <returns>Server session</returns>
        IMessageConsumer GetSession(WireFormat wireFormat, INetworkSender networkSender);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        MaxSizeSettings GetMaxSizeSettings { get; }
    }
}