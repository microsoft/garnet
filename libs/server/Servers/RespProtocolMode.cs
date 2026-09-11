// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// RESP protocol versions accepted by the server.
    /// </summary>
    public enum RespProtocolMode
    {
        /// <summary>
        /// Accept both RESP2 and RESP3 clients.
        /// </summary>
        Both,

        /// <summary>
        /// Accept RESP2 clients only.
        /// </summary>
        Resp2,

        /// <summary>
        /// Accept RESP3 clients only.
        /// </summary>
        Resp3,
    }
}
