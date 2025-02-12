// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server.Auth.Settings
{
    /// <summary>
    /// Certain commands can be set at configuration to be limited to a particular connection type.
    /// They can be blocked for all connections, allowed only for local connections or be allowed for all connections.
    /// </summary>
    public enum ConnectionProtectionOption
    {
        /// <summary>
        /// Block for all connections.
        /// </summary>
        No = 0,
        /// <summary>
        ///  Allow only for local connections.
        /// </summary>
        Local = 1,
        /// <summary>
        /// Allow for every connection including remote connections.
        /// </summary>
        Yes = 2
    }
}