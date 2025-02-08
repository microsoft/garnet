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
        Block = 0,
        AllowForLocalConnections = 1,
        AllowForAll = 2
    }
}