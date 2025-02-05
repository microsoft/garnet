// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server.Auth.Settings
{
    public enum ConnectionProtectionOption
    {
        Block = 0,
        AllowForLocalConnections = 1,
        AllowForAll = 2
    }

    public static class ConnectionProtectionOptionExtensions
    {
        public static ConnectionProtectionOption FromString(string s)
        {
            return s switch
            {
                "yes" or "all" => ConnectionProtectionOption.AllowForAll,
                "local" => ConnectionProtectionOption.AllowForLocalConnections,
                _ => ConnectionProtectionOption.Block
            };
        }
    }
}