// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server.Auth.Settings
{
    public enum ConnectionProtectionOption
    {
        No = 0, // Block
        Local = 1, // AllowForLocalConnections
        Yes = 2 // AllowForAll
    }
}