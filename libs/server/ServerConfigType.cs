// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    internal enum ServerConfigType : int
    {
        NONE,
        ALL,
        TIMEOUT,
        SAVE,
        APPENDONLY,
        SLAVE_READ_ONLY,
        DATABASES,
        CLUSTER_NODE_TIMEOUT
    }
}
