// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.common
{
    public class RoleReplicaInfo
    {
        public string master_host;
        public int master_port;
        public long replication_offset;
        public string replication_state;
    }
}