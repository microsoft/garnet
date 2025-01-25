// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Garnet.common
{
    public class RoleInfo
    {
        public string role;
        public long replication_offset;
        public long replication_offset2;
        public string master_host;
        public int master_port;
        public string replication_state;
        public List<(int, ReplicaInfo)> replicaInfo;
    }
}
