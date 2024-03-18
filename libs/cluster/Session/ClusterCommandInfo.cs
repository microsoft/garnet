// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace Garnet.cluster
{
    /// <summary>
    /// Info on what cluster commands is supported
    /// </summary>
    public static class ClusterCommandInfo
    {
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public static List<string> GetClusterCommands()
        {
            return new List<string>()
            {
                "CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "ADDSLOTS <slot> [<slot> ...]",
                "\tAssign slots to current node.",
                "ADDSLOTSRANGE start-slot end-slot [start-slot end-slot ...]",
                "\tAssign slot ranges to current node.",
                "BUMPEPOCH",
                "\tAdvance the cluster config epoch.",
                "BANLIST",
                "\t Return banlist of nodeids",
                "COUNTKEYSINSLOT <slot>",
                "\tReturn the number of keys in <slot>.",
                "DELSLOTS <slot> [<slot> ...]",
                "\tDelete slots information from current node.",
                "DELSLOTSRANGE start-slot end-slot [start-slot end-slot ...]",
                "\tDelete slot ranges information from current node.",
                "DELKEYSINSLOT slot",
                "\tScan the DB and delete keys mapping to corresponding slot.",
                "DELKEYSINSLOTRANGE start-slot end-slot [start-slot end-slot ...]",
                "\tScan the DB and delete keys mapping to corresponding slot ranges.",
                "FAILOVER [FORCE | TAKEOVER]",
                "\tSend only to replica, forces the replica to start a manual failover of its master instance",
                "FORGET <node-id> [ban node for seconds = default(60)]",
                "\tRemove a node from the cluster.",
                "GETKEYSINSLOT <slot> <count>",
                "\tGETKEYSINSLOT <slot> <count>",
                "INFO",
                "\tReturn information about the cluster.",
                "KEYSLOT",
                "\tReturn the SLOT a provided KEY is mapped to.",
                "MEET <ip> <port> [<bus-port>]",
                "\tConnect nodes into a working cluster.",
                "MTASKS",
                "\tReturn number of outstanding migration tasks.",
                "MYID",
                "\tReturn the node id.",
                "MYPARENTID",
                "\tReturn primary id or own id if instance not a replica",
                "ENDPOINT",
                "\tEndpoint <nodeid>",
                "\tReturn 'ip:port' for nodeid. 'unassigned:0' if nodeid is not known",
                "NODES",
                "\tReturn cluster configuration seen by node. Output format:",
                "\t<id> <ip:port> <flags> <master> <pings> <pongs> <epoch> <link> <slot> ...",
                "REPLICATE <node-id>",
                "\tConfigure current node as replica to <node-id>.",
                "REPLICAS <node-id>",
                "\tReturn <node-id> replicas.",
                "RESET [HARD|SOFT]",
                "\tReset node configuration (default:SOFT). Default SOFT option resets configuration by forgetting slot mapping and nodes. HARD resets config epoch and generates new nodeid and flushes DB data.",
                "SET-CONFIG-EPOCH <epoch>",
                "\tSet config epoch of current node.",
                "SETSLOT <slot> (IMPORTING|MIGRATING|STABLE|NODE <node-id>)",
                "\tSet slot state.",
                "SETSLOTRANGE start-slot end-slot [start-slot end-slot ...]",
                "\tSet state of slots in range.",
                "SLOTS",
                "\tReturn information about slots range mappings. Each range is made of:",
                "SLOTSTATE",
                "\tReturn information about slot state",
                "\tstart, end, master and replicas IP addresses, ports and ids",
                "SHARDS",
                "\tReturns details about the shards of the cluster. A shard is defined as a collection of nodes that serve the same set of slots and that replicate from each other",
                "HELP",
                "\tPrints this help."
            };
        }
    }
}