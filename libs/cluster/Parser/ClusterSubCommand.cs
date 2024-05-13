// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.cluster
{
    internal enum ClusterSubcommand : byte
    {
        NONE,

        // Basic cluster management commands
        BUMPEPOCH,
        FORGET,
        INFO,
        HELP,
        MEET,
        MYID,
        MYPARENTID,
        ENDPOINT,
        NODES,
        SETCONFIGEPOCH,
        SHARDS,
        GOSSIP,
        RESET,

        // Failover management commands
        FAILOVER,
        FAILAUTHREQ,
        FAILSTOPWRITES,
        FAILREPLICATIONOFFSET,

        // Slot management commands
        ADDSLOTS,
        ADDSLOTSRANGE,
        BANLIST,
        COUNTKEYSINSLOT,
        DELSLOTS,
        DELSLOTSRANGE,
        DELKEYSINSLOT,
        DELKEYSINSLOTRANGE,
        GETKEYSINSLOT,
        KEYSLOT,
        SETSLOT,
        SETSLOTSRANGE,
        SLOTS,
        SLOTSTATE,

        // Migrate management commands
        MIGRATE,
        MTASKS,

        // Replication management commands
        REPLICAS,
        REPLICATE,
        AOFSYNC,
        APPENDLOG,
        INITIATE_REPLICA_SYNC,
        SEND_CKPT_METADATA,
        SEND_CKPT_FILE_SEGMENT,
        BEGIN_REPLICA_RECOVER
    }
}
