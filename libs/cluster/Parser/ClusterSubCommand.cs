// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.server;

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

    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        private bool ParseClusterSubcommand(ReadOnlySpan<byte> bufSpan, out ClusterSubcommand subcmd)
        {
            subcmd = ClusterSubcommand.NONE;
            var param = GetNextTokenUpperCase(bufSpan, out var success1);
            if (!success1) return false;

            subcmd = param switch
            {
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.appendlog) => ClusterSubcommand.APPENDLOG,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.GOSSIP) => ClusterSubcommand.GOSSIP,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.send_ckpt_file_segment) => ClusterSubcommand.SEND_CKPT_FILE_SEGMENT,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.send_ckpt_metadata) => ClusterSubcommand.SEND_CKPT_METADATA,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.aofsync) => ClusterSubcommand.AOFSYNC,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.begin_replica_recover) => ClusterSubcommand.BEGIN_REPLICA_RECOVER,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.initiate_replica_sync) => ClusterSubcommand.INITIATE_REPLICA_SYNC,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.MEET) => ClusterSubcommand.MEET,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.BUMPEPOCH) => ClusterSubcommand.BUMPEPOCH,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.FORGET) => ClusterSubcommand.FORGET,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.INFO) => ClusterSubcommand.INFO,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.HELP) => ClusterSubcommand.HELP,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.MYID) => ClusterSubcommand.MYID,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.MYPARENTID) => ClusterSubcommand.MYPARENTID,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.ENDPOINT) => ClusterSubcommand.ENDPOINT,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.NODES) => ClusterSubcommand.NODES,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.SET_CONFIG_EPOCH) => ClusterSubcommand.SETCONFIGEPOCH,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.SHARDS) => ClusterSubcommand.SHARDS,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.RESET) => ClusterSubcommand.RESET,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.FAILOVER) => ClusterSubcommand.FAILOVER,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.failauthreq) => ClusterSubcommand.FAILAUTHREQ,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.failstopwrites) => ClusterSubcommand.FAILSTOPWRITES,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.failreplicationoffset) => ClusterSubcommand.FAILREPLICATIONOFFSET,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.ADDSLOTS) => ClusterSubcommand.ADDSLOTS,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.ADDSLOTSRANGE) => ClusterSubcommand.ADDSLOTSRANGE,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.BANLIST) => ClusterSubcommand.BANLIST,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.COUNTKEYSINSLOT) => ClusterSubcommand.COUNTKEYSINSLOT,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.DELSLOTS) => ClusterSubcommand.DELSLOTS,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.DELSLOTSRANGE) => ClusterSubcommand.DELSLOTSRANGE,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.DELKEYSINSLOT) => ClusterSubcommand.DELKEYSINSLOT,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.DELKEYSINSLOTRANGE) => ClusterSubcommand.DELKEYSINSLOTRANGE,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.GETKEYSINSLOT) => ClusterSubcommand.GETKEYSINSLOT,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.KEYSLOT) => ClusterSubcommand.KEYSLOT,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.SETSLOT) => ClusterSubcommand.SETSLOT,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.SETSLOTSRANGE) => ClusterSubcommand.SETSLOTSRANGE,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.SLOTSTATE) => ClusterSubcommand.SLOTSTATE,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.SLOTS) => ClusterSubcommand.SLOTS,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.MIGRATE) => ClusterSubcommand.MIGRATE,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.MTASKS) => ClusterSubcommand.MTASKS,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.REPLICAS) => ClusterSubcommand.REPLICAS,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.REPLICATE) => ClusterSubcommand.REPLICATE,
                _ => ClusterSubcommand.NONE
            };

            return true;
        }
    }
}