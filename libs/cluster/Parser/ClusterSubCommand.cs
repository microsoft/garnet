// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
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
        /// <summary>
        /// Parse cluster subcommand and convert to ClusterSubcommand type.
        /// </summary>
        /// <param name="bufSpan"></param>
        /// <param name="subcmd"></param>
        /// <returns>True if parsing succeeded without any errors, otherwise false</returns>
        private bool ParseClusterSubcommand(ReadOnlySpan<byte> bufSpan, out Span<byte> param, out ClusterSubcommand subcmd)
        {
            subcmd = ClusterSubcommand.NONE;
            param = GetNextToken(bufSpan, out var success1);
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
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.failauthreq) => ClusterSubcommand.FAILAUTHREQ,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.failstopwrites) => ClusterSubcommand.FAILSTOPWRITES,
                _ when MemoryExtensions.SequenceEqual(param, CmdStrings.failreplicationoffset) => ClusterSubcommand.FAILREPLICATIONOFFSET,
                _ => ConvertToClusterSubcommandIgnoreCase(ref param)
            };

            return true;
        }

        /// <summary>
        /// Convert cluster subcommand sequence to ClusterSubcommand type by ignoring case
        /// </summary>
        /// <param name="subcommand"></param>
        /// <returns>ClusterSubcommand type</returns>
        private static ClusterSubcommand ConvertToClusterSubcommandIgnoreCase(ref Span<byte> subcommand)
        {
            ConvertUtils.MakeUpperCase(subcommand);
            var subcmd = subcommand switch
            {
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.MEET) => ClusterSubcommand.MEET,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.BUMPEPOCH) => ClusterSubcommand.BUMPEPOCH,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.FORGET) => ClusterSubcommand.FORGET,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.INFO) => ClusterSubcommand.INFO,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.HELP) => ClusterSubcommand.HELP,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.MYID) => ClusterSubcommand.MYID,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.MYPARENTID) => ClusterSubcommand.MYPARENTID,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.ENDPOINT) => ClusterSubcommand.ENDPOINT,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.NODES) => ClusterSubcommand.NODES,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.SET_CONFIG_EPOCH) => ClusterSubcommand.SETCONFIGEPOCH,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.SHARDS) => ClusterSubcommand.SHARDS,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.RESET) => ClusterSubcommand.RESET,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.FAILOVER) => ClusterSubcommand.FAILOVER,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.ADDSLOTS) => ClusterSubcommand.ADDSLOTS,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.ADDSLOTSRANGE) => ClusterSubcommand.ADDSLOTSRANGE,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.BANLIST) => ClusterSubcommand.BANLIST,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.COUNTKEYSINSLOT) => ClusterSubcommand.COUNTKEYSINSLOT,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.DELSLOTS) => ClusterSubcommand.DELSLOTS,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.DELSLOTSRANGE) => ClusterSubcommand.DELSLOTSRANGE,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.DELKEYSINSLOT) => ClusterSubcommand.DELKEYSINSLOT,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.DELKEYSINSLOTRANGE) => ClusterSubcommand.DELKEYSINSLOTRANGE,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.GETKEYSINSLOT) => ClusterSubcommand.GETKEYSINSLOT,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.KEYSLOT) => ClusterSubcommand.KEYSLOT,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.SETSLOT) => ClusterSubcommand.SETSLOT,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.SETSLOTSRANGE) => ClusterSubcommand.SETSLOTSRANGE,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.SLOTSTATE) => ClusterSubcommand.SLOTSTATE,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.SLOTS) => ClusterSubcommand.SLOTS,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.MIGRATE) => ClusterSubcommand.MIGRATE,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.MTASKS) => ClusterSubcommand.MTASKS,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.REPLICAS) => ClusterSubcommand.REPLICAS,
                _ when MemoryExtensions.SequenceEqual(subcommand, CmdStrings.REPLICATE) => ClusterSubcommand.REPLICATE,
                _ => ClusterSubcommand.NONE
            };

            return subcmd;
        }
    }
}