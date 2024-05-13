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
                _ when param.SequenceEqual(CmdStrings.appendlog) => ClusterSubcommand.APPENDLOG,
                _ when param.SequenceEqual(CmdStrings.GOSSIP) => ClusterSubcommand.GOSSIP,
                _ when param.SequenceEqual(CmdStrings.send_ckpt_file_segment) => ClusterSubcommand.SEND_CKPT_FILE_SEGMENT,
                _ when param.SequenceEqual(CmdStrings.send_ckpt_metadata) => ClusterSubcommand.SEND_CKPT_METADATA,
                _ when param.SequenceEqual(CmdStrings.aofsync) => ClusterSubcommand.AOFSYNC,
                _ when param.SequenceEqual(CmdStrings.begin_replica_recover) => ClusterSubcommand.BEGIN_REPLICA_RECOVER,
                _ when param.SequenceEqual(CmdStrings.initiate_replica_sync) => ClusterSubcommand.INITIATE_REPLICA_SYNC,
                _ when param.SequenceEqual(CmdStrings.failauthreq) => ClusterSubcommand.FAILAUTHREQ,
                _ when param.SequenceEqual(CmdStrings.failstopwrites) => ClusterSubcommand.FAILSTOPWRITES,
                _ when param.SequenceEqual(CmdStrings.failreplicationoffset) => ClusterSubcommand.FAILREPLICATIONOFFSET,
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
                _ when subcommand.SequenceEqual(CmdStrings.MEET) => ClusterSubcommand.MEET,
                _ when subcommand.SequenceEqual(CmdStrings.BUMPEPOCH) => ClusterSubcommand.BUMPEPOCH,
                _ when subcommand.SequenceEqual(CmdStrings.FORGET) => ClusterSubcommand.FORGET,
                _ when subcommand.SequenceEqual(CmdStrings.INFO) => ClusterSubcommand.INFO,
                _ when subcommand.SequenceEqual(CmdStrings.HELP) => ClusterSubcommand.HELP,
                _ when subcommand.SequenceEqual(CmdStrings.MYID) => ClusterSubcommand.MYID,
                _ when subcommand.SequenceEqual(CmdStrings.MYPARENTID) => ClusterSubcommand.MYPARENTID,
                _ when subcommand.SequenceEqual(CmdStrings.ENDPOINT) => ClusterSubcommand.ENDPOINT,
                _ when subcommand.SequenceEqual(CmdStrings.NODES) => ClusterSubcommand.NODES,
                _ when subcommand.SequenceEqual(CmdStrings.SET_CONFIG_EPOCH) => ClusterSubcommand.SETCONFIGEPOCH,
                _ when subcommand.SequenceEqual(CmdStrings.SHARDS) => ClusterSubcommand.SHARDS,
                _ when subcommand.SequenceEqual(CmdStrings.RESET) => ClusterSubcommand.RESET,
                _ when subcommand.SequenceEqual(CmdStrings.FAILOVER) => ClusterSubcommand.FAILOVER,
                _ when subcommand.SequenceEqual(CmdStrings.ADDSLOTS) => ClusterSubcommand.ADDSLOTS,
                _ when subcommand.SequenceEqual(CmdStrings.ADDSLOTSRANGE) => ClusterSubcommand.ADDSLOTSRANGE,
                _ when subcommand.SequenceEqual(CmdStrings.BANLIST) => ClusterSubcommand.BANLIST,
                _ when subcommand.SequenceEqual(CmdStrings.COUNTKEYSINSLOT) => ClusterSubcommand.COUNTKEYSINSLOT,
                _ when subcommand.SequenceEqual(CmdStrings.DELSLOTS) => ClusterSubcommand.DELSLOTS,
                _ when subcommand.SequenceEqual(CmdStrings.DELSLOTSRANGE) => ClusterSubcommand.DELSLOTSRANGE,
                _ when subcommand.SequenceEqual(CmdStrings.DELKEYSINSLOT) => ClusterSubcommand.DELKEYSINSLOT,
                _ when subcommand.SequenceEqual(CmdStrings.DELKEYSINSLOTRANGE) => ClusterSubcommand.DELKEYSINSLOTRANGE,
                _ when subcommand.SequenceEqual(CmdStrings.GETKEYSINSLOT) => ClusterSubcommand.GETKEYSINSLOT,
                _ when subcommand.SequenceEqual(CmdStrings.KEYSLOT) => ClusterSubcommand.KEYSLOT,
                _ when subcommand.SequenceEqual(CmdStrings.SETSLOT) => ClusterSubcommand.SETSLOT,
                _ when subcommand.SequenceEqual(CmdStrings.SETSLOTSRANGE) => ClusterSubcommand.SETSLOTSRANGE,
                _ when subcommand.SequenceEqual(CmdStrings.SLOTSTATE) => ClusterSubcommand.SLOTSTATE,
                _ when subcommand.SequenceEqual(CmdStrings.SLOTS) => ClusterSubcommand.SLOTS,
                _ when subcommand.SequenceEqual(CmdStrings.MIGRATE) => ClusterSubcommand.MIGRATE,
                _ when subcommand.SequenceEqual(CmdStrings.MTASKS) => ClusterSubcommand.MTASKS,
                _ when subcommand.SequenceEqual(CmdStrings.REPLICAS) => ClusterSubcommand.REPLICAS,
                _ when subcommand.SequenceEqual(CmdStrings.REPLICATE) => ClusterSubcommand.REPLICATE,
                _ => ClusterSubcommand.NONE
            };

            return subcmd;
        }
    }
}