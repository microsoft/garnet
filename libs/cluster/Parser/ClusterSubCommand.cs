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
            var param = GetCommand(bufSpan, out var success1);
            if (!success1) return false;

            if (MemoryExtensions.SequenceEqual(CmdStrings.BUMPEPOCH, param) || MemoryExtensions.SequenceEqual(CmdStrings.bumpepoch, param))
            {
                subcmd = ClusterSubcommand.BUMPEPOCH;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.FORGET, param) || MemoryExtensions.SequenceEqual(CmdStrings.forget, param))
            {
                subcmd = ClusterSubcommand.FORGET;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.INFO, param) || MemoryExtensions.SequenceEqual(CmdStrings.info, param))
            {
                subcmd = ClusterSubcommand.INFO;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.HELP, param) || MemoryExtensions.SequenceEqual(CmdStrings.help, param))
            {
                subcmd = ClusterSubcommand.HELP;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.MEET, param) || MemoryExtensions.SequenceEqual(CmdStrings.meet, param))
            {
                subcmd = ClusterSubcommand.MEET;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.MYID, param) || MemoryExtensions.SequenceEqual(CmdStrings.myid, param))
            {
                subcmd = ClusterSubcommand.MYID;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.MYPARENTID, param) || MemoryExtensions.SequenceEqual(CmdStrings.myparentid, param))
            {
                subcmd = ClusterSubcommand.MYPARENTID;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.ENDPOINT, param) || MemoryExtensions.SequenceEqual(CmdStrings.endpoint, param))
            {
                subcmd = ClusterSubcommand.ENDPOINT;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.NODES, param) || MemoryExtensions.SequenceEqual(CmdStrings.nodes, param))
            {
                subcmd = ClusterSubcommand.NODES;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.SET_CONFIG_EPOCH, param) || MemoryExtensions.SequenceEqual(CmdStrings.set_config_epoch, param))
            {
                subcmd = ClusterSubcommand.SETCONFIGEPOCH;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.SHARDS, param) || MemoryExtensions.SequenceEqual(CmdStrings.shards, param))
            {
                subcmd = ClusterSubcommand.SHARDS;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.GOSSIP, param))
            {
                subcmd = ClusterSubcommand.GOSSIP;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.RESET, param) || MemoryExtensions.SequenceEqual(CmdStrings.reset, param))
            {
                subcmd = ClusterSubcommand.RESET;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.FAILOVER, param) || MemoryExtensions.SequenceEqual(CmdStrings.failover, param))
            {
                subcmd = ClusterSubcommand.FAILOVER;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.failauthreq, param))
            {
                subcmd = ClusterSubcommand.FAILAUTHREQ;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.failstopwrites, param))
            {
                subcmd = ClusterSubcommand.FAILSTOPWRITES;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.failreplicationoffset, param))
            {
                subcmd = ClusterSubcommand.FAILREPLICATIONOFFSET;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.ADDSLOTS, param) || MemoryExtensions.SequenceEqual(CmdStrings.addslots, param))
            {
                subcmd = ClusterSubcommand.ADDSLOTS;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.ADDSLOTSRANGE, param) || MemoryExtensions.SequenceEqual(CmdStrings.addslotsrange, param))
            {
                subcmd = ClusterSubcommand.ADDSLOTSRANGE;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.BANLIST, param) || MemoryExtensions.SequenceEqual(CmdStrings.banlist, param))
            {
                subcmd = ClusterSubcommand.BANLIST;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.COUNTKEYSINSLOT, param) || MemoryExtensions.SequenceEqual(CmdStrings.countkeysinslot, param))
            {
                subcmd = ClusterSubcommand.COUNTKEYSINSLOT;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.COUNTKEYSINSLOT, param) || MemoryExtensions.SequenceEqual(CmdStrings.countkeysinslot, param))
            {
                subcmd = ClusterSubcommand.COUNTKEYSINSLOT;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.DELSLOTS, param) || MemoryExtensions.SequenceEqual(CmdStrings.delslots, param))
            {
                subcmd = ClusterSubcommand.DELSLOTS;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.DELSLOTSRANGE, param) || MemoryExtensions.SequenceEqual(CmdStrings.delslotsrange, param))
            {
                subcmd = ClusterSubcommand.DELSLOTSRANGE;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.DELKEYSINSLOT, param) || MemoryExtensions.SequenceEqual(CmdStrings.delkeysinslot, param))
            {
                subcmd = ClusterSubcommand.DELKEYSINSLOT;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.DELKEYSINSLOTRANGE, param) || MemoryExtensions.SequenceEqual(CmdStrings.delkeysinslotrange, param))
            {
                subcmd = ClusterSubcommand.DELKEYSINSLOTRANGE;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.GETKEYSINSLOT, param) || MemoryExtensions.SequenceEqual(CmdStrings.getkeysinslot, param))
            {
                subcmd = ClusterSubcommand.GETKEYSINSLOT;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.KEYSLOT, param) || MemoryExtensions.SequenceEqual(CmdStrings.keyslot, param))
            {
                subcmd = ClusterSubcommand.KEYSLOT;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.SETSLOT, param) || MemoryExtensions.SequenceEqual(CmdStrings.setslot, param))
            {
                subcmd = ClusterSubcommand.SETSLOT;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.SETSLOTSRANGE, param) || MemoryExtensions.SequenceEqual(CmdStrings.setslotsrange, param))
            {
                subcmd = ClusterSubcommand.SETSLOTSRANGE;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.SLOTS, param) || MemoryExtensions.SequenceEqual(CmdStrings.slots, param))
            {
                subcmd = ClusterSubcommand.SLOTS;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.SLOTSTATE, param) || MemoryExtensions.SequenceEqual(CmdStrings.slotstate, param))
            {
                subcmd = ClusterSubcommand.SLOTSTATE;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.MIGRATE, param))
            {
                subcmd = ClusterSubcommand.MIGRATE;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.MTASKS, param) || MemoryExtensions.SequenceEqual(CmdStrings.mtasks, param))
            {
                subcmd = ClusterSubcommand.MTASKS;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.REPLICAS, param) || MemoryExtensions.SequenceEqual(CmdStrings.replicas, param))
            {
                subcmd = ClusterSubcommand.REPLICAS;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.REPLICATE, param) || MemoryExtensions.SequenceEqual(CmdStrings.replicate, param))
            {
                subcmd = ClusterSubcommand.REPLICATE;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.aofsync, param))
            {
                subcmd = ClusterSubcommand.AOFSYNC;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.appendlog, param))
            {
                subcmd = ClusterSubcommand.APPENDLOG;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.initiate_replica_sync, param))
            {
                subcmd = ClusterSubcommand.INITIATE_REPLICA_SYNC;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.send_ckpt_metadata, param))
            {
                subcmd = ClusterSubcommand.SEND_CKPT_METADATA;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.send_ckpt_file_segment, param))
            {
                subcmd = ClusterSubcommand.SEND_CKPT_FILE_SEGMENT;
            }
            else if (MemoryExtensions.SequenceEqual(CmdStrings.begin_replica_recover, param))
            {
                subcmd = ClusterSubcommand.BEGIN_REPLICA_RECOVER;
            }

            return true;
        }
    }
}