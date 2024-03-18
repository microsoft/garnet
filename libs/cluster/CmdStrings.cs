// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.cluster
{
    /// <summary>
    /// Command strings for RESP protocol
    /// </summary>
    static class CmdStrings
    {
        /// <summary>
        /// Request strings
        /// </summary>
        public static ReadOnlySpan<byte> INFO => "INFO"u8;
        public static ReadOnlySpan<byte> info => "info"u8;
        public static ReadOnlySpan<byte> CLUSTER => "CLUSTER"u8;
        public static ReadOnlySpan<byte> NODES => "NODES"u8;
        public static ReadOnlySpan<byte> nodes => "nodes"u8;
        public static ReadOnlySpan<byte> ADDSLOTS => "ADDSLOTS"u8;
        public static ReadOnlySpan<byte> addslots => "addslots"u8;
        public static ReadOnlySpan<byte> ADDSLOTSRANGE => "ADDSLOTSRANGE"u8;
        public static ReadOnlySpan<byte> addslotsrange => "addslotsrange"u8;
        public static ReadOnlySpan<byte> aofsync => "aofsync"u8;
        public static ReadOnlySpan<byte> appendlog => "appendlog"u8;
        public static ReadOnlySpan<byte> initiate_replica_sync => "initiate_replica_sync"u8;
        public static ReadOnlySpan<byte> send_ckpt_metadata => "send_ckpt_metadata"u8;
        public static ReadOnlySpan<byte> send_ckpt_file_segment => "send_ckpt_file_segment"u8;
        public static ReadOnlySpan<byte> begin_replica_recover => "begin_replica_recover"u8;
        public static ReadOnlySpan<byte> BUMPEPOCH => "BUMPEPOCH"u8;
        public static ReadOnlySpan<byte> bumpepoch => "bumpepoch"u8;
        public static ReadOnlySpan<byte> BANLIST => "BANLIST"u8;
        public static ReadOnlySpan<byte> banlist => "banlist"u8;
        public static ReadOnlySpan<byte> COUNTKEYSINSLOT => "COUNTKEYSINSLOT"u8;
        public static ReadOnlySpan<byte> countkeysinslot => "countkeysinslot"u8;
        public static ReadOnlySpan<byte> delkeysinslot => "delkeysinslot"u8;
        public static ReadOnlySpan<byte> DELKEYSINSLOT => "DELKEYSINSLOT"u8;
        public static ReadOnlySpan<byte> delkeysinslotrange => "delkeysinslotrange"u8;
        public static ReadOnlySpan<byte> DELKEYSINSLOTRANGE => "DELKEYSINSLOTRANGE"u8;
        public static ReadOnlySpan<byte> DELSLOTS => "DELSLOTS"u8;
        public static ReadOnlySpan<byte> delslots => "delslots"u8;
        public static ReadOnlySpan<byte> DELSLOTSRANGE => "DELSLOTSRANGE"u8;
        public static ReadOnlySpan<byte> delslotsrange => "delslotsrange"u8;
        public static ReadOnlySpan<byte> FAILOVER => "FAILOVER"u8;
        public static ReadOnlySpan<byte> failover => "failover"u8;
        public static ReadOnlySpan<byte> REPLICAOF => "REPLICAOF"u8;
        public static ReadOnlySpan<byte> SECONDARYOF => "SLAVEOF"u8;
        public static ReadOnlySpan<byte> failauthreq => "failauthreq"u8;
        public static ReadOnlySpan<byte> failstopwrites => "failstopwrites"u8;
        public static ReadOnlySpan<byte> failreplicationoffset => "failreplicationoffset"u8;
        public static ReadOnlySpan<byte> FORGET => "FORGET"u8;
        public static ReadOnlySpan<byte> forget => "forget"u8;
        public static ReadOnlySpan<byte> GETKEYSINSLOT => "GETKEYSINSLOT"u8;
        public static ReadOnlySpan<byte> getkeysinslot => "getkeysinslot"u8;
        public static ReadOnlySpan<byte> KEYSLOT => "KEYSLOT"u8;
        public static ReadOnlySpan<byte> keyslot => "keyslot"u8;
        public static ReadOnlySpan<byte> HELP => "HELP"u8;
        public static ReadOnlySpan<byte> help => "help"u8;
        public static ReadOnlySpan<byte> MEET => "MEET"u8;
        public static ReadOnlySpan<byte> meet => "meet"u8;
        public static ReadOnlySpan<byte> MIGRATE => "MIGRATE"u8;
        public static ReadOnlySpan<byte> MTASKS => "MTASKS"u8;
        public static ReadOnlySpan<byte> MYID => "MYID"u8;
        public static ReadOnlySpan<byte> myid => "myid"u8;
        public static ReadOnlySpan<byte> MYPARENTID => "MYPARENTID"u8;
        public static ReadOnlySpan<byte> myparentid => "myparentid"u8;
        public static ReadOnlySpan<byte> ENDPOINT => "ENDPOINT"u8;
        public static ReadOnlySpan<byte> endpoint => "endpoint"u8;
        public static ReadOnlySpan<byte> REPLICAS => "REPLICAS"u8;
        public static ReadOnlySpan<byte> replicas => "replicas"u8;
        public static ReadOnlySpan<byte> REPLICATE => "REPLICATE"u8;
        public static ReadOnlySpan<byte> replicate => "replicate"u8;
        public static ReadOnlySpan<byte> SET_CONFIG_EPOCH => "SET-CONFIG-EPOCH"u8;
        public static ReadOnlySpan<byte> set_config_epoch => "set-config-epoch"u8;
        public static ReadOnlySpan<byte> SETSLOT => "SETSLOT"u8;
        public static ReadOnlySpan<byte> setslot => "setslot"u8;
        public static ReadOnlySpan<byte> SETSLOTSRANGE => "SETSLOTSRANGE"u8;
        public static ReadOnlySpan<byte> setslotsrange => "setslotsrange"u8;
        public static ReadOnlySpan<byte> SHARDS => "SHARDS"u8;
        public static ReadOnlySpan<byte> shards => "shards"u8;
        public static ReadOnlySpan<byte> SLOTS => "SLOTS"u8;
        public static ReadOnlySpan<byte> slots => "slots"u8;
        public static ReadOnlySpan<byte> SLOTSTATE => "SLOTSTATE"u8;
        public static ReadOnlySpan<byte> slotstate => "slotstate"u8;
        public static ReadOnlySpan<byte> GOSSIP => "GOSSIP"u8;
        public static ReadOnlySpan<byte> WITHMEET => "WITHMEET"u8;
        public static ReadOnlySpan<byte> RESET => "RESET"u8;
        public static ReadOnlySpan<byte> reset => "reset"u8;

        /// <summary>
        /// Response strings
        /// </summary>
        public static ReadOnlySpan<byte> RESP_OK => "+OK\r\n"u8;
        public static ReadOnlySpan<byte> RESP_NOAUTH => "-NOAUTH Authentication required.\r\n"u8;
        public static ReadOnlySpan<byte> RESP_RETURN_VAL_1 => ":1\r\n"u8;
        public static ReadOnlySpan<byte> RESP_RETURN_VAL_0 => ":0\r\n"u8;
        public static ReadOnlySpan<byte> RESP_ERRCLUSTER => "-ERR This instance has cluster support disabled\r\n"u8;
        public static ReadOnlySpan<byte> RESP_SLOT_OUT_OFF_RANGE => "-ERR Slot out of range\r\n"u8;

        /// <summary>
        /// Response string templates
        /// </summary>
        public const string ErrMissingParam = "-ERR wrong number of arguments for '{0}' command\r\n";
    }
}