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
        public static ReadOnlySpan<byte> RESP_RETURN_VAL_1 => ":1\r\n"u8;
        public static ReadOnlySpan<byte> RESP_RETURN_VAL_0 => ":0\r\n"u8;
        public static ReadOnlySpan<byte> RESP_RETURN_VAL_N1 => ":-1\r\n"u8;

        /// <summary>
        /// Response string templates
        /// </summary>
        public const string GenericErrMissingParam = "ERR wrong number of arguments for '{0}' command";

        public static ReadOnlySpan<byte> RESP_NOT_ASSIGNED_PRIMARY_ERROR => "ERR Don't have primary"u8;

        /// <summary>
        /// Generic error respone strings, i.e. these are sent in the form "-ERR responseString\r\n"
        /// </summary>
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_CLUSTER => "ERR This instance has cluster support disabled"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_SLOT_OUT_OFF_RANGE => "ERR Slot out of range"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_CONFIG_UPDATE => "ERR Updating the config epoch"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_CONFIG_EPOCH_ASSIGNMENT => "ERR The user can assign a config epoch only when the node does not know any other node"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_REPLICATION_AOF_TURNEDOFF => "ERR Replication unaivalable because AOF is switched off, please restart replica with --aof option"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_SLOTSTATE_TRANSITION => "ERR Slot already in that state"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_CANNOT_FORGET_MYSELF => "ERR I tried hard but I can't forget myself"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_CANNOT_FORGET_MY_PRIMARY => "ERR Can't forget my primary"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_CANNOT_FAILOVER_FROM_NON_MASTER => "ERR Cannot failover a non-master node"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_UNKNOWN_ENDPOINT => "ERR Unknown endpoint"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_CANNOT_MAKE_REPLICA_WITH_ASSIGNED_SLOTS => "ERR Primary has been assigned slots and cannot be a replica"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_CANNOT_REPLICATE_SELF => "ERR Can't replicate myself."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_NOT_ASSIGNED_PRIMARY_ERROR => "ERR Don't have primary"u8;

        /// <summary>
        /// Generic error response strings for <c>MIGRATE</c> command
        /// </summary>
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_MIGRATE_TO_MYSELF => "ERR Can't MIGRATE to myself"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_INCOMPLETESLOTSRANGE => "ERR incomplete slotrange"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_PARSING => "ERR Parsing error"u8;

        /// <summary>
        /// Simple error respone strings, i.e. these are of the form "-errorString\r\n"
        /// </summary>
        public static ReadOnlySpan<byte> RESP_ERR_NOAUTH => "NOAUTH Authentication required."u8;
        public static ReadOnlySpan<byte> RESP_ERR_CROSSLOT => "CROSSSLOT Keys in request do not hash to the same slot"u8;
        public static ReadOnlySpan<byte> RESP_ERR_CLUSTERDOWN => "CLUSTERDOWN Hash slot not served"u8;
        public static ReadOnlySpan<byte> RESP_ERR_MIGRATING => "MIGRATING"u8;
        public static ReadOnlySpan<byte> RESP_ERR_CREATE_SYNC_SESSION_ERROR => "PRIMARY-ERR Failed creating replica sync session task"u8;
        public static ReadOnlySpan<byte> RESP_ERR_RETRIEVE_SYNC_SESSION_ERROR => "PRIMARY-ERR Failed retrieving replica sync session"u8;
    }
}