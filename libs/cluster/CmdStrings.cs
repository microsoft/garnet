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
        public static ReadOnlySpan<byte> WITHMEET => "WITHMEET"u8;

        /// <summary>
        /// Internode communication cluster commands
        /// </summary>
        public static ReadOnlySpan<byte> failstopwrites => "FAILSTOPWRITES"u8;
        public static ReadOnlySpan<byte> failreplicationoffset => "FAILREPLICATIONOFFSET"u8;


        /// <summary>
        /// Response strings
        /// </summary>
        public static ReadOnlySpan<byte> RESP_OK => "+OK\r\n"u8;
        public static ReadOnlySpan<byte> RESP_RETURN_VAL_N1 => ":-1\r\n"u8;
        public static ReadOnlySpan<byte> RESP_RETURN_NULL => "$-1\r\n"u8;
        
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
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_CANNOT_ACQUIRE_RECOVERY_LOCK => "ERR Recovery in progress, could not acquire recoverLock"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_CANNOT_TAKEOVER_FROM_PRIMARY => "ERR Could not take over from primary"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_CANNOT_REPLICATE_SELF => "ERR Can't replicate myself"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_NOT_ASSIGNED_PRIMARY_ERROR => "ERR Don't have primary"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_WORKERS_NOT_INITIALIZED => "ERR workers not initialized"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_CONFIG_EPOCH_NOT_SET => "ERR Node config epoch was not set due to invalid epoch specified"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_NOT_IN_IMPORTING_STATE => "ERR Node not in IMPORTING state"u8;
        public static ReadOnlySpan<byte> RESP_ERR_INVALID_SLOT => "ERR Invalid or out of range slot"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER => "ERR value is not an integer or out of range."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_VALUE_IS_NOT_BOOLEAN => "ERR value is not a boolean."u8;
        public static ReadOnlySpan<byte> RESP_ERR_RESET_WITH_KEYS_ASSIGNED => "-ERR CLUSTER RESET can't be called with master nodes containing keys\r\n"u8;
        public static ReadOnlySpan<byte> RESP_SYNTAX_ERROR => "ERR syntax error"u8;

        /// <summary>
        /// Generic error response strings for <c>MIGRATE</c> command
        /// </summary>
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_MIGRATE_TO_MYSELF => "ERR Can't MIGRATE to myself"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_INCOMPLETESLOTSRANGE => "ERR incomplete slotrange"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_SLOTNOTMIGRATING => "ERR slot state not set to MIGRATING state"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_FAILEDTOADDKEY => "ERR Failed to add key for migration tracking"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_PARSING => "ERR Parsing error"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_SLOT_STATE => "ERR Invalid slot state"u8;

        /// <summary>
        /// Simple error respone strings, i.e. these are of the form "-errorString\r\n"
        /// </summary>
        public static ReadOnlySpan<byte> RESP_ERR_CROSSSLOT => "CROSSSLOT Keys in request do not hash to the same slot"u8;
        public static ReadOnlySpan<byte> RESP_ERR_CLUSTERDOWN => "CLUSTERDOWN Hash slot not served"u8;
        public static ReadOnlySpan<byte> RESP_ERR_TRYAGAIN => "TRYAGAIN Multiple keys request during rehashing of slot"u8;
        public static ReadOnlySpan<byte> RESP_ERR_CREATE_SYNC_SESSION_ERROR => "PRIMARY-ERR Failed creating replica sync session task"u8;
        public static ReadOnlySpan<byte> RESP_ERR_RETRIEVE_SYNC_SESSION_ERROR => "PRIMARY-ERR Failed retrieving replica sync session"u8;
        public static ReadOnlySpan<byte> RESP_ERR_IOERR => "IOERR Migrate keys failed"u8;

        /// <summary>
        /// Response string templates
        /// </summary>
        public const string GenericErrWrongNumArgs = "ERR wrong number of arguments for '{0}' command";

        public const string GenericErrInvalidPort = "ERR Invalid TCP base port specified: {0}";

        public const string GenericNullValue = "$-1\r\n";
    }
}