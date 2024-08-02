// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    /// <summary>
    /// Command strings for RESP protocol
    /// </summary>
    static partial class CmdStrings
    {
        /// <summary>
        /// Request strings
        /// </summary>
        public static ReadOnlySpan<byte> CLIENT => "CLIENT"u8;
        public static ReadOnlySpan<byte> SUBSCRIBE => "SUBSCRIBE"u8;
        public static ReadOnlySpan<byte> RUNTXP => "RUNTXP"u8;
        public static ReadOnlySpan<byte> GET => "GET"u8;
        public static ReadOnlySpan<byte> get => "get"u8;
        public static ReadOnlySpan<byte> SET => "SET"u8;
        public static ReadOnlySpan<byte> set => "set"u8;
        public static ReadOnlySpan<byte> REWRITE => "REWRITE"u8;
        public static ReadOnlySpan<byte> rewrite => "rewrite"u8;
        public static ReadOnlySpan<byte> CONFIG => "CONFIG"u8;
        public static ReadOnlySpan<byte> CertFileName => "cert-file-name"u8;
        public static ReadOnlySpan<byte> CertPassword => "cert-password"u8;
        public static ReadOnlySpan<byte> ClusterUsername => "cluster-username"u8;
        public static ReadOnlySpan<byte> ClusterPassword => "cluster-password"u8;
        public static ReadOnlySpan<byte> ECHO => "ECHO"u8;
        public static ReadOnlySpan<byte> ACL => "ACL"u8;
        public static ReadOnlySpan<byte> AUTH => "AUTH"u8;
        public static ReadOnlySpan<byte> auth => "auth"u8;
        public static ReadOnlySpan<byte> SETNAME => "SETNAME"u8;
        public static ReadOnlySpan<byte> INFO => "INFO"u8;
        public static ReadOnlySpan<byte> info => "info"u8;
        public static ReadOnlySpan<byte> DOCS => "DOCS"u8;
        public static ReadOnlySpan<byte> docs => "docs"u8;
        public static ReadOnlySpan<byte> COMMAND => "COMMAND"u8;
        public static ReadOnlySpan<byte> LATENCY => "LATENCY"u8;
        public static ReadOnlySpan<byte> CLUSTER => "CLUSTER"u8;
        public static ReadOnlySpan<byte> MIGRATE => "MIGRATE"u8;
        public static ReadOnlySpan<byte> FAILOVER => "FAILOVER"u8;
        public static ReadOnlySpan<byte> HISTOGRAM => "HISTOGRAM"u8;
        public static ReadOnlySpan<byte> histogram => "histogram"u8;
        public static ReadOnlySpan<byte> REPLICAOF => "REPLICAOF"u8;
        public static ReadOnlySpan<byte> SLAVEOF => "SLAVEOF"u8;
        public static ReadOnlySpan<byte> SECONDARYOF => "SECONDARYOF"u8;
        public static ReadOnlySpan<byte> HELP => "HELP"u8;
        public static ReadOnlySpan<byte> help => "help"u8;
        public static ReadOnlySpan<byte> PING => "PING"u8;
        public static ReadOnlySpan<byte> HELLO => "HELLO"u8;
        public static ReadOnlySpan<byte> TIME => "TIME"u8;
        public static ReadOnlySpan<byte> RESET => "RESET"u8;
        public static ReadOnlySpan<byte> reset => "reset"u8;
        public static ReadOnlySpan<byte> QUIT => "QUIT"u8;
        public static ReadOnlySpan<byte> SAVE => "SAVE"u8;
        public static ReadOnlySpan<byte> LASTSAVE => "LASTSAVE"u8;
        public static ReadOnlySpan<byte> BGSAVE => "BGSAVE"u8;
        public static ReadOnlySpan<byte> BEFORE => "BEFORE"u8;
        public static ReadOnlySpan<byte> MEMORY => "MEMORY"u8;
        public static ReadOnlySpan<byte> memory => "memory"u8;
        public static ReadOnlySpan<byte> MONITOR => "MONITOR"u8;
        public static ReadOnlySpan<byte> monitor => "monitor"u8;
        public static ReadOnlySpan<byte> COMMITAOF => "COMMITAOF"u8;
        public static ReadOnlySpan<byte> FLUSHDB => "FLUSHDB"u8;
        public static ReadOnlySpan<byte> FORCEGC => "FORCEGC"u8;
        public static ReadOnlySpan<byte> MATCH => "MATCH"u8;
        public static ReadOnlySpan<byte> match => "match"u8;
        public static ReadOnlySpan<byte> COUNT => "COUNT"u8;
        public static ReadOnlySpan<byte> count => "count"u8;
        public static ReadOnlySpan<byte> TYPE => "TYPE"u8;
        public static ReadOnlySpan<byte> type => "type"u8;
        public static ReadOnlySpan<byte> REGISTERCS => "REGISTERCS"u8;
        public static ReadOnlySpan<byte> registercs => "registercs"u8;
        public static ReadOnlySpan<byte> ASYNC => "ASYNC"u8;
        public static ReadOnlySpan<byte> async => "async"u8;
        public static ReadOnlySpan<byte> SYNC => "SYNC"u8;
        public static ReadOnlySpan<byte> ON => "ON"u8;
        public static ReadOnlySpan<byte> on => "on"u8;
        public static ReadOnlySpan<byte> OFF => "OFF"u8;
        public static ReadOnlySpan<byte> off => "off"u8;
        public static ReadOnlySpan<byte> BARRIER => "BARRIER"u8;
        public static ReadOnlySpan<byte> barrier => "barrier"u8;
        public static ReadOnlySpan<byte> MODULE => "MODULE"u8;
        public static ReadOnlySpan<byte> WITHSCORE => "WITHSCORE"u8;
        public static ReadOnlySpan<byte> WITHSCORES => "WITHSCORES"u8;
        public static ReadOnlySpan<byte> WITHVALUES => "WITHVALUES"u8;
        public static ReadOnlySpan<byte> EX => "EX"u8;
        public static ReadOnlySpan<byte> PX => "PX"u8;
        public static ReadOnlySpan<byte> KEEPTTL => "KEEPTTL"u8;
        public static ReadOnlySpan<byte> NX => "NX"u8;
        public static ReadOnlySpan<byte> XX => "XX"u8;
        public static ReadOnlySpan<byte> UNSAFETRUNCATELOG => "UNSAFETRUNCATELOG"u8;
        public static ReadOnlySpan<byte> SAMPLES => "SAMPLES"u8;

        /// <summary>
        /// Response strings
        /// </summary>
        public static ReadOnlySpan<byte> RESP_OK => "+OK\r\n"u8;
        public static ReadOnlySpan<byte> RESP_ERRNOTFOUND => "$-1\r\n"u8;
        public static ReadOnlySpan<byte> RESP_EMPTYLIST => "*0\r\n"u8;
        public static ReadOnlySpan<byte> RESP_RETURN_VAL_1 => ":1\r\n"u8;
        public static ReadOnlySpan<byte> RESP_RETURN_VAL_0 => ":0\r\n"u8;
        public static ReadOnlySpan<byte> RESP_RETURN_VAL_N1 => ":-1\r\n"u8;
        public static ReadOnlySpan<byte> RESP_RETURN_VAL_N2 => ":-2\r\n"u8;
        public static ReadOnlySpan<byte> SUSCRIBE_PONG => "*2\r\n$4\r\npong\r\n$0\r\n\r\n"u8;
        public static ReadOnlySpan<byte> RESP_PONG => "+PONG\r\n"u8;
        public static ReadOnlySpan<byte> RESP_EMPTY => "$0\r\n\r\n"u8;
        public static ReadOnlySpan<byte> RESP_QUEUED => "+QUEUED\r\n"u8;

        /// <summary>
        /// Simple error response strings, i.e. these are of the form "-errorString\r\n"
        /// </summary>
        public static ReadOnlySpan<byte> RESP_ERR_NOAUTH => "NOAUTH Authentication required."u8;
        public static ReadOnlySpan<byte> RESP_ERR_WRONG_TYPE => "WRONGTYPE Operation against a key holding the wrong kind of value."u8;
        public static ReadOnlySpan<byte> RESP_ERR_WRONG_TYPE_HLL => "WRONGTYPE Key is not a valid HyperLogLog string value."u8;
        public static ReadOnlySpan<byte> RESP_ERR_EXEC_ABORT => "EXECABORT Transaction discarded because of previous errors."u8;

        /// <summary>
        /// Generic error response strings, i.e. these are of the form "-ERR error message\r\n"
        /// </summary>
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_UNK_CMD => "ERR unknown command"u8;
        public static ReadOnlySpan<byte> RESP_ERR_NOT_SUPPORTED_RESP2 => "ERR command not supported in RESP2"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_CLUSTER_DISABLED => "ERR This instance has cluster support disabled"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_WRONG_ARGUMENTS => "ERR wrong number of arguments for 'config|set' command"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_NOSUCHKEY => "ERR no such key"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_NESTED_MULTI => "ERR MULTI calls can not be nested"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_EXEC_WO_MULTI => "ERR EXEC without MULTI"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_DISCARD_WO_MULTI => "ERR DISCARD without MULTI"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_WATCH_IN_MULTI => "ERR WATCH inside MULTI is not allowed"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_INVALIDEXP_IN_SET => "ERR invalid expire time in 'set' command"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_SYNTAX_ERROR => "ERR syntax error"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_OFFSETOUTOFRANGE => "ERR offset is out of range"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER => "ERR bit offset is not an integer or out of range"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_CURSORVALUE => "ERR cursor value should be equal or greater than 0."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_INVALIDCURSOR => "ERR invalid cursor"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_MALFORMED_REGISTERCS_COMMAND => "ERR malformed REGISTERCS command."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_MALFORMED_COMMAND_INFO_JSON => "ERR malformed command info JSON."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_GETTING_BINARY_FILES => "ERR unable to access one or more binary files."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_GETTING_CMD_INFO_FILE => "ERR unable to access command info file."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_BINARY_FILES_NOT_IN_ALLOWED_PATHS => "ERR one or more binary file are not contained in allowed paths."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_CMD_INFO_FILE_NOT_IN_ALLOWED_PATHS => "ERR command info file is not contained in allowed paths."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_LOADING_ASSEMBLIES => "ERR unable to load one or more assemblies."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_ASSEMBLY_NOT_SIGNED => "ERR one or more assemblies loaded is not digitally signed."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_INSTANTIATING_CLASS => "ERR unable to instantiate one or more classes from given assemblies."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_REGISTERCS_UNSUPPORTED_CLASS => "ERR unable to register one or more unsupported classes."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER => "ERR value is not an integer or out of range."u8;
        public static ReadOnlySpan<byte> RESP_ERR_HASH_VALUE_IS_NOT_INTEGER => "ERR hash value is not an integer."u8;
        public static ReadOnlySpan<byte> RESP_ERR_HASH_VALUE_IS_NOT_FLOAT => "ERR hash value is not a float."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE => "ERR value is out of range, must be positive."u8;
        public static ReadOnlySpan<byte> RESP_ERR_PROTOCOL_VALUE_IS_NOT_INTEGER => "ERR Protocol version is not an integer or out of range."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_UKNOWN_SUBCOMMAND => "ERR Unknown subcommand. Try LATENCY HELP."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_INDEX_OUT_RANGE => "ERR index out of range"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_SELECT_INVALID_INDEX => "ERR invalid database index."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_SELECT_CLUSTER_MODE => "ERR SELECT is not allowed in cluster mode"u8;
        public static ReadOnlySpan<byte> RESP_ERR_NO_TRANSACTION_PROCEDURE => "ERR Could not get transaction procedure"u8;
        public static ReadOnlySpan<byte> RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS => "ERR wrong number of arguments for command"u8;
        public static ReadOnlySpan<byte> RESP_ERR_UNSUPPORTED_PROTOCOL_VERSION => "ERR Unsupported protocol version"u8;
        public static ReadOnlySpan<byte> RESP_ERR_ASYNC_PROTOCOL_CHANGE => "ERR protocol change is not allowed with pending async operations"u8;
        public static ReadOnlySpan<byte> RESP_ERR_NOT_VALID_FLOAT => "ERR value is not a valid float"u8;
        public static ReadOnlySpan<byte> RESP_ERR_MIN_MAX_NOT_VALID_FLOAT => "ERR min or max is not a float"u8;
        public static ReadOnlySpan<byte> RESP_ERR_MIN_MAX_NOT_VALID_STRING => "ERR min or max not valid string range item"u8;
        public static ReadOnlySpan<byte> RESP_ERR_TIMEOUT_NOT_VALID_FLOAT => "ERR timeout is not a float or out of range"u8;
        public static ReadOnlySpan<byte> RESP_WRONGPASS_INVALID_PASSWORD => "WRONGPASS Invalid password"u8;
        public static ReadOnlySpan<byte> RESP_WRONGPASS_INVALID_USERNAME_PASSWORD => "WRONGPASS Invalid username/password combination"u8;
        public static ReadOnlySpan<byte> RESP_SYNTAX_ERROR => "ERR syntax error"u8;
        public static ReadOnlySpan<byte> RESP_ERR_BITOP_KEY_LIMIT => "ERR Bitop source key limit (64) exceeded"u8;
        public static ReadOnlySpan<byte> RESP_ERR_MODULE_NO_INTERFACE => "ERR Module does not implement the required interface"u8;
        public static ReadOnlySpan<byte> RESP_ERR_MODULE_MULTIPLE_INTERFACES => "ERR Multiple modules present"u8;
        public static ReadOnlySpan<byte> RESP_ERR_MODULE_ONLOAD => "ERR Error during module OnLoad"u8;
        public static ReadOnlySpan<byte> RESP_ERR_LIMIT_NOT_SUPPORTED => "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"u8;


        /// <summary>
        /// Response string templates
        /// </summary>
        public const string GenericErrWrongNumArgs = "ERR wrong number of arguments for '{0}' command";
        public const string GenericErrUnknownOptionConfigSet = "ERR Unknown option or number of arguments for CONFIG SET - '{0}'";
        public const string GenericErrUnknownOption = "ERR Unknown option or number of arguments for '{0}' command";
        public const string GenericErrUnknownSubCommand = "ERR unknown subcommand '{0}'. Try {1} HELP";
        public const string GenericErrWrongNumArgsTxn =
            "ERR Invalid number of parameters to stored proc {0}, expected {1}, actual {2}";
        public const string GenericSyntaxErrorOption = "ERR Syntax error in {0} option '{1}'";
        public const string GenericParamShouldBeGreaterThanZero = "ERR {0} should be greater than 0";

        /// <summary>
        /// Object types
        /// </summary>
        public static ReadOnlySpan<byte> ZSET => "ZSET"u8;
        public static ReadOnlySpan<byte> zset => "zset"u8;
        public static ReadOnlySpan<byte> LIST => "LIST"u8;
        public static ReadOnlySpan<byte> list => "list"u8;
        public static ReadOnlySpan<byte> HASH => "HASH"u8;
        public static ReadOnlySpan<byte> hash => "hash"u8;
        public static ReadOnlySpan<byte> STRING => "STRING"u8;
        public static ReadOnlySpan<byte> stringt => "string"u8;

        /// <summary>
        /// Register object types
        /// </summary>
        public static ReadOnlySpan<byte> READ => "READ"u8;
        public static ReadOnlySpan<byte> read => "read"u8;
        public static ReadOnlySpan<byte> READMODIFYWRITE => "READMODIFYWRITE"u8;
        public static ReadOnlySpan<byte> readmodifywrite => "readmodifywrite"u8;
        public static ReadOnlySpan<byte> RMW => "RMW"u8;
        public static ReadOnlySpan<byte> rmw => "rmw"u8;
        public static ReadOnlySpan<byte> TRANSACTION => "TRANSACTION"u8;
        public static ReadOnlySpan<byte> transaction => "transaction"u8;
        public static ReadOnlySpan<byte> TXN => "TXN"u8;
        public static ReadOnlySpan<byte> txn => "txn"u8;
        public static ReadOnlySpan<byte> SRC => "SRC"u8;
        public static ReadOnlySpan<byte> src => "src"u8;

        public static ReadOnlySpan<byte> AND => "AND"u8;
        public static ReadOnlySpan<byte> and => "and"u8;
        public static ReadOnlySpan<byte> OR => "OR"u8;
        public static ReadOnlySpan<byte> or => "or"u8;
        public static ReadOnlySpan<byte> XOR => "XOR"u8;
        public static ReadOnlySpan<byte> xor => "xor"u8;
        public static ReadOnlySpan<byte> NOT => "NOT"u8;
        public static ReadOnlySpan<byte> not => "not"u8;

        // subcommand parsing strings
        public static ReadOnlySpan<byte> CAT => "CAT"u8;
        public static ReadOnlySpan<byte> DELUSER => "DELUSER"u8;
        public static ReadOnlySpan<byte> LOAD => "LOAD"u8;
        public static ReadOnlySpan<byte> LOADCS => "LOADCS"u8;
        public static ReadOnlySpan<byte> SETUSER => "SETUSER"u8;
        public static ReadOnlySpan<byte> USERS => "USERS"u8;
        public static ReadOnlySpan<byte> WHOAMI => "WHOAMI"u8;
        public static ReadOnlySpan<byte> USAGE => "USAGE"u8;
        public static ReadOnlySpan<byte> BUMPEPOCH => "BUMPEPOCH"u8;
        public static ReadOnlySpan<byte> FORGET => "FORGET"u8;
        public static ReadOnlySpan<byte> MEET => "MEET"u8;
        public static ReadOnlySpan<byte> MYID => "MYID"u8;
        public static ReadOnlySpan<byte> NODES => "NODES"u8;
        public static ReadOnlySpan<byte> SETCONFIGEPOCH => "SET-CONFIG-EPOCH"u8;
        public static ReadOnlySpan<byte> SHARDS => "SHARDS"u8;
        public static ReadOnlySpan<byte> ADDSLOTS => "ADDSLOTS"u8;
        public static ReadOnlySpan<byte> ADDSLOTSRANGE => "ADDSLOTSRANGE"u8;
        public static ReadOnlySpan<byte> COUNTKEYSINSLOT => "COUNTKEYSINSLOT"u8;
        public static ReadOnlySpan<byte> DELSLOTS => "DELSLOTS"u8;
        public static ReadOnlySpan<byte> DELSLOTSRANGE => "DELSLOTSRANGE"u8;
        public static ReadOnlySpan<byte> GETKEYSINSLOT => "GETKEYSINSLOT"u8;
        public static ReadOnlySpan<byte> KEYSLOT => "KEYSLOT"u8;
        public static ReadOnlySpan<byte> SETSLOT => "SETSLOT"u8;
        public static ReadOnlySpan<byte> SLOTS => "SLOTS"u8;
        public static ReadOnlySpan<byte> REPLICAS => "REPLICAS"u8;
        public static ReadOnlySpan<byte> REPLICATE => "REPLICATE"u8;

        // Cluster subcommands which are internal and thus undocumented
        // 
        // Because these are internal, they have lower case property names
        public static ReadOnlySpan<byte> gossip => "GOSSIP"u8;
        public static ReadOnlySpan<byte> myparentid => "MYPARENTID"u8;
        public static ReadOnlySpan<byte> delkeysinslot => "DELKEYSINSLOT"u8;
        public static ReadOnlySpan<byte> delkeysinslotrange => "DELKEYSINSLOTRANGE"u8;
        public static ReadOnlySpan<byte> setslotsrange => "SETSLOTSRANGE"u8;
        public static ReadOnlySpan<byte> slotstate => "SLOTSTATE"u8;
        public static ReadOnlySpan<byte> mtasks => "MTASKS"u8;
        public static ReadOnlySpan<byte> aofsync => "AOFSYNC"u8;
        public static ReadOnlySpan<byte> appendlog => "APPENDLOG"u8;
        public static ReadOnlySpan<byte> banlist => "BANLIST"u8;
        public static ReadOnlySpan<byte> begin_replica_recover => "BEGIN_REPLICA_RECOVER"u8;
        public static ReadOnlySpan<byte> endpoint => "ENDPOINT"u8;
        public static ReadOnlySpan<byte> failreplicationoffset => "FAILREPLICATIONOFFSET"u8;
        public static ReadOnlySpan<byte> failstopwrites => "FAILSTOPWRITES"u8;
        public static ReadOnlySpan<byte> initiate_replica_sync => "INITIATE_REPLICA_SYNC"u8;
        public static ReadOnlySpan<byte> send_ckpt_file_segment => "SEND_CKPT_FILE_SEGMENT"u8;
        public static ReadOnlySpan<byte> send_ckpt_metadata => "SEND_CKPT_METADATA"u8;
    }
}