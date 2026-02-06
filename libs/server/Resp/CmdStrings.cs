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
        public static ReadOnlySpan<byte> subscribe => "subscribe"u8;
        public static ReadOnlySpan<byte> SSUBSCRIBE => "SSUBSCRIBE"u8;
        public static ReadOnlySpan<byte> ssubscribe => "ssubscribe"u8;
        public static ReadOnlySpan<byte> RUNTXP => "RUNTXP"u8;
        public static ReadOnlySpan<byte> GET => "GET"u8;
        public static ReadOnlySpan<byte> get => "get"u8;
        public static ReadOnlySpan<byte> SET => "SET"u8;
        public static ReadOnlySpan<byte> set => "set"u8;
        public static ReadOnlySpan<byte> GEORADIUS => "GEORADIUS"u8;
        public static ReadOnlySpan<byte> GEORADIUS_RO => "GEORADIUS_RO"u8;
        public static ReadOnlySpan<byte> GEORADIUSBYMEMBER => "GEORADIUSBYMEMBER"u8;
        public static ReadOnlySpan<byte> GEORADIUSBYMEMBER_RO => "GEORADIUSBYMEMBER_RO"u8;

        public static ReadOnlySpan<byte> REWRITE => "REWRITE"u8;
        public static ReadOnlySpan<byte> rewrite => "rewrite"u8;
        public static ReadOnlySpan<byte> CONFIG => "CONFIG"u8;
        public static ReadOnlySpan<byte> Memory => "memory"u8;
        public static ReadOnlySpan<byte> HeapMemory => "heap-memory"u8;
        public static ReadOnlySpan<byte> Index => "index"u8;
        public static ReadOnlySpan<byte> ObjIndex => "obj-index"u8;
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
        public static ReadOnlySpan<byte> DEBUG => "DEBUG"u8;
        public static ReadOnlySpan<byte> PANIC => "PANIC"u8;
        public static ReadOnlySpan<byte> DOCS => "DOCS"u8;
        public static ReadOnlySpan<byte> docs => "docs"u8;
        public static ReadOnlySpan<byte> GETKEYS => "GETKEYS"u8;
        public static ReadOnlySpan<byte> GETKEYSANDFLAGS => "GETKEYSANDFLAGS"u8;
        public static ReadOnlySpan<byte> COMMAND => "COMMAND"u8;
        public static ReadOnlySpan<byte> LATENCY => "LATENCY"u8;
        public static ReadOnlySpan<byte> SLOWLOG => "SLOWLOG"u8;
        public static ReadOnlySpan<byte> CLUSTER => "CLUSTER"u8;
        public static ReadOnlySpan<byte> MIGRATE => "MIGRATE"u8;
        public static ReadOnlySpan<byte> PURGEBP => "PURGEBP"u8;
        public static ReadOnlySpan<byte> FAILOVER => "FAILOVER"u8;
        public static ReadOnlySpan<byte> HISTOGRAM => "HISTOGRAM"u8;
        public static ReadOnlySpan<byte> histogram => "histogram"u8;
        public static ReadOnlySpan<byte> REPLICAOF => "REPLICAOF"u8;
        public static ReadOnlySpan<byte> SLAVEOF => "SLAVEOF"u8;
        public static ReadOnlySpan<byte> SECONDARYOF => "SECONDARYOF"u8;
        public static ReadOnlySpan<byte> HELP => "HELP"u8;
        public static ReadOnlySpan<byte> help => "help"u8;
        public static ReadOnlySpan<byte> PING => "PING"u8;
        public static ReadOnlySpan<byte> SCRIPT => "SCRIPT"u8;
        public static ReadOnlySpan<byte> HELLO => "HELLO"u8;
        public static ReadOnlySpan<byte> TIME => "TIME"u8;
        public static ReadOnlySpan<byte> RESET => "RESET"u8;
        public static ReadOnlySpan<byte> reset => "reset"u8;
        public static ReadOnlySpan<byte> ROLE => "ROLE"u8;
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
        public static ReadOnlySpan<byte> FLUSHALL => "FLUSHALL"u8;
        public static ReadOnlySpan<byte> FLUSHDB => "FLUSHDB"u8;
        public static ReadOnlySpan<byte> FORCEGC => "FORCEGC"u8;
        public static ReadOnlySpan<byte> MATCH => "MATCH"u8;
        public static ReadOnlySpan<byte> COUNT => "COUNT"u8;
        public static ReadOnlySpan<byte> count => "count"u8;
        public static ReadOnlySpan<byte> NOVALUES => "NOVALUES"u8;
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
        public static ReadOnlySpan<byte> EXAT => "EXAT"u8;
        public static ReadOnlySpan<byte> PX => "PX"u8;
        public static ReadOnlySpan<byte> PXAT => "PXAT"u8;
        public static ReadOnlySpan<byte> PERSIST => "PERSIST"u8;
        public static ReadOnlySpan<byte> KEEPTTL => "KEEPTTL"u8;
        public static ReadOnlySpan<byte> NX => "NX"u8;
        public static ReadOnlySpan<byte> XX => "XX"u8;
        public static ReadOnlySpan<byte> CH => "CH"u8;
        public static ReadOnlySpan<byte> WITHETAG => "WITHETAG"u8;
        public static ReadOnlySpan<byte> UNSAFETRUNCATELOG => "UNSAFETRUNCATELOG"u8;
        public static ReadOnlySpan<byte> SAMPLES => "SAMPLES"u8;
        public static ReadOnlySpan<byte> RANK => "RANK"u8;
        public static ReadOnlySpan<byte> rank => "rank"u8;
        public static ReadOnlySpan<byte> MAXLEN => "MAXLEN"u8;
        public static ReadOnlySpan<byte> maxlen => "maxlen"u8;
        public static ReadOnlySpan<byte> PUBSUB => "PUBSUB"u8;
        public static ReadOnlySpan<byte> HCOLLECT => "HCOLLECT"u8;
        public static ReadOnlySpan<byte> ZCOLLECT => "ZCOLLECT"u8;
        public static ReadOnlySpan<byte> CHANNELS => "CHANNELS"u8;
        public static ReadOnlySpan<byte> NUMPAT => "NUMPAT"u8;
        public static ReadOnlySpan<byte> NUMSUB => "NUMSUB"u8;
        public static ReadOnlySpan<byte> FROMMEMBER => "FROMMEMBER"u8;
        public static ReadOnlySpan<byte> STORE => "STORE"u8;
        public static ReadOnlySpan<byte> STOREDIST => "STOREDIST"u8;
        public static ReadOnlySpan<byte> WITHCOORD => "WITHCOORD"u8;
        public static ReadOnlySpan<byte> WITHDIST => "WITHDIST"u8;
        public static ReadOnlySpan<byte> WITHHASH => "WITHHASH"u8;
        public static ReadOnlySpan<byte> LIB_NAME => "LIB-NAME"u8;
        public static ReadOnlySpan<byte> lib_name => "lib-name"u8;
        public static ReadOnlySpan<byte> LIB_VER => "LIB-VER"u8;
        public static ReadOnlySpan<byte> lib_ver => "lib-ver"u8;
        public static ReadOnlySpan<byte> RIGHT => "RIGHT"u8;
        public static ReadOnlySpan<byte> LEFT => "LEFT"u8;
        public static ReadOnlySpan<byte> BYLEX => "BYLEX"u8;
        public static ReadOnlySpan<byte> REV => "REV"u8;
        public static ReadOnlySpan<byte> LIMIT => "LIMIT"u8;
        public static ReadOnlySpan<byte> MIN => "MIN"u8;
        public static ReadOnlySpan<byte> MAX => "MAX"u8;
        public static ReadOnlySpan<byte> WEIGHTS => "WEIGHTS"u8;
        public static ReadOnlySpan<byte> AGGREGATE => "AGGREGATE"u8;
        public static ReadOnlySpan<byte> SUM => "SUM"u8;
        public static ReadOnlySpan<byte> LEN => "LEN"u8;
        public static ReadOnlySpan<byte> IDX => "IDX"u8;
        public static ReadOnlySpan<byte> MINMATCHLEN => "MINMATCHLEN"u8;
        public static ReadOnlySpan<byte> WITHMATCHLEN => "WITHMATCHLEN"u8;
        public static ReadOnlySpan<byte> GETWITHETAG => "GETWITHETAG"u8;
        public static ReadOnlySpan<byte> GETIFNOTMATCH => "GETIFNOTMATCH"u8;
        public static ReadOnlySpan<byte> SETIFMATCH => "SETIFMATCH"u8;
        public static ReadOnlySpan<byte> SETIFGREATER => "SETIFGREATER"u8;
        public static ReadOnlySpan<byte> DELIFGREATER => "DELIFGREATER"u8;
        public static ReadOnlySpan<byte> FIELDS => "FIELDS"u8;
        public static ReadOnlySpan<byte> MEMBERS => "MEMBERS"u8;
        public static ReadOnlySpan<byte> TIMEOUT => "TIMEOUT"u8;
        public static ReadOnlySpan<byte> ERROR => "ERROR"u8;
        public static ReadOnlySpan<byte> LOG => "LOG"u8;
        public static ReadOnlySpan<byte> INCRBY => "INCRBY"u8;
        public static ReadOnlySpan<byte> NOGET => "NOGET"u8;
        public static ReadOnlySpan<byte> SCHEDULE => "SCHEDULE"u8;

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
        public static ReadOnlySpan<byte> matches => "matches"u8;
        public static ReadOnlySpan<byte> len => "len"u8;
        public static ReadOnlySpan<byte> RESP3_NULL_REPLY => "_\r\n"u8;

        /// <summary>
        /// Simple error response strings, i.e. these are of the form "-errorString\r\n"
        /// </summary>
        public static ReadOnlySpan<byte> RESP_ERR_NOAUTH => "NOAUTH Authentication required."u8;
        public static ReadOnlySpan<byte> RESP_ERR_WRONG_TYPE => "WRONGTYPE Operation against a key holding the wrong kind of value."u8;
        public static ReadOnlySpan<byte> RESP_ERR_WRONG_TYPE_HLL => "WRONGTYPE Key is not a valid HyperLogLog string value."u8;
        public static ReadOnlySpan<byte> RESP_ERR_EXEC_ABORT => "EXECABORT Transaction discarded because of previous errors."u8;
        public static ReadOnlySpan<byte> RESP_ERR_ETAG_ON_CUSTOM_PROC => "WRONGTYPE Key with etag cannot be used for custom procedure."u8;

        public static ReadOnlySpan<byte> RESP_ERR_NOSCRIPT => "ERR This Redis command is not allowed from script"u8;

        /// <summary>
        /// Generic error response strings, i.e. these are of the form "-ERR error message\r\n"
        /// </summary>
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_UNK_CMD => "ERR unknown command"u8;
        public static ReadOnlySpan<byte> RESP_ERR_NOT_SUPPORTED_RESP2 => "ERR command not supported in RESP2"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_CLUSTER_DISABLED => "ERR This instance has cluster support disabled"u8;
        public static ReadOnlySpan<byte> RESP_ERR_LUA_DISABLED => "ERR This instance has Lua scripting support disabled"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_WRONG_ARGUMENTS => "ERR wrong number of arguments for 'config|set' command"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_NOSUCHKEY => "ERR no such key"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_NESTED_MULTI => "ERR MULTI calls can not be nested"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_EXEC_WO_MULTI => "ERR EXEC without MULTI"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_DISCARD_WO_MULTI => "ERR DISCARD without MULTI"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_WATCH_IN_MULTI => "ERR WATCH inside MULTI is not allowed"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_INVALIDEXP_IN_SET => "ERR invalid expire time in 'set' command"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_SYNTAX_ERROR => "ERR syntax error"u8;
        public static ReadOnlySpan<byte> RESP_ERR_WITHETAG_AND_GETVALUE => "ERR WITHETAG option not allowed with GET inside of SET"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_NAN_INFINITY => "ERR value is NaN or Infinity"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_NAN_INFINITY_INCR => "ERR increment would produce NaN or Infinity"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_SCORE_NAN => "ERR resulting score is not a number (NaN)"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_OFFSETOUTOFRANGE => "ERR offset is out of range"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_BIT_IS_NOT_INTEGER => "ERR bit is not an integer or out of range"u8;
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
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_MUST_DEFINE_ASSEMBLY_BINPATH => "To enable client-side assembly loading, you must specify a list of allowed paths from which assemblies can potentially be loaded using the ExtensionBinPath directive."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_ACCESSING_ASSEMBLIES => "ERR unable to access one or more assemblies to check if it's digitally signed."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_ASSEMBLY_NOT_SIGNED => "ERR one or more assemblies loaded is not digitally signed."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_INSTANTIATING_CLASS => "ERR unable to instantiate one or more classes from given assemblies."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_REGISTERCS_UNSUPPORTED_CLASS => "ERR unable to register one or more unsupported classes."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER => "ERR value is not an integer or out of range."u8;
        public static ReadOnlySpan<byte> RESP_ERR_HASH_VALUE_IS_NOT_INTEGER => "ERR hash value is not an integer."u8;
        public static ReadOnlySpan<byte> RESP_ERR_HASH_VALUE_IS_NOT_FLOAT => "ERR hash value is not a float."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE => "ERR value is out of range, must be positive."u8;
        public static ReadOnlySpan<byte> RESP_ERR_PROTOCOL_VALUE_IS_NOT_INTEGER => "ERR Protocol version is not an integer or out of range."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_INDEX_OUT_RANGE => "ERR index out of range"u8;
        public static ReadOnlySpan<byte> RESP_ERR_DB_INDEX_OUT_OF_RANGE => "ERR DB index is out of range."u8;
        public static ReadOnlySpan<byte> RESP_ERR_INVALID_FIRST_DB_INDEX => "ERR invalid first DB index."u8;
        public static ReadOnlySpan<byte> RESP_ERR_INVALID_SECOND_DB_INDEX => "ERR invalid second DB index."u8;
        public static ReadOnlySpan<byte> RESP_ERR_SELECT_IN_TXN_UNSUPPORTED => "ERR SELECT is currently unsupported inside a transaction."u8;
        public static ReadOnlySpan<byte> RESP_ERR_SWAPDB_IN_TXN_UNSUPPORTED => "ERR SWAPDB is currently unsupported inside a transaction."u8;
        public static ReadOnlySpan<byte> RESP_ERR_SWAPDB_UNSUPPORTED => "ERR SWAPDB is currently unsupported when multiple clients are connected."u8;
        public static ReadOnlySpan<byte> RESP_ERR_SELECT_UNSUCCESSFUL => "ERR unable to select database."u8;
        public static ReadOnlySpan<byte> RESP_ERR_DB_ID_CLUSTER_MODE => "ERR specifying non-zero DBID is not allowed in cluster mode"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_SELECT_CLUSTER_MODE => "ERR SELECT is not allowed in cluster mode"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_SWAPDB_CLUSTER_MODE => "ERR SWAPDB is not allowed in cluster mode"u8;
        public static ReadOnlySpan<byte> RESP_ERR_NO_TRANSACTION_PROCEDURE => "ERR Could not get transaction procedure"u8;
        public static ReadOnlySpan<byte> RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS => "ERR wrong number of arguments for command"u8;
        public static ReadOnlySpan<byte> RESP_ERR_UNSUPPORTED_PROTOCOL_VERSION => "ERR Unsupported protocol version"u8;
        public static ReadOnlySpan<byte> RESP_ERR_ASYNC_PROTOCOL_CHANGE => "ERR protocol change is not allowed with pending async operations"u8;
        public static ReadOnlySpan<byte> RESP_ERR_NOT_VALID_FLOAT => "ERR value is not a valid float"u8;
        public static ReadOnlySpan<byte> RESP_ERR_MIN_MAX_NOT_VALID_FLOAT => "ERR min or max is not a float"u8;
        public static ReadOnlySpan<byte> RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT => "ERR unsupported unit provided. please use M, KM, FT, MI"u8;
        public static ReadOnlySpan<byte> RESP_ERR_NOT_VALID_HEIGHT => "ERR need numeric height"u8;
        public static ReadOnlySpan<byte> RESP_ERR_HEIGHT_OR_WIDTH_NEGATIVE => "ERR height or width cannot be negative"u8;
        public static ReadOnlySpan<byte> RESP_ERR_NOT_VALID_RADIUS => "ERR need numeric radius"u8;
        public static ReadOnlySpan<byte> RESP_ERR_NOT_VALID_WIDTH => "ERR need numeric width"u8;
        public static ReadOnlySpan<byte> RESP_ERR_MIN_MAX_NOT_VALID_STRING => "ERR min or max not valid string range item"u8;
        public static ReadOnlySpan<byte> RESP_ERR_RADIUS_IS_NEGATIVE => "ERR radius cannot be negative"u8;
        public static ReadOnlySpan<byte> RESP_ERR_TIMEOUT_IS_NEGATIVE => "ERR timeout is negative"u8;
        public static ReadOnlySpan<byte> RESP_ERR_TIMEOUT_NOT_VALID_FLOAT => "ERR timeout is not a float or out of range"u8;
        public static ReadOnlySpan<byte> RESP_ERR_TIMEOUT_IS_OUT_OF_RANGE => "ERR timeout is out of range"u8;
        public static ReadOnlySpan<byte> RESP_WRONGPASS_INVALID_PASSWORD => "WRONGPASS Invalid password"u8;
        public static ReadOnlySpan<byte> RESP_WRONGPASS_INVALID_USERNAME_PASSWORD => "WRONGPASS Invalid username/password combination"u8;
        public static ReadOnlySpan<byte> RESP_SYNTAX_ERROR => "ERR syntax error"u8;
        public static ReadOnlySpan<byte> RESP_ERR_BITOP_KEY_LIMIT => "ERR Bitop source key limit (64) exceeded"u8;
        public static ReadOnlySpan<byte> RESP_ERR_BITOP_DIFF_TWO_SOURCE_KEYS_REQUIRED => "ERR BITOP DIFF must be called with at least two source keys."u8;
        public static ReadOnlySpan<byte> RESP_ERR_COUNT_IS_NOT_POSITIVE => "ERR COUNT must be > 0"u8;
        public static ReadOnlySpan<byte> RESP_ERR_COUNT_IS_OUT_OF_RANGE_N1 => "ERR count should be greater than or equal to -1."u8;
        public static ReadOnlySpan<byte> RESP_ERR_MODULE_LOADED_TYPES => "ERR Unable to load types from module. Ensure that the module is compatible with the current runtime."u8;
        public static ReadOnlySpan<byte> RESP_ERR_MODULE_NO_INTERFACE => "ERR Module does not implement the required interface"u8;
        public static ReadOnlySpan<byte> RESP_ERR_MODULE_MULTIPLE_INTERFACES => "ERR Multiple modules present"u8;
        public static ReadOnlySpan<byte> RESP_ERR_MODULE_ONLOAD => "ERR Error during module OnLoad"u8;
        public static ReadOnlySpan<byte> RESP_ERR_LIMIT_NOT_SUPPORTED => "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"u8;
        public static ReadOnlySpan<byte> RESP_ERR_NO_SCRIPT => "NOSCRIPT No matching script. Please use EVAL."u8;
        public static ReadOnlySpan<byte> RESP_ERR_CANNOT_LIST_CLIENTS => "ERR Clients cannot be listed."u8;
        public static ReadOnlySpan<byte> RESP_ERR_UBLOCKING_CLINET => "ERR Unable to unblock client because of error."u8;
        public static ReadOnlySpan<byte> RESP_ERR_NO_SUCH_CLIENT => "ERR No such client"u8;
        public static ReadOnlySpan<byte> RESP_ERR_INVALID_CLIENT_ID => "ERR Invalid client ID"u8;
        public static ReadOnlySpan<byte> RESP_ERR_INVALID_CLIENT_NAME => "ERR Client names cannot contain spaces, newlines or special characters."u8;
        public static ReadOnlySpan<byte> RESP_ERR_ACL_AUTH_DISABLED => "ERR ACL Authenticator is disabled."u8;
        public static ReadOnlySpan<byte> RESP_ERR_ACL_AUTH_FILE_DISABLED => "ERR This Garnet instance is not configured to use an ACL file. Please restart server with --acl-file option."u8;
        public static ReadOnlySpan<byte> RESP_ERR_XX_NX_NOT_COMPATIBLE => "ERR XX and NX options at the same time are not compatible"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GT_LT_NX_NOT_COMPATIBLE => "ERR GT, LT, and/or NX options at the same time are not compatible"u8;
        public static ReadOnlySpan<byte> RESP_ERR_INCR_SUPPORTS_ONLY_SINGLE_PAIR => "ERR INCR option supports a single increment-element pair"u8;
        public static ReadOnlySpan<byte> RESP_ERR_INVALID_BITFIELD_TYPE => "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is"u8;
        public static ReadOnlySpan<byte> RESP_ERR_INVALID_OVERFLOW_TYPE => "ERR Invalid OVERFLOW type specified"u8;
        public static ReadOnlySpan<byte> RESP_ERR_SCRIPT_FLUSH_OPTIONS => "ERR SCRIPT FLUSH only support SYNC|ASYNC option"u8;
        public static ReadOnlySpan<byte> RESP_ERR_BUSSYKEY => "BUSYKEY Target key name already exists."u8;
        public static ReadOnlySpan<byte> RESP_ERR_LENGTH_AND_INDEXES => "If you want both the length and indexes, please just use IDX."u8;
        public static ReadOnlySpan<byte> RESP_ERR_INVALID_EXPIRE_TIME => "ERR invalid expire time, must be >= 0"u8;
        public static ReadOnlySpan<byte> RESP_ERR_HCOLLECT_ALREADY_IN_PROGRESS => "ERR HCOLLECT scan already in progress"u8;
        public static ReadOnlySpan<byte> RESP_ERR_ZCOLLECT_ALREADY_IN_PROGRESS => "ERR ZCOLLECT scan already in progress"u8;
        public static ReadOnlySpan<byte> RESP_INVALID_COMMAND_SPECIFIED => "Invalid command specified"u8;
        public static ReadOnlySpan<byte> RESP_COMMAND_HAS_NO_KEY_ARGS => "The command has no key arguments"u8;
        public static ReadOnlySpan<byte> RESP_ERR_INVALID_CLIENT_UNBLOCK_REASON => "ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR"u8;
        public static ReadOnlySpan<byte> RESP_UNBLOCKED_CLIENT_VIA_CLIENT_UNBLOCK => "UNBLOCKED client unblocked via CLIENT UNBLOCK"u8;
        public static ReadOnlySpan<byte> RESP_ERR_INVALID_ETAG => "ETAG must be a numerical value greater than or equal to 0"u8;
        public static ReadOnlySpan<byte> RESP_ERR_FLUSHALL_READONLY_REPLICA => "ERR You can't write against a read only replica."u8;
        public static ReadOnlySpan<byte> RESP_ERR_ZSET_MEMBER => "ERR could not decode requested zset member"u8;
        public static ReadOnlySpan<byte> RESP_ERR_EXPDELSCAN_INVALID => "ERR Cannot execute EXPDELSCAN with background expired key deletion scan enabled"u8;
        public static ReadOnlySpan<byte> RESP_ERR_CHECKPOINT_ALREADY_IN_PROGRESS => "ERR checkpoint already in progress"u8;

        /// <summary>
        /// Response string templates
        /// </summary>
        public const string GenericErrWrongNumArgs = "ERR wrong number of arguments for '{0}' command";
        public const string GenericErrUnknownOptionConfigSet = "ERR Unknown option or number of arguments for CONFIG SET - '{0}'";
        public const string GenericErrUnknownOption = "ERR Unknown option or number of arguments for '{0}' command";
        public const string GenericErrUnsupportedOption = "ERR Unsupported option {0}";
        public const string GenericErrUnknownSubCommand = "ERR unknown subcommand '{0}'. Try {1} HELP";
        public const string GenericErrUnknownSubCommandNoHelp = "ERR unknown subcommand '{0}'.";
        public const string GenericErrUnknownSubCommandOrWrongNumberOfArguments = "ERR unknown subcommand or wrong number of arguments for '{0}'. Try {1} HELP";
        public const string GenericErrWrongNumArgsTxn =
            "ERR Invalid number of parameters to stored proc {0}, expected {1}, actual {2}";
        public const string GenericSyntaxErrorOption = "ERR Syntax error in {0} option '{1}'";
        public const string GenericParamShouldBeGreaterThanZero = "ERR Parameter `{0}` should be greater than 0";
        public const string GenericErrNotAFloat = "ERR {0} value is not a valid float";
        public const string GenericErrCantBeNegative = "ERR {0} can't be negative";
        public const string GenericErrAtLeastOneKey = "ERR at least 1 input key is needed for '{0}' command";
        public const string GenericErrShouldBeGreaterThanZero = "ERR {0} should be greater than 0";
        public const string GenericErrMandatoryMissing = "Mandatory argument {0} is missing or not at the right position";
        public const string GenericErrMustMatchNoOfArgs = "The `{0}` parameter must match the number of arguments";
        public const string GenericUnknownClientType = "ERR Unknown client type '{0}'";
        public const string GenericErrDuplicateFilter = "ERR Filter '{0}' defined multiple times";
        public const string GenericPubSubCommandDisabled = "ERR {0} is disabled, enable it with --pubsub option.";
        public const string GenericErrLonLat = "ERR invalid longitude,latitude pair {0:F6},{1:F6}";
        public const string GenericErrStoreCommand = "ERR STORE option in {0} is not compatible with WITHDIST, WITHHASH and WITHCOORD options";
        public const string GenericErrCommandDisallowedWithOption =
            @"ERR {0} command not allowed. If the {1} option is set to ""local"", you can run it from a local connection, otherwise you need to set this option in the configuration file, and then restart the server.";
        public const string GenericErrIncorrectSizeFormat = "ERR Incorrect size format in (option: '{0}')";
        public const string GenericErrIndexSizePowerOfTwo = "ERR Index size must be a power of 2 (option: '{0}')";
        public const string GenericErrIndexSizeAutoGrow = "ERR Cannot adjust index size when auto-grow task is running (option: '{0}')";
        public const string GenericErrIndexSizeSmallerThanCurrent = "ERR Cannot set dynamic index size smaller than current index size (option: '{0}')";
        public const string GenericErrIndexSizeGrowFailed = "ERR failed to grow index size beyond current size (option: '{0}')";
        public const string GenericErrMemorySizeGreaterThanBuffer = "ERR Cannot set dynamic memory size greater than configured circular buffer size (option: '{0}')";
        public const string GenericErrHeapMemorySizeTrackerNotRunning = "ERR Cannot adjust object store heap memory size when size tracker is not running (option: '{0}')";

        /// <summary>
        /// Response errors while scripting
        /// </summary>
        public static ReadOnlySpan<byte> RESP_ERR => "ERR server while running script"u8;
        public static ReadOnlySpan<byte> RESP_SERVER_BUSY => "ERR Error server busy"u8;

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
        public static ReadOnlySpan<byte> none => "none"u8;

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
        public static ReadOnlySpan<byte> EXISTS => "EXISTS"u8;
        public static ReadOnlySpan<byte> FLUSH => "FLUSH"u8;
        public static ReadOnlySpan<byte> GENPASS => "GENPASS"u8;
        public static ReadOnlySpan<byte> GETUSER => "GETUSER"u8;
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
        public static ReadOnlySpan<byte> ID => "ID"u8;
        public static ReadOnlySpan<byte> KILL => "KILL"u8;
        public static ReadOnlySpan<byte> GETNAME => "GETNAME"u8;
        public static ReadOnlySpan<byte> SETINFO => "SETINFO"u8;
        public static ReadOnlySpan<byte> UNBLOCK => "UNBLOCK"u8;
        public static ReadOnlySpan<byte> USER => "USER"u8;
        public static ReadOnlySpan<byte> ADDR => "ADDR"u8;
        public static ReadOnlySpan<byte> LADDR => "LADDR"u8;
        public static ReadOnlySpan<byte> SKIPME => "SKIPME"u8;
        public static ReadOnlySpan<byte> MAXAGE => "MAXAGE"u8;
        public static ReadOnlySpan<byte> YES => "YES"u8;
        public static ReadOnlySpan<byte> NO => "NO"u8;

        // Cluster subcommands which are internal and thus undocumented
        // 
        // Because these are internal, they have lower case property names
        public static ReadOnlySpan<byte> gossip => "GOSSIP"u8;
        public static ReadOnlySpan<byte> myparentid => "MYPARENTID"u8;
        public static ReadOnlySpan<byte> delkeysinslot => "DELKEYSINSLOT"u8;
        public static ReadOnlySpan<byte> delkeysinslotrange => "DELKEYSINSLOTRANGE"u8;
        public static ReadOnlySpan<byte> setslotsrange => "SETSLOTSRANGE"u8;
        public static ReadOnlySpan<byte> slotstate => "SLOTSTATE"u8;
        public static ReadOnlySpan<byte> publish => "PUBLISH"u8;
        public static ReadOnlySpan<byte> spublish => "SPUBLISH"u8;
        public static ReadOnlySpan<byte> mtasks => "MTASKS"u8;
        public static ReadOnlySpan<byte> appendlog => "APPENDLOG"u8;
        public static ReadOnlySpan<byte> attach_sync => "ATTACH_SYNC"u8;
        public static ReadOnlySpan<byte> banlist => "BANLIST"u8;
        public static ReadOnlySpan<byte> begin_replica_recover => "BEGIN_REPLICA_RECOVER"u8;
        public static ReadOnlySpan<byte> endpoint => "ENDPOINT"u8;
        public static ReadOnlySpan<byte> failreplicationoffset => "FAILREPLICATIONOFFSET"u8;
        public static ReadOnlySpan<byte> failstopwrites => "FAILSTOPWRITES"u8;
        public static ReadOnlySpan<byte> initiate_replica_sync => "INITIATE_REPLICA_SYNC"u8;
        public static ReadOnlySpan<byte> send_ckpt_file_segment => "SEND_CKPT_FILE_SEGMENT"u8;
        public static ReadOnlySpan<byte> send_ckpt_metadata => "SEND_CKPT_METADATA"u8;
        public static ReadOnlySpan<byte> cluster_sync => "SYNC"u8;
        public static ReadOnlySpan<byte> cluster_advance_time => "ADVANCE_TIME"u8;

        // Lua scripting strings
        public static ReadOnlySpan<byte> LUA_OK => "OK"u8;
        public static ReadOnlySpan<byte> LUA_ok => "ok"u8;
        public static ReadOnlySpan<byte> LUA_err => "err"u8;
        public static ReadOnlySpan<byte> LUA_No_session_available => "No session available"u8;
        public static ReadOnlySpan<byte> LUA_ERR_Please_specify_at_least_one_argument_for_this_redis_lib_call => "ERR Please specify at least one argument for this redis lib call"u8;
        public static ReadOnlySpan<byte> LUA_ERR_Unknown_Redis_command_called_from_script => "ERR Unknown Redis command called from script"u8;
        public static ReadOnlySpan<byte> LUA_ERR_Lua_redis_lib_command_arguments_must_be_strings_or_integers => "ERR Lua redis lib command arguments must be strings or integers"u8;
        public static ReadOnlySpan<byte> LUA_ERR_wrong_number_of_arguments => "ERR wrong number of arguments"u8;
        public static ReadOnlySpan<byte> LUA_ERR_redis_log_requires_two_arguments_or_more => "ERR redis.log() requires two arguments or more."u8;
        public static ReadOnlySpan<byte> LUA_ERR_First_argument_must_be_a_number_log_level => "ERR First argument must be a number (log level)."u8;
        public static ReadOnlySpan<byte> LUA_ERR_Invalid_debug_level => "ERR Invalid debug level."u8;
        public static ReadOnlySpan<byte> LUA_ERR_Invalid_command_passed_to_redis_acl_check_cmd => "ERR Invalid command passed to redis.acl_check_cmd()"u8;
        public static ReadOnlySpan<byte> LUA_ERR_redis_setresp_requires_one_argument => "ERR redis.setresp() requires one argument."u8;
        public static ReadOnlySpan<byte> LUA_ERR_RESP_version_must_be_2_or_3 => "ERR RESP version must be 2 or 3."u8;
        public static ReadOnlySpan<byte> LUA_ERR_redis_log_disabled => "ERR redis.log(...) disabled in Garnet config"u8;
        public static ReadOnlySpan<byte> LUA_double => "double"u8;
        public static ReadOnlySpan<byte> LUA_map => "map"u8;
        public static ReadOnlySpan<byte> Lua_set => "set"u8;
        public static ReadOnlySpan<byte> LUA_big_number => "big_number"u8;
        public static ReadOnlySpan<byte> LUA_format => "format"u8;
        public static ReadOnlySpan<byte> LUA_string => "string"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_atan2 => "bad argument to atan2"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_cosh => "bad argument to cosh"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_frexp => "bad argument to frexp"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_ldexp => "bad argument to ldexp"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_log10 => "bad argument to log10"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_pow => "bad argument to pow"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_sinh => "bad argument to sinh"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_tanh => "bad argument to tanh"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_maxn => "bad argument to maxn"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_loadstring => "bad argument to loadstring"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_loadstring_null_byte => "bad argument to loadstring, interior null byte"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_tobit => "bad argument to tobit"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_tohex => "bad argument to tohex"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_bswap => "bad argument to bswap"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_bnot => "bad argument to bnot"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_encode => "bad argument to encode"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_decode => "bad argument to decode"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_pack => "bad argument to pack"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_unpack => "bad argument to unpack"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_format => "bad argument to format"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_bor => "bad argument to bor"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_band => "bad argument to band"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_bxor => "bad argument to bxor"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_lshift => "bad argument to lshift"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_arshift => "bad argument to arshift"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_rshift => "bad argument to rshift"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_rol => "bad argument to rol"u8;
        public static ReadOnlySpan<byte> LUA_bad_arg_ror => "bad argument to ror"u8;
        public static ReadOnlySpan<byte> LUA_unexpected_json_value_kind => "Unexpected json value kind"u8;
        public static ReadOnlySpan<byte> LUA_cannot_serialise_to_json => "Cannot serialise Lua type to JSON"u8;
        public static ReadOnlySpan<byte> LUA_unexpected_error => "Unexpected Lua error"u8;
        public static ReadOnlySpan<byte> LUA_cannot_serialise_excessive_nesting => "Cannot serialise, excessive nesting (1001)"u8;
        public static ReadOnlySpan<byte> LUA_unable_to_format_number => "Unable to format number"u8;
        public static ReadOnlySpan<byte> LUA_found_too_many_nested => "Found too many nested data structures (1001)"u8;
        public static ReadOnlySpan<byte> LUA_expected_value_but_found_invalid => "Expected value but found invalid token."u8;
        public static ReadOnlySpan<byte> LUA_missing_bytes_in_input => "Missing bytes in input."u8;
        public static ReadOnlySpan<byte> LUA_unexpected_msgpack_sigil => "Unexpected MsgPack sigil"u8;
        public static ReadOnlySpan<byte> LUA_msgpack_string_too_long => "MsgPack string is too long"u8;
        public static ReadOnlySpan<byte> LUA_msgpack_array_too_long => "MsgPack array is too long"u8;
        public static ReadOnlySpan<byte> LUA_msgpack_map_too_long => "MsgPack map is too long"u8;
        public static ReadOnlySpan<byte> LUA_insufficient_lua_stack_space => "Insufficient Lua stack space"u8;
        public static ReadOnlySpan<byte> LUA_parameter_reset_failed_memory => "Resetting parameters to Lua script failed: Memory"u8;
        public static ReadOnlySpan<byte> LUA_parameter_reset_failed_syntax => "Resetting parameters to Lua script failed: Syntax"u8;
        public static ReadOnlySpan<byte> LUA_parameter_reset_failed_runtime => "Resetting parameters to Lua script failed: Runtime"u8;
        public static ReadOnlySpan<byte> LUA_parameter_reset_failed_other => "Resetting parameters to Lua script failed: Other"u8;
        public static ReadOnlySpan<byte> LUA_out_of_memory => "Lua VM ran out of memory"u8;
        public static ReadOnlySpan<byte> LUA_load_string_error => "load_string encountered error"u8;
        public static ReadOnlySpan<byte> LUA_AND => "AND"u8;
        public static ReadOnlySpan<byte> LUA_OR => "OR"u8;
        public static ReadOnlySpan<byte> LUA_XOR => "XOR"u8;
        public static ReadOnlySpan<byte> LUA_NOT => "NOT"u8;
        public static ReadOnlySpan<byte> LUA_DIFF => "DIFF"u8;
        public static ReadOnlySpan<byte> LUA_KEYS => "KEYS"u8;
        public static ReadOnlySpan<byte> LUA_ARGV => "ARGV"u8;
        public static ReadOnlySpan<byte> EXPDELSCAN => "EXPDELSCAN"u8;
    }
}