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
        public static ReadOnlySpan<byte> SECONDARYOF => "SLAVEOF"u8;
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

        /// <summary>
        /// Response strings
        /// </summary>
        public static ReadOnlySpan<byte> RESP_OK => "+OK\r\n"u8;
        public static ReadOnlySpan<byte> RESP_ERRNOTFOUND => "$-1\r\n"u8;
        public static ReadOnlySpan<byte> RESP_EMPTYLIST => "*0\r\n"u8;
        public static ReadOnlySpan<byte> RESP_RETURN_VAL_1 => ":1\r\n"u8;
        public static ReadOnlySpan<byte> RESP_RETURN_VAL_0 => ":0\r\n"u8;
        public static ReadOnlySpan<byte> RESP_RETURN_VAL_N1 => ":-1\r\n"u8;
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
        /// Generic error respone strings, i.e. these are of the form "-ERR error message\r\n"
        /// </summary>
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_UNK_CMD => "ERR unknown command"u8;
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
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_CURSORVALUE => "ERR cursor value should be equal or greater than 0."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_MALFORMED_REGISTERCS_COMMAND => "ERR malformed REGISTERCS command."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_GETTING_BINARY_FILES => "ERR unable to access one or more binary files."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_BINARY_FILES_NOT_IN_ALLOWED_PATHS => "ERR one or more binary file are not contained in allowed paths."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_LOADING_ASSEMBLIES => "ERR unable to load one or more assemblies."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_ASSEMBLY_NOT_SIGNED => "ERR one or more assemblies loaded is not digitally signed."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_INSTANTIATING_CLASS => "ERR unable to instantiate one or more classes from given assemblies."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_REGISTERCS_UNSUPPORTED_CLASS => "ERR unable to register one or more unsupported classes."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER => "ERR value is not an integer or out of range."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_UKNOWN_SUBCOMMAND => "ERR Unknown subcommand. Try LATENCY HELP."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_INDEX_OUT_RANGE => "ERR index out of range"u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_SELECT_INVALID_INDEX => "ERR invalid database index."u8;
        public static ReadOnlySpan<byte> RESP_ERR_GENERIC_SELECT_CLUSTER_MODE => "ERR SELECT is not allowed in cluster mode"u8;
        public static ReadOnlySpan<byte> RESP_ERR_UNSUPPORTED_PROTOCOL_VERSION => "ERR Unsupported protocol version"u8;
        public static ReadOnlySpan<byte> RESP_WRONGPASS_INVALID_PASSWORD => "WRONGPASS Invalid password"u8;
        public static ReadOnlySpan<byte> RESP_WRONGPASS_INVALID_USERNAME_PASSWORD => "WRONGPASS Invalid username/password combination"u8;
        public static ReadOnlySpan<byte> RESP_SYNTAX_ERROR => "syntax error"u8;


        /// <summary>
        /// Response string templates
        /// </summary>
        public const string GenericErrWrongNumArgs = "ERR wrong number of arguments for '{0}' command";
        public const string GenericErrUnknownOption = "ERR Unknown option or number of arguments for CONFIG SET - '{0}'";
        public const string GenericErrUnknownSubCommand = "ERR unknown subcommand '{0}'. Try {1} HELP";

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
    }
}