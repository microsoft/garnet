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
        public static ReadOnlySpan<byte> GetConfig(ReadOnlySpan<byte> key)
        {
            if (key.SequenceEqual("timeout"u8)) return "*2\r\n$7\r\ntimeout\r\n$1\r\n0\r\n"u8;
            else if (key.SequenceEqual("save"u8)) return "*2\r\n$4\r\nsave\r\n$0\r\n\r\n"u8;
            else if (key.SequenceEqual("appendonly"u8)) return "*2\r\n$10\r\nappendonly\r\n$2\r\nno\r\n"u8;
            else if (key.SequenceEqual("slave-read-only"u8)) return "$3\r\nyes\r\n"u8;
            else if (key.SequenceEqual("databases"u8)) return "$2\r\n16\r\n"u8;
            else return RESP_EMPTYLIST;
        }

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
        public static ReadOnlySpan<byte> INFO => "INFO"u8;
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
        public static ReadOnlySpan<byte> RESP_ERR => "-ERR unknown command\r\n"u8;
        public static ReadOnlySpan<byte> RESP_CLUSTER_DISABLED => "-ERR This instance has cluster support disabled\r\n"u8;
        public static ReadOnlySpan<byte> RESP_WRONG_ARGUMENTS => "-ERR wrong number of arguments for 'config|set' command\r\n"u8;
        public static ReadOnlySpan<byte> RESP_ERRNOTFOUND => "$-1\r\n"u8;
        public static ReadOnlySpan<byte> RESP_ERRNOSUCHKEY => "-ERR no such key\r\n"u8;
        public static ReadOnlySpan<byte> RESP_NOAUTH => "-NOAUTH Authentication required.\r\n"u8;
        public static ReadOnlySpan<byte> RESP_EMPTYLIST => "*0\r\n"u8;
        public static ReadOnlySpan<byte> RESP_RETURN_VAL_1 => ":1\r\n"u8;
        public static ReadOnlySpan<byte> RESP_RETURN_VAL_0 => ":0\r\n"u8;
        public static ReadOnlySpan<byte> RESP_RETURN_VAL_N1 => ":-1\r\n"u8;
        public static ReadOnlySpan<byte> RESP_PONG => "+PONG\r\n"u8;
        public static ReadOnlySpan<byte> RESP_HLL_TYPE_ERROR => "-WRONGTYPE Key is not a valid HyperLogLog string value.\r\n"u8;
        public static ReadOnlySpan<byte> RESP_EMPTY => "$0\r\n\r\n"u8;
        public static ReadOnlySpan<byte> RESP_NESTED_MULTI => "-ERR MULTI calls can not be nested\r\n"u8;
        public static ReadOnlySpan<byte> RESP_EXEC_WO_MULTI => "-ERR EXEC without MULTI\r\n"u8;
        public static ReadOnlySpan<byte> RESP_DISCARD_WO_MULTI => "-ERR DISCARD without MULTI\r\n"u8;
        public static ReadOnlySpan<byte> RESP_WATCH_IN_MULTI => "-ERR WATCH inside MULTI is not allowed\r\n"u8;
        public static ReadOnlySpan<byte> RESP_EXEC_ABORT => "-EXECABORT Transaction discarded because of previous errors.\r\n"u8;
        public static ReadOnlySpan<byte> RESP_QUEUED => "+QUEUED\r\n"u8;
        public static ReadOnlySpan<byte> RESP_ERRINVALIDEXP_IN_SET => "-ERR invalid expire time in 'set' command\r\n"u8;
        public static ReadOnlySpan<byte> RESP_SYNTAX_ERROR => "-ERR syntax error\r\n"u8;
        public static ReadOnlySpan<byte> RESP_ERROFFSETOUTOFRANGE => "-ERR offset is out of range\r\n"u8;
        public static ReadOnlySpan<byte> RESP_ERRORCURSORVALUE => "-ERR cursor value should be equal or greater than 0.\r\n"u8;
        public static ReadOnlySpan<byte> RESP_MALFORMED_REGISTERCS_COMMAND => "-ERR malformed REGISTERCS command.\r\n"u8;
        public static ReadOnlySpan<byte> RESP_ERROR_GETTING_BINARY_FILES => "-ERR unable to access one or more binary files.\r\n"u8;
        public static ReadOnlySpan<byte> RESP_ERROR_BINARY_FILES_NOT_IN_ALLOWED_PATHS => "-ERR one or more binary file are not contained in allowed paths.\r\n"u8;
        public static ReadOnlySpan<byte> RESP_ERROR_LOADING_ASSEMBLIES => "-ERR unable to load one or more assemblies.\r\n"u8;
        public static ReadOnlySpan<byte> RESP_ERROR_ASSEMBLY_NOT_SIGNED => "-ERR one or more assemblies loaded is not digitally signed.\r\n"u8;
        public static ReadOnlySpan<byte> RESP_ERROR_INSTANTIATING_CLASS => "-ERR unable to instantiate one or more classes from given assemblies.\r\n"u8;
        public static ReadOnlySpan<byte> RESP_ERROR_REGISTERCS_UNSUPPORTED_CLASS => "-ERR unable to register one or more unsupported classes.\r\n"u8;
        public static ReadOnlySpan<byte> RESP_ERROR_VALUE_IS_NOT_INTEGER => "-ERR value is not an integer or out of range.\r\n"u8;

        /// <summary>
        /// Response string templates
        /// </summary>
        public const string ErrMissingParam = "-ERR wrong number of arguments for '{0}' command\r\n";
        public const string UnknownOption = "Unknown option or number of arguments for CONFIG SET - '{0}'";

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