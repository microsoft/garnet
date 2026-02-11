// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Basic RESP command enum
    /// </summary>
    public enum RespCommand : ushort
    {
        NONE = 0x00,

        // Read-only commands. NOTE: Should immediately follow NONE.
        BITCOUNT,
        BITFIELD_RO,
        BITPOS,
        COSCAN,
        DBSIZE,
        DUMP,
        EXISTS,
        EXPIRETIME,
        GEODIST,
        GEOHASH,
        GEOPOS,
        GEORADIUS_RO,
        GEORADIUSBYMEMBER_RO,
        GEOSEARCH,
        GET,
        GETBIT,
        GETETAG,
        GETRANGE,
        HEXISTS,
        HGET,
        HGETALL,
        HKEYS,
        HLEN,
        HMGET,
        HRANDFIELD,
        HSCAN,
        HSTRLEN,
        HVALS,
        KEYS,
        LCS,
        HTTL,
        HPTTL,
        HEXPIRETIME,
        HPEXPIRETIME,
        LINDEX,
        LLEN,
        LPOS,
        LRANGE,
        MEMORY_USAGE,
        MGET,
        PEXPIRETIME,
        PFCOUNT,
        PTTL,
        SCAN,
        SCARD,
        SDIFF,
        SINTER,
        SINTERCARD,
        SISMEMBER,
        SMEMBERS,
        SMISMEMBER,
        SPUBLISH,
        SRANDMEMBER,
        SSCAN,
        SSUBSCRIBE,
        STRLEN,
        SUBSTR,
        SUNION,
        TTL,
        TYPE,
        WATCH,
        WATCHMS,
        WATCHOS,
        ZCARD,
        ZCOUNT,
        ZDIFF,
        ZINTER,
        ZINTERCARD,
        ZLEXCOUNT,
        ZMSCORE,
        ZRANDMEMBER,
        ZRANGE,
        ZRANGEBYLEX,
        ZRANGEBYSCORE,
        ZRANK,
        ZREVRANGE,
        ZREVRANGEBYLEX,
        ZREVRANGEBYSCORE,
        ZREVRANK,
        ZTTL,
        ZPTTL,
        ZEXPIRETIME,
        ZPEXPIRETIME,
        ZSCAN,
        ZSCORE, // Note: Last read command should immediately precede FirstWriteCommand
        ZUNION,

        // Write commands
        APPEND, // Note: Update FirstWriteCommand if adding new write commands before this
        BITFIELD,
        BZMPOP,
        BZPOPMAX,
        BZPOPMIN,
        DECR,
        DECRBY,
        DEL,
        DELIFEXPIM,
        EXPIRE,
        EXPIREAT,
        FLUSHALL,
        FLUSHDB,
        GEOADD,
        GEORADIUS,
        GEORADIUSBYMEMBER,
        GEOSEARCHSTORE,
        GETDEL,
        GETEX,
        GETSET,
        HCOLLECT,
        HDEL,
        HEXPIRE,
        HPEXPIRE,
        HEXPIREAT,
        HPEXPIREAT,
        HPERSIST,
        HINCRBY,
        HINCRBYFLOAT,
        HMSET,
        HSET,
        HSETNX,
        INCR,
        INCRBY,
        INCRBYFLOAT,
        LINSERT,
        LMOVE,
        LMPOP,
        LPOP,
        LPUSH,
        LPUSHX,
        LREM,
        LSET,
        LTRIM,
        BLPOP,
        BRPOP,
        BLMOVE,
        BRPOPLPUSH,
        BLMPOP,
        MIGRATE,
        MSET,
        MSETNX,
        PERSIST,
        PEXPIRE,
        PEXPIREAT,
        PFADD,
        PFMERGE,
        PSETEX,
        RENAME,
        RESTORE,
        RENAMENX,
        RPOP,
        RPOPLPUSH,
        RPUSH,
        RPUSHX,
        SADD,
        SDIFFSTORE,
        SET,
        SETBIT,
        SETEX,
        SETEXNX,
        SETEXXX,
        SETNX,
        SETKEEPTTL,
        SETKEEPTTLXX,
        SETRANGE,
        SINTERSTORE,
        SMOVE,
        SPOP,
        SREM,
        SUNIONSTORE,
        SWAPDB,
        UNLINK,
        ZADD,
        ZCOLLECT,
        ZDIFFSTORE,
        ZEXPIRE,
        ZPEXPIRE,
        ZEXPIREAT,
        ZPEXPIREAT,
        ZPERSIST,
        ZINCRBY,
        ZMPOP,
        ZINTERSTORE,
        ZPOPMAX,
        ZPOPMIN,
        ZRANGESTORE,
        ZREM,
        ZREMRANGEBYLEX,
        ZREMRANGEBYRANK,
        ZREMRANGEBYSCORE,
        ZUNIONSTORE,

        // BITOP is the true command, AND|OR|XOR|NOT are pseudo-subcommands
        BITOP,
        BITOP_AND,
        BITOP_OR,
        BITOP_XOR,
        BITOP_NOT,
        BITOP_DIFF, // Note: Update LastWriteCommand if adding new write commands after this

        // Script execution commands
        EVAL,
        EVALSHA, // Note: Update LastDataCommand if adding new data commands after this

        // Neither read nor write key commands
        ASYNC,

        PING,

        // Pub/Sub commands
        PUBSUB,
        PUBSUB_CHANNELS,
        PUBSUB_NUMPAT,
        PUBSUB_NUMSUB,
        PUBLISH,
        SUBSCRIBE,
        PSUBSCRIBE,
        UNSUBSCRIBE,
        PUNSUBSCRIBE,

        ASKING,
        SELECT,
        ECHO,

        CLIENT,
        CLIENT_ID,
        CLIENT_INFO,
        CLIENT_LIST,
        CLIENT_KILL,
        CLIENT_GETNAME,
        CLIENT_SETNAME,
        CLIENT_SETINFO,
        CLIENT_UNBLOCK,

        MONITOR,
        MODULE,
        MODULE_LOADCS,
        REGISTERCS,

        MULTI,
        EXEC,
        DISCARD,
        UNWATCH,
        RUNTXP,

        READONLY,
        READWRITE,
        REPLICAOF,
        SECONDARYOF,

        INFO,
        TIME,
        ROLE,
        SAVE,
        EXPDELSCAN,
        LASTSAVE,
        BGSAVE,
        COMMITAOF,
        FORCEGC,
        PURGEBP,
        FAILOVER,

        // Custom commands
        CustomTxn,
        CustomRawStringCmd,
        CustomObjCmd,
        CustomProcedure,

        // Script commands
        SCRIPT,
        SCRIPT_EXISTS,
        SCRIPT_FLUSH,
        SCRIPT_LOAD,

        ACL,
        ACL_CAT,
        ACL_DELUSER,
        ACL_GENPASS,
        ACL_GETUSER,
        ACL_LIST,
        ACL_LOAD,
        ACL_SAVE,
        ACL_SETUSER,
        ACL_USERS,
        ACL_WHOAMI,

        COMMAND,
        COMMAND_COUNT,
        COMMAND_DOCS,
        COMMAND_INFO,
        COMMAND_GETKEYS,
        COMMAND_GETKEYSANDFLAGS,

        MEMORY,
        // MEMORY_USAGE is a read-only command, so moved up

        CONFIG,
        CONFIG_GET,
        CONFIG_REWRITE,
        CONFIG_SET,

        DEBUG,

        LATENCY,
        LATENCY_HELP,
        LATENCY_HISTOGRAM,
        LATENCY_RESET,

        // SLOWLOG commands
        SLOWLOG,
        SLOWLOG_HELP,
        SLOWLOG_LEN,
        SLOWLOG_GET,
        SLOWLOG_RESET,

        CLUSTER,
        CLUSTER_ADDSLOTS, // Note: Update IsClusterSubCommand if adding new cluster subcommands before this
        CLUSTER_ADDSLOTSRANGE,
        CLUSTER_AOFSYNC,
        CLUSTER_APPENDLOG,
        CLUSTER_ATTACH_SYNC,
        CLUSTER_BANLIST,
        CLUSTER_BEGIN_REPLICA_RECOVER,
        CLUSTER_BUMPEPOCH,
        CLUSTER_COUNTKEYSINSLOT,
        CLUSTER_DELKEYSINSLOT,
        CLUSTER_DELKEYSINSLOTRANGE,
        CLUSTER_DELSLOTS,
        CLUSTER_DELSLOTSRANGE,
        CLUSTER_ENDPOINT,
        CLUSTER_FAILOVER,
        CLUSTER_FAILREPLICATIONOFFSET,
        CLUSTER_FAILSTOPWRITES,
        CLUSTER_FLUSHALL,
        CLUSTER_FORGET,
        CLUSTER_GETKEYSINSLOT,
        CLUSTER_GOSSIP,
        CLUSTER_HELP,
        CLUSTER_INFO,
        CLUSTER_INITIATE_REPLICA_SYNC,
        CLUSTER_KEYSLOT,
        CLUSTER_MEET,
        CLUSTER_MIGRATE,
        CLUSTER_MTASKS,
        CLUSTER_MYID,
        CLUSTER_MYPARENTID,
        CLUSTER_NODES,
        CLUSTER_PUBLISH,
        CLUSTER_SPUBLISH,
        CLUSTER_REPLICAS,
        CLUSTER_REPLICATE,
        CLUSTER_RESET,
        CLUSTER_SEND_CKPT_FILE_SEGMENT,
        CLUSTER_SEND_CKPT_METADATA,
        CLUSTER_SETCONFIGEPOCH,
        CLUSTER_SETSLOT,
        CLUSTER_SETSLOTSRANGE,
        CLUSTER_SHARDS,
        CLUSTER_SLOTS,
        CLUSTER_SLOTSTATE,
        CLUSTER_SYNC, // Note: Update IsClusterSubCommand if adding new cluster subcommands after this

        // Meta commands (commands that envelop other commands)
        EXECWITHETAG,
        EXECIFMATCH,
        EXECIFNOTMATCH,
        EXECIFGREATER, // Note: Update IsMetaCommand if adding new etag commands after this

        // Don't require AUTH (if auth is enabled)
        AUTH, // Note: Update IsNoAuth if adding new no-auth commands before this
        HELLO,
        QUIT, // Note: Update IsNoAuth if adding new no-auth commands after this

        // Max value of this enum (not including INVALID) will determine the size of RespCommand.AofIndependentBitLookup and CommandPermissionSet._commandList,
        // so avoid manually setting high values unless necessary

        INVALID = 0xFFFF,
    }

    /// <summary>
    /// Extension methods for <see cref="RespCommand"/>.
    /// </summary>
    public static class RespCommandExtensions
    {
        private static readonly RespCommand[] ExpandedSET = [RespCommand.SETEXNX, RespCommand.SETEXXX, RespCommand.SETKEEPTTL, RespCommand.SETKEEPTTLXX];
        private static readonly RespCommand[] ExpandedBITOP = [RespCommand.BITOP_AND, RespCommand.BITOP_NOT, RespCommand.BITOP_OR, RespCommand.BITOP_XOR, RespCommand.BITOP_DIFF];

        // Commands that are either returning static data or commands that cannot have issues from concurrent AOF interaction in another session
        private static readonly RespCommand[] AofIndependentCommands = [
            RespCommand.ASYNC,
            RespCommand.PING,
            RespCommand.SELECT,
            RespCommand.SWAPDB,
            RespCommand.ECHO,
            RespCommand.MONITOR,
            RespCommand.MODULE_LOADCS,
            RespCommand.REGISTERCS,
            RespCommand.INFO,
            RespCommand.TIME,
            RespCommand.LASTSAVE,
            // ACL
            RespCommand.ACL_CAT,
            RespCommand.ACL_DELUSER,
            RespCommand.ACL_GENPASS,
            RespCommand.ACL_GETUSER,
            RespCommand.ACL_LIST,
            RespCommand.ACL_LOAD,
            RespCommand.ACL_SAVE,
            RespCommand.ACL_SETUSER,
            RespCommand.ACL_USERS,
            RespCommand.ACL_WHOAMI,
            // Client
            RespCommand.CLIENT_ID,
            RespCommand.CLIENT_INFO,
            RespCommand.CLIENT_LIST,
            RespCommand.CLIENT_KILL,
            RespCommand.CLIENT_GETNAME,
            RespCommand.CLIENT_SETNAME,
            RespCommand.CLIENT_SETINFO,
            RespCommand.CLIENT_UNBLOCK,
            // Command
            RespCommand.COMMAND,
            RespCommand.COMMAND_COUNT,
            RespCommand.COMMAND_DOCS,
            RespCommand.COMMAND_INFO,
            RespCommand.COMMAND_GETKEYS,
            RespCommand.COMMAND_GETKEYSANDFLAGS,
            RespCommand.MEMORY_USAGE,
            // Config
            RespCommand.CONFIG_GET,
            RespCommand.CONFIG_REWRITE,
            RespCommand.CONFIG_SET,
            // Latency
            RespCommand.LATENCY_HELP,
            RespCommand.LATENCY_HISTOGRAM,
            RespCommand.LATENCY_RESET,
            // Slowlog
            RespCommand.SLOWLOG_HELP,
            RespCommand.SLOWLOG_LEN,
            RespCommand.SLOWLOG_GET,
            RespCommand.SLOWLOG_RESET,
            // Transactions
            RespCommand.MULTI,
        ];

        private static readonly ulong[] AofIndependentBitLookup;

        private const int sizeOfLong = 64;

        // The static ctor maybe expensive but it is only ever run once, and doesn't interfere with common path
        static RespCommandExtensions()
        {
            // # of bits needed to represent all valid commands
            var maxBitsNeeded = (ushort)LastValidCommand + 1;
            var lookupTableSize = (maxBitsNeeded / 64) + (maxBitsNeeded % 64 == 0 ? 0 : 1);
            AofIndependentBitLookup = new ulong[lookupTableSize];

            foreach (var cmd in Enum.GetValues<RespCommand>())
            {
                if (Array.IndexOf(AofIndependentCommands, cmd) == -1)
                    continue;

                // mark the command as an AOF independent command for lookups later by setting the bit in bit vec
                int bitIdxToUse = (int)cmd / sizeOfLong;
                // set the respCommand's bit to indicate
                int bitIdxOffset = (int)cmd % sizeOfLong;
                ulong bitmask = 1UL << bitIdxOffset;
                AofIndependentBitLookup[bitIdxToUse] |= bitmask;
            }
        }

        /// <summary>
        /// Returns whether or not a Resp command can have a dirty read or is dependent on AOF or not
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsAofIndependent(this RespCommand cmd)
        {
            if (cmd > LastValidCommand) return false;

            // check if cmd maps to a bit vec that was set back when static ctor was run
            int bitIdxToUse = (int)cmd / sizeOfLong;
            int bitIdxOffset = (int)cmd % sizeOfLong;
            ulong bitmask = 1UL << bitIdxOffset;
            return (AofIndependentBitLookup[bitIdxToUse] & bitmask) != 0;
        }

        /// <summary>
        /// Turns any not-quite-a-real-command entries in <see cref="RespCommand"/> into the equivalent command
        /// for ACL'ing purposes and reading command info purposes
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static RespCommand NormalizeForACLs(this RespCommand cmd)
        {
            return
                cmd switch
                {
                    RespCommand.SETEXNX => RespCommand.SET,
                    RespCommand.SETEXXX => RespCommand.SET,
                    RespCommand.SETKEEPTTL => RespCommand.SET,
                    RespCommand.SETKEEPTTLXX => RespCommand.SET,
                    RespCommand.BITOP_AND or RespCommand.BITOP_NOT or RespCommand.BITOP_OR or RespCommand.BITOP_XOR or RespCommand.BITOP_DIFF => RespCommand.BITOP,
                    _ => cmd
                };
        }

        /// <summary>
        /// Reverses <see cref="NormalizeForACLs(RespCommand)"/>, producing all the equivalent <see cref="RespCommand"/>s which are covered by <paramref name="cmd"/>.
        /// </summary>
        public static ReadOnlySpan<RespCommand> ExpandForACLs(this RespCommand cmd)
        {
            return
                cmd switch
                {
                    RespCommand.SET => ExpandedSET,
                    RespCommand.BITOP => ExpandedBITOP,
                    _ => default
                };
        }

        internal const RespCommand FirstReadCommand = RespCommand.NONE + 1;

        internal const RespCommand LastReadCommand = RespCommand.APPEND - 1;

        internal const RespCommand FirstWriteCommand = RespCommand.APPEND;

        internal const RespCommand LastWriteCommand = RespCommand.BITOP_DIFF;

        internal const RespCommand LastDataCommand = RespCommand.EVALSHA;

        /// <summary>
        /// Last valid command (i.e. RespCommand with the largest value excluding INVALID).
        /// </summary>
        public static RespCommand LastValidCommand { get; } = Enum.GetValues<RespCommand>().Where(cmd => cmd != RespCommand.INVALID).Max();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsReadOnly(this RespCommand cmd)
            => cmd <= LastReadCommand;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsDataCommand(this RespCommand cmd)
        {
            return cmd switch
            {
                // TODO: validate if these cases need to be excluded
                RespCommand.MIGRATE => false,
                RespCommand.DBSIZE => false,
                RespCommand.MEMORY_USAGE => false,
                RespCommand.FLUSHDB => false,
                RespCommand.FLUSHALL => false,
                RespCommand.KEYS => false,
                RespCommand.SCAN => false,
                RespCommand.SWAPDB => false,
                _ => cmd >= FirstReadCommand && cmd <= LastDataCommand
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsMultiKeyCommand(this RespCommand cmd)
        {
            return cmd switch
            {
                RespCommand.EXISTS or
                RespCommand.MGET or
                RespCommand.PFCOUNT or
                RespCommand.SDIFF or
                RespCommand.SINTER or
                RespCommand.SINTERCARD or
                RespCommand.SSUBSCRIBE or
                RespCommand.SUNION or
                RespCommand.WATCH or
                RespCommand.WATCHMS or
                RespCommand.WATCHOS or
                RespCommand.ZDIFF or
                RespCommand.ZINTER or
                RespCommand.ZINTERCARD or
                RespCommand.ZUNION or
                RespCommand.BZMPOP or
                RespCommand.BZPOPMAX or
                RespCommand.BZPOPMIN or
                RespCommand.DEL or
                RespCommand.GEORADIUS or
                RespCommand.GEORADIUSBYMEMBER or
                RespCommand.GEOSEARCHSTORE or
                RespCommand.HCOLLECT or
                RespCommand.LMOVE or
                RespCommand.LMPOP or
                RespCommand.BLPOP or
                RespCommand.BRPOP or
                RespCommand.BLMOVE or
                RespCommand.BRPOPLPUSH or
                RespCommand.BLMPOP or
                RespCommand.MSET or
                RespCommand.MSETNX or
                RespCommand.PFMERGE or
                RespCommand.RENAME or
                RespCommand.RENAMENX or
                RespCommand.RPOPLPUSH or
                RespCommand.SDIFFSTORE or
                RespCommand.SINTERSTORE or
                RespCommand.SMOVE or
                RespCommand.SUNIONSTORE or
                RespCommand.UNLINK or
                RespCommand.ZCOLLECT or
                RespCommand.ZDIFFSTORE or
                RespCommand.ZMPOP or
                RespCommand.ZINTERSTORE or
                RespCommand.ZRANGESTORE or
                RespCommand.ZUNIONSTORE or
                RespCommand.BITOP or
                RespCommand.EVAL or
                RespCommand.EVALSHA => true,
                _ => false
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsWriteOnly(this RespCommand cmd)
        {
            // If cmd < RespCommand.Append - underflows, setting high bits
            var test = (uint)((int)cmd - (int)FirstWriteCommand);

            // Force to be branchless for same reasons as OneIfRead
            return test <= (LastWriteCommand - FirstWriteCommand);
        }

        /// <summary>
        /// Returns 1 if <paramref name="cmd"/> is a write command.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong OneIfWrite(this RespCommand cmd)
        {
            var inRange = cmd.IsWriteOnly();
            return Unsafe.As<bool, byte>(ref inRange);
        }

        /// <summary>
        /// Returns 1 if <paramref name="cmd"/> is a read command.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong OneIfRead(this RespCommand cmd)
        {
            // Force this to be branchless (as predictability is poor)
            // and we're in the hot path
            var inRange = cmd.IsReadOnly();
            return Unsafe.As<bool, byte>(ref inRange);
        }

        /// <summary>
        /// Returns true if <paramref name="cmd"/> can be run even if the user is not authenticated.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsNoAuth(this RespCommand cmd)
        {
            // If cmd < RespCommand.Auth - underflows, setting high bits
            uint test = (uint)((int)cmd - (int)RespCommand.AUTH);
            bool inRange = test <= (RespCommand.QUIT - RespCommand.AUTH);
            return inRange;
        }

        /// <summary>
        /// Returns true if <paramref name="cmd"/> can is a cluster subcommand.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsClusterSubCommand(this RespCommand cmd)
        {
            // If cmd < RespCommand.CLUSTER_ADDSLOTS - underflows, setting high bits
            uint test = (uint)((int)cmd - (int)RespCommand.CLUSTER_ADDSLOTS);
            bool inRange = test <= (RespCommand.CLUSTER_SYNC - RespCommand.CLUSTER_ADDSLOTS);
            return inRange;
        }

        /// <summary>
        /// Returns true if <paramref name="cmd"/> is a meta command.
        /// </summary>
        public static bool IsMetaCommand(this RespCommand cmd)
        {
            // If cmd < RespCommand.EXECWITHETAG - underflows, setting high bits
            var test = (uint)((int)cmd - (int)RespCommand.EXECWITHETAG);
            return test <= (RespCommand.EXECIFGREATER - RespCommand.EXECWITHETAG);
        }

        /// <summary>
        /// Returns true if <paramref name="cmd"/> is a write command that only modifies record metadata.
        /// </summary>
        public static bool IsMetadataCommand(this RespCommand cmd)
        {
            if (!IsDataCommand(cmd) || IsReadOnly(cmd)) 
                return false;

            return cmd switch
            {
                RespCommand.EXPIRE or 
                RespCommand.EXPIREAT or 
                RespCommand.PEXPIRE or 
                RespCommand.PEXPIREAT or 
                RespCommand.ZEXPIRE or 
                RespCommand.ZEXPIREAT or
                RespCommand.ZPEXPIRE or 
                RespCommand.ZPEXPIREAT or
                RespCommand.HEXPIRE or 
                RespCommand.HEXPIREAT or 
                RespCommand.HPEXPIRE or
                RespCommand.HPEXPIREAT or 
                RespCommand.PERSIST or
                RespCommand.ZPERSIST or
                RespCommand.HPERSIST or 
                RespCommand.ZCOLLECT or 
                RespCommand.HCOLLECT => true,
                _ => false
            };
        }
    }

    /// <summary>
    /// RESP command options enum
    /// </summary>
    enum RespCommandOption : byte
    {
        EX, NX, XX, GET, PX, EXAT, PXAT, PERSIST, GT, LT
    }

    /// <summary>
    /// Server session for RESP protocol - command definitions and fast parsing
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Fast-parses command type for inline RESP commands, starting at the current read head in the receive buffer
        /// and advances read head.
        /// </summary>
        /// <param name="count">Outputs the number of arguments stored with the command.</param>
        /// <returns>RespCommand that was parsed or RespCommand.NONE, if no command was matched in this pass.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private RespCommand FastParseInlineCommand(ref int count)
        {
            byte* ptr = recvBufferPtr + readHead;

            if (bytesRead - readHead >= 6)
            {
                if ((*(ushort*)(ptr + 4) == MemoryMarshal.Read<ushort>("\r\n"u8)))
                {
                    // Optimistically increase read head
                    readHead += 6;

                    if ((*(uint*)ptr) == MemoryMarshal.Read<uint>("PING"u8))
                    {
                        count = 0;
                        return RespCommand.PING;
                    }

                    if ((*(uint*)ptr) == MemoryMarshal.Read<uint>("QUIT"u8))
                    {
                        count = 0;
                        return RespCommand.QUIT;
                    }

                    // Decrease read head, if no match was found
                    readHead -= 6;
                }
            }

            return RespCommand.NONE;
        }

        /// <summary>
        /// Fast-parses for command type, starting at the current read head in the receive buffer
        /// and advances the read head to the position after the parsed command.
        /// </summary>
        /// <param name="count">Outputs the number of arguments stored with the command</param>
        /// <returns>RespCommand that was parsed or RespCommand.NONE, if no command was matched in this pass.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private RespCommand FastParseCommand(ref int count)
        {
            var ptr = recvBufferPtr + readHead;
            var remainingBytes = bytesRead - readHead;
            var skipCountParse = count != -1;

            // Check if the package starts with "*_\r\n$_\r\n" (_ = masked out),
            // i.e. an array with a single-digit length and single-digit first string length.
            if ((!skipCountParse && (remainingBytes >= 8) && (*(ulong*)ptr & 0xFFFF00FFFFFF00FF) == MemoryMarshal.Read<ulong>("*\0\r\n$\0\r\n"u8)) ||
                (skipCountParse && ((remainingBytes >= 4) && (*(uint*)ptr & 0xFFFF00FF) == MemoryMarshal.Read<uint>("$\0\r\n"u8))))
            {
                // Extract total element count from the array header.
                // NOTE: Subtracting one to account for first token being parsed.
                count = !skipCountParse ? ptr[1] - '1' : count - 1;

                // Extract length of the first string header
                var lengthIdx = skipCountParse ? 1 : 5;
                var length = ptr[lengthIdx] - '0';
                Debug.Assert(length is > 0 and <= 9);

                var oldReadHead = readHead;

                // Ensure that the complete command string is contained in the package. Otherwise exit early.
                // Include 10 bytes to account for array and command string headers, and terminator
                // 10 bytes = "*_\r\n$_\r\n" (8 bytes) + "\r\n" (2 bytes) at end of command name
                var lenWithoutCommand = skipCountParse ? 6 : 10;
                var totalLen = length + lenWithoutCommand;
                if (remainingBytes >= totalLen)
                {
                    // Optimistically advance read head to the end of the command name
                    readHead += totalLen;

                    // Last 8 byte word of the command name, for quick comparison
                    var lastWord = *(ulong*)(ptr + length + (lenWithoutCommand - 8));

                    // Pointer to the start of the command
                    var commandStart = ptr + lenWithoutCommand - 2;

                    //
                    // Fast path for common commands with fixed numbers of arguments
                    //

                    // Only check against commands with the correct count and length.

                    return ((count << 4) | length) switch
                    {
                        // Commands without arguments
                        4 when lastWord == MemoryMarshal.Read<ulong>("\r\nPING\r\n"u8) => RespCommand.PING,
                        4 when lastWord == MemoryMarshal.Read<ulong>("\r\nEXEC\r\n"u8) => RespCommand.EXEC,
                        5 when lastWord == MemoryMarshal.Read<ulong>("\nMULTI\r\n"u8) => RespCommand.MULTI,
                        6 when lastWord == MemoryMarshal.Read<ulong>("ASKING\r\n"u8) => RespCommand.ASKING,
                        7 when lastWord == MemoryMarshal.Read<ulong>("ISCARD\r\n"u8) && ptr[8] == 'D' => RespCommand.DISCARD,
                        7 when lastWord == MemoryMarshal.Read<ulong>("NWATCH\r\n"u8) && ptr[8] == 'U' => RespCommand.UNWATCH,
                        8 when lastWord == MemoryMarshal.Read<ulong>("ADONLY\r\n"u8) && *(ushort*)(ptr + 8) == MemoryMarshal.Read<ushort>("RE"u8) => RespCommand.READONLY,
                        9 when lastWord == MemoryMarshal.Read<ulong>("DWRITE\r\n"u8) && *(uint*)(ptr + 8) == MemoryMarshal.Read<uint>("READ"u8) => RespCommand.READWRITE,

                        // Commands with fixed number of arguments
                        (1 << 4) | 3 when lastWord == MemoryMarshal.Read<ulong>("3\r\nGET\r\n"u8) => RespCommand.GET,
                        (1 << 4) | 3 when lastWord == MemoryMarshal.Read<ulong>("3\r\nDEL\r\n"u8) => RespCommand.DEL,
                        (1 << 4) | 3 when lastWord == MemoryMarshal.Read<ulong>("3\r\nTTL\r\n"u8) => RespCommand.TTL,
                        (1 << 4) | 4 when lastWord == MemoryMarshal.Read<ulong>("\r\nDUMP\r\n"u8) => RespCommand.DUMP,
                        (1 << 4) | 4 when lastWord == MemoryMarshal.Read<ulong>("\r\nINCR\r\n"u8) => RespCommand.INCR,
                        (1 << 4) | 4 when lastWord == MemoryMarshal.Read<ulong>("\r\nPTTL\r\n"u8) => RespCommand.PTTL,
                        (1 << 4) | 4 when lastWord == MemoryMarshal.Read<ulong>("\r\nDECR\r\n"u8) => RespCommand.DECR,
                        (1 << 4) | 4 when lastWord == MemoryMarshal.Read<ulong>("EXISTS\r\n"u8) => RespCommand.EXISTS,
                        (1 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("GETDEL\r\n"u8) => RespCommand.GETDEL,
                        (1 << 4) | 7 when lastWord == MemoryMarshal.Read<ulong>("ERSIST\r\n"u8) && *commandStart == 'P' => RespCommand.PERSIST,
                        (1 << 4) | 7 when lastWord == MemoryMarshal.Read<ulong>("PFCOUNT\r\n"u8) && ptr[8] == 'P' => RespCommand.PFCOUNT,
                        (2 << 4) | 3 when lastWord == MemoryMarshal.Read<ulong>("3\r\nSET\r\n"u8) => RespCommand.SET,
                        (2 << 4) | 5 when lastWord == MemoryMarshal.Read<ulong>("\nPFADD\r\n"u8) => RespCommand.PFADD,
                        (2 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("INCRBY\r\n"u8) => RespCommand.INCRBY,
                        (2 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("DECRBY\r\n"u8) => RespCommand.DECRBY,
                        (2 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("GETBIT\r\n"u8) => RespCommand.GETBIT,
                        (2 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("APPEND\r\n"u8) => RespCommand.APPEND,
                        (2 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("GETSET\r\n"u8) => RespCommand.GETSET,
                        (2 << 4) | 7 when lastWord == MemoryMarshal.Read<ulong>("UBLISH\r\n"u8) && ptr[8] == 'P' => RespCommand.PUBLISH,
                        (2 << 4) | 7 when lastWord == MemoryMarshal.Read<ulong>("FMERGE\r\n"u8) && ptr[8] == 'P' => RespCommand.PFMERGE,
                        (2 << 4) | 8 when lastWord == MemoryMarshal.Read<ulong>("UBLISH\r\n"u8) && *(ushort*)(ptr + 8) == MemoryMarshal.Read<ushort>("SP"u8) => RespCommand.SPUBLISH,
                        (2 << 4) | 5 when lastWord == MemoryMarshal.Read<ulong>("\nSETNX\r\n"u8) => RespCommand.SETNX,
                        (3 << 4) | 5 when lastWord == MemoryMarshal.Read<ulong>("\nSETEX\r\n"u8) => RespCommand.SETEX,
                        (3 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("PSETEX\r\n"u8) => RespCommand.PSETEX,
                        (3 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("SETBIT\r\n"u8) => RespCommand.SETBIT,
                        (3 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("SUBSTR\r\n"u8) => RespCommand.SUBSTR,
                        (3 << 4) | 7 when lastWord == MemoryMarshal.Read<ulong>("ESTORE\r\n"u8) && ptr[8] == 'R' => RespCommand.RESTORE,
                        (3 << 4) | 8 when lastWord == MemoryMarshal.Read<ulong>("TRANGE\r\n"u8) && *(ushort*)(ptr + 8) == MemoryMarshal.Read<ushort>("SE"u8) => RespCommand.SETRANGE,
                        (3 << 4) | 8 when lastWord == MemoryMarshal.Read<ulong>("TRANGE\r\n"u8) && *(ushort*)(ptr + 8) == MemoryMarshal.Read<ushort>("GE"u8) => RespCommand.GETRANGE,

                        _ => ((length << 4) | count) switch
                        {
                            // Commands with dynamic number of arguments
                            >= ((6 << 4) | 2) and <= ((6 << 4) | 3) when lastWord == MemoryMarshal.Read<ulong>("RENAME\r\n"u8) => RespCommand.RENAME,
                            >= ((8 << 4) | 2) and <= ((8 << 4) | 3) when lastWord == MemoryMarshal.Read<ulong>("NAMENX\r\n"u8) && *(ushort*)(ptr + 8) == MemoryMarshal.Read<ushort>("RE"u8) => RespCommand.RENAMENX,
                            >= ((3 << 4) | 3) and <= ((3 << 4) | 7) when lastWord == MemoryMarshal.Read<ulong>("3\r\nSET\r\n"u8) => RespCommand.SETEXNX,
                            >= ((5 << 4) | 1) and <= ((5 << 4) | 3) when lastWord == MemoryMarshal.Read<ulong>("\nGETEX\r\n"u8) => RespCommand.GETEX,
                            >= ((6 << 4) | 0) and <= ((6 << 4) | 9) when lastWord == MemoryMarshal.Read<ulong>("RUNTXP\r\n"u8) => RespCommand.RUNTXP,
                            >= ((6 << 4) | 2) and <= ((6 << 4) | 3) when lastWord == MemoryMarshal.Read<ulong>("EXPIRE\r\n"u8) => RespCommand.EXPIRE,
                            >= ((6 << 4) | 2) and <= ((6 << 4) | 5) when lastWord == MemoryMarshal.Read<ulong>("BITPOS\r\n"u8) => RespCommand.BITPOS,
                            >= ((7 << 4) | 2) and <= ((7 << 4) | 3) when lastWord == MemoryMarshal.Read<ulong>("EXPIRE\r\n"u8) && ptr[8] == 'P' => RespCommand.PEXPIRE,
                            >= ((8 << 4) | 1) and <= ((8 << 4) | 4) when lastWord == MemoryMarshal.Read<ulong>("TCOUNT\r\n"u8) && *(ushort*)(ptr + 8) == MemoryMarshal.Read<ushort>("BI"u8) => RespCommand.BITCOUNT,
                            _ => MatchedNone(this, oldReadHead)
                        }
                    };

                    [MethodImpl(MethodImplOptions.AggressiveInlining)]
                    static RespCommand MatchedNone(RespServerSession session, int oldReadHead)
                    {
                        // Backup the read head, if we didn't find a command and need to continue in the more expensive parsing loop
                        session.readHead = oldReadHead;

                        return RespCommand.NONE;
                    }
                }
            }
            else
            {
                count = !skipCountParse ? 0 : count - 1;
                return FastParseInlineCommand(ref count);
            }

            // Couldn't find a matching command in this pass
            count = -1;
            return RespCommand.NONE;
        }

        /// <summary>
        /// Fast parsing function for common command names.
        /// Parses the receive buffer starting from the current read head and advances it to the end of
        /// the parsed command/subcommand name.
        /// </summary>
        /// <param name="count">Reference to the number of remaining tokens in the packet. Will be reduced to number of command arguments.</param>
        /// <returns>The parsed command name.</returns>
        private RespCommand FastParseArrayCommand(ref int count, ref ReadOnlySpan<byte> specificErrorMessage)
        {
            // Bytes remaining in the read buffer
            int remainingBytes = bytesRead - readHead;

            // The current read head to continue reading from
            byte* ptr = recvBufferPtr + readHead;

            //
            // Fast-path parsing by (1) command string length, (2) First character of command name (optional) and (3) priority (manual order)
            //

            // NOTE: A valid RESP string is at a minimum 7 characters long "$_\r\n_\r\n"
            if (remainingBytes >= 7)
            {
                var oldReadHead = readHead;

                // Check if this is a string with a single-digit length ("$_\r\n" -> _ omitted)
                if ((*(uint*)ptr & 0xFFFF00FF) == MemoryMarshal.Read<uint>("$\0\r\n"u8))
                {
                    // Extract length from string header
                    var length = ptr[1] - '0';

                    // Ensure that the complete command string is contained in the package. Otherwise exit early.
                    // Include 6 bytes to account for command string header and name terminator.
                    // 6 bytes = "$_\r\n" (4 bytes) + "\r\n" (2 bytes) at end of command name
                    if (remainingBytes >= length + 6)
                    {
                        // Optimistically increase read head and decrease the number of remaining elements
                        readHead += length + 6;
                        count -= 1;

                        switch (length)
                        {
                            case 3:
                                if (*(ulong*)(ptr + 1) == MemoryMarshal.Read<ulong>("3\r\nDEL\r\n"u8))
                                {
                                    return RespCommand.DEL;
                                }
                                else if (*(ulong*)(ptr + 1) == MemoryMarshal.Read<ulong>("3\r\nLCS\r\n"u8))
                                {
                                    return RespCommand.LCS;
                                }

                                break;

                            case 4:
                                switch ((ushort)ptr[4])
                                {
                                    case 'E':
                                        if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nEVAL\r\n"u8))
                                        {
                                            return RespCommand.EVAL;
                                        }
                                        break;

                                    case 'H':
                                        if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nHSET\r\n"u8))
                                        {
                                            return RespCommand.HSET;
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nHGET\r\n"u8))
                                        {
                                            return RespCommand.HGET;
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nHDEL\r\n"u8))
                                        {
                                            return RespCommand.HDEL;
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nHLEN\r\n"u8))
                                        {
                                            return RespCommand.HLEN;
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nHTTL\r\n"u8))
                                        {
                                            return RespCommand.HTTL;
                                        }
                                        break;

                                    case 'K':
                                        if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nKEYS\r\n"u8))
                                        {
                                            return RespCommand.KEYS;
                                        }
                                        break;

                                    case 'L':
                                        if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nLPOP\r\n"u8))
                                        {
                                            return RespCommand.LPOP;
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nLLEN\r\n"u8))
                                        {
                                            return RespCommand.LLEN;
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nLREM\r\n"u8))
                                        {
                                            return RespCommand.LREM;
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nLSET\r\n"u8))
                                        {
                                            return RespCommand.LSET;
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nLPOS\r\n"u8))
                                        {
                                            return RespCommand.LPOS;
                                        }
                                        break;

                                    case 'M':
                                        if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nMGET\r\n"u8))
                                        {
                                            return RespCommand.MGET;
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nMSET\r\n"u8))
                                        {
                                            return RespCommand.MSET;
                                        }
                                        break;

                                    case 'R':
                                        if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nRPOP\r\n"u8))
                                        {
                                            return RespCommand.RPOP;
                                        }
                                        break;

                                    case 'S':
                                        if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nSCAN\r\n"u8))
                                        {
                                            return RespCommand.SCAN;
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nSADD\r\n"u8))
                                        {
                                            return RespCommand.SADD;
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nSREM\r\n"u8))
                                        {
                                            return RespCommand.SREM;
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nSPOP\r\n"u8))
                                        {
                                            return RespCommand.SPOP;
                                        }
                                        break;

                                    case 'T':
                                        if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nTYPE\r\n"u8))
                                        {
                                            return RespCommand.TYPE;
                                        }
                                        break;

                                    case 'Z':
                                        if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nZADD\r\n"u8))
                                        {
                                            return RespCommand.ZADD;
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nZREM\r\n"u8))
                                        {
                                            return RespCommand.ZREM;
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nZTTL\r\n"u8))
                                        {
                                            return RespCommand.ZTTL;
                                        }
                                        break;
                                }
                                break;

                            case 5:
                                switch ((ushort)ptr[4])
                                {
                                    case 'B':
                                        if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nBITOP\r\n"u8))
                                        {
                                            // Check for matching bit-operation
                                            if (remainingBytes > length + 6 + 8)
                                            {
                                                // TODO: AND|OR|XOR|NOT|DIFF may not correctly handle mixed cases?

                                                var tag64 = *(ulong*)(ptr + 11);
                                                var tag32 = (uint)tag64;

                                                if (tag32 == MemoryMarshal.Read<uint>("$2\r\n"u8))
                                                {
                                                    if (tag64 == MemoryMarshal.Read<ulong>("$2\r\nOR\r\n"u8) || tag64 == MemoryMarshal.Read<ulong>("$2\r\nor\r\n"u8))
                                                    {
                                                        readHead += 8; // "$2\r\n" + "OR" + "\r\n"
                                                        count -= 1;
                                                        return RespCommand.BITOP_OR;
                                                    }
                                                }
                                                else if (tag32 == MemoryMarshal.Read<uint>("$3\r\n"u8) && remainingBytes > length + 6 + 9)
                                                {
                                                    // Optimistically adjust
                                                    readHead += 9; // "$3\r\n" + AND|XOR|NOT + "\r\n"
                                                    count -= 1;

                                                    tag64 = *(ulong*)(ptr + 12);

                                                    if (tag64 == MemoryMarshal.Read<ulong>("3\r\nAND\r\n"u8) || tag64 == MemoryMarshal.Read<ulong>("3\r\nand\r\n"u8))
                                                    {
                                                        return RespCommand.BITOP_AND;
                                                    }
                                                    else if (tag64 == MemoryMarshal.Read<ulong>("3\r\nXOR\r\n"u8) || tag64 == MemoryMarshal.Read<ulong>("3\r\nxor\r\n"u8))
                                                    {
                                                        return RespCommand.BITOP_XOR;
                                                    }
                                                    else if (tag64 == MemoryMarshal.Read<ulong>("3\r\nNOT\r\n"u8) || tag64 == MemoryMarshal.Read<ulong>("3\r\nnot\r\n"u8))
                                                    {
                                                        return RespCommand.BITOP_NOT;
                                                    }

                                                    // Reset if no match
                                                    readHead -= 9;
                                                    count += 1;
                                                }
                                                else if (tag32 == MemoryMarshal.Read<uint>("$4\r\n"u8) && remainingBytes > length + 6 + 10)
                                                {
                                                    // Optimistically adjust
                                                    readHead += 10; // "$4\r\nDIFF\r\n"
                                                    count -= 1;

                                                    tag64 = *(ulong*)(ptr + 12);

                                                    // Compare first 8 bytes then the trailing '\n' for "4\r\nDIFF\r\n"
                                                    if ((*(ulong*)(ptr + 12) == MemoryMarshal.Read<ulong>("4\r\nDIFF\r"u8) ||
                                                        *(ulong*)(ptr + 12) == MemoryMarshal.Read<ulong>("4\r\ndiff\r"u8)) &&
                                                        *(ptr + 20) == (byte)'\n')
                                                    {
                                                        return RespCommand.BITOP_DIFF;
                                                    }

                                                    // Reset if no match
                                                    readHead -= 10;
                                                    count += 1;
                                                }

                                                // Although we recognize BITOP, the pseudo-subcommand isn't recognized so fail early
                                                specificErrorMessage = CmdStrings.RESP_SYNTAX_ERROR;
                                                return RespCommand.NONE;
                                            }
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nBRPOP\r\n"u8))
                                        {
                                            return RespCommand.BRPOP;
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nBLPOP\r\n"u8))
                                        {
                                            return RespCommand.BLPOP;
                                        }
                                        break;

                                    case 'H':
                                        if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nHMSET\r\n"u8))
                                        {
                                            return RespCommand.HMSET;
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nHMGET\r\n"u8))
                                        {
                                            return RespCommand.HMGET;
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nHKEYS\r\n"u8))
                                        {
                                            return RespCommand.HKEYS;
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nHVALS\r\n"u8))
                                        {
                                            return RespCommand.HVALS;
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nHSCAN\r\n"u8))
                                        {
                                            return RespCommand.HSCAN;
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nHPTTL\r\n"u8))
                                        {
                                            return RespCommand.HPTTL;
                                        }
                                        break;

                                    case 'L':
                                        if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nLPUSH\r\n"u8))
                                        {
                                            return RespCommand.LPUSH;
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nLTRIM\r\n"u8))
                                        {
                                            return RespCommand.LTRIM;
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nLMOVE\r\n"u8))
                                        {
                                            return RespCommand.LMOVE;
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nLMPOP\r\n"u8))
                                        {
                                            return RespCommand.LMPOP;
                                        }
                                        break;

                                    case 'P':
                                        if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nPFADD\r\n"u8))
                                        {
                                            return RespCommand.PFADD;
                                        }
                                        break;

                                    case 'R':
                                        if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nRPUSH\r\n"u8))
                                        {
                                            return RespCommand.RPUSH;
                                        }
                                        break;

                                    case 'S':
                                        if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nSCARD\r\n"u8))
                                        {
                                            return RespCommand.SCARD;
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nSSCAN\r\n"u8))
                                        {
                                            return RespCommand.SSCAN;
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nSMOVE\r\n"u8))
                                        {
                                            return RespCommand.SMOVE;
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nSDIFF\r\n"u8))
                                        {
                                            return RespCommand.SDIFF;
                                        }
                                        break;

                                    case 'W':
                                        if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nWATCH\r\n"u8))
                                        {
                                            return RespCommand.WATCH;
                                        }
                                        break;

                                    case 'Z':
                                        if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nZCARD\r\n"u8))
                                        {
                                            return RespCommand.ZCARD;
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nZRANK\r\n"u8))
                                        {
                                            return RespCommand.ZRANK;
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nZDIFF\r\n"u8))
                                        {
                                            return RespCommand.ZDIFF;
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nZSCAN\r\n"u8))
                                        {
                                            return RespCommand.ZSCAN;
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nZMPOP\r\n"u8))
                                        {
                                            return RespCommand.ZMPOP;
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nZPTTL\r\n"u8))
                                        {
                                            return RespCommand.ZPTTL;
                                        }
                                        break;
                                }
                                break;

                            case 6:
                                switch ((ushort)ptr[4])
                                {
                                    case 'B':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("BLMOVE\r\n"u8))
                                        {
                                            return RespCommand.BLMOVE;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("BLMPOP\r\n"u8))
                                        {
                                            return RespCommand.BLMPOP;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("BZMPOP\r\n"u8))
                                        {
                                            return RespCommand.BZMPOP;
                                        }
                                        break;
                                    case 'D':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("DBSIZE\r\n"u8))
                                        {
                                            return RespCommand.DBSIZE;
                                        }
                                        break;

                                    case 'E':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("EXISTS\r\n"u8))
                                        {
                                            return RespCommand.EXISTS;
                                        }
                                        break;

                                    case 'G':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("GEOADD\r\n"u8))
                                        {
                                            return RespCommand.GEOADD;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("GEOPOS\r\n"u8))
                                        {
                                            return RespCommand.GEOPOS;
                                        }
                                        break;

                                    case 'H':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("HSETNX\r\n"u8))
                                        {
                                            return RespCommand.HSETNX;
                                        }
                                        break;

                                    case 'L':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("LPUSHX\r\n"u8))
                                        {
                                            return RespCommand.LPUSHX;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("LRANGE\r\n"u8))
                                        {
                                            return RespCommand.LRANGE;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("LINDEX\r\n"u8))
                                        {
                                            return RespCommand.LINDEX;
                                        }
                                        break;

                                    case 'M':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("MSETNX\r\n"u8))
                                        {
                                            return RespCommand.MSETNX;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("MEMORY\r\n"u8))
                                        {
                                            // MEMORY USAGE
                                            // 11 = "$5\r\nUSAGE\r\n".Length
                                            if (remainingBytes >= length + 11)
                                            {
                                                if (*(ulong*)(ptr + 12) == MemoryMarshal.Read<ulong>("$5\r\nUSAG"u8) && *(ulong*)(ptr + 15) == MemoryMarshal.Read<ulong>("\nUSAGE\r\n"u8))
                                                {
                                                    count--;
                                                    readHead += 11;
                                                    return RespCommand.MEMORY_USAGE;
                                                }
                                            }
                                        }
                                        break;

                                    case 'R':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("RPUSHX\r\n"u8))
                                        {
                                            return RespCommand.RPUSHX;
                                        }
                                        break;

                                    case 'S':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("SELECT\r\n"u8))
                                        {
                                            return RespCommand.SELECT;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("STRLEN\r\n"u8))
                                        {
                                            return RespCommand.STRLEN;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("SUNION\r\n"u8))
                                        {
                                            return RespCommand.SUNION;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("SINTER\r\n"u8))
                                        {
                                            return RespCommand.SINTER;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("SCRIPT\r\n"u8))
                                        {
                                            // SCRIPT EXISTS => "$6\r\nEXISTS\r\n".Length == 12
                                            // SCRIPT FLUSH  => "$5\r\nFLUSH\r\n".Length  == 11
                                            // SCRIPT LOAD   => "$4\r\nLOAD\r\n".Length   == 10

                                            if (remainingBytes >= length + 10)
                                            {
                                                if (*(ulong*)(ptr + 4 + 8) == MemoryMarshal.Read<ulong>("$4\r\nLOAD"u8) && *(ulong*)(ptr + 4 + 8 + 2) == MemoryMarshal.Read<ulong>("\r\nLOAD\r\n"u8))
                                                {
                                                    count--;
                                                    readHead += 10;
                                                    return RespCommand.SCRIPT_LOAD;
                                                }

                                                if (remainingBytes >= length + 11)
                                                {
                                                    if (*(ulong*)(ptr + 4 + 8) == MemoryMarshal.Read<ulong>("$5\r\nFLUS"u8) && *(ulong*)(ptr + 4 + 8 + 3) == MemoryMarshal.Read<ulong>("\nFLUSH\r\n"u8))
                                                    {
                                                        count--;
                                                        readHead += 11;
                                                        return RespCommand.SCRIPT_FLUSH;
                                                    }

                                                    if (remainingBytes >= length + 12)
                                                    {
                                                        if (*(ulong*)(ptr + 4 + 8) == MemoryMarshal.Read<ulong>("$6\r\nEXIS"u8) && *(ulong*)(ptr + 4 + 8 + 4) == MemoryMarshal.Read<ulong>("EXISTS\r\n"u8))
                                                        {
                                                            count--;
                                                            readHead += 12;
                                                            return RespCommand.SCRIPT_EXISTS;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("SWAPDB\r\n"u8))
                                        {
                                            return RespCommand.SWAPDB;
                                        }
                                        break;

                                    case 'U':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("UNLINK\r\n"u8))
                                        {
                                            return RespCommand.UNLINK;
                                        }
                                        break;

                                    case 'Z':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZCOUNT\r\n"u8))
                                        {
                                            return RespCommand.ZCOUNT;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZRANGE\r\n"u8))
                                        {
                                            return RespCommand.ZRANGE;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZUNION\r\n"u8))
                                        {
                                            return RespCommand.ZUNION;
                                        }
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZSCORE\r\n"u8))
                                        {
                                            return RespCommand.ZSCORE;
                                        }
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZINTER\r\n"u8))
                                        {
                                            return RespCommand.ZINTER;
                                        }
                                        break;
                                }

                                break;
                            case 7:
                                switch ((ushort)ptr[4])
                                {
                                    case 'E':
                                        if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nEVALSHA\r\n"u8))
                                        {
                                            return RespCommand.EVALSHA;
                                        }
                                        break;

                                    case 'G':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("GEOHASH\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return RespCommand.GEOHASH;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("GEODIST\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return RespCommand.GEODIST;
                                        }
                                        break;

                                    case 'H':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("HGETALL\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return RespCommand.HGETALL;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("HEXISTS\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return RespCommand.HEXISTS;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("HEXPIRE\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return RespCommand.HEXPIRE;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("HINCRBY\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return RespCommand.HINCRBY;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("HSTRLEN\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return RespCommand.HSTRLEN;
                                        }
                                        break;

                                    case 'L':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("LINSERT\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return RespCommand.LINSERT;
                                        }
                                        break;

                                    case 'M':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("MONITOR\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return RespCommand.MONITOR;
                                        }
                                        break;

                                    case 'P':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("PFCOUNT\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return RespCommand.PFCOUNT;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("PFMERGE\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return RespCommand.PFMERGE;
                                        }
                                        break;
                                    case 'W':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("WATCHMS\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return RespCommand.WATCHMS;
                                        }

                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("WATCHOS\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return RespCommand.WATCHOS;
                                        }

                                        break;

                                    case 'Z':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZPOPMIN\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return RespCommand.ZPOPMIN;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZEXPIRE\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return RespCommand.ZEXPIRE;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZPOPMAX\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return RespCommand.ZPOPMAX;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZINCRBY\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return RespCommand.ZINCRBY;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZMSCORE\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return RespCommand.ZMSCORE;
                                        }
                                        break;
                                }
                                break;
                            case 8:
                                if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZREVRANK"u8) && *(ushort*)(ptr + 12) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return RespCommand.ZREVRANK;
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("SMEMBERS"u8) && *(ushort*)(ptr + 12) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return RespCommand.SMEMBERS;
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("BITFIELD"u8) && *(ushort*)(ptr + 12) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return RespCommand.BITFIELD;
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("EXPIREAT"u8) && *(ushort*)(ptr + 12) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return RespCommand.EXPIREAT;
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("HPEXPIRE"u8) && *(ushort*)(ptr + 12) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return RespCommand.HPEXPIRE;
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("HPERSIST"u8) && *(ushort*)(ptr + 12) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return RespCommand.HPERSIST;
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZPEXPIRE"u8) && *(ushort*)(ptr + 12) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return RespCommand.ZPEXPIRE;
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZPERSIST"u8) && *(ushort*)(ptr + 12) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return RespCommand.ZPERSIST;
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("BZPOPMAX"u8) && *(ushort*)(ptr + 12) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return RespCommand.BZPOPMAX;
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("BZPOPMIN"u8) && *(ushort*)(ptr + 12) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return RespCommand.BZPOPMIN;
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("SPUBLISH"u8) && *(ushort*)(ptr + 12) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return RespCommand.SPUBLISH;
                                }
                                break;
                            case 9:
                                if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("SUBSCRIB"u8) && *(uint*)(ptr + 11) == MemoryMarshal.Read<uint>("BE\r\n"u8))
                                {
                                    return RespCommand.SUBSCRIBE;
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("SISMEMBE"u8) && *(uint*)(ptr + 11) == MemoryMarshal.Read<uint>("ER\r\n"u8))
                                {
                                    return RespCommand.SISMEMBER;
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZLEXCOUN"u8) && *(uint*)(ptr + 11) == MemoryMarshal.Read<uint>("NT\r\n"u8))
                                {
                                    return RespCommand.ZLEXCOUNT;
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("GEOSEARC"u8) && *(uint*)(ptr + 11) == MemoryMarshal.Read<uint>("CH\r\n"u8))
                                {
                                    return RespCommand.GEOSEARCH;
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZREVRANG"u8) && *(uint*)(ptr + 11) == MemoryMarshal.Read<uint>("GE\r\n"u8))
                                {
                                    return RespCommand.ZREVRANGE;
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("RPOPLPUS"u8) && *(uint*)(ptr + 11) == MemoryMarshal.Read<uint>("SH\r\n"u8))
                                {
                                    return RespCommand.RPOPLPUSH;
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("PEXPIREA"u8) && *(uint*)(ptr + 11) == MemoryMarshal.Read<uint>("AT\r\n"u8))
                                {
                                    return RespCommand.PEXPIREAT;
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("HEXPIREA"u8) && *(uint*)(ptr + 11) == MemoryMarshal.Read<uint>("AT\r\n"u8))
                                {
                                    return RespCommand.HEXPIREAT;
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZEXPIREA"u8) && *(uint*)(ptr + 11) == MemoryMarshal.Read<uint>("AT\r\n"u8))
                                {
                                    return RespCommand.ZEXPIREAT;
                                }
                                break;
                            case 10:
                                if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("SSUBSCRI"u8) && *(uint*)(ptr + 11) == MemoryMarshal.Read<uint>("BE\r\n"u8))
                                {
                                    return RespCommand.SSUBSCRIBE;
                                }
                                break;
                        }

                        // Reset optimistically changed state, if no matching command was found
                        count += 1;
                        readHead = oldReadHead;
                    }
                }
                // Check if this is a string with a double-digit length ("$__\r" -> _ omitted)
                else if ((*(uint*)ptr & 0xFF0000FF) == MemoryMarshal.Read<uint>("$\0\0\r"u8))
                {
                    // Extract length from string header
                    var length = ptr[2] - '0' + 10;

                    // Ensure that the complete command string is contained in the package. Otherwise exit early.
                    // Include 7 bytes to account for command string header and name terminator.
                    // 7 bytes = "$__\r\n" (5 bytes) + "\r\n" (2 bytes) at end of command name
                    if (remainingBytes >= length + 7)
                    {
                        // Optimistically increase read head and decrease the number of remaining elements
                        readHead += length + 7;
                        count -= 1;

                        // Match remaining character by length
                        // NOTE: Check should include the remaining array length terminator '\n'
                        switch (length)
                        {
                            case 10:
                                if (*(ulong*)(ptr + 1) == MemoryMarshal.Read<ulong>("10\r\nPSUB"u8) && *(ulong*)(ptr + 9) == MemoryMarshal.Read<ulong>("SCRIBE\r\n"u8))
                                {
                                    return RespCommand.PSUBSCRIBE;
                                }
                                else if (*(ulong*)(ptr + 1) == MemoryMarshal.Read<ulong>("10\r\nHRAN"u8) && *(ulong*)(ptr + 9) == MemoryMarshal.Read<ulong>("DFIELD\r\n"u8))
                                {
                                    return RespCommand.HRANDFIELD;
                                }
                                else if (*(ulong*)(ptr + 1) == MemoryMarshal.Read<ulong>("10\r\nSDIF"u8) && *(ulong*)(ptr + 9) == MemoryMarshal.Read<ulong>("FSTORE\r\n"u8))
                                {
                                    return RespCommand.SDIFFSTORE;
                                }
                                else if (*(ulong*)(ptr + 1) == MemoryMarshal.Read<ulong>("10\r\nEXPI"u8) && *(ulong*)(ptr + 9) == MemoryMarshal.Read<ulong>("RETIME\r\n"u8))
                                {
                                    return RespCommand.EXPIRETIME;
                                }
                                else if (*(ulong*)(ptr + 1) == MemoryMarshal.Read<ulong>("10\r\nSMIS"u8) && *(ulong*)(ptr + 9) == MemoryMarshal.Read<ulong>("MEMBER\r\n"u8))
                                {
                                    return RespCommand.SMISMEMBER;
                                }
                                else if (*(ulong*)(ptr + 1) == MemoryMarshal.Read<ulong>("10\r\nSINT"u8) && *(ulong*)(ptr + 9) == MemoryMarshal.Read<ulong>("ERCARD\r\n"u8))
                                {
                                    return RespCommand.SINTERCARD;
                                }
                                else if (*(ulong*)(ptr + 1) == MemoryMarshal.Read<ulong>("10\r\nZDIF"u8) && *(uint*)(ptr + 9) == MemoryMarshal.Read<uint>("FSTORE\r\n"u8))
                                {
                                    return RespCommand.ZDIFFSTORE;
                                }
                                else if (*(ulong*)(ptr + 1) == MemoryMarshal.Read<ulong>("10\r\nBRPO"u8) && *(uint*)(ptr + 9) == MemoryMarshal.Read<uint>("PLPUSH\r\n"u8))
                                {
                                    return RespCommand.BRPOPLPUSH;
                                }
                                else if (*(ulong*)(ptr + 1) == MemoryMarshal.Read<ulong>("10\r\nZINT"u8) && *(ulong*)(ptr + 9) == MemoryMarshal.Read<ulong>("ERCARD\r\n"u8))
                                {
                                    return RespCommand.ZINTERCARD;
                                }
                                else if (*(ulong*)(ptr + 1) == MemoryMarshal.Read<ulong>("10\r\nHPEX"u8) && *(uint*)(ptr + 9) == MemoryMarshal.Read<uint>("PIREAT\r\n"u8))
                                {
                                    return RespCommand.HPEXPIREAT;
                                }
                                else if (*(ulong*)(ptr + 1) == MemoryMarshal.Read<ulong>("10\r\nZPEX"u8) && *(uint*)(ptr + 9) == MemoryMarshal.Read<uint>("PIREAT\r\n"u8))
                                {
                                    return RespCommand.ZPEXPIREAT;
                                }
                                break;
                            case 11:
                                if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("1\r\nUNSUB"u8) && *(ulong*)(ptr + 10) == MemoryMarshal.Read<ulong>("SCRIBE\r\n"u8))
                                {
                                    return RespCommand.UNSUBSCRIBE;
                                }
                                else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("1\r\nZRAND"u8) && *(ulong*)(ptr + 10) == MemoryMarshal.Read<ulong>("MEMBER\r\n"u8))
                                {
                                    return RespCommand.ZRANDMEMBER;
                                }
                                else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("1\r\nBITFI"u8) && *(ulong*)(ptr + 10) == MemoryMarshal.Read<ulong>("ELD_RO\r\n"u8))
                                {
                                    return RespCommand.BITFIELD_RO;
                                }
                                else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("1\r\nSRAND"u8) && *(ulong*)(ptr + 10) == MemoryMarshal.Read<ulong>("MEMBER\r\n"u8))
                                {
                                    return RespCommand.SRANDMEMBER;
                                }
                                else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("1\r\nSUNIO"u8) && *(ulong*)(ptr + 10) == MemoryMarshal.Read<ulong>("NSTORE\r\n"u8))
                                {
                                    return RespCommand.SUNIONSTORE;
                                }
                                else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("1\r\nSINTE"u8) && *(ulong*)(ptr + 10) == MemoryMarshal.Read<ulong>("RSTORE\r\n"u8))
                                {
                                    return RespCommand.SINTERSTORE;
                                }
                                else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("1\r\nPEXPI"u8) && *(ulong*)(ptr + 10) == MemoryMarshal.Read<ulong>("RETIME\r\n"u8))
                                {
                                    return RespCommand.PEXPIRETIME;
                                }
                                else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("1\r\nHEXPI"u8) && *(ulong*)(ptr + 10) == MemoryMarshal.Read<ulong>("RETIME\r\n"u8))
                                {
                                    return RespCommand.HEXPIRETIME;
                                }
                                else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("1\r\nINCRB"u8) && *(ulong*)(ptr + 10) == MemoryMarshal.Read<ulong>("YFLOAT\r\n"u8))
                                {
                                    return RespCommand.INCRBYFLOAT;
                                }
                                else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("1\r\nZRANG"u8) && *(ulong*)(ptr + 10) == MemoryMarshal.Read<ulong>("ESTORE\r\n"u8))
                                {
                                    return RespCommand.ZRANGESTORE;
                                }
                                else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("1\r\nZRANG"u8) && *(ulong*)(ptr + 10) == MemoryMarshal.Read<ulong>("EBYLEX\r\n"u8))
                                {
                                    return RespCommand.ZRANGEBYLEX;
                                }
                                else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("1\r\nZINTE"u8) && *(ulong*)(ptr + 10) == MemoryMarshal.Read<ulong>("RSTORE\r\n"u8))
                                {
                                    return RespCommand.ZINTERSTORE;
                                }
                                else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("1\r\nZUNIO"u8) && *(ulong*)(ptr + 10) == MemoryMarshal.Read<ulong>("NSTORE\r\n"u8))
                                {
                                    return RespCommand.ZUNIONSTORE;
                                }
                                else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("1\r\nZEXPI"u8) && *(ulong*)(ptr + 10) == MemoryMarshal.Read<ulong>("RETIME\r\n"u8))
                                {
                                    return RespCommand.ZEXPIRETIME;
                                }
                                else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("1\r\nEXECI"u8) && *(ulong*)(ptr + 10) == MemoryMarshal.Read<ulong>("FMATCH\r\n"u8))
                                {
                                    return RespCommand.EXECIFMATCH;
                                }
                                break;

                            case 12:
                                if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\r\nPUNSUB"u8) && *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("SCRIBE\r\n"u8))
                                {
                                    return RespCommand.PUNSUBSCRIBE;
                                }
                                else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\r\nHINCRB"u8) && *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("YFLOAT\r\n"u8))
                                {
                                    return RespCommand.HINCRBYFLOAT;
                                }
                                else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\r\nHPEXPI"u8) && *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("RETIME\r\n"u8))
                                {
                                    return RespCommand.HPEXPIRETIME;
                                }
                                else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\r\nZPEXPI"u8) && *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("RETIME\r\n"u8))
                                {
                                    return RespCommand.ZPEXPIRETIME;
                                }
                                else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\r\nEXECWI"u8) && *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("THETAG\r\n"u8))
                                {
                                    return RespCommand.EXECWITHETAG;
                                }
                                break;

                            case 13:
                                if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("\nZRANGEB"u8) && *(ulong*)(ptr + 12) == MemoryMarshal.Read<ulong>("YSCORE\r\n"u8))
                                {
                                    return RespCommand.ZRANGEBYSCORE;
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("\nEXECIFG"u8) && *(ulong*)(ptr + 12) == MemoryMarshal.Read<ulong>("REATER\r\n"u8))
                                {
                                    return RespCommand.EXECIFGREATER;
                                }
                                break;

                            case 14:
                                if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\r\nZREMRA"u8) && *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("NGEBYLEX"u8) && *(ushort*)(ptr + 19) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return RespCommand.ZREMRANGEBYLEX;
                                }
                                else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\r\nGEOSEA"u8) && *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("RCHSTORE"u8) && *(ushort*)(ptr + 19) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return RespCommand.GEOSEARCHSTORE;
                                }
                                else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\r\nZREVRA"u8) && *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("NGEBYLEX"u8) && *(ushort*)(ptr + 19) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return RespCommand.ZREVRANGEBYLEX;
                                }
                                else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\r\nEXECIF"u8) && *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("NOTMATCH"u8) && *(ushort*)(ptr + 19) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return RespCommand.EXECIFNOTMATCH;
                                }
                                break;

                            case 15:
                                if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("\nZREMRAN"u8) && *(ulong*)(ptr + 12) == MemoryMarshal.Read<ulong>("GEBYRANK"u8) && *(ushort*)(ptr + 20) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return RespCommand.ZREMRANGEBYRANK;
                                }
                                break;

                            case 16:
                                if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\r\nCUSTOM"u8) && *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("OBJECTSC"u8) && *(ushort*)(ptr + 19) == MemoryMarshal.Read<ushort>("AN\r\n"u8))
                                {
                                    return RespCommand.COSCAN;
                                }
                                else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\r\nZREMRA"u8) && *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("NGEBYSCO"u8) && *(ushort*)(ptr + 19) == MemoryMarshal.Read<ushort>("RE\r\n"u8))
                                {
                                    return RespCommand.ZREMRANGEBYSCORE;
                                }
                                else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\r\nZREVRA"u8) && *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("NGEBYSCO"u8) && *(ushort*)(ptr + 19) == MemoryMarshal.Read<ushort>("RE\r\n"u8))
                                {
                                    return RespCommand.ZREVRANGEBYSCORE;
                                }
                                break;
                        }

                        // Reset optimistically changed state, if no matching command was found
                        count += 1;
                        readHead = oldReadHead;
                    }
                }
            }

            // No matching command name found in this pass
            return RespCommand.NONE;
        }

        private bool TryParseCustomCommand(ReadOnlySpan<byte> command, out RespCommand cmd)
        {
            if (customCommandManagerSession.Match(command, out currentCustomTransaction))
            {
                cmd = RespCommand.CustomTxn;
                return true;
            }
            else if (customCommandManagerSession.Match(command, out currentCustomProcedure))
            {
                cmd = RespCommand.CustomProcedure;
                return true;
            }
            else if (customCommandManagerSession.Match(command, out currentCustomRawStringCommand))
            {
                cmd = RespCommand.CustomRawStringCmd;
                return true;
            }
            else if (customCommandManagerSession.Match(command, out currentCustomObjectCommand))
            {
                cmd = RespCommand.CustomObjCmd;
                return true;
            }
            cmd = RespCommand.NONE;
            return false;
        }

        /// <summary>
        /// Parses the receive buffer, starting from the current read head, for all command names that are
        /// not covered by FastParseArrayCommand() and advances the read head to the end of the command name.
        /// 
        /// NOTE: Assumes the input command names have already been converted to upper-case.
        /// </summary>
        /// <param name="count">Reference to the number of remaining tokens in the packet. Will be reduced to number of command arguments.</param>
        /// <param name="specificErrorMsg">If the command could not be parsed, will be non-empty if a specific error message should be returned.</param>
        /// <param name="success">True if the input RESP string was completely included in the buffer, false if we couldn't read the full command name.</param>
        /// <returns>The parsed command name.</returns>
        private RespCommand SlowParseCommand(ref int count, ref ReadOnlySpan<byte> specificErrorMsg, out bool success)
        {
            // Try to extract the current string from the front of the read head
            var command = GetCommand(out success);

            if (!success)
            {
                return RespCommand.INVALID;
            }

            // Account for the command name being taken off the read head
            count -= 1;

            if (TryParseCustomCommand(command, out var cmd))
            {
                return cmd;
            }
            else
            {
                return SlowParseCommand(command, ref count, ref specificErrorMsg, out success);
            }
        }

        private RespCommand SlowParseCommand(ReadOnlySpan<byte> command, ref int count, ref ReadOnlySpan<byte> specificErrorMsg, out bool success)
        {
            success = true;
            if (command.SequenceEqual(CmdStrings.SUBSCRIBE))
            {
                return RespCommand.SUBSCRIBE;
            }
            else if (command.SequenceEqual(CmdStrings.SSUBSCRIBE))
            {
                return RespCommand.SSUBSCRIBE;
            }
            else if (command.SequenceEqual(CmdStrings.RUNTXP))
            {
                return RespCommand.RUNTXP;
            }
            else if (command.SequenceEqual(CmdStrings.SCRIPT))
            {
                if (count == 0)
                {
                    specificErrorMsg = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs,
                        nameof(RespCommand.SCRIPT)));
                    return RespCommand.INVALID;
                }

                var subCommand = GetUpperCaseCommand(out var gotSubCommand);
                if (!gotSubCommand)
                {
                    success = false;
                    return RespCommand.NONE;
                }

                count--;

                if (subCommand.SequenceEqual(CmdStrings.LOAD))
                {
                    return RespCommand.SCRIPT_LOAD;
                }

                if (subCommand.SequenceEqual(CmdStrings.FLUSH))
                {
                    return RespCommand.SCRIPT_FLUSH;
                }

                if (subCommand.SequenceEqual(CmdStrings.EXISTS))
                {
                    return RespCommand.SCRIPT_EXISTS;
                }

                string errMsg = string.Format(CmdStrings.GenericErrUnknownSubCommandNoHelp,
                                              Encoding.UTF8.GetString(subCommand),
                                              nameof(RespCommand.SCRIPT));
                specificErrorMsg = Encoding.UTF8.GetBytes(errMsg);
                return RespCommand.INVALID;
            }
            else if (command.SequenceEqual(CmdStrings.ECHO))
            {
                return RespCommand.ECHO;
            }
            else if (command.SequenceEqual(CmdStrings.GEORADIUS))
            {
                return RespCommand.GEORADIUS;
            }
            else if (command.SequenceEqual(CmdStrings.GEORADIUS_RO))
            {
                return RespCommand.GEORADIUS_RO;
            }
            else if (command.SequenceEqual(CmdStrings.GEORADIUSBYMEMBER))
            {
                return RespCommand.GEORADIUSBYMEMBER;
            }
            else if (command.SequenceEqual(CmdStrings.GEORADIUSBYMEMBER_RO))
            {
                return RespCommand.GEORADIUSBYMEMBER_RO;
            }
            else if (command.SequenceEqual(CmdStrings.REPLICAOF))
            {
                return RespCommand.REPLICAOF;
            }
            else if (command.SequenceEqual(CmdStrings.SECONDARYOF) || command.SequenceEqual(CmdStrings.SLAVEOF))
            {
                return RespCommand.SECONDARYOF;
            }
            else if (command.SequenceEqual(CmdStrings.CONFIG))
            {
                if (count == 0)
                {
                    specificErrorMsg = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs,
                        nameof(RespCommand.CONFIG)));
                    return RespCommand.INVALID;
                }

                var subCommand = GetUpperCaseCommand(out var gotSubCommand);
                if (!gotSubCommand)
                {
                    success = false;
                    return RespCommand.NONE;
                }

                count--;

                if (subCommand.SequenceEqual(CmdStrings.GET))
                {
                    return RespCommand.CONFIG_GET;
                }
                else if (subCommand.SequenceEqual(CmdStrings.REWRITE))
                {
                    return RespCommand.CONFIG_REWRITE;
                }
                else if (subCommand.SequenceEqual(CmdStrings.SET))
                {
                    return RespCommand.CONFIG_SET;
                }

                string errMsg = string.Format(CmdStrings.GenericErrUnknownSubCommandNoHelp,
                                              Encoding.UTF8.GetString(subCommand),
                                              nameof(RespCommand.CONFIG));
                specificErrorMsg = Encoding.UTF8.GetBytes(errMsg);
                return RespCommand.INVALID;
            }
            else if (command.SequenceEqual(CmdStrings.CLIENT))
            {
                if (count == 0)
                {
                    specificErrorMsg = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs,
                        nameof(RespCommand.CLIENT)));
                    return RespCommand.INVALID;
                }

                var subCommand = GetUpperCaseCommand(out var gotSubCommand);
                if (!gotSubCommand)
                {
                    success = false;
                    return RespCommand.NONE;
                }

                count--;

                if (subCommand.SequenceEqual(CmdStrings.ID))
                {
                    return RespCommand.CLIENT_ID;
                }
                else if (subCommand.SequenceEqual(CmdStrings.INFO))
                {
                    return RespCommand.CLIENT_INFO;
                }
                else if (subCommand.SequenceEqual(CmdStrings.LIST))
                {
                    return RespCommand.CLIENT_LIST;
                }
                else if (subCommand.SequenceEqual(CmdStrings.KILL))
                {
                    return RespCommand.CLIENT_KILL;
                }
                else if (subCommand.SequenceEqual(CmdStrings.GETNAME))
                {
                    return RespCommand.CLIENT_GETNAME;
                }
                else if (subCommand.SequenceEqual(CmdStrings.SETNAME))
                {
                    return RespCommand.CLIENT_SETNAME;
                }
                else if (subCommand.SequenceEqual(CmdStrings.SETINFO))
                {
                    return RespCommand.CLIENT_SETINFO;
                }
                else if (subCommand.SequenceEqual(CmdStrings.UNBLOCK))
                {
                    return RespCommand.CLIENT_UNBLOCK;
                }

                string errMsg = string.Format(CmdStrings.GenericErrUnknownSubCommandNoHelp,
                                              Encoding.UTF8.GetString(subCommand),
                                              nameof(RespCommand.CLIENT));
                specificErrorMsg = Encoding.UTF8.GetBytes(errMsg);
                return RespCommand.INVALID;
            }
            else if (command.SequenceEqual(CmdStrings.AUTH))
            {
                return RespCommand.AUTH;
            }
            else if (command.SequenceEqual(CmdStrings.INFO))
            {
                return RespCommand.INFO;
            }
            else if (command.SequenceEqual(CmdStrings.ROLE))
            {
                return RespCommand.ROLE;
            }
            else if (command.SequenceEqual(CmdStrings.COMMAND))
            {
                if (count == 0)
                {
                    return RespCommand.COMMAND;
                }

                var subCommand = GetUpperCaseCommand(out var gotSubCommand);
                if (!gotSubCommand)
                {
                    success = false;
                    return RespCommand.NONE;
                }

                count--;

                if (subCommand.SequenceEqual(CmdStrings.COUNT))
                {
                    return RespCommand.COMMAND_COUNT;
                }

                if (subCommand.SequenceEqual(CmdStrings.INFO))
                {
                    return RespCommand.COMMAND_INFO;
                }

                if (subCommand.SequenceEqual(CmdStrings.DOCS))
                {
                    return RespCommand.COMMAND_DOCS;
                }

                if (subCommand.EqualsUpperCaseSpanIgnoringCase(CmdStrings.GETKEYS))
                {
                    return RespCommand.COMMAND_GETKEYS;
                }

                if (subCommand.EqualsUpperCaseSpanIgnoringCase(CmdStrings.GETKEYSANDFLAGS))
                {
                    return RespCommand.COMMAND_GETKEYSANDFLAGS;
                }

                string errMsg = string.Format(CmdStrings.GenericErrUnknownSubCommandNoHelp,
                                              Encoding.UTF8.GetString(subCommand),
                                              nameof(RespCommand.COMMAND));
                specificErrorMsg = Encoding.UTF8.GetBytes(errMsg);
                return RespCommand.INVALID;
            }
            else if (command.SequenceEqual(CmdStrings.PING))
            {
                return RespCommand.PING;
            }
            else if (command.SequenceEqual(CmdStrings.HELLO))
            {
                return RespCommand.HELLO;
            }
            else if (command.SequenceEqual(CmdStrings.CLUSTER))
            {
                if (count == 0)
                {
                    specificErrorMsg = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs,
                        nameof(RespCommand.CLUSTER)));
                    return RespCommand.INVALID;
                }

                var subCommand = GetUpperCaseCommand(out var gotSubCommand);
                if (!gotSubCommand)
                {
                    success = false;
                    return RespCommand.NONE;
                }

                count--;

                if (subCommand.SequenceEqual(CmdStrings.BUMPEPOCH))
                {
                    return RespCommand.CLUSTER_BUMPEPOCH;
                }
                else if (subCommand.SequenceEqual(CmdStrings.FORGET))
                {
                    return RespCommand.CLUSTER_FORGET;
                }
                else if (subCommand.SequenceEqual(CmdStrings.gossip))
                {
                    return RespCommand.CLUSTER_GOSSIP;
                }
                else if (subCommand.SequenceEqual(CmdStrings.INFO))
                {
                    return RespCommand.CLUSTER_INFO;
                }
                else if (subCommand.SequenceEqual(CmdStrings.MEET))
                {
                    return RespCommand.CLUSTER_MEET;
                }
                else if (subCommand.SequenceEqual(CmdStrings.MYID))
                {
                    return RespCommand.CLUSTER_MYID;
                }
                else if (subCommand.SequenceEqual(CmdStrings.myparentid))
                {
                    return RespCommand.CLUSTER_MYPARENTID;
                }
                else if (subCommand.SequenceEqual(CmdStrings.NODES))
                {
                    return RespCommand.CLUSTER_NODES;
                }
                else if (subCommand.SequenceEqual(CmdStrings.SHARDS))
                {
                    return RespCommand.CLUSTER_SHARDS;
                }
                else if (subCommand.SequenceEqual(CmdStrings.RESET))
                {
                    return RespCommand.CLUSTER_RESET;
                }
                else if (subCommand.SequenceEqual(CmdStrings.FAILOVER))
                {
                    return RespCommand.CLUSTER_FAILOVER;
                }
                else if (subCommand.SequenceEqual(CmdStrings.ADDSLOTS))
                {
                    return RespCommand.CLUSTER_ADDSLOTS;
                }
                else if (subCommand.SequenceEqual(CmdStrings.ADDSLOTSRANGE))
                {
                    return RespCommand.CLUSTER_ADDSLOTSRANGE;
                }
                else if (subCommand.SequenceEqual(CmdStrings.COUNTKEYSINSLOT))
                {
                    return RespCommand.CLUSTER_COUNTKEYSINSLOT;
                }
                else if (subCommand.SequenceEqual(CmdStrings.DELSLOTS))
                {
                    return RespCommand.CLUSTER_DELSLOTS;
                }
                else if (subCommand.SequenceEqual(CmdStrings.DELSLOTSRANGE))
                {
                    return RespCommand.CLUSTER_DELSLOTSRANGE;
                }
                else if (subCommand.SequenceEqual(CmdStrings.GETKEYSINSLOT))
                {
                    return RespCommand.CLUSTER_GETKEYSINSLOT;
                }
                else if (subCommand.SequenceEqual(CmdStrings.HELP))
                {
                    return RespCommand.CLUSTER_HELP;
                }
                else if (subCommand.SequenceEqual(CmdStrings.KEYSLOT))
                {
                    return RespCommand.CLUSTER_KEYSLOT;
                }
                else if (subCommand.SequenceEqual(CmdStrings.SETSLOT))
                {
                    return RespCommand.CLUSTER_SETSLOT;
                }
                else if (subCommand.SequenceEqual(CmdStrings.SLOTS))
                {
                    return RespCommand.CLUSTER_SLOTS;
                }
                else if (subCommand.SequenceEqual(CmdStrings.REPLICAS))
                {
                    return RespCommand.CLUSTER_REPLICAS;
                }
                else if (subCommand.SequenceEqual(CmdStrings.REPLICATE))
                {
                    return RespCommand.CLUSTER_REPLICATE;
                }
                else if (subCommand.SequenceEqual(CmdStrings.delkeysinslot))
                {
                    return RespCommand.CLUSTER_DELKEYSINSLOT;
                }
                else if (subCommand.SequenceEqual(CmdStrings.delkeysinslotrange))
                {
                    return RespCommand.CLUSTER_DELKEYSINSLOTRANGE;
                }
                else if (subCommand.SequenceEqual(CmdStrings.setslotsrange))
                {
                    return RespCommand.CLUSTER_SETSLOTSRANGE;
                }
                else if (subCommand.SequenceEqual(CmdStrings.slotstate))
                {
                    return RespCommand.CLUSTER_SLOTSTATE;
                }
                else if (subCommand.SequenceEqual(CmdStrings.publish))
                {
                    return RespCommand.CLUSTER_PUBLISH;
                }
                else if (subCommand.SequenceEqual(CmdStrings.spublish))
                {
                    return RespCommand.CLUSTER_SPUBLISH;
                }
                else if (subCommand.SequenceEqual(CmdStrings.MIGRATE))
                {
                    return RespCommand.CLUSTER_MIGRATE;
                }
                else if (subCommand.SequenceEqual(CmdStrings.mtasks))
                {
                    return RespCommand.CLUSTER_MTASKS;
                }
                else if (subCommand.SequenceEqual(CmdStrings.aofsync))
                {
                    return RespCommand.CLUSTER_AOFSYNC;
                }
                else if (subCommand.SequenceEqual(CmdStrings.appendlog))
                {
                    return RespCommand.CLUSTER_APPENDLOG;
                }
                else if (subCommand.SequenceEqual(CmdStrings.attach_sync))
                {
                    return RespCommand.CLUSTER_ATTACH_SYNC;
                }
                else if (subCommand.SequenceEqual(CmdStrings.banlist))
                {
                    return RespCommand.CLUSTER_BANLIST;
                }
                else if (subCommand.SequenceEqual(CmdStrings.begin_replica_recover))
                {
                    return RespCommand.CLUSTER_BEGIN_REPLICA_RECOVER;
                }
                else if (subCommand.SequenceEqual(CmdStrings.endpoint))
                {
                    return RespCommand.CLUSTER_ENDPOINT;
                }
                else if (subCommand.SequenceEqual(CmdStrings.failreplicationoffset))
                {
                    return RespCommand.CLUSTER_FAILREPLICATIONOFFSET;
                }
                else if (subCommand.SequenceEqual(CmdStrings.failstopwrites))
                {
                    return RespCommand.CLUSTER_FAILSTOPWRITES;
                }
                else if (subCommand.SequenceEqual(CmdStrings.FLUSHALL))
                {
                    return RespCommand.CLUSTER_FLUSHALL;
                }
                else if (subCommand.SequenceEqual(CmdStrings.SETCONFIGEPOCH))
                {
                    return RespCommand.CLUSTER_SETCONFIGEPOCH;
                }
                else if (subCommand.SequenceEqual(CmdStrings.initiate_replica_sync))
                {
                    return RespCommand.CLUSTER_INITIATE_REPLICA_SYNC;
                }
                else if (subCommand.SequenceEqual(CmdStrings.send_ckpt_file_segment))
                {
                    return RespCommand.CLUSTER_SEND_CKPT_FILE_SEGMENT;
                }
                else if (subCommand.SequenceEqual(CmdStrings.send_ckpt_metadata))
                {
                    return RespCommand.CLUSTER_SEND_CKPT_METADATA;
                }
                else if (subCommand.SequenceEqual(CmdStrings.cluster_sync))
                {
                    return RespCommand.CLUSTER_SYNC;
                }

                string errMsg = string.Format(CmdStrings.GenericErrUnknownSubCommand,
                                              Encoding.UTF8.GetString(subCommand),
                                              nameof(RespCommand.CLUSTER));
                specificErrorMsg = Encoding.UTF8.GetBytes(errMsg);
                return RespCommand.INVALID;
            }
            else if (command.SequenceEqual(CmdStrings.LATENCY))
            {
                if (count == 0)
                {
                    specificErrorMsg = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs,
                        nameof(RespCommand.LATENCY)));
                    return RespCommand.INVALID;
                }

                var subCommand = GetUpperCaseCommand(out var gotSubCommand);
                if (!gotSubCommand)
                {
                    success = false;
                    return RespCommand.NONE;
                }

                count--;

                if (subCommand.SequenceEqual(CmdStrings.HELP))
                {
                    return RespCommand.LATENCY_HELP;
                }
                else if (subCommand.SequenceEqual(CmdStrings.HISTOGRAM))
                {
                    return RespCommand.LATENCY_HISTOGRAM;
                }
                else if (subCommand.SequenceEqual(CmdStrings.RESET))
                {
                    return RespCommand.LATENCY_RESET;
                }

                string errMsg = string.Format(CmdStrings.GenericErrUnknownSubCommand,
                                              Encoding.UTF8.GetString(subCommand),
                                              nameof(RespCommand.LATENCY));
                specificErrorMsg = Encoding.UTF8.GetBytes(errMsg);
                return RespCommand.INVALID;
            }
            else if (command.SequenceEqual(CmdStrings.SLOWLOG))
            {
                if (count == 0)
                {
                    specificErrorMsg = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs,
                        nameof(RespCommand.SLOWLOG)));
                }
                else if (count >= 1)
                {
                    var subCommand = GetUpperCaseCommand(out var gotSubCommand);
                    if (!gotSubCommand)
                    {
                        success = false;
                        return RespCommand.NONE;
                    }

                    count--;

                    if (subCommand.SequenceEqual(CmdStrings.HELP))
                    {
                        return RespCommand.SLOWLOG_HELP;
                    }
                    else if (subCommand.SequenceEqual(CmdStrings.GET))
                    {
                        return RespCommand.SLOWLOG_GET;
                    }
                    else if (subCommand.SequenceEqual(CmdStrings.LEN))
                    {
                        return RespCommand.SLOWLOG_LEN;
                    }
                    else if (subCommand.SequenceEqual(CmdStrings.RESET))
                    {
                        return RespCommand.SLOWLOG_RESET;
                    }
                }
            }
            else if (command.SequenceEqual(CmdStrings.TIME))
            {
                return RespCommand.TIME;
            }
            else if (command.SequenceEqual(CmdStrings.QUIT))
            {
                return RespCommand.QUIT;
            }
            else if (command.SequenceEqual(CmdStrings.SAVE))
            {
                return RespCommand.SAVE;
            }
            else if (command.SequenceEqual(CmdStrings.EXPDELSCAN))
            {
                return RespCommand.EXPDELSCAN;
            }
            else if (command.SequenceEqual(CmdStrings.LASTSAVE))
            {
                return RespCommand.LASTSAVE;
            }
            else if (command.SequenceEqual(CmdStrings.BGSAVE))
            {
                return RespCommand.BGSAVE;
            }
            else if (command.SequenceEqual(CmdStrings.COMMITAOF))
            {
                return RespCommand.COMMITAOF;
            }
            else if (command.SequenceEqual(CmdStrings.FLUSHALL))
            {
                return RespCommand.FLUSHALL;
            }
            else if (command.SequenceEqual(CmdStrings.FLUSHDB))
            {
                return RespCommand.FLUSHDB;
            }
            else if (command.SequenceEqual(CmdStrings.FORCEGC))
            {
                return RespCommand.FORCEGC;
            }
            else if (command.SequenceEqual(CmdStrings.MIGRATE))
            {
                return RespCommand.MIGRATE;
            }
            else if (command.SequenceEqual(CmdStrings.PURGEBP))
            {
                return RespCommand.PURGEBP;
            }
            else if (command.SequenceEqual(CmdStrings.FAILOVER))
            {
                return RespCommand.FAILOVER;
            }
            else if (command.SequenceEqual(CmdStrings.MEMORY))
            {
                if (count == 0)
                {
                    specificErrorMsg = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs,
                        nameof(RespCommand.MEMORY)));
                    return RespCommand.INVALID;
                }

                var subCommand = GetUpperCaseCommand(out var gotSubCommand);
                if (!gotSubCommand)
                {
                    success = false;
                    return RespCommand.NONE;
                }

                count--;

                if (subCommand.EqualsUpperCaseSpanIgnoringCase(CmdStrings.USAGE))
                {
                    return RespCommand.MEMORY_USAGE;
                }

                string errMsg = string.Format(CmdStrings.GenericErrUnknownSubCommandNoHelp,
                                              Encoding.UTF8.GetString(subCommand),
                                              nameof(RespCommand.MEMORY));
                specificErrorMsg = Encoding.UTF8.GetBytes(errMsg);
                return RespCommand.INVALID;
            }
            else if (command.SequenceEqual(CmdStrings.MONITOR))
            {
                return RespCommand.MONITOR;
            }
            else if (command.SequenceEqual(CmdStrings.ACL))
            {
                if (count == 0)
                {
                    specificErrorMsg = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs,
                        nameof(RespCommand.ACL)));
                    return RespCommand.INVALID;
                }

                var subCommand = GetUpperCaseCommand(out var gotSubCommand);
                if (!gotSubCommand)
                {
                    success = false;
                    return RespCommand.NONE;
                }

                count--;

                if (subCommand.SequenceEqual(CmdStrings.CAT))
                {
                    return RespCommand.ACL_CAT;
                }
                else if (subCommand.SequenceEqual(CmdStrings.DELUSER))
                {
                    return RespCommand.ACL_DELUSER;
                }
                else if (subCommand.SequenceEqual(CmdStrings.GENPASS))
                {
                    return RespCommand.ACL_GENPASS;
                }
                else if (subCommand.SequenceEqual(CmdStrings.GETUSER))
                {
                    return RespCommand.ACL_GETUSER;
                }
                else if (subCommand.SequenceEqual(CmdStrings.LIST))
                {
                    return RespCommand.ACL_LIST;
                }
                else if (subCommand.SequenceEqual(CmdStrings.LOAD))
                {
                    return RespCommand.ACL_LOAD;
                }
                else if (subCommand.SequenceEqual(CmdStrings.SAVE))
                {
                    return RespCommand.ACL_SAVE;
                }
                else if (subCommand.SequenceEqual(CmdStrings.SETUSER))
                {
                    return RespCommand.ACL_SETUSER;
                }
                else if (subCommand.SequenceEqual(CmdStrings.USERS))
                {
                    return RespCommand.ACL_USERS;
                }
                else if (subCommand.SequenceEqual(CmdStrings.WHOAMI))
                {
                    return RespCommand.ACL_WHOAMI;
                }

                string errMsg = string.Format(CmdStrings.GenericErrUnknownSubCommandNoHelp,
                                              Encoding.UTF8.GetString(subCommand),
                                              nameof(RespCommand.ACL));
                specificErrorMsg = Encoding.UTF8.GetBytes(errMsg);
                return RespCommand.INVALID;
            }
            else if (command.SequenceEqual(CmdStrings.REGISTERCS))
            {
                return RespCommand.REGISTERCS;
            }
            else if (command.SequenceEqual(CmdStrings.ASYNC))
            {
                return RespCommand.ASYNC;
            }
            else if (command.SequenceEqual(CmdStrings.MODULE))
            {
                if (count == 0)
                {
                    specificErrorMsg = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs,
                        nameof(RespCommand.MODULE)));
                    return RespCommand.INVALID;
                }

                var subCommand = GetUpperCaseCommand(out var gotSubCommand);
                if (!gotSubCommand)
                {
                    success = false;
                    return RespCommand.NONE;
                }

                count--;

                if (subCommand.SequenceEqual(CmdStrings.LOADCS))
                {
                    return RespCommand.MODULE_LOADCS;
                }

                string errMsg = string.Format(CmdStrings.GenericErrUnknownSubCommandNoHelp,
                                              Encoding.UTF8.GetString(subCommand),
                                              nameof(RespCommand.MODULE));
                specificErrorMsg = Encoding.UTF8.GetBytes(errMsg);
                return RespCommand.INVALID;
            }
            else if (command.SequenceEqual(CmdStrings.PUBSUB))
            {
                if (count == 0)
                {
                    specificErrorMsg = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs,
                        nameof(RespCommand.PUBSUB)));
                    return RespCommand.INVALID;
                }

                var subCommand = GetUpperCaseCommand(out var gotSubCommand);
                if (!gotSubCommand)
                {
                    success = false;
                    return RespCommand.NONE;
                }

                count--;

                if (subCommand.SequenceEqual(CmdStrings.CHANNELS))
                {
                    return RespCommand.PUBSUB_CHANNELS;
                }
                else if (subCommand.SequenceEqual(CmdStrings.NUMSUB))
                {
                    return RespCommand.PUBSUB_NUMSUB;
                }
                else if (subCommand.SequenceEqual(CmdStrings.NUMPAT))
                {
                    return RespCommand.PUBSUB_NUMPAT;
                }

                string errMsg = string.Format(CmdStrings.GenericErrUnknownSubCommandNoHelp,
                                              Encoding.UTF8.GetString(subCommand),
                                              nameof(RespCommand.PUBSUB));
                specificErrorMsg = Encoding.UTF8.GetBytes(errMsg);
                return RespCommand.INVALID;
            }
            else if (command.SequenceEqual(CmdStrings.HCOLLECT))
            {
                return RespCommand.HCOLLECT;
            }
            else if (command.SequenceEqual(CmdStrings.DEBUG))
            {
                return RespCommand.DEBUG;
            }
            else if (command.SequenceEqual(CmdStrings.ZCOLLECT))
            {
                return RespCommand.ZCOLLECT;
            }
            // Note: The commands below are not slow path commands, so they should probably move to earlier.
            else if (command.SequenceEqual(CmdStrings.GETETAG))
            {
                return RespCommand.GETETAG;
            }

            // If this command name was not known to the slow pass, we are out of options and the command is unknown.
            return RespCommand.INVALID;
        }

        /// <summary>
        /// Attempts to skip to the end of the line ("\r\n") under the current read head.
        /// </summary>
        /// <returns>True if string terminator was found and readHead and endReadHead was changed, otherwise false. </returns>
        private bool AttemptSkipLine()
        {
            // We might have received an inline command package.Try to find the end of the line.
            logger?.LogWarning("Received malformed input message. Trying to skip line.");

            for (int stringEnd = readHead; stringEnd < bytesRead - 1; stringEnd++)
            {
                if (recvBufferPtr[stringEnd] == '\r' && recvBufferPtr[stringEnd + 1] == '\n')
                {
                    // Skip to the end of the string
                    readHead = endReadHead = stringEnd + 2;
                    return true;
                }
            }

            // We received an incomplete string and require more input.
            return false;
        }

        /// <summary>
        /// Try to parse a command out of a provided buffer.
        /// 
        /// Useful for when we have a command to validate somewhere, but aren't actually running it.
        /// </summary>
        internal RespCommand ParseRespCommandBuffer(ReadOnlySpan<byte> buffer)
        {
            var oldRecvBufferPtr = recvBufferPtr;
            var oldReadHead = readHead;
            var oldBytesRead = bytesRead;

            try
            {
                recvBufferPtr = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(buffer));
                readHead = 0;
                bytesRead = buffer.Length;

                var parsed = ParseCommand(writeErrorOnFailure: false, out var res);
                Debug.Assert(res, "Should never pass an incomplete buffer");

                return parsed;
            }
            finally
            {
                recvBufferPtr = oldRecvBufferPtr;
                readHead = oldReadHead;
                bytesRead = oldBytesRead;
            }
        }

        /// <summary>
        /// Version of <see cref="ParseRespCommandBuffer(ReadOnlySpan{byte})"/> for fuzzing.
        /// 
        /// Expects (and allows) partial commands.
        /// 
        /// Returns true if a command was succesfully parsed
        /// </summary>
        internal bool FuzzParseCommandBuffer(ReadOnlySpan<byte> buffer, out RespCommand cmd)
        {
            var oldRecvBufferPtr = recvBufferPtr;
            var oldReadHead = readHead;
            var oldBytesRead = bytesRead;

            try
            {
                // Slap the buffer in
                recvBufferPtr = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(buffer));
                readHead = 0;
                bytesRead = buffer.Length;

                // Duplicate check in ProcessMessages
                if ((bytesRead - readHead) >= 4)
                {
                    cmd = ParseCommand(writeErrorOnFailure: false, out _);
                }
                else
                {
                    cmd = RespCommand.INVALID;
                }

                return cmd != RespCommand.INVALID;
            }
            finally
            {
                // Validate state post-fuzz
                if (readHead > buffer.Length)
                {
                    throw new InvalidOperationException("readHead was left past end of buffer");
                }

                if (!buffer.IsEmpty)
                {
                    if (Unsafe.IsAddressLessThan(ref Unsafe.AsRef<byte>(recvBufferPtr), ref MemoryMarshal.GetReference(buffer)))
                    {
                        throw new InvalidOperationException("recvBufferPtr was left before buffer");
                    }

                    if (Unsafe.IsAddressGreaterThan(ref Unsafe.AsRef<byte>(recvBufferPtr), ref Unsafe.Add(ref MemoryMarshal.GetReference(buffer), buffer.Length)))
                    {
                        throw new InvalidOperationException("recvBufferPtr was left after buffer");
                    }
                }

                // Remove references to temporary buffer
                recvBufferPtr = oldRecvBufferPtr;
                readHead = oldReadHead;
                bytesRead = oldBytesRead;
            }
        }

        /// <summary>
        /// Parses the command from the given input buffer.
        /// </summary>
        /// <param name="writeErrorOnFailure">If true, when a parsing error occurs an error response will written.</param>
        /// <param name="success">Whether processing should continue or a parsing error occurred (e.g. out of tokens).</param>
        /// <returns>Command parsed from the input buffer.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private RespCommand ParseCommand(bool writeErrorOnFailure, out bool success)
        {
            metaCommandInfo.MetaCommand = RespMetaCommand.None;

            // Initialize count as -1 (i.e., read head has not been advanced)
            var count = -1;
            success = true;
            endReadHead = readHead;

            // Attempt parsing using fast parse pass for most common operations
            var cmd = FastParseCommand(ref count);

            // If we have not found a command, continue parsing on slow path
            if (cmd == RespCommand.NONE)
            {
                cmd = ArrayParseCommand(writeErrorOnFailure, ref count, ref success);
                if (!success) return cmd;
            }

            if (cmd.IsMetaCommand())
            {
                // Get the meta command and its argument count
                var (metaCmd, metaCmdArgCount) = GetMetaCommandAndArgumentCount(cmd);
                count -= metaCmdArgCount;

                metaCommandInfo.MetaCommand = metaCmd;

                // Set up meta command parse state
                metaCommandInfo.MetaCommandParseState.Initialize(metaCmdArgCount);
                var currPtr = recvBufferPtr + readHead;

                if (metaCmdArgCount > 0)
                {
                    for (var i = 0; i < metaCmdArgCount; i++)
                    {
                        if (!metaCommandInfo.MetaCommandParseState.Read(i, ref currPtr, recvBufferPtr + bytesRead))
                            return RespCommand.INVALID;
                    }

                    // Move read head to the start of the main command
                    readHead = (int)(currPtr - recvBufferPtr);
                }

                // Attempt parsing nested main command
                cmd = FastParseCommand(ref count);
                if (cmd == RespCommand.SET)
                    cmd = RespCommand.SETEXNX;

                // If we have not found a command, continue parsing on slow path
                if (cmd == RespCommand.NONE)
                {
                    count++;
                    cmd = ArrayParseCommand(writeErrorOnFailure, ref count, ref success, skipReadCount: true);
                    if (!success) return cmd;
                }
            }
            else
            {
                metaCommandInfo.MetaCommand = RespMetaCommand.None;
            }

            // Set up parse state
            parseState.Initialize(count);

            var ptr = recvBufferPtr + readHead;

            // Read main command arguments
            for (var i = 0; i < count; i++)
            {
                if (!parseState.Read(i, ref ptr, recvBufferPtr + bytesRead))
                {
                    success = false;
                    return RespCommand.INVALID;
                }
            }

            endReadHead = (int)(ptr - recvBufferPtr);

            if (storeWrapper.serverOptions.EnableAOF && storeWrapper.serverOptions.WaitForCommit)
                HandleAofCommitMode(cmd);

            return cmd;
        }

        private (RespMetaCommand metaCommand, int argCount) GetMetaCommandAndArgumentCount(RespCommand cmd)
        {
            if (!RespCommandsInfo.TryGetSimpleRespCommandInfo(cmd, out var info, logger: logger))
                throw new GarnetException($"Unable to retrieve simple command info for command: {cmd}");

            var argCount = info.Arity - 1;

            var metaCommand = cmd switch
            {
                RespCommand.EXECWITHETAG => RespMetaCommand.ExecWithEtag,
                RespCommand.EXECIFMATCH => RespMetaCommand.ExecIfMatch,
                RespCommand.EXECIFNOTMATCH => RespMetaCommand.ExecIfNotMatch,
                RespCommand.EXECIFGREATER => RespMetaCommand.ExecIfGreater,
                _ => throw new GarnetException($"Invalid meta command: {cmd}")
            };

            return (metaCommand, argCount);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void HandleAofCommitMode(RespCommand cmd)
        {
            // Reset waitForAofBlocking if there is no pending unsent data on the network
            if (dcurr == networkSender.GetResponseObjectHead())
                waitForAofBlocking = false;

            // During a started transaction (Network Skip mode) the cmd is not executed so we can safely skip setting the AOF blocking flag
            if (txnManager.state == TxnState.Started)
                return;

            /* 
                If a previous command marked AOF for blocking we should not change AOF blocking flag.
                If no previous command marked AOF for blocking, then we only change AOF flag to block
                if the current command is AOF dependent.
            */
            waitForAofBlocking = waitForAofBlocking || !cmd.IsAofIndependent();
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private RespCommand ArrayParseCommand(bool writeErrorOnFailure, ref int count, ref bool success, bool skipReadCount = false)
        {
            RespCommand cmd;
            ReadOnlySpan<byte> specificErrorMessage = default;
            endReadHead = readHead;
            var ptr = recvBufferPtr + readHead;

            // See if input command is all upper-case. If not, convert and try fast parse pass again.
            if (MakeUpperCase(ptr, bytesRead - readHead))
            {
                count = -1;
                cmd = FastParseCommand(ref count);
                if (cmd != RespCommand.NONE)
                {
                    return cmd;
                }
            }

            if (!skipReadCount)
            {
                // Ensure we are attempting to read a RESP array header
                if (recvBufferPtr[readHead] != '*')
                {
                    // We might have received an inline command package. Skip until the end of the line in the input package.
                    success = AttemptSkipLine();
                    return RespCommand.INVALID;
                }

                // Read the array length
                if (!RespReadUtils.TryReadUnsignedArrayLength(out count, ref ptr, recvBufferPtr + bytesRead))
                {
                    success = false;
                    return RespCommand.INVALID;
                }

                // Move readHead to start of command payload
                readHead = (int)(ptr - recvBufferPtr);
            }

            // Try parsing the most important variable-length commands
            cmd = FastParseArrayCommand(ref count, ref specificErrorMessage);

            if (cmd == RespCommand.NONE)
            {
                cmd = SlowParseCommand(ref count, ref specificErrorMessage, out success);
            }

            // Parsing for command name was successful, but the command is unknown
            if (writeErrorOnFailure && success && cmd == RespCommand.INVALID)
            {
                if (!specificErrorMessage.IsEmpty)
                {
                    while (!RespWriteUtils.TryWriteError(specificErrorMessage, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    // Return "Unknown RESP Command" message
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_UNK_CMD, ref dcurr, dend))
                        SendAndReset();
                }
            }
            return cmd;
        }
    }
}