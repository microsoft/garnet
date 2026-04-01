// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
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
        GETIFNOTMATCH,
        GETRANGE,
        GETWITHETAG,
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
        DELIFGREATER,
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
        SETIFMATCH,
        SETIFGREATER,
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
        // Per-session MRU command cache: 2 entries caching the last matched command patterns.
        // Sits after SIMD patterns but before scalar switch — catches repeated Tier 1b/2 commands
        // (HSET, LPUSH, ZADD etc.) in 3 ops instead of falling through to the hash table.
        // NOTE: All fields default to zero (RespCommand.NONE = 0x00, Vector128 = all zeros),
        // so the cache starts empty and the _cachedCmd0 != RespCommand.NONE check is safe.
        private Vector128<byte> _cachedPattern0, _cachedMask0;
        private RespCommand _cachedCmd0;
        private byte _cachedLen0, _cachedCount0;

        private Vector128<byte> _cachedPattern1, _cachedMask1;
        private RespCommand _cachedCmd1;
        private byte _cachedLen1, _cachedCount1;
        /// <summary>
        /// Fast-parses command type for inline RESP commands, starting at the current read head in the receive buffer
        /// and advances read head.
        /// </summary>
        /// <param name="count">Outputs the number of arguments stored with the command.</param>
        /// <returns>RespCommand that was parsed or RespCommand.NONE, if no command was matched in this pass.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private RespCommand FastParseInlineCommand(out int count)
        {
            byte* ptr = recvBufferPtr + readHead;
            count = 0;

            if (bytesRead - readHead >= 6)
            {
                if ((*(ushort*)(ptr + 4) == MemoryMarshal.Read<ushort>("\r\n"u8)))
                {
                    // Optimistically increase read head
                    readHead += 6;

                    if ((*(uint*)ptr) == MemoryMarshal.Read<uint>("PING"u8))
                    {
                        return RespCommand.PING;
                    }

                    if ((*(uint*)ptr) == MemoryMarshal.Read<uint>("QUIT"u8))
                    {
                        return RespCommand.QUIT;
                    }

                    // Decrease read head, if no match was found
                    readHead -= 6;
                }
            }

            return RespCommand.NONE;
        }

        /// <summary>
        /// Fast-parses for command type using SIMD Vector128 matching for the most common commands,
        /// falling back to scalar ulong matching for the rest.
        /// </summary>
        /// <param name="count">Outputs the number of arguments stored with the command</param>
        /// <returns>RespCommand that was parsed or RespCommand.NONE, if no command was matched in this pass.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private RespCommand FastParseCommand(out int count)
        {
            var ptr = recvBufferPtr + readHead;
            var remainingBytes = bytesRead - readHead;

            // SIMD fast path: match the full RESP-encoded pattern (*N\r\n$L\r\nCMD\r\n) in a
            // single Vector128 comparison. Each check validates the header AND command name
            // simultaneously: 1 load + 1 AND (mask) + 1 EqualsAll = 3 ops per candidate.
            if (Vector128.IsHardwareAccelerated && remainingBytes >= 16)
            {
                var input = Vector128.LoadUnsafe(ref Unsafe.AsRef<byte>(ptr));

                // 13-byte patterns: 3-char commands (GET, SET, DEL, TTL)
                var m13 = Vector128.BitwiseAnd(input, s_mask13);
                if (Vector128.EqualsAll(m13, s_GET)) { readHead += 13; count = 1; return RespCommand.GET; }
                if (Vector128.EqualsAll(m13, s_SET)) { readHead += 13; count = 2; return RespCommand.SET; }
                if (Vector128.EqualsAll(m13, s_DEL)) { readHead += 13; count = 1; return RespCommand.DEL; }
                if (Vector128.EqualsAll(m13, s_TTL)) { readHead += 13; count = 1; return RespCommand.TTL; }

                // 14-byte patterns: 4-char commands (PING, INCR, DECR, EXEC, PTTL)
                var m14 = Vector128.BitwiseAnd(input, s_mask14);
                if (Vector128.EqualsAll(m14, s_PING)) { readHead += 14; count = 0; return RespCommand.PING; }
                if (Vector128.EqualsAll(m14, s_INCR)) { readHead += 14; count = 1; return RespCommand.INCR; }
                if (Vector128.EqualsAll(m14, s_DECR)) { readHead += 14; count = 1; return RespCommand.DECR; }
                if (Vector128.EqualsAll(m14, s_EXEC)) { readHead += 14; count = 0; return RespCommand.EXEC; }
                if (Vector128.EqualsAll(m14, s_PTTL)) { readHead += 14; count = 1; return RespCommand.PTTL; }

                // 15-byte patterns: 5-char commands (MULTI, SETNX, SETEX)
                var m15 = Vector128.BitwiseAnd(input, s_mask15);
                if (Vector128.EqualsAll(m15, s_MULTI)) { readHead += 15; count = 0; return RespCommand.MULTI; }
                if (Vector128.EqualsAll(m15, s_SETNX)) { readHead += 15; count = 2; return RespCommand.SETNX; }
                if (Vector128.EqualsAll(m15, s_SETEX)) { readHead += 15; count = 3; return RespCommand.SETEX; }

                // 16-byte patterns: 6-char commands (no mask — exact 16-byte match)
                if (Vector128.EqualsAll(input, s_EXISTS)) { readHead += 16; count = 1; return RespCommand.EXISTS; }
                if (Vector128.EqualsAll(input, s_GETDEL)) { readHead += 16; count = 1; return RespCommand.GETDEL; }
                if (Vector128.EqualsAll(input, s_APPEND)) { readHead += 16; count = 2; return RespCommand.APPEND; }
                if (Vector128.EqualsAll(input, s_INCRBY)) { readHead += 16; count = 2; return RespCommand.INCRBY; }
                if (Vector128.EqualsAll(input, s_DECRBY)) { readHead += 16; count = 2; return RespCommand.DECRBY; }
                if (Vector128.EqualsAll(input, s_PSETEX)) { readHead += 16; count = 3; return RespCommand.PSETEX; }

                // MRU cache check: catches repeated commands that aren't in the SIMD pattern table
                // (e.g., HSET, LPUSH, ZADD, ZRANGEBYSCORE). Same 3-op cost as one SIMD pattern check.
                if (_cachedCmd0 != RespCommand.NONE)
                {
                    if (Vector128.EqualsAll(Vector128.BitwiseAnd(input, _cachedMask0), _cachedPattern0))
                    {
                        readHead += _cachedLen0;
                        count = _cachedCount0;
                        return _cachedCmd0;
                    }

                    if (_cachedCmd1 != RespCommand.NONE &&
                        Vector128.EqualsAll(Vector128.BitwiseAnd(input, _cachedMask1), _cachedPattern1))
                    {
                        readHead += _cachedLen1;
                        count = _cachedCount1;
                        // Promote slot 1 → slot 0 (swap)
                        (_cachedPattern0, _cachedPattern1) = (_cachedPattern1, _cachedPattern0);
                        (_cachedMask0, _cachedMask1) = (_cachedMask1, _cachedMask0);
                        (_cachedCmd0, _cachedCmd1) = (_cachedCmd1, _cachedCmd0);
                        (_cachedLen0, _cachedLen1) = (_cachedLen1, _cachedLen0);
                        (_cachedCount0, _cachedCount1) = (_cachedCount1, _cachedCount0);
                        return _cachedCmd0;
                    }
                }
            }

            // Scalar fast path: uses ulong comparisons to match common commands.
            // On SIMD hardware, this catches commands when remainingBytes < 16 (SIMD needs 16).
            // On non-SIMD hardware, this is the primary fast path.

            // Check if the package starts with "*_\r\n$_\r\n" (_ = masked out),
            // i.e. an array with a single-digit length and single-digit first string length.
            if ((remainingBytes >= 8) && (*(ulong*)ptr & 0xFFFF00FFFFFF00FF) == MemoryMarshal.Read<ulong>("*\0\r\n$\0\r\n"u8))
            {
                // Extract total element count from the array header.
                // NOTE: Subtracting one to account for first token being parsed.
                count = ptr[1] - '1';

                // Extract length of the first string header
                var length = ptr[5] - '0';

                var oldReadHead = readHead;

                // Ensure valid command name length (1-9) and that the complete command string
                // is contained in the package. Otherwise fall through to return NONE.
                // 10 bytes = "*_\r\n$_\r\n" (8 bytes) + "\r\n" (2 bytes) at end of command name
                if (length is > 0 and <= 9 && remainingBytes >= length + 10)
                {
                    // Optimistically advance read head to the end of the command name
                    readHead += length + 10;

                    // Last 8 byte word of the command name, for quick comparison
                    var lastWord = *(ulong*)(ptr + length + 2);

                    //
                    // Fast path for common commands with fixed numbers of arguments
                    //

                    // Only check against commands with the correct count and length.

                    return ((count << 4) | length) switch
                    {
                        // (1) Same fixed-arg hot commands as SIMD path — fallback when remainingBytes < 16
                        4 when lastWord == MemoryMarshal.Read<ulong>("\r\nPING\r\n"u8) => RespCommand.PING,
                        4 when lastWord == MemoryMarshal.Read<ulong>("\r\nEXEC\r\n"u8) => RespCommand.EXEC,
                        5 when lastWord == MemoryMarshal.Read<ulong>("\nMULTI\r\n"u8) => RespCommand.MULTI,
                        (1 << 4) | 3 when lastWord == MemoryMarshal.Read<ulong>("3\r\nGET\r\n"u8) => RespCommand.GET,
                        (1 << 4) | 3 when lastWord == MemoryMarshal.Read<ulong>("3\r\nDEL\r\n"u8) => RespCommand.DEL,
                        (1 << 4) | 3 when lastWord == MemoryMarshal.Read<ulong>("3\r\nTTL\r\n"u8) => RespCommand.TTL,
                        (1 << 4) | 4 when lastWord == MemoryMarshal.Read<ulong>("\r\nINCR\r\n"u8) => RespCommand.INCR,
                        (1 << 4) | 4 when lastWord == MemoryMarshal.Read<ulong>("\r\nPTTL\r\n"u8) => RespCommand.PTTL,
                        (1 << 4) | 4 when lastWord == MemoryMarshal.Read<ulong>("\r\nDECR\r\n"u8) => RespCommand.DECR,
                        (1 << 4) | 4 when lastWord == MemoryMarshal.Read<ulong>("EXISTS\r\n"u8) => RespCommand.EXISTS,
                        (1 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("GETDEL\r\n"u8) => RespCommand.GETDEL,
                        (2 << 4) | 3 when lastWord == MemoryMarshal.Read<ulong>("3\r\nSET\r\n"u8) => RespCommand.SET,
                        (2 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("INCRBY\r\n"u8) => RespCommand.INCRBY,
                        (2 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("DECRBY\r\n"u8) => RespCommand.DECRBY,
                        (2 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("APPEND\r\n"u8) => RespCommand.APPEND,
                        (2 << 4) | 5 when lastWord == MemoryMarshal.Read<ulong>("\nSETNX\r\n"u8) => RespCommand.SETNX,
                        (3 << 4) | 5 when lastWord == MemoryMarshal.Read<ulong>("\nSETEX\r\n"u8) => RespCommand.SETEX,
                        (3 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("PSETEX\r\n"u8) => RespCommand.PSETEX,

                        // (2) Hot commands too long for SIMD (name > 6 chars, exceeds 16-byte Vector128)
                        (2 << 4) | 7 when lastWord == MemoryMarshal.Read<ulong>("UBLISH\r\n"u8) && ptr[8] == 'P' => RespCommand.PUBLISH,
                        (2 << 4) | 8 when lastWord == MemoryMarshal.Read<ulong>("UBLISH\r\n"u8) && *(ushort*)(ptr + 8) == MemoryMarshal.Read<ushort>("SP"u8) => RespCommand.SPUBLISH,
                        (3 << 4) | 8 when lastWord == MemoryMarshal.Read<ulong>("TRANGE\r\n"u8) && *(ushort*)(ptr + 8) == MemoryMarshal.Read<ushort>("SE"u8) => RespCommand.SETRANGE,
                        (3 << 4) | 8 when lastWord == MemoryMarshal.Read<ulong>("TRANGE\r\n"u8) && *(ushort*)(ptr + 8) == MemoryMarshal.Read<ushort>("GE"u8) => RespCommand.GETRANGE,

                        // (3) Hot variable-arg commands (arg count varies, cannot be SIMD or MRU cached)
                        _ => ((length << 4) | count) switch
                        {
                            >= ((3 << 4) | 3) and <= ((3 << 4) | 7) when lastWord == MemoryMarshal.Read<ulong>("3\r\nSET\r\n"u8) => RespCommand.SETEXNX,
                            >= ((5 << 4) | 1) and <= ((5 << 4) | 3) when lastWord == MemoryMarshal.Read<ulong>("\nGETEX\r\n"u8) => RespCommand.GETEX,
                            >= ((6 << 4) | 2) and <= ((6 << 4) | 3) when lastWord == MemoryMarshal.Read<ulong>("EXPIRE\r\n"u8) => RespCommand.EXPIRE,
                            >= ((7 << 4) | 2) and <= ((7 << 4) | 3) when lastWord == MemoryMarshal.Read<ulong>("EXPIRE\r\n"u8) && ptr[8] == 'P' => RespCommand.PEXPIRE,
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
                return FastParseInlineCommand(out count);
            }

            count = -1;
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
            RespCommand cmd = RespCommand.INVALID;

            // Initialize count as -1 (i.e., read head has not been advanced)
            int count = -1;
            success = true;
            endReadHead = readHead;

            // Attempt parsing using fast parse pass for most common operations
            cmd = FastParseCommand(out count);

            // If we have not found a command, continue parsing on slow path
            if (cmd == RespCommand.NONE)
            {
                var cmdStartOffset = readHead; // Save position before ArrayParseCommand advances it
                cmd = ArrayParseCommand(writeErrorOnFailure, ref count, ref success);
                if (!success) return cmd;

                // Update MRU cache for commands resolved by hash table.
                // Exclude custom commands — they have runtime-registered names.
                if (Vector128.IsHardwareAccelerated &&
                    cmd != RespCommand.INVALID && cmd != RespCommand.NONE &&
                    cmd != RespCommand.CustomTxn && cmd != RespCommand.CustomProcedure &&
                    cmd != RespCommand.CustomRawStringCmd && cmd != RespCommand.CustomObjCmd)
                {
                    UpdateCommandCache(cmdStartOffset, cmd, count);
                }
            }

            Debug.Assert(cmd != RespCommand.NONE, "ParseCommand should never return NONE - expected INVALID for unrecognized commands");
            Debug.Assert(count >= 0 || cmd == RespCommand.INVALID, "Argument count must be non-negative for valid commands");

            // Set up parse state
            parseState.Initialize(count);
            var ptr = recvBufferPtr + readHead;
            for (int i = 0; i < count; i++)
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

        /// <summary>
        /// Update the MRU command cache with a newly matched command from the hash table.
        /// Captures the first 16 bytes of the RESP encoding and the appropriate mask so that
        /// the cache check in FastParseCommand can match it via Vector128.EqualsAll.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private void UpdateCommandCache(int cmdStartOffset, RespCommand cmd, int argCount)
        {
            var ptr = recvBufferPtr + cmdStartOffset;
            var availableBytes = bytesRead - cmdStartOffset;

            // Only cache commands where we have at least 16 bytes to load a full Vector128
            if (availableBytes < 16) return;

            // Compute how many bytes the parse actually consumed (command + subcommand if any)
            var consumedBytes = readHead - cmdStartOffset;

            // Only cache if the full parse fits within 16 bytes (one Vector128)
            if (consumedBytes < 13 || consumedBytes > 16) return;

            var input = Vector128.LoadUnsafe(ref Unsafe.AsRef<byte>(ptr));

            // Select the appropriate mask — covers exactly the consumed bytes
            Vector128<byte> mask = consumedBytes switch
            {
                16 => Vector128<byte>.AllBitsSet,
                15 => s_mask15,
                14 => s_mask14,
                13 => s_mask13,
                _ => Vector128<byte>.Zero
            };

            if (mask == Vector128<byte>.Zero) return;

            var pattern = Vector128.BitwiseAnd(input, mask);

            // Demote slot 0 → slot 1, promote new match → slot 0
            _cachedPattern1 = _cachedPattern0;
            _cachedMask1 = _cachedMask0;
            _cachedCmd1 = _cachedCmd0;
            _cachedLen1 = _cachedLen0;
            _cachedCount1 = _cachedCount0;

            _cachedPattern0 = pattern;
            _cachedMask0 = mask;
            _cachedCmd0 = cmd;
            _cachedLen0 = (byte)consumedBytes;
            _cachedCount0 = (byte)argCount;
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
        private RespCommand ArrayParseCommand(bool writeErrorOnFailure, ref int count, ref bool success)
        {
            RespCommand cmd = RespCommand.INVALID;
            ReadOnlySpan<byte> specificErrorMessage = default;
            endReadHead = readHead;
            var ptr = recvBufferPtr + readHead;

            // See if input command is all upper-case. If not, convert and try fast parse pass again.
            if (MakeUpperCase(ptr, bytesRead - readHead))
            {
                cmd = FastParseCommand(out count);
                if (cmd != RespCommand.NONE)
                {
                    return cmd;
                }
            }

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

            // Extract command name via HashLookupCommand (reads $len\r\n header via GetCommand, relies on prior MakeUpperCase() pass, and advances readHead)
            cmd = HashLookupCommand(ref count, ref specificErrorMessage, out success);

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

        /// <summary>
        /// Hash-based command lookup. Extracts the command name from the RESP buffer,
        /// looks it up in the static hash table, and handles subcommand dispatch.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private RespCommand HashLookupCommand(ref int count, ref ReadOnlySpan<byte> specificErrorMessage, out bool success)
        {
            // Extract the command name (reads $len\r\n...\r\n, advances readHead).
            // MakeUpperCase has already uppercased the first command name token in the buffer,
            // so we use GetCommand (no redundant ToUpperInPlace call).
            // NOTE: MakeUpperCase only uppercases the first token — subcommand names are
            // uppercased separately via GetUpperCaseCommand in HandleSubcommandLookup.
            var command = GetCommand(out success);
            if (!success)
            {
                return RespCommand.INVALID;
            }

            // Account for the command name being taken off the read head
            count -= 1;

            // Hash table lookup for the primary command (checked before custom commands
            // since built-in commands are far more common).
            // Single probe returns both the command and whether it has subcommands.
            fixed (byte* namePtr = command)
            {
                var cmd = RespCommandHashLookup.Lookup(namePtr, command.Length, out var hasSubcommands);

                if (cmd == RespCommand.NONE)
                {
                    // Not a built-in command — check custom commands
                    if (TryParseCustomCommand(command, out var customCmd))
                    {
                        return customCmd;
                    }

                    // Not a built-in or custom command
                    return RespCommand.INVALID;
                }

                // Commands with subcommands — dispatch via subcommand hash table
                if (hasSubcommands)
                {
                    return HandleSubcommandLookup(cmd, ref count, ref specificErrorMessage, out success);
                }

                return cmd;
            }
        }

        /// <summary>
        /// Handles subcommand dispatch for parent commands (CLUSTER, CONFIG, CLIENT, etc.)
        /// using per-parent hash tables.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private RespCommand HandleSubcommandLookup(RespCommand parentCmd, ref int count, ref ReadOnlySpan<byte> specificErrorMessage, out bool success)
        {
            success = true;

            // COMMAND with no args returns RespCommand.COMMAND (lists all commands)
            if (parentCmd == RespCommand.COMMAND && count == 0)
            {
                return RespCommand.COMMAND;
            }

            // Most parent commands require at least one subcommand argument
            if (count == 0)
            {
                specificErrorMessage = parentCmd == RespCommand.BITOP
                    ? CmdStrings.RESP_SYNTAX_ERROR
                    : Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs, parentCmd.ToString()));
                return RespCommand.INVALID;
            }

            // Extract and uppercase the subcommand name
            var subCommand = GetUpperCaseCommand(out var gotSubCommand);
            if (!gotSubCommand)
            {
                success = false;
                return RespCommand.NONE;
            }

            count--;

            // Hash table lookup for the subcommand
            fixed (byte* subNamePtr = subCommand)
            {
                var subCmd = RespCommandHashLookup.LookupSubcommand(parentCmd, subNamePtr, subCommand.Length);
                if (subCmd != RespCommand.NONE)
                {
                    return subCmd;
                }
            }

            // Generate error message for unknown subcommand
            specificErrorMessage = parentCmd switch
            {
                RespCommand.BITOP => CmdStrings.RESP_SYNTAX_ERROR,
                RespCommand.CLUSTER or RespCommand.LATENCY =>
                    Encoding.UTF8.GetBytes(string.Format(CmdStrings.GenericErrUnknownSubCommand,
                        Encoding.UTF8.GetString(subCommand), parentCmd.ToString())),
                _ =>
                    Encoding.UTF8.GetBytes(string.Format(CmdStrings.GenericErrUnknownSubCommandNoHelp,
                        Encoding.UTF8.GetString(subCommand), parentCmd.ToString())),
            };
            return RespCommand.INVALID;
        }
    }
}