// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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
    public enum RespCommand : byte
    {
        NONE = 0x00,

        // Read-only commands
        BITCOUNT,
        BITFIELD_RO,
        BITPOS,
        COSCAN,
        DBSIZE,
        EXISTS,
        GEODIST,
        GEOHASH,
        GEOPOS,
        GEOSEARCH,
        GET,
        GETBIT,
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
        LINDEX,
        LLEN,
        LRANGE,
        MEMORY_USAGE,
        MGET,
        PFCOUNT,
        PTTL,
        SCAN,
        SCARD,
        SDIFF,
        SINTER,
        SISMEMBER,
        SMEMBERS,
        SRANDMEMBER,
        SSCAN,
        STRLEN,
        SUNION,
        TTL,
        TYPE,
        ZCARD,
        ZCOUNT,
        ZDIFF,
        ZLEXCOUNT,
        ZMSCORE,
        ZRANDMEMBER,
        ZRANGE,
        ZRANGEBYSCORE,
        ZRANK,
        ZREVRANGE,
        ZREVRANGEBYSCORE,
        ZREVRANK,
        ZSCAN,
        ZSCORE, // Note: Update OneIfRead if adding new read commands after this

        // Write commands
        APPEND, // Note: Update OneIfWrite if adding new write commands before this
        BITFIELD,
        DECR,
        DECRBY,
        DEL,
        EXPIRE,
        FLUSHDB,
        GEOADD,
        GETDEL,
        HDEL,
        HINCRBY,
        HINCRBYFLOAT,
        HMSET,
        HSET,
        HSETNX,
        INCR,
        INCRBY,
        LINSERT,
        LMOVE,
        LPOP,
        LPUSH,
        LPUSHX,
        LREM,
        LSET,
        LTRIM,
        BLPOP,
        BRPOP,
        BLMOVE,
        MIGRATE,
        MSET,
        MSETNX,
        PERSIST,
        PEXPIRE,
        PFADD,
        PFMERGE,
        PSETEX,
        RENAME,
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
        SETKEEPTTL,
        SETKEEPTTLXX,
        SETRANGE,
        SINTERSTORE,
        SMOVE,
        SPOP,
        SREM,
        SUNIONSTORE,
        UNLINK,
        ZADD,
        ZINCRBY,
        ZPOPMAX,
        ZPOPMIN,
        ZREM,
        ZREMRANGEBYLEX,
        ZREMRANGEBYRANK,
        ZREMRANGEBYSCORE,

        // BITOP is the true command, AND|OR|XOR|NOT are psuedo-subcommands
        BITOP,
        BITOP_AND,
        BITOP_OR,
        BITOP_XOR,
        BITOP_NOT, // Note: Update OneIfWrite if adding new write commands after this

        // Neither read nor write commands
        ASYNC,

        PING,

        PUBLISH,
        SUBSCRIBE,
        PSUBSCRIBE,
        UNSUBSCRIBE,
        PUNSUBSCRIBE,
        ASKING,
        SELECT,
        ECHO,
        CLIENT,

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
        SAVE,
        LASTSAVE,
        BGSAVE,
        COMMITAOF,
        FORCEGC,
        FAILOVER,

        // Custom commands
        CustomTxn,
        CustomCmd,
        CustomObjCmd,

        ACL,
        ACL_CAT,
        ACL_DELUSER,
        ACL_LIST,
        ACL_LOAD,
        ACL_SAVE,
        ACL_SETUSER,
        ACL_USERS,
        ACL_WHOAMI,

        COMMAND,
        COMMAND_COUNT,
        COMMAND_INFO,

        MEMORY,
        // MEMORY_USAGE is a read-only command, so moved up

        WATCH,
        WATCH_MS,
        WATCH_OS,

        CONFIG,
        CONFIG_GET,
        CONFIG_REWRITE,
        CONFIG_SET,

        LATENCY,
        LATENCY_HELP,
        LATENCY_HISTOGRAM,
        LATENCY_RESET,

        CLUSTER,
        CLUSTER_ADDSLOTS, // Note: Update IsClusterSubCommand if adding new cluster subcommands before this
        CLUSTER_ADDSLOTSRANGE,
        CLUSTER_AOFSYNC,
        CLUSTER_APPENDLOG,
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
        CLUSTER_SLOTSTATE, // Note: Update IsClusterSubCommand if adding new cluster subcommands after this

        // Don't require AUTH (if auth is enabled)
        AUTH, // Note: Update IsNoAuth if adding new no-auth commands before this
        HELLO,
        QUIT, // Note: Update IsNoAuth if adding new no-auth commands after this

        INVALID = 0xFF,
    }

    /// <summary>
    /// Extension methods for <see cref="RespCommand"/>.
    /// </summary>
    public static class RespCommandExtensions
    {
        private static readonly RespCommand[] ExpandedSET = [RespCommand.SETEXNX, RespCommand.SETEXXX, RespCommand.SETKEEPTTL, RespCommand.SETKEEPTTLXX];
        private static readonly RespCommand[] ExpandedBITOP = [RespCommand.BITOP_AND, RespCommand.BITOP_NOT, RespCommand.BITOP_OR, RespCommand.BITOP_XOR];

        /// <summary>
        /// Turns any not-quite-a-real-command entries in <see cref="RespCommand"/> into the equivalent command
        /// for ACL'ing purposes.
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
                    RespCommand.BITOP_AND or RespCommand.BITOP_NOT or RespCommand.BITOP_OR or RespCommand.BITOP_XOR => RespCommand.BITOP,
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

        /// <summary>
        /// Returns 1 if <paramref name="cmd"/> is a write command.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong OneIfWrite(this RespCommand cmd)
        {
            // If cmd < RespCommand.Append - underflows, setting high bits
            uint test = (uint)((int)cmd - (int)RespCommand.APPEND);

            // Force to be branchless for same reasons as OneIfRead
            bool inRange = test <= (RespCommand.BITOP_NOT - RespCommand.APPEND);
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
            bool inRange = cmd <= RespCommand.ZSCORE;
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
            bool inRange = test <= (RespCommand.CLUSTER_SLOTSTATE - RespCommand.CLUSTER_ADDSLOTS);
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
        /// Fast-parses for command type, starting at the current read head in the receive buffer
        /// and advances the read head to the position after the parsed command.
        /// </summary>
        /// <param name="count">Outputs the number of arguments stored with the command</param>
        /// <returns>RespCommand that was parsed or RespCommand.NONE, if no command was matched in this pass.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private RespCommand FastParseCommand(out int count)
        {
            var ptr = recvBufferPtr + readHead;
            var remainingBytes = bytesRead - readHead;

            // Check if the package starts with "*_\r\n$_\r\n" (_ = masked out),
            // i.e. an array with a single-digit length and single-digit first string length.
            if ((remainingBytes >= 8) && (*(ulong*)ptr & 0xFFFF00FFFFFF00FF) == MemoryMarshal.Read<ulong>("*\0\r\n$\0\r\n"u8))
            {
                // Extract total element count from the array header.
                // NOTE: Subtracting one to account for first token being parsed.
                count = ptr[1] - '1';
                Debug.Assert(count is >= 0 and < 9);

                // Extract length of the first string header
                var length = ptr[5] - '0';
                Debug.Assert(length is > 0 and <= 9);

                var oldReadHead = readHead;

                // Ensure that the complete command string is contained in the package. Otherwise exit early.
                // Include 10 bytes to account for array and command string headers, and terminator
                // 10 bytes = "*_\r\n$_\r\n" (8 bytes) + "\r\n" (2 bytes) at end of command name
                if (remainingBytes >= length + 10)
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
                        (1 << 4) | 4 when lastWord == MemoryMarshal.Read<ulong>("\r\nINCR\r\n"u8) => RespCommand.INCR,
                        (1 << 4) | 4 when lastWord == MemoryMarshal.Read<ulong>("\r\nPTTL\r\n"u8) => RespCommand.PTTL,
                        (1 << 4) | 4 when lastWord == MemoryMarshal.Read<ulong>("\r\nDECR\r\n"u8) => RespCommand.DECR,
                        (1 << 4) | 4 when lastWord == MemoryMarshal.Read<ulong>("EXISTS\r\n"u8) => RespCommand.EXISTS,
                        (1 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("GETDEL\r\n"u8) => RespCommand.GETDEL,
                        (1 << 4) | 7 when lastWord == MemoryMarshal.Read<ulong>("ERSIST\r\n"u8) && ptr[8] == 'P' => RespCommand.PERSIST,
                        (1 << 4) | 7 when lastWord == MemoryMarshal.Read<ulong>("PFCOUNT\r\n"u8) && ptr[8] == 'P' => RespCommand.PFCOUNT,
                        (2 << 4) | 3 when lastWord == MemoryMarshal.Read<ulong>("3\r\nSET\r\n"u8) => RespCommand.SET,
                        (2 << 4) | 5 when lastWord == MemoryMarshal.Read<ulong>("\nPFADD\r\n"u8) => RespCommand.PFADD,
                        (2 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("INCRBY\r\n"u8) => RespCommand.INCRBY,
                        (2 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("DECRBY\r\n"u8) => RespCommand.DECRBY,
                        (2 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("RENAME\r\n"u8) => RespCommand.RENAME,
                        (2 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("GETBIT\r\n"u8) => RespCommand.GETBIT,
                        (2 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("APPEND\r\n"u8) => RespCommand.APPEND,
                        (2 << 4) | 7 when lastWord == MemoryMarshal.Read<ulong>("UBLISH\r\n"u8) && ptr[8] == 'P' => RespCommand.PUBLISH,
                        (2 << 4) | 7 when lastWord == MemoryMarshal.Read<ulong>("FMERGE\r\n"u8) && ptr[8] == 'P' => RespCommand.PFMERGE,
                        (3 << 4) | 5 when lastWord == MemoryMarshal.Read<ulong>("\nSETEX\r\n"u8) => RespCommand.SETEX,
                        (3 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("PSETEX\r\n"u8) => RespCommand.PSETEX,
                        (3 << 4) | 6 when lastWord == MemoryMarshal.Read<ulong>("SETBIT\r\n"u8) => RespCommand.SETBIT,
                        (3 << 4) | 8 when lastWord == MemoryMarshal.Read<ulong>("TRANGE\r\n"u8) && *(ushort*)(ptr + 8) == MemoryMarshal.Read<ushort>("SE"u8) => RespCommand.SETRANGE,
                        (3 << 4) | 8 when lastWord == MemoryMarshal.Read<ulong>("TRANGE\r\n"u8) && *(ushort*)(ptr + 8) == MemoryMarshal.Read<ushort>("GE"u8) => RespCommand.GETRANGE,

                        _ => ((length << 4) | count) switch
                        {
                            // Commands with dynamic number of arguments
                            >= ((3 << 4) | 3) and <= ((3 << 4) | 6) when lastWord == MemoryMarshal.Read<ulong>("3\r\nSET\r\n"u8) => RespCommand.SETEXNX,
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
                return FastParseInlineCommand(out count);
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
                    Debug.Assert(length is > 0 and <= 9);

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

                                break;

                            case 4:
                                switch ((ushort)ptr[4])
                                {
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
                                                // TODO: AND|OR|XOR|NOT may not correctly handle mixed cases?

                                                // 2-character operations
                                                if (*(uint*)(ptr + 11) == MemoryMarshal.Read<uint>("$2\r\n"u8))
                                                {
                                                    if (*(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("$2\r\nOR\r\n"u8) || *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("$2\r\nor\r\n"u8))
                                                    {
                                                        readHead += 8;
                                                        count -= 1;
                                                        return RespCommand.BITOP_OR;
                                                    }
                                                }
                                                // 3-character operations
                                                else if (remainingBytes > length + 6 + 9)
                                                {
                                                    if (*(uint*)(ptr + 11) == MemoryMarshal.Read<uint>("$3\r\n"u8))
                                                    {
                                                        // Optimistically adjust read head and count
                                                        readHead += 9;
                                                        count -= 1;

                                                        if (*(ulong*)(ptr + 12) == MemoryMarshal.Read<ulong>("3\r\nAND\r\n"u8) || *(ulong*)(ptr + 12) == MemoryMarshal.Read<ulong>("3\r\nand\r\n"u8))
                                                        {
                                                            return RespCommand.BITOP_AND;
                                                        }
                                                        else if (*(ulong*)(ptr + 12) == MemoryMarshal.Read<ulong>("3\r\nXOR\r\n"u8) || *(ulong*)(ptr + 12) == MemoryMarshal.Read<ulong>("3\r\nxor\r\n"u8))
                                                        {
                                                            return RespCommand.BITOP_XOR;
                                                        }
                                                        else if (*(ulong*)(ptr + 12) == MemoryMarshal.Read<ulong>("3\r\nNOT\r\n"u8) || *(ulong*)(ptr + 12) == MemoryMarshal.Read<ulong>("3\r\nnot\r\n"u8))
                                                        {
                                                            return RespCommand.BITOP_NOT;
                                                        }

                                                        // Reset read head and count if we didn't match operator.
                                                        readHead -= 9;
                                                        count += 1;
                                                    }
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
                                            // WATCH OS|MS key
                                            // 8 = "$2\r\nOS\r\n".Length
                                            if (count > 1 && remainingBytes >= length + 8)
                                            {
                                                // Optimistically consume the subcommand
                                                count--;
                                                readHead += 8;

                                                if (*(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("$2\r\nOS\r\n"u8))
                                                {
                                                    return RespCommand.WATCH_OS;
                                                }
                                                else if (*(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("$2\r\nMS\r\n"u8))
                                                {
                                                    return RespCommand.WATCH_MS;
                                                }

                                                // Undo the optimistic advance
                                                count++;
                                                readHead -= 8;
                                            }

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
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZSCORE\r\n"u8))
                                        {
                                            return RespCommand.ZSCORE;
                                        }
                                        break;
                                }

                                break;
                            case 7:
                                switch ((ushort)ptr[4])
                                {
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

                                    case 'Z':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZPOPMIN\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return RespCommand.ZPOPMIN;
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
                    Debug.Assert(length is >= 10 and <= 19);

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
                                break;

                            case 13:
                                if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("\nZRANGEB"u8) && *(ulong*)(ptr + 12) == MemoryMarshal.Read<ulong>("YSCORE\r\n"u8))
                                {
                                    return RespCommand.ZRANGEBYSCORE;
                                }
                                break;

                            case 14:
                                if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\r\nZREMRA"u8) && *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("NGEBYLEX"u8) && *(ushort*)(ptr + 19) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return RespCommand.ZREMRANGEBYLEX;
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

            if (command.SequenceEqual(CmdStrings.SUBSCRIBE))
            {
                return RespCommand.SUBSCRIBE;
            }
            else if (command.SequenceEqual(CmdStrings.RUNTXP))
            {
                return RespCommand.RUNTXP;
            }
            else if (command.SequenceEqual(CmdStrings.ECHO))
            {
                return RespCommand.ECHO;
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
                    specificErrorMsg = CmdStrings.RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS_CONFIG;
                }
                else if (count >= 1)
                {
                    Span<byte> subCommand = GetCommand(out bool gotSubCommand);
                    if (!gotSubCommand)
                    {
                        success = false;
                        return RespCommand.NONE;
                    }

                    AsciiUtils.ToUpperInPlace(subCommand);

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
                }
            }
            else if (command.SequenceEqual(CmdStrings.CLIENT))
            {
                return RespCommand.CLIENT;
            }
            else if (command.SequenceEqual(CmdStrings.AUTH))
            {
                return RespCommand.AUTH;
            }
            else if (command.SequenceEqual(CmdStrings.INFO))
            {
                return RespCommand.INFO;
            }
            else if (command.SequenceEqual(CmdStrings.COMMAND))
            {
                if (count == 0)
                {
                    return RespCommand.COMMAND;
                }

                Span<byte> subCommand = GetCommand(out bool gotSubCommand);
                if (!gotSubCommand)
                {
                    success = false;
                    return RespCommand.NONE;
                }

                AsciiUtils.ToUpperInPlace(subCommand);

                count--;

                if (subCommand.SequenceEqual(CmdStrings.COUNT))
                {
                    return RespCommand.COMMAND_COUNT;
                }
                else if (subCommand.SequenceEqual(CmdStrings.INFO))
                {
                    return RespCommand.COMMAND_INFO;
                }
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
                Span<byte> subCommand = GetCommand(out var gotSubCommand);
                if (!gotSubCommand)
                {
                    success = false;
                    return RespCommand.NONE;
                }

                AsciiUtils.ToUpperInPlace(subCommand);

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

                string errMsg = string.Format(CmdStrings.GenericErrUnknownSubCommand, Encoding.UTF8.GetString(subCommand), "CLUSTER");
                specificErrorMsg = Encoding.UTF8.GetBytes(errMsg);
            }
            else if (command.SequenceEqual(CmdStrings.LATENCY))
            {
                if (count >= 1)
                {
                    Span<byte> subCommand = GetCommand(out bool gotSubCommand);
                    if (!gotSubCommand)
                    {
                        success = false;
                        return RespCommand.NONE;
                    }

                    AsciiUtils.ToUpperInPlace(subCommand);

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
            else if (command.SequenceEqual(CmdStrings.FAILOVER))
            {
                return RespCommand.FAILOVER;
            }
            else if (command.SequenceEqual(CmdStrings.MEMORY))
            {
                if (count > 0)
                {
                    ReadOnlySpan<byte> subCommand = GetCommand(out bool gotSubCommand);
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
                }
            }
            else if (command.SequenceEqual(CmdStrings.MONITOR))
            {
                return RespCommand.MONITOR;
            }
            else if (command.SequenceEqual(CmdStrings.ACL))
            {
                Span<byte> subCommand = GetCommand(out bool gotSubCommand);
                if (!gotSubCommand)
                {
                    success = false;
                    return RespCommand.NONE;
                }

                AsciiUtils.ToUpperInPlace(subCommand);

                count--;

                if (subCommand.SequenceEqual(CmdStrings.CAT))
                {
                    return RespCommand.ACL_CAT;
                }
                else if (subCommand.SequenceEqual(CmdStrings.DELUSER))
                {
                    return RespCommand.ACL_DELUSER;
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
                Span<byte> subCommand = GetCommand(out var gotSubCommand);
                if (!gotSubCommand)
                {
                    success = false;
                    return RespCommand.NONE;
                }

                count--;
                AsciiUtils.ToUpperInPlace(subCommand);
                if (subCommand.SequenceEqual(CmdStrings.LOADCS))
                {
                    return RespCommand.MODULE_LOADCS;
                }
            }
            else
            {
                // Custom commands should have never been set when we reach this point
                // (they should have been executed and reset)
                Debug.Assert(currentCustomTransaction == null);
                Debug.Assert(currentCustomCommand == null);
                Debug.Assert(currentCustomObjectCommand == null);

                if (storeWrapper.customCommandManager.Match(command, out currentCustomTransaction))
                {
                    return RespCommand.CustomTxn;
                }
                else if (storeWrapper.customCommandManager.Match(command, out currentCustomCommand))
                {
                    return RespCommand.CustomCmd;
                }
                else if (storeWrapper.customCommandManager.Match(command, out currentCustomObjectCommand))
                {
                    return RespCommand.CustomObjCmd;
                }
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
        /// Parses the command from the given input buffer.
        /// </summary>
        /// <param name="success">Whether processing should continue or a parsing error occurred (e.g. out of tokens).</param>
        /// <returns>Command parsed from the input buffer.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private RespCommand ParseCommand(out bool success)
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
                cmd = ArrayParseCommand(ref count, ref success);
                if (!success) return cmd;
            }

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

            return cmd;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private RespCommand ArrayParseCommand(ref int count, ref bool success)
        {
            RespCommand cmd = RespCommand.INVALID;
            ReadOnlySpan<byte> specificErrorMessage = default;
            endReadHead = readHead;
            var ptr = recvBufferPtr + readHead;

            // See if input command is all upper-case. If not, convert and try fast parse pass again.
            if (MakeUpperCase(ptr))
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
            if (!RespReadUtils.ReadUnsignedArrayLength(out count, ref ptr, recvBufferPtr + bytesRead))
            {
                success = false;
                return RespCommand.INVALID;
            }

            // Move readHead to start of command payload
            readHead = (int)(ptr - recvBufferPtr);

            // Try parsing the most important variable-length commands
            cmd = FastParseArrayCommand(ref count, ref specificErrorMessage);

            if (cmd == RespCommand.NONE)
            {
                cmd = SlowParseCommand(ref count, ref specificErrorMessage, out success);
            }

            // Parsing for command name was successful, but the command is unknown
            if (success && cmd == RespCommand.INVALID)
            {
                if (!specificErrorMessage.IsEmpty)
                {
                    while (!RespWriteUtils.WriteError(specificErrorMessage, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    // Return "Unknown RESP Command" message
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_UNK_CMD, ref dcurr, dend))
                        SendAndReset();
                }
            }
            return cmd;
        }
    }
}