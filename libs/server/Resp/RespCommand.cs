// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
        GET,
        GETRANGE,
        MGET,
        GETBIT,
        BITCOUNT,
        BITPOS,
        BITFIELD_RO,
        PFCOUNT,
        EXISTS,
        TTL,
        PTTL,
        STRLEN,
        COSCAN,

        // Upsert or RMW commands
        SET,
        MSET,
        PSETEX,
        SETEX,
        SETEXNX,
        SETEXXX,
        SETKEEPTTL,
        SETKEEPTTLXX,
        SETBIT,
        BITOP,
        BITFIELD,
        PFADD,
        PFMERGE,
        INCR,
        INCRBY,
        DECR,
        DECRBY,
        RENAME,
        PERSIST,
        EXPIRE,
        DEL,
        PEXPIRE,
        SETRANGE,
        GETDEL,
        MSETNX,
        APPEND,

        // List commands
        LPOP,
        LPUSH,
        LPUSHX,
        RPOP,
        RPUSH,
        RPUSHX,
        LLEN,
        LTRIM,
        LRANGE,
        LINDEX,
        LINSERT,
        LREM,
        RPOPLPUSH,
        LMOVE,
        LSET,

        // Hash commands
        HDEL,
        HEXISTS,
        HGET,
        HGETALL,
        HINCRBY,
        HINCRBYFLOAT,
        HKEYS,
        HLEN,
        HMGET,
        HMSET,
        HRANDFIELD,
        HSCAN,
        HSET,
        HSETNX,
        HSTRLEN,
        HVALS,

        // Set commands
        SADD,
        SREM,
        SPOP,
        SMEMBERS,
        SCARD,
        SSCAN,
        SMOVE,
        SRANDMEMBER,
        SISMEMBER,
        SUNION,
        SUNIONSTORE,
        SDIFF,
        SDIFFSTORE,

        // GeoHash (implemented as SortedSet) commands
        GEOADD,
        GEOHASH,
        GEODIST,
        GEOPOS,
        GEOSEARCH,

        // SortedSet commands
        ZADD,
        ZCARD,
        ZPOPMAX,
        ZSCORE,
        ZREM,
        ZCOUNT,
        ZINCRBY,
        ZRANK,
        ZRANGE,
        ZRANGEBYSCORE,
        ZREVRANGE,
        ZREVRANK,
        ZREMRANGEBYLEX,
        ZREMRANGEBYRANK,
        ZREMRANGEBYSCORE,
        ZLEXCOUNT,
        ZPOPMIN,
        ZRANDMEMBER,
        ZDIFF,
        ZSCAN,
        ZMSCORE,

        // Admin commands
        PING,
        QUIT,
        AUTH,
        COMMAND,
        DBSIZE,
        KEYS,
        PUBLISH,
        SUBSCRIBE,
        PSUBSCRIBE,
        UNSUBSCRIBE,
        PUNSUBSCRIBE,
        ASKING,
        MIGRATE,
        SELECT,
        ECHO,
        CONFIG,
        CLIENT,
        UNLINK,
        TYPE,
        SCAN,
        MEMORY,
        MONITOR,
        MODULE,
        REGISTERCS,

        // Transaction commands
        MULTI,
        EXEC,
        DISCARD,
        WATCH,
        UNWATCH,
        RUNTXP,

        // Cluster commands
        READONLY,
        READWRITE,
        REPLICAOF,
        SECONDARYOF,

        // Misc. commands
        INFO,
        CLUSTER,
        LATENCY,
        TIME,
        SAVE,
        LASTSAVE,
        BGSAVE,
        COMMITAOF,
        FLUSHDB,
        FORCEGC,
        FAILOVER,
        ACL,

        // Custom commands
        CustomTxn,
        CustomCmd,
        CustomObjCmd,

        INVALID = 0xFF,
    }

    /// <summary>
    /// Extension methods for <see cref="RespCommand"/>.
    /// </summary>
    public static class RespCommandExtensions
    {
        /// <summary>
        /// Returns 1 if <paramref name="cmd"/> is a write command.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong OneIfWrite(this RespCommand cmd)
        {
            // todo: this was a mask and shift before hand, speed it up
            return (cmd >= RespCommand.SET && cmd <= RespCommand.ZMSCORE) ? 1UL : 0;
        }

        /// <summary>
        /// Returns 1 if <paramref name="cmd"/> is a read command.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong OneIfRead(this RespCommand cmd)
        {
            // todo: this was a mask and shift before hand, speed it up
            return (cmd >= RespCommand.GET && cmd <= RespCommand.COSCAN) ? 1UL : 0;
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
        private RespCommand FastParseArrayCommand(ref int count)
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
                                            return RespCommand.BITOP;
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
                                            return RespCommand.MEMORY;
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("MODULE\r\n"u8))
                                        {
                                            return RespCommand.MODULE;
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
        /// <param name="success">True if the input RESP string was completely included in the buffer, false if we couldn't read the full command name.</param>
        /// <returns>The parsed command name.</returns>
        private RespCommand SlowParseCommand(ref int count, out bool success)
        {
            // Try to extract the current string from the front of the read head
            ReadOnlySpan<byte> bufSpan = new(recvBufferPtr, bytesRead);
            var command = GetCommand(bufSpan, out success);

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
            else if (command.SequenceEqual(CmdStrings.SECONDARYOF))
            {
                return RespCommand.SECONDARYOF;
            }
            else if (command.SequenceEqual(CmdStrings.CONFIG))
            {
                return RespCommand.CONFIG;
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
                return RespCommand.COMMAND;
            }
            else if (command.SequenceEqual(CmdStrings.PING))
            {
                return RespCommand.PING;
            }
            else if (command.SequenceEqual(CmdStrings.CLUSTER))
            {
                return RespCommand.CLUSTER;
            }
            else if (command.SequenceEqual(CmdStrings.LATENCY))
            {
                return RespCommand.LATENCY;
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
                return RespCommand.MEMORY;
            }
            else if (command.SequenceEqual(CmdStrings.MONITOR))
            {
                return RespCommand.MONITOR;
            }
            else if (command.SequenceEqual(CmdStrings.ACL))
            {
                return RespCommand.ACL;
            }
            else if (command.SequenceEqual(CmdStrings.REGISTERCS))
            {
                return RespCommand.REGISTERCS;
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
            // Drain the commands to advance the read head to the end of the command.
            if (!DrainCommands(bufSpan, count))
            {
                success = false;
            }

            return RespCommand.INVALID;
        }

        /// <summary>
        /// Attempts to skip to the end of the line ("\r\n") under the current read head.
        /// </summary>
        /// <returns>True if string terminator was found and readHead was changed, otherwise false. </returns>
        private bool AttemptSkipLine()
        {
            // We might have received an inline command package.Try to find the end of the line.
            logger?.LogWarning("Received malformed input message. Trying to skip line.");

            for (int stringEnd = readHead; stringEnd < bytesRead - 1; stringEnd++)
            {
                if (recvBufferPtr[stringEnd] == '\r' && recvBufferPtr[stringEnd + 1] == '\n')
                {
                    // Skip to the end of the string
                    readHead = stringEnd + 2;
                    return true;
                }
            }

            // We received an incomplete string and require more input.
            return false;
        }

        /// <summary>
        /// Parses the command from the given input buffer.
        /// </summary>
        /// <param name="ptr">Pointer to the beginning of the input buffer.</param>
        /// <param name="count">Returns the number of unparsed arguments of the command (including any unparsed subcommands).</param>
        /// <param name="success">Whether processing should continue or a parsing error occurred (e.g. out of tokens).</param>
        /// <returns>Command parsed from the input buffer.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private RespCommand ParseCommand(out int count, byte* ptr, out bool success)
        {
            RespCommand cmd = RespCommand.INVALID;

            // Initialize count as -1 (i.e., read head has not been advanced)
            count = -1;
            success = true;

            // Attempt parsing using fast parse pass for most common operations
            cmd = FastParseCommand(out count);

            if (cmd == RespCommand.NONE)
            {
                // See if input command is all upper-case. If not, convert and try fast parse pass again.
                if (MakeUpperCase(ptr))
                {
                    cmd = FastParseCommand(out count);
                }

                // If we have not found a command, continue parsing for array commands
                if (cmd == RespCommand.NONE)
                {
                    // Ensure we are attempting to read a RESP array header
                    if (recvBufferPtr[readHead] != '*')
                    {
                        // We might have received an inline command package. Skip until the end of the line in the input package.
                        success = AttemptSkipLine();

                        return RespCommand.INVALID;
                    }

                    // ... and read the array length; Move the read head
                    var tmp = recvBufferPtr + readHead;
                    if (!RespReadUtils.ReadArrayLength(out count, ref tmp, recvBufferPtr + bytesRead))
                    {
                        success = false;
                        return RespCommand.INVALID;
                    }

                    readHead += (int)(tmp - (recvBufferPtr + readHead));

                    // Try parsing the most important variable-length commands
                    cmd = FastParseArrayCommand(ref count);

                    if (cmd == RespCommand.NONE)
                    {
                        cmd = SlowParseCommand(ref count, out success);
                    }
                }
            }

            return cmd;
        }
    }
}