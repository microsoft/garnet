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
        NONE = 0x0,

        // Read-only commands
        GET = (0x40 | 0x0),
        GETRANGE = (0x40 | 0x1),
        MGET = (0x40 | 0x2),
        GETBIT = (0x40 | 0x3),
        BITCOUNT = (0x40 | 0x4),
        BITPOS = (0x40 | 0x5),
        BITFIELD_RO = (0x40 | 0x6),
        PFCOUNT = (0x40 | 0x7),
        EXISTS = (0x40 | 0x8),
        TTL = (0x40 | 0x9),
        PTTL = (0x40 | 0x10),
        STRLEN = 0x26,
        COSCAN = 0x27,

        // Upsert or RMW commands
        SET = (0x80 | 0x0),
        MSET = (0x80 | 0x1),
        PSETEX = (0x80 | 0x2),
        SETEX = (0x80 | 0x3),
        SETEXNX = (0x80 | 0x4),
        SETEXXX = (0x80 | 0x5),
        SETKEEPTTL = (0x80 | 0x6),
        SETKEEPTTLXX = (0x80 | 0x7),
        SETBIT = (0x80 | 0x8),
        BITOP = (0x80 | 0x9),
        BITFIELD = (0x80 | 0xA),
        PFADD = (0x80 | 0xB),
        PFMERGE = (0x80 | 0xC),
        INCR = (0x80 | 0xD),
        INCRBY = (0x80 | 0xE),
        DECR = (0x80 | 0xF),
        DECRBY = (0x80 | 0x10),
        RENAME = (0x80 | 0x11),
        PERSIST = (0x80 | 0x12),
        EXPIRE = (0x80 | 0x13),
        DEL = (0x80 | 0x14),
        PEXPIRE = (0x80 | 0x15),
        SETRANGE = (0x80 | 0x16),
        GETDEL = (0x80 | 0x17),
        MSETNX = (0x80 | 0x18),
        APPEND = (0x80 | 0x19),

        // Object store commands
        SortedSet = (0xC0 | 0x0),
        List = (0xC0 | 0x1),
        Hash = (0xC0 | 0x2),
        Set = (0xC0 | 0x3),
        All = (0xC0 | 0x26),

        // Admin commands
        PING = 0x1,
        QUIT = 0x2,
        AUTH = 0x3,
        COMMAND = 0x4,
        DBSIZE = 0x5,
        KEYS = 0x6,
        PUBLISH = 0x7,
        SUBSCRIBE = 0x8,
        PSUBSCRIBE = 0x9,
        UNSUBSCRIBE = 0xA,
        PUNSUBSCRIBE = 0xB,
        ASKING = 0xD,
        MIGRATE = 0xE,
        SELECT = 0xF,
        ECHO = 0x10,
        CONFIG = 0x11,
        CLIENT = 0x12,
        UNLINK = 0x13,
        TYPE = 0x20,
        SCAN = 0x21,
        MEMORY = 0x23,
        MONITOR = 0x24,
        MODULE = 0x25,
        REGISTERCS = 0x28,

        // Transaction commands
        MULTI = 0x14,
        EXEC = 0x15,
        DISCARD = 0x16,
        WATCH = 0x17,
        WATCHMS = 0x18,
        WATCHOS = 0x19,
        UNWATCH = 0x1A,
        RUNTXP = 0x1B,

        // Cluster commands
        READONLY = 0x1C,
        READWRITE = 0x1D,
        REPLICAOF = 0x1E,
        SECONDARYOF = 0x1F,

        // Misc. commands
        INFO = 0x31,
        CLUSTER = 0x32,
        LATENCY = 0x33,
        TIME = 0x34,
        RESET = 0x35,
        SAVE = 0x36,
        LASTSAVE = 0x37,
        BGSAVE = 0x38,
        COMMITAOF = 0x39,
        FLUSHDB = 0x3A,
        FORCEGC = 0x3B,
        FAILOVER = 0x3C,
        ACL = 0x3D,
        HELLO = 0x3E,

        // Custom commands
        CustomTxn = 0x29,
        CustomCmd = 0x2A,
        CustomObjCmd = 0x2B,

        INVALID = 0xFF
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
        /// <returns>The parsed command name and (optional) subcommand name.</returns>
        private (RespCommand, byte) FastParseArrayCommand(ref int count)
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
                                    return (RespCommand.DEL, 0);
                                }

                                break;

                            case 4:
                                switch ((ushort)ptr[4])
                                {
                                    case 'H':
                                        if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nHSET\r\n"u8))
                                        {
                                            return (RespCommand.Hash, (byte)HashOperation.HSET);
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nHGET\r\n"u8))
                                        {
                                            return (RespCommand.Hash, (byte)HashOperation.HGET);
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nHDEL\r\n"u8))
                                        {
                                            return (RespCommand.Hash, (byte)HashOperation.HDEL);
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nHLEN\r\n"u8))
                                        {
                                            return (RespCommand.Hash, (byte)HashOperation.HLEN);
                                        }
                                        break;

                                    case 'K':
                                        if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nKEYS\r\n"u8))
                                        {
                                            return (RespCommand.KEYS, 0);
                                        }
                                        break;

                                    case 'L':
                                        if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nLPOP\r\n"u8))
                                        {
                                            return (RespCommand.List, (byte)ListOperation.LPOP);
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nLLEN\r\n"u8))
                                        {
                                            return (RespCommand.List, (byte)ListOperation.LLEN);
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nLREM\r\n"u8))
                                        {
                                            return (RespCommand.List, (byte)ListOperation.LREM);
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nLSET\r\n"u8))
                                        {
                                            return (RespCommand.List, (byte)ListOperation.LSET);
                                        }
                                        break;

                                    case 'M':
                                        if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nMGET\r\n"u8))
                                        {
                                            return (RespCommand.MGET, 0);
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nMSET\r\n"u8))
                                        {
                                            return (RespCommand.MSET, 0);
                                        }
                                        break;

                                    case 'R':
                                        if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nRPOP\r\n"u8))
                                        {
                                            return (RespCommand.List, (byte)ListOperation.RPOP);
                                        }
                                        break;

                                    case 'S':
                                        if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nSCAN\r\n"u8))
                                        {
                                            return (RespCommand.SCAN, 0);
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nSADD\r\n"u8))
                                        {
                                            return (RespCommand.Set, (byte)SetOperation.SADD);
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nSREM\r\n"u8))
                                        {
                                            return (RespCommand.Set, (byte)SetOperation.SREM);
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nSPOP\r\n"u8))
                                        {
                                            return (RespCommand.Set, (byte)SetOperation.SPOP);
                                        }
                                        break;

                                    case 'T':
                                        if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nTYPE\r\n"u8))
                                        {
                                            return (RespCommand.TYPE, 0);
                                        }
                                        break;

                                    case 'Z':
                                        if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nZADD\r\n"u8))
                                        {
                                            return (RespCommand.SortedSet, (byte)SortedSetOperation.ZADD);
                                        }
                                        else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("\r\nZREM\r\n"u8))
                                        {
                                            return (RespCommand.SortedSet, (byte)SortedSetOperation.ZREM);
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
                                                // 2-character operations
                                                if (*(uint*)(ptr + 11) == MemoryMarshal.Read<uint>("$2\r\n"u8))
                                                {
                                                    if (*(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("$2\r\nOR\r\n"u8) || *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("$2\r\nor\r\n"u8))
                                                    {
                                                        readHead += 8;
                                                        count -= 1;
                                                        return (RespCommand.BITOP, (byte)BitmapOperation.OR);
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
                                                            return (RespCommand.BITOP, (byte)BitmapOperation.AND);
                                                        }
                                                        else if (*(ulong*)(ptr + 12) == MemoryMarshal.Read<ulong>("3\r\nXOR\r\n"u8) || *(ulong*)(ptr + 12) == MemoryMarshal.Read<ulong>("3\r\nxor\r\n"u8))
                                                        {
                                                            return (RespCommand.BITOP, (byte)BitmapOperation.XOR);
                                                        }
                                                        else if (*(ulong*)(ptr + 12) == MemoryMarshal.Read<ulong>("3\r\nNOT\r\n"u8) || *(ulong*)(ptr + 12) == MemoryMarshal.Read<ulong>("3\r\nnot\r\n"u8))
                                                        {
                                                            return (RespCommand.BITOP, (byte)BitmapOperation.NOT);
                                                        }

                                                        // Reset read head and count if we didn't match operator.
                                                        readHead -= 9;
                                                        count += 1;
                                                    }
                                                }

                                                return (RespCommand.BITOP, (byte)BitmapOperation.NONE);
                                            }
                                        }
                                        break;

                                    case 'H':
                                        if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nHMSET\r\n"u8))
                                        {
                                            return (RespCommand.Hash, (byte)HashOperation.HMSET);
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nHMGET\r\n"u8))
                                        {
                                            return (RespCommand.Hash, (byte)HashOperation.HMGET);
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nHKEYS\r\n"u8))
                                        {
                                            return (RespCommand.Hash, (byte)HashOperation.HKEYS);
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nHVALS\r\n"u8))
                                        {
                                            return (RespCommand.Hash, (byte)HashOperation.HVALS);
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nHSCAN\r\n"u8))
                                        {
                                            return (RespCommand.Hash, (byte)HashOperation.HSCAN);
                                        }
                                        break;

                                    case 'L':
                                        if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nLPUSH\r\n"u8))
                                        {
                                            return (RespCommand.List, (byte)ListOperation.LPUSH);
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nLTRIM\r\n"u8))
                                        {
                                            return (RespCommand.List, (byte)ListOperation.LTRIM);
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nLMOVE\r\n"u8))
                                        {
                                            return (RespCommand.List, (byte)ListOperation.LMOVE);
                                        }
                                        break;

                                    case 'P':
                                        if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nPFADD\r\n"u8))
                                        {
                                            return (RespCommand.PFADD, 0);
                                        }
                                        break;

                                    case 'R':
                                        if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nRPUSH\r\n"u8))
                                        {
                                            return (RespCommand.List, (byte)ListOperation.RPUSH);
                                        }
                                        break;

                                    case 'S':
                                        if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nSCARD\r\n"u8))
                                        {
                                            return (RespCommand.Set, (byte)SetOperation.SCARD);
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nSSCAN\r\n"u8))
                                        {
                                            return (RespCommand.Set, (byte)SetOperation.SSCAN);
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nSMOVE\r\n"u8))
                                        {
                                            return (RespCommand.Set, (byte)SetOperation.SMOVE);
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nSDIFF\r\n"u8))
                                        {
                                            return (RespCommand.Set, (byte)SetOperation.SDIFF);
                                        }
                                        break;

                                    case 'W':
                                        if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nWATCH\r\n"u8))
                                        {
                                            // Check for invocations with subcommand
                                            if (remainingBytes > length + 6 + 4)
                                            {
                                                if (*(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("$2\r\nOS\r\n"u8) || *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("$2\r\nos\r\n"u8))
                                                {
                                                    // Account for parsed subcommand
                                                    readHead += 4;
                                                    count -= 1;

                                                    return (RespCommand.WATCHOS, 0);
                                                }
                                                else if (*(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("$2\r\nMS\r\n"u8) || *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("$2\r\nms\r\n"u8))
                                                {
                                                    // Account for parsed subcommand
                                                    readHead += 4;
                                                    count -= 1;

                                                    return (RespCommand.WATCHMS, 0);
                                                }
                                            }

                                            return (RespCommand.WATCH, 0);
                                        }
                                        break;

                                    case 'Z':
                                        if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nZCARD\r\n"u8))
                                        {
                                            return (RespCommand.SortedSet, (byte)SortedSetOperation.ZCARD);
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nZRANK\r\n"u8))
                                        {
                                            return (RespCommand.SortedSet, (byte)SortedSetOperation.ZRANK);
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nZDIFF\r\n"u8))
                                        {
                                            return (RespCommand.SortedSet, (byte)SortedSetOperation.ZDIFF);
                                        }
                                        else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\nZSCAN\r\n"u8))
                                        {
                                            return (RespCommand.SortedSet, (byte)SortedSetOperation.ZSCAN);
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
                                            return (RespCommand.DBSIZE, 0);
                                        }
                                        break;

                                    case 'E':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("EXISTS\r\n"u8))
                                        {
                                            return (RespCommand.EXISTS, 0);
                                        }
                                        break;

                                    case 'G':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("GEOADD\r\n"u8))
                                        {
                                            return (RespCommand.SortedSet, (byte)SortedSetOperation.GEOADD);
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("GEOPOS\r\n"u8))
                                        {
                                            return (RespCommand.SortedSet, (byte)SortedSetOperation.GEOPOS);
                                        }
                                        break;

                                    case 'H':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("HSETNX\r\n"u8))
                                        {
                                            return (RespCommand.Hash, (byte)HashOperation.HSETNX);
                                        }
                                        break;

                                    case 'L':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("LPUSHX\r\n"u8))
                                        {
                                            return (RespCommand.List, (byte)ListOperation.LPUSHX);
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("LRANGE\r\n"u8))
                                        {
                                            return (RespCommand.List, (byte)ListOperation.LRANGE);
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("LINDEX\r\n"u8))
                                        {
                                            return (RespCommand.List, (byte)ListOperation.LINDEX);
                                        }
                                        break;

                                    case 'M':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("MSETNX\r\n"u8))
                                        {
                                            return (RespCommand.MSETNX, 0);
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("MEMORY\r\n"u8))
                                        {
                                            return (RespCommand.MEMORY, 0);
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("MODULE\r\n"u8))
                                        {
                                            return (RespCommand.MODULE, 0);
                                        }
                                        break;

                                    case 'R':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("RPUSHX\r\n"u8))
                                        {
                                            return (RespCommand.List, (byte)ListOperation.RPUSHX);
                                        }
                                        break;

                                    case 'S':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("SELECT\r\n"u8))
                                        {
                                            return (RespCommand.SELECT, 0);
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("STRLEN\r\n"u8))
                                        {
                                            return (RespCommand.STRLEN, 0);
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("SUNION\r\n"u8))
                                        {
                                            return (RespCommand.Set, (byte)SetOperation.SUNION);
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("SINTER\r\n"u8))
                                        {
                                            return (RespCommand.Set, (byte)SetOperation.SINTER);
                                        }
                                        break;

                                    case 'U':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("UNLINK\r\n"u8))
                                        {
                                            return (RespCommand.UNLINK, 0);
                                        }
                                        break;

                                    case 'Z':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZCOUNT\r\n"u8))
                                        {
                                            return (RespCommand.SortedSet, (byte)SortedSetOperation.ZCOUNT);
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZRANGE\r\n"u8))
                                        {
                                            return (RespCommand.SortedSet, (byte)SortedSetOperation.ZRANGE);
                                        }
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZSCORE\r\n"u8))
                                        {
                                            return (RespCommand.SortedSet, (byte)SortedSetOperation.ZSCORE);
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
                                            return (RespCommand.SortedSet, (byte)SortedSetOperation.GEOHASH);
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("GEODIST\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return (RespCommand.SortedSet, (byte)SortedSetOperation.GEODIST);
                                        }
                                        break;

                                    case 'H':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("HGETALL\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return (RespCommand.Hash, (byte)HashOperation.HGETALL);
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("HEXISTS\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return (RespCommand.Hash, (byte)HashOperation.HEXISTS);
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("HINCRBY\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return (RespCommand.Hash, (byte)HashOperation.HINCRBY);
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("HSTRLEN\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return (RespCommand.Hash, (byte)HashOperation.HSTRLEN);
                                        }
                                        break;

                                    case 'L':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("LINSERT\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return (RespCommand.List, (byte)ListOperation.LINSERT);
                                        }
                                        break;

                                    case 'M':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("MONITOR\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return (RespCommand.MONITOR, 0);
                                        }
                                        break;

                                    case 'P':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("PFCOUNT\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return (RespCommand.PFCOUNT, 0);
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("PFMERGE\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return (RespCommand.PFMERGE, 0);
                                        }
                                        break;

                                    case 'Z':
                                        if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZPOPMIN\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return (RespCommand.SortedSet, (byte)SortedSetOperation.ZPOPMIN);
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZPOPMAX\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return (RespCommand.SortedSet, (byte)SortedSetOperation.ZPOPMAX);
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZINCRBY\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return (RespCommand.SortedSet, (byte)SortedSetOperation.ZINCRBY);
                                        }
                                        else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZMSCORE\r"u8) && *(byte*)(ptr + 12) == '\n')
                                        {
                                            return (RespCommand.SortedSet, (byte)SortedSetOperation.ZMSCORE);
                                        }
                                        break;
                                }
                                break;
                            case 8:
                                if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZREVRANK"u8) && *(ushort*)(ptr + 12) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return (RespCommand.SortedSet, (byte)SortedSetOperation.ZREVRANK);
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("SMEMBERS"u8) && *(ushort*)(ptr + 12) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return (RespCommand.Set, (byte)SetOperation.SMEMBERS);
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("BITFIELD"u8) && *(ushort*)(ptr + 12) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return (RespCommand.BITFIELD, 0);
                                }
                                break;
                            case 9:
                                if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("SUBSCRIB"u8) && *(uint*)(ptr + 11) == MemoryMarshal.Read<uint>("BE\r\n"u8))
                                {
                                    return (RespCommand.SUBSCRIBE, 0);
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("SISMEMBE"u8) && *(uint*)(ptr + 11) == MemoryMarshal.Read<uint>("ER\r\n"u8))
                                {
                                    return (RespCommand.Set, (byte)SetOperation.SISMEMBER);
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZLEXCOUN"u8) && *(uint*)(ptr + 11) == MemoryMarshal.Read<uint>("NT\r\n"u8))
                                {
                                    return (RespCommand.SortedSet, (byte)SortedSetOperation.ZLEXCOUNT);
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("GEOSEARC"u8) && *(uint*)(ptr + 11) == MemoryMarshal.Read<uint>("CH\r\n"u8))
                                {
                                    return (RespCommand.SortedSet, (byte)SortedSetOperation.GEOSEARCH);
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("ZREVRANG"u8) && *(uint*)(ptr + 11) == MemoryMarshal.Read<uint>("GE\r\n"u8))
                                {
                                    return (RespCommand.SortedSet, (byte)SortedSetOperation.ZREVRANGE);
                                }
                                else if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("RPOPLPUS"u8) && *(uint*)(ptr + 11) == MemoryMarshal.Read<uint>("SH\r\n"u8))
                                {
                                    return (RespCommand.List, (byte)ListOperation.RPOPLPUSH);
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
                                    return (RespCommand.PSUBSCRIBE, 0);
                                }
                                else if (*(ulong*)(ptr + 1) == MemoryMarshal.Read<ulong>("10\r\nHRAN"u8) && *(ulong*)(ptr + 9) == MemoryMarshal.Read<ulong>("DFIELD\r\n"u8))
                                {
                                    return (RespCommand.Hash, (byte)HashOperation.HRANDFIELD);
                                }
                                else if (*(ulong*)(ptr + 1) == MemoryMarshal.Read<ulong>("10\r\nSDIF"u8) && *(ulong*)(ptr + 9) == MemoryMarshal.Read<ulong>("FSTORE\r\n"u8))
                                {
                                    return (RespCommand.Set, (byte)SetOperation.SDIFFSTORE);
                                }
                                break;

                            case 11:
                                if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("1\r\nUNSUB"u8) && *(ulong*)(ptr + 10) == MemoryMarshal.Read<ulong>("SCRIBE\r\n"u8))
                                {
                                    return (RespCommand.UNSUBSCRIBE, 0);
                                }
                                else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("1\r\nZRAND"u8) && *(ulong*)(ptr + 10) == MemoryMarshal.Read<ulong>("MEMBER\r\n"u8))
                                {
                                    return (RespCommand.SortedSet, (byte)SortedSetOperation.ZRANDMEMBER);
                                }
                                else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("1\r\nBITFI"u8) && *(ulong*)(ptr + 10) == MemoryMarshal.Read<ulong>("ELD_RO\r\n"u8))
                                {
                                    return (RespCommand.BITFIELD_RO, 0);
                                }
                                else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("1\r\nSRAND"u8) && *(ulong*)(ptr + 10) == MemoryMarshal.Read<ulong>("MEMBER\r\n"u8))
                                {
                                    return (RespCommand.Set, (byte)SetOperation.SRANDMEMBER);
                                }
                                else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("1\r\nSUNIO"u8) && *(ulong*)(ptr + 10) == MemoryMarshal.Read<ulong>("NSTORE\r\n"u8))
                                {
                                    return (RespCommand.Set, (byte)SetOperation.SUNIONSTORE);
                                }
                                else if (*(ulong*)(ptr + 2) == MemoryMarshal.Read<ulong>("1\r\nSINTE"u8) && *(ulong*)(ptr + 10) == MemoryMarshal.Read<ulong>("RSTORE\r\n"u8))
                                {
                                    return (RespCommand.Set, (byte)SetOperation.SINTERSTORE);
                                }
                                break;

                            case 12:
                                if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\r\nPUNSUB"u8) && *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("SCRIBE\r\n"u8))
                                {
                                    return (RespCommand.PUNSUBSCRIBE, 0);
                                }
                                else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\r\nHINCRB"u8) && *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("YFLOAT\r\n"u8))
                                {
                                    return (RespCommand.Hash, (byte)HashOperation.HINCRBYFLOAT);
                                }
                                break;

                            case 13:
                                if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("\nZRANGEB"u8) && *(ulong*)(ptr + 12) == MemoryMarshal.Read<ulong>("YSCORE\r\n"u8))
                                {
                                    return (RespCommand.SortedSet, (byte)SortedSetOperation.ZRANGEBYSCORE);
                                }
                                break;

                            case 14:
                                if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\r\nZREMRA"u8) && *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("NGEBYLEX"u8) && *(ushort*)(ptr + 19) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return (RespCommand.SortedSet, (byte)SortedSetOperation.ZREMRANGEBYLEX);
                                }
                                break;

                            case 15:
                                if (*(ulong*)(ptr + 4) == MemoryMarshal.Read<ulong>("\nZREMRAN"u8) && *(ulong*)(ptr + 12) == MemoryMarshal.Read<ulong>("GEBYRANK"u8) && *(ushort*)(ptr + 20) == MemoryMarshal.Read<ushort>("\r\n"u8))
                                {
                                    return (RespCommand.SortedSet, (byte)SortedSetOperation.ZREMRANGEBYRANK);
                                }
                                break;

                            case 16:
                                if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\r\nCUSTOM"u8) && *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("OBJECTSC"u8) && *(ushort*)(ptr + 19) == MemoryMarshal.Read<ushort>("AN\r\n"u8))
                                {
                                    return (RespCommand.All, (byte)RespCommand.COSCAN);
                                }
                                else if (*(ulong*)(ptr + 3) == MemoryMarshal.Read<ulong>("\r\nZREMRA"u8) && *(ulong*)(ptr + 11) == MemoryMarshal.Read<ulong>("NGEBYSCO"u8) && *(ushort*)(ptr + 19) == MemoryMarshal.Read<ushort>("RE\r\n"u8))
                                {
                                    return (RespCommand.SortedSet, (byte)SortedSetOperation.ZREMRANGEBYSCORE);
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
            return (RespCommand.NONE, 0);
        }

        /// <summary>
        /// Parses the receive buffer, starting from the current read head, for all command names that are
        /// not covered by FastParseArrayCommand() and advances the read head to the end of the command name.
        /// 
        /// NOTE: Assumes the input command names have already been converted to upper-case.
        /// </summary>
        /// <param name="count">Reference to the number of remaining tokens in the packet. Will be reduced to number of command arguments.</param>
        /// <param name="success">True if the input RESP string was completely included in the buffer, false if we couldn't read the full command name.</param>
        /// <returns>The parsed command name and (optional) subcommand name.</returns>
        private (RespCommand, byte) SlowParseCommand(ref int count, out bool success)
        {
            // Try to extract the current string from the front of the read head
            ReadOnlySpan<byte> bufSpan = new(recvBufferPtr, bytesRead);
            var command = GetCommand(bufSpan, out success);

            if (!success)
            {
                return (RespCommand.INVALID, (byte)RespCommand.INVALID);
            }

            // Account for the command name being taken off the read head
            count -= 1;

            if (command.SequenceEqual(CmdStrings.SUBSCRIBE))
            {
                return (RespCommand.SUBSCRIBE, 0);
            }
            else if (command.SequenceEqual(CmdStrings.RUNTXP))
            {
                return (RespCommand.RUNTXP, 0);
            }
            else if (command.SequenceEqual(CmdStrings.ECHO))
            {
                return (RespCommand.ECHO, 0);
            }
            else if (command.SequenceEqual(CmdStrings.REPLICAOF))
            {
                return (RespCommand.REPLICAOF, 0);
            }
            else if (command.SequenceEqual(CmdStrings.SECONDARYOF))
            {
                return (RespCommand.SECONDARYOF, 0);
            }
            else if (command.SequenceEqual(CmdStrings.CONFIG))
            {
                return (RespCommand.CONFIG, 0);
            }
            else if (command.SequenceEqual(CmdStrings.CLIENT))
            {
                return (RespCommand.CLIENT, 0);
            }
            else if (command.SequenceEqual(CmdStrings.AUTH))
            {
                return (RespCommand.AUTH, 0);
            }
            else if (command.SequenceEqual(CmdStrings.INFO))
            {
                return (RespCommand.INFO, 0);
            }
            else if (command.SequenceEqual(CmdStrings.COMMAND))
            {
                return (RespCommand.COMMAND, 0);
            }
            else if (command.SequenceEqual(CmdStrings.PING))
            {
                return (RespCommand.PING, 0);
            }
            else if (command.SequenceEqual(CmdStrings.HELLO))
            {
                return (RespCommand.HELLO, 0);
            }
            else if (command.SequenceEqual(CmdStrings.CLUSTER))
            {
                return (RespCommand.CLUSTER, 0);
            }
            else if (command.SequenceEqual(CmdStrings.LATENCY))
            {
                return (RespCommand.LATENCY, 0);
            }
            else if (command.SequenceEqual(CmdStrings.TIME))
            {
                return (RespCommand.TIME, 0);
            }
            else if (command.SequenceEqual(CmdStrings.QUIT))
            {
                return (RespCommand.QUIT, 0);
            }
            else if (command.SequenceEqual(CmdStrings.SAVE))
            {
                return (RespCommand.SAVE, 0);
            }
            else if (command.SequenceEqual(CmdStrings.LASTSAVE))
            {
                return (RespCommand.LASTSAVE, 0);
            }
            else if (command.SequenceEqual(CmdStrings.BGSAVE))
            {
                return (RespCommand.BGSAVE, 0);
            }
            else if (command.SequenceEqual(CmdStrings.COMMITAOF))
            {
                return (RespCommand.COMMITAOF, 0);
            }
            else if (command.SequenceEqual(CmdStrings.FLUSHDB))
            {
                return (RespCommand.FLUSHDB, 0);
            }
            else if (command.SequenceEqual(CmdStrings.FORCEGC))
            {
                return (RespCommand.FORCEGC, 0);
            }
            else if (command.SequenceEqual(CmdStrings.MIGRATE))
            {
                return (RespCommand.MIGRATE, 0);
            }
            else if (command.SequenceEqual(CmdStrings.FAILOVER))
            {
                return (RespCommand.FAILOVER, 0);
            }
            else if (command.SequenceEqual(CmdStrings.MEMORY))
            {
                return (RespCommand.MEMORY, 0);
            }
            else if (command.SequenceEqual(CmdStrings.MONITOR))
            {
                return (RespCommand.MONITOR, 0);
            }
            else if (command.SequenceEqual(CmdStrings.ACL))
            {
                return (RespCommand.ACL, 0);
            }
            else if (command.SequenceEqual(CmdStrings.REGISTERCS))
            {
                return (RespCommand.REGISTERCS, 0);
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
                    return (RespCommand.CustomTxn, 0);
                }
                else if (storeWrapper.customCommandManager.Match(command, out currentCustomCommand))
                {
                    return (RespCommand.CustomCmd, 0);
                }
                else if (storeWrapper.customCommandManager.Match(command, out currentCustomObjectCommand))
                {
                    return (RespCommand.CustomObjCmd, 0);
                }
            }

            // If this command name was not known to the slow pass, we are out of options and the command is unknown.
            // Drain the commands to advance the read head to the end of the command.
            if (!DrainCommands(bufSpan, count))
            {
                success = false;
            }

            return (RespCommand.INVALID, (byte)RespCommand.INVALID);
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
        /// Parses the command and subcommand from the given input buffer.
        /// </summary>
        /// <param name="ptr">Pointer to the beginning of the input buffer.</param>
        /// <param name="count">Returns the number of unparsed arguments of the command (including any unparsed subcommands).</param>
        /// <param name="success">Whether processing should continue or a parsing error occurred (e.g. out of tokens).</param>
        /// <returns>Tuple of command and subcommand parsed from the input buffer.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private (RespCommand, byte) ParseCommand(out int count, byte* ptr, out bool success)
        {
            RespCommand cmd = RespCommand.INVALID;
            byte subCmd = 0;

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

                        return (RespCommand.INVALID, 0);
                    }

                    // ... and read the array length; Move the read head
                    var tmp = recvBufferPtr + readHead;
                    if (!RespReadUtils.ReadArrayLength(out count, ref tmp, recvBufferPtr + bytesRead))
                    {
                        success = false;
                        return (RespCommand.INVALID, subCmd);
                    }

                    readHead += (int)(tmp - (recvBufferPtr + readHead));

                    // Try parsing the most important variable-length commands
                    (cmd, subCmd) = FastParseArrayCommand(ref count);

                    if (cmd == RespCommand.NONE)
                    {
                        (cmd, subCmd) = SlowParseCommand(ref count, out success);
                    }
                }
            }

            return (cmd, subCmd);
        }
    }
}