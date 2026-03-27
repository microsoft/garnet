// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.X86;

namespace Garnet.server
{
    /// <summary>
    /// Cache-friendly O(1) hash table for RESP command name lookup.
    /// Replaces the nested switch/if-else chains in FastParseArrayCommand and SlowParseCommand.
    ///
    /// Design:
    /// - 32-byte entries (half a cache line) with open addressing and linear probing
    /// - CRC32 hardware hash (single instruction) with multiply-shift software fallback
    /// - Vector128 validation for command names > 8 bytes
    /// - Table fits in L1 cache (~8-16KB)
    /// - Single-threaded read-only access after static init
    /// </summary>
    internal static unsafe class RespCommandHashLookup
    {
        /// <summary>
        /// Entry in the command hash table. Exactly 32 bytes = half a cache line.
        /// Two entries per cache line gives excellent spatial locality during linear probing.
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Size = 32)]
        private struct CommandEntry
        {
            /// <summary>The command enum value.</summary>
            [FieldOffset(0)]
            public RespCommand Command;

            /// <summary>Length of the command name in bytes.</summary>
            [FieldOffset(2)]
            public byte NameLength;

            /// <summary>Flags (e.g., HasSubcommands).</summary>
            [FieldOffset(3)]
            public byte Flags;

            /// <summary>First 8 bytes of the uppercase command name.</summary>
            [FieldOffset(8)]
            public ulong NameWord0;

            /// <summary>Bytes 8-15 of the uppercase command name (zero-padded).</summary>
            [FieldOffset(16)]
            public ulong NameWord1;

            /// <summary>Bytes 16-23 of the uppercase command name (zero-padded).</summary>
            [FieldOffset(24)]
            public ulong NameWord2;
        }

        /// <summary>Flag indicating the command has subcommands that require a second lookup.</summary>
        internal const byte FlagHasSubcommands = 1;

        // Primary command table: 512 entries = 16KB, fits in L1 cache
        private const int PrimaryTableBits = 9;
        private const int PrimaryTableSize = 1 << PrimaryTableBits;
        private const int PrimaryTableMask = PrimaryTableSize - 1;
        private const int MaxProbes = 16;

        private static readonly CommandEntry[] primaryTable;

        // Subcommand tables (per parent command)
        private static readonly CommandEntry[] clusterSubTable;
        private static readonly int clusterSubTableMask;

        private static readonly CommandEntry[] clientSubTable;
        private static readonly int clientSubTableMask;

        private static readonly CommandEntry[] aclSubTable;
        private static readonly int aclSubTableMask;

        private static readonly CommandEntry[] commandSubTable;
        private static readonly int commandSubTableMask;

        private static readonly CommandEntry[] configSubTable;
        private static readonly int configSubTableMask;

        private static readonly CommandEntry[] scriptSubTable;
        private static readonly int scriptSubTableMask;

        private static readonly CommandEntry[] latencySubTable;
        private static readonly int latencySubTableMask;

        private static readonly CommandEntry[] slowlogSubTable;
        private static readonly int slowlogSubTableMask;

        private static readonly CommandEntry[] moduleSubTable;
        private static readonly int moduleSubTableMask;

        private static readonly CommandEntry[] pubsubSubTable;
        private static readonly int pubsubSubTableMask;

        private static readonly CommandEntry[] memorySubTable;
        private static readonly int memorySubTableMask;

        private static readonly CommandEntry[] bitopSubTable;
        private static readonly int bitopSubTableMask;

        static RespCommandHashLookup()
        {
            // Build primary command table
            primaryTable = GC.AllocateArray<CommandEntry>(PrimaryTableSize, pinned: true);
            PopulatePrimaryTable();

            // Build subcommand tables
            clusterSubTable = BuildSubTable(ClusterSubcommands, out clusterSubTableMask);
            clientSubTable = BuildSubTable(ClientSubcommands, out clientSubTableMask);
            aclSubTable = BuildSubTable(AclSubcommands, out aclSubTableMask);
            commandSubTable = BuildSubTable(CommandSubcommands, out commandSubTableMask);
            configSubTable = BuildSubTable(ConfigSubcommands, out configSubTableMask);
            scriptSubTable = BuildSubTable(ScriptSubcommands, out scriptSubTableMask);
            latencySubTable = BuildSubTable(LatencySubcommands, out latencySubTableMask);
            slowlogSubTable = BuildSubTable(SlowlogSubcommands, out slowlogSubTableMask);
            moduleSubTable = BuildSubTable(ModuleSubcommands, out moduleSubTableMask);
            pubsubSubTable = BuildSubTable(PubsubSubcommands, out pubsubSubTableMask);
            memorySubTable = BuildSubTable(MemorySubcommands, out memorySubTableMask);
            bitopSubTable = BuildSubTable(BitopSubcommands, out bitopSubTableMask);
        }

        #region Public API

        /// <summary>
        /// Look up a primary command name in the hash table.
        /// </summary>
        /// <param name="name">Pointer to the uppercase command name bytes.</param>
        /// <param name="length">Length of the command name.</param>
        /// <returns>The matching RespCommand, or RespCommand.NONE if not found.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static RespCommand Lookup(byte* name, int length)
        {
            return LookupInTable(primaryTable, PrimaryTableMask, name, length);
        }

        /// <summary>
        /// Check if the given command has subcommands.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool HasSubcommands(byte* name, int length)
        {
            uint hash = ComputeHash(name, length);
            int idx = (int)(hash & (uint)PrimaryTableMask);

            for (int probe = 0; probe < MaxProbes; probe++)
            {
                ref CommandEntry entry = ref primaryTable[idx];
                if (entry.NameLength == 0) return false;
                if (entry.NameLength == (byte)length && MatchName(ref entry, name, length))
                    return (entry.Flags & FlagHasSubcommands) != 0;
                idx = (idx + 1) & PrimaryTableMask;
            }
            return false;
        }

        /// <summary>
        /// Look up a subcommand for a given parent command.
        /// </summary>
        /// <param name="parent">The parent command.</param>
        /// <param name="name">Pointer to the uppercase subcommand name bytes.</param>
        /// <param name="length">Length of the subcommand name.</param>
        /// <returns>The matching RespCommand, or RespCommand.NONE if not found.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static RespCommand LookupSubcommand(RespCommand parent, byte* name, int length)
        {
            var (table, mask) = parent switch
            {
                RespCommand.CLUSTER => (clusterSubTable, clusterSubTableMask),
                RespCommand.CLIENT => (clientSubTable, clientSubTableMask),
                RespCommand.ACL => (aclSubTable, aclSubTableMask),
                RespCommand.COMMAND => (commandSubTable, commandSubTableMask),
                RespCommand.CONFIG => (configSubTable, configSubTableMask),
                RespCommand.SCRIPT => (scriptSubTable, scriptSubTableMask),
                RespCommand.LATENCY => (latencySubTable, latencySubTableMask),
                RespCommand.SLOWLOG => (slowlogSubTable, slowlogSubTableMask),
                RespCommand.MODULE => (moduleSubTable, moduleSubTableMask),
                RespCommand.PUBSUB => (pubsubSubTable, pubsubSubTableMask),
                RespCommand.MEMORY => (memorySubTable, memorySubTableMask),
                RespCommand.BITOP => (bitopSubTable, bitopSubTableMask),
                _ => (null, 0)
            };

            if (table == null) return RespCommand.NONE;
            return LookupInTable(table, mask, name, length);
        }

        #endregion

        #region Hash and Match

        /// <summary>
        /// Compute hash from command name bytes using hardware CRC32 when available.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static uint ComputeHash(byte* name, int length)
        {
            ulong word0 = length >= 8 ? *(ulong*)name : ReadPartialWord(name, length);

            if (Sse42.X64.IsSupported)
            {
                uint crc = (uint)Sse42.X64.Crc32(0UL, word0);
                return Sse42.Crc32(crc, (uint)length);
            }

            if (System.Runtime.Intrinsics.Arm.Crc32.Arm64.IsSupported)
            {
                uint crc = (uint)System.Runtime.Intrinsics.Arm.Crc32.Arm64.ComputeCrc32C(0U, word0);
                return System.Runtime.Intrinsics.Arm.Crc32.ComputeCrc32C(crc, (uint)length);
            }

            // Software fallback: Fibonacci multiply-shift
            return (uint)((word0 * 0x9E3779B97F4A7C15UL) >> 32) ^ (uint)(length * 2654435761U);
        }

        /// <summary>
        /// Compute hash from a ReadOnlySpan (used during table construction).
        /// </summary>
        private static uint ComputeHash(ReadOnlySpan<byte> name)
        {
            ulong word0 = GetWordFromSpan(name, 0);

            if (Sse42.X64.IsSupported)
            {
                uint crc = (uint)Sse42.X64.Crc32(0UL, word0);
                return Sse42.Crc32(crc, (uint)name.Length);
            }

            if (System.Runtime.Intrinsics.Arm.Crc32.Arm64.IsSupported)
            {
                uint crc = (uint)System.Runtime.Intrinsics.Arm.Crc32.Arm64.ComputeCrc32C(0U, word0);
                return System.Runtime.Intrinsics.Arm.Crc32.ComputeCrc32C(crc, (uint)name.Length);
            }

            return (uint)((word0 * 0x9E3779B97F4A7C15UL) >> 32) ^ (uint)(name.Length * 2654435761U);
        }

        /// <summary>
        /// Read up to 8 bytes from a pointer, zero-extending short reads.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong ReadPartialWord(byte* p, int len)
        {
            // For len >= 8, just read the full ulong
            // For len < 8, read and mask
            Debug.Assert(len > 0 && len < 8);

            return len switch
            {
                1 => *p,
                2 => *(ushort*)p,
                3 => *(ushort*)p | ((ulong)p[2] << 16),
                4 => *(uint*)p,
                5 => *(uint*)p | ((ulong)p[4] << 32),
                6 => *(uint*)p | ((ulong)*(ushort*)(p + 4) << 32),
                7 => *(uint*)p | ((ulong)*(ushort*)(p + 4) << 32) | ((ulong)p[6] << 48),
                _ => 0
            };
        }

        /// <summary>
        /// Compare entry name against input name bytes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool MatchName(ref CommandEntry entry, byte* name, int length)
        {
            if (length <= 8)
            {
                ulong inputWord = length == 8 ? *(ulong*)name : ReadPartialWord(name, length);
                return entry.NameWord0 == inputWord;
            }
            else if (length <= 16)
            {
                // Compare first 8 bytes and last 8 bytes (may overlap for 9-15 byte names)
                return entry.NameWord0 == *(ulong*)name &&
                       entry.NameWord1 == *(ulong*)(name + length - 8);
            }
            else
            {
                // Compare first 8, middle 8, and last 8 bytes
                return entry.NameWord0 == *(ulong*)name &&
                       entry.NameWord1 == *(ulong*)(name + 8) &&
                       entry.NameWord2 == *(ulong*)(name + length - 8);
            }
        }

        /// <summary>
        /// Core lookup in any hash table (primary or subcommand).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static RespCommand LookupInTable(CommandEntry[] table, int tableMask, byte* name, int length)
        {
            uint hash = ComputeHash(name, length);
            int idx = (int)(hash & (uint)tableMask);

            for (int probe = 0; probe < MaxProbes; probe++)
            {
                ref CommandEntry entry = ref table[idx];

                // Empty slot — command not found
                if (entry.NameLength == 0) return RespCommand.NONE;

                // Fast rejection: length mismatch (single byte compare)
                if (entry.NameLength == (byte)length && MatchName(ref entry, name, length))
                    return entry.Command;

                idx = (idx + 1) & tableMask;
            }

            return RespCommand.NONE;
        }

        #endregion

        #region Table Construction

        /// <summary>
        /// Read a ulong from a span at the given offset, zero-padding short reads.
        /// </summary>
        private static ulong GetWordFromSpan(ReadOnlySpan<byte> span, int offset)
        {
            if (offset >= span.Length) return 0;
            int remaining = span.Length - offset;
            if (remaining >= 8) return MemoryMarshal.Read<ulong>(span.Slice(offset));

            ulong word = 0;
            for (int i = 0; i < remaining; i++)
                word |= (ulong)span[offset + i] << (i * 8);
            return word;
        }

        /// <summary>
        /// Insert a command into a hash table.
        /// </summary>
        private static void InsertIntoTable(CommandEntry[] table, int tableMask, ReadOnlySpan<byte> name, RespCommand command, byte flags = 0)
        {
            uint hash = ComputeHash(name);
            int idx = (int)(hash & (uint)tableMask);

            for (int probe = 0; probe < MaxProbes; probe++)
            {
                ref CommandEntry entry = ref table[idx];
                if (entry.NameLength == 0)
                {
                    entry.Command = command;
                    entry.NameLength = (byte)name.Length;
                    entry.Flags = flags;
                    entry.NameWord0 = GetWordFromSpan(name, 0);
                    entry.NameWord1 = name.Length > 8 ? GetWordFromSpan(name, name.Length - 8) : 0;
                    entry.NameWord2 = name.Length > 16 ? GetWordFromSpan(name, name.Length - 8) : 0;

                    // For names > 16 bytes, store bytes 8-15 exactly and last 8 bytes in word2
                    if (name.Length > 16)
                    {
                        entry.NameWord1 = GetWordFromSpan(name, 8);
                        entry.NameWord2 = GetWordFromSpan(name, name.Length - 8);
                    }
                    else if (name.Length > 8)
                    {
                        entry.NameWord1 = GetWordFromSpan(name, name.Length - 8);
                    }

                    return;
                }
                idx = (idx + 1) & tableMask;
            }

            throw new InvalidOperationException(
                $"Hash table overflow: could not insert command '{System.Text.Encoding.ASCII.GetString(name)}' after {MaxProbes} probes. Increase table size.");
        }

        /// <summary>
        /// Build a subcommand hash table from a list of (name, command) pairs.
        /// </summary>
        private static CommandEntry[] BuildSubTable(ReadOnlySpan<(string Name, RespCommand Command)> subcommands, out int mask)
        {
            // Find next power of 2 that gives at most ~70% load factor
            int size = 16;
            while (size * 7 / 10 < subcommands.Length) size <<= 1;
            mask = size - 1;

            var table = GC.AllocateArray<CommandEntry>(size, pinned: true);
            foreach (var (name, command) in subcommands)
            {
                InsertIntoTable(table, mask, System.Text.Encoding.ASCII.GetBytes(name), command);
            }
            return table;
        }

        #endregion

        #region Command Definitions

        private static void PopulatePrimaryTable()
        {
            // Helper to insert with optional subcommand flag
            void Add(string name, RespCommand cmd, bool hasSub = false)
            {
                InsertIntoTable(primaryTable, PrimaryTableMask,
                    System.Text.Encoding.ASCII.GetBytes(name), cmd,
                    hasSub ? FlagHasSubcommands : (byte)0);
            }

            // ===== Data commands (read + write) =====

            // String commands
            Add("GET", RespCommand.GET);
            Add("SET", RespCommand.SET);
            Add("DEL", RespCommand.DEL);
            Add("INCR", RespCommand.INCR);
            Add("DECR", RespCommand.DECR);
            Add("INCRBY", RespCommand.INCRBY);
            Add("DECRBY", RespCommand.DECRBY);
            Add("INCRBYFLOAT", RespCommand.INCRBYFLOAT);
            Add("APPEND", RespCommand.APPEND);
            Add("GETSET", RespCommand.GETSET);
            Add("GETDEL", RespCommand.GETDEL);
            Add("GETEX", RespCommand.GETEX);
            Add("GETRANGE", RespCommand.GETRANGE);
            Add("SETRANGE", RespCommand.SETRANGE);
            Add("STRLEN", RespCommand.STRLEN);
            Add("SUBSTR", RespCommand.SUBSTR);
            Add("SETNX", RespCommand.SETNX);
            Add("SETEX", RespCommand.SETEX);
            Add("PSETEX", RespCommand.PSETEX);
            Add("MGET", RespCommand.MGET);
            Add("MSET", RespCommand.MSET);
            Add("MSETNX", RespCommand.MSETNX);
            Add("DUMP", RespCommand.DUMP);
            Add("RESTORE", RespCommand.RESTORE);
            Add("GETBIT", RespCommand.GETBIT);
            Add("SETBIT", RespCommand.SETBIT);
            Add("GETWITHETAG", RespCommand.GETWITHETAG);
            Add("GETIFNOTMATCH", RespCommand.GETIFNOTMATCH);
            Add("SETIFMATCH", RespCommand.SETIFMATCH);
            Add("SETIFGREATER", RespCommand.SETIFGREATER);
            Add("DELIFGREATER", RespCommand.DELIFGREATER);
            Add("LCS", RespCommand.LCS);

            // Key commands
            Add("EXISTS", RespCommand.EXISTS);
            Add("TTL", RespCommand.TTL);
            Add("PTTL", RespCommand.PTTL);
            Add("EXPIRE", RespCommand.EXPIRE);
            Add("PEXPIRE", RespCommand.PEXPIRE);
            Add("EXPIREAT", RespCommand.EXPIREAT);
            Add("PEXPIREAT", RespCommand.PEXPIREAT);
            Add("EXPIRETIME", RespCommand.EXPIRETIME);
            Add("PEXPIRETIME", RespCommand.PEXPIRETIME);
            Add("PERSIST", RespCommand.PERSIST);
            Add("TYPE", RespCommand.TYPE);
            Add("RENAME", RespCommand.RENAME);
            Add("RENAMENX", RespCommand.RENAMENX);
            Add("UNLINK", RespCommand.UNLINK);
            Add("KEYS", RespCommand.KEYS);
            Add("SCAN", RespCommand.SCAN);
            Add("DBSIZE", RespCommand.DBSIZE);
            Add("SELECT", RespCommand.SELECT);
            Add("SWAPDB", RespCommand.SWAPDB);
            Add("MIGRATE", RespCommand.MIGRATE);

            // Bitmap commands
            Add("BITCOUNT", RespCommand.BITCOUNT);
            Add("BITPOS", RespCommand.BITPOS);
            Add("BITFIELD", RespCommand.BITFIELD);
            Add("BITFIELD_RO", RespCommand.BITFIELD_RO);
            Add("BITOP", RespCommand.BITOP);

            // HyperLogLog commands
            Add("PFADD", RespCommand.PFADD);
            Add("PFCOUNT", RespCommand.PFCOUNT);
            Add("PFMERGE", RespCommand.PFMERGE);

            // Hash commands
            Add("HSET", RespCommand.HSET);
            Add("HGET", RespCommand.HGET);
            Add("HDEL", RespCommand.HDEL);
            Add("HLEN", RespCommand.HLEN);
            Add("HEXISTS", RespCommand.HEXISTS);
            Add("HGETALL", RespCommand.HGETALL);
            Add("HKEYS", RespCommand.HKEYS);
            Add("HVALS", RespCommand.HVALS);
            Add("HMSET", RespCommand.HMSET);
            Add("HMGET", RespCommand.HMGET);
            Add("HSETNX", RespCommand.HSETNX);
            Add("HINCRBY", RespCommand.HINCRBY);
            Add("HINCRBYFLOAT", RespCommand.HINCRBYFLOAT);
            Add("HRANDFIELD", RespCommand.HRANDFIELD);
            Add("HSCAN", RespCommand.HSCAN);
            Add("HSTRLEN", RespCommand.HSTRLEN);
            Add("HTTL", RespCommand.HTTL);
            Add("HPTTL", RespCommand.HPTTL);
            Add("HEXPIRE", RespCommand.HEXPIRE);
            Add("HPEXPIRE", RespCommand.HPEXPIRE);
            Add("HEXPIREAT", RespCommand.HEXPIREAT);
            Add("HPEXPIREAT", RespCommand.HPEXPIREAT);
            Add("HEXPIRETIME", RespCommand.HEXPIRETIME);
            Add("HPEXPIRETIME", RespCommand.HPEXPIRETIME);
            Add("HPERSIST", RespCommand.HPERSIST);
            Add("HCOLLECT", RespCommand.HCOLLECT);

            // List commands
            Add("LPUSH", RespCommand.LPUSH);
            Add("RPUSH", RespCommand.RPUSH);
            Add("LPUSHX", RespCommand.LPUSHX);
            Add("RPUSHX", RespCommand.RPUSHX);
            Add("LPOP", RespCommand.LPOP);
            Add("RPOP", RespCommand.RPOP);
            Add("LLEN", RespCommand.LLEN);
            Add("LINDEX", RespCommand.LINDEX);
            Add("LINSERT", RespCommand.LINSERT);
            Add("LRANGE", RespCommand.LRANGE);
            Add("LREM", RespCommand.LREM);
            Add("LSET", RespCommand.LSET);
            Add("LTRIM", RespCommand.LTRIM);
            Add("LPOS", RespCommand.LPOS);
            Add("LMOVE", RespCommand.LMOVE);
            Add("LMPOP", RespCommand.LMPOP);
            Add("RPOPLPUSH", RespCommand.RPOPLPUSH);
            Add("BLPOP", RespCommand.BLPOP);
            Add("BRPOP", RespCommand.BRPOP);
            Add("BLMOVE", RespCommand.BLMOVE);
            Add("BRPOPLPUSH", RespCommand.BRPOPLPUSH);
            Add("BLMPOP", RespCommand.BLMPOP);

            // Set commands
            Add("SADD", RespCommand.SADD);
            Add("SREM", RespCommand.SREM);
            Add("SPOP", RespCommand.SPOP);
            Add("SCARD", RespCommand.SCARD);
            Add("SMEMBERS", RespCommand.SMEMBERS);
            Add("SISMEMBER", RespCommand.SISMEMBER);
            Add("SMISMEMBER", RespCommand.SMISMEMBER);
            Add("SRANDMEMBER", RespCommand.SRANDMEMBER);
            Add("SMOVE", RespCommand.SMOVE);
            Add("SSCAN", RespCommand.SSCAN);
            Add("SDIFF", RespCommand.SDIFF);
            Add("SDIFFSTORE", RespCommand.SDIFFSTORE);
            Add("SINTER", RespCommand.SINTER);
            Add("SINTERCARD", RespCommand.SINTERCARD);
            Add("SINTERSTORE", RespCommand.SINTERSTORE);
            Add("SUNION", RespCommand.SUNION);
            Add("SUNIONSTORE", RespCommand.SUNIONSTORE);

            // Sorted set commands
            Add("ZADD", RespCommand.ZADD);
            Add("ZREM", RespCommand.ZREM);
            Add("ZCARD", RespCommand.ZCARD);
            Add("ZSCORE", RespCommand.ZSCORE);
            Add("ZMSCORE", RespCommand.ZMSCORE);
            Add("ZRANK", RespCommand.ZRANK);
            Add("ZREVRANK", RespCommand.ZREVRANK);
            Add("ZCOUNT", RespCommand.ZCOUNT);
            Add("ZLEXCOUNT", RespCommand.ZLEXCOUNT);
            Add("ZRANGE", RespCommand.ZRANGE);
            Add("ZRANGEBYLEX", RespCommand.ZRANGEBYLEX);
            Add("ZRANGEBYSCORE", RespCommand.ZRANGEBYSCORE);
            Add("ZRANGESTORE", RespCommand.ZRANGESTORE);
            Add("ZREVRANGE", RespCommand.ZREVRANGE);
            Add("ZREVRANGEBYLEX", RespCommand.ZREVRANGEBYLEX);
            Add("ZREVRANGEBYSCORE", RespCommand.ZREVRANGEBYSCORE);
            Add("ZPOPMIN", RespCommand.ZPOPMIN);
            Add("ZPOPMAX", RespCommand.ZPOPMAX);
            Add("ZRANDMEMBER", RespCommand.ZRANDMEMBER);
            Add("ZSCAN", RespCommand.ZSCAN);
            Add("ZINCRBY", RespCommand.ZINCRBY);
            Add("ZDIFF", RespCommand.ZDIFF);
            Add("ZDIFFSTORE", RespCommand.ZDIFFSTORE);
            Add("ZINTER", RespCommand.ZINTER);
            Add("ZINTERCARD", RespCommand.ZINTERCARD);
            Add("ZINTERSTORE", RespCommand.ZINTERSTORE);
            Add("ZUNION", RespCommand.ZUNION);
            Add("ZUNIONSTORE", RespCommand.ZUNIONSTORE);
            Add("ZMPOP", RespCommand.ZMPOP);
            Add("BZMPOP", RespCommand.BZMPOP);
            Add("BZPOPMAX", RespCommand.BZPOPMAX);
            Add("BZPOPMIN", RespCommand.BZPOPMIN);
            Add("ZREMRANGEBYLEX", RespCommand.ZREMRANGEBYLEX);
            Add("ZREMRANGEBYRANK", RespCommand.ZREMRANGEBYRANK);
            Add("ZREMRANGEBYSCORE", RespCommand.ZREMRANGEBYSCORE);
            Add("ZTTL", RespCommand.ZTTL);
            Add("ZPTTL", RespCommand.ZPTTL);
            Add("ZEXPIRE", RespCommand.ZEXPIRE);
            Add("ZPEXPIRE", RespCommand.ZPEXPIRE);
            Add("ZEXPIREAT", RespCommand.ZEXPIREAT);
            Add("ZPEXPIREAT", RespCommand.ZPEXPIREAT);
            Add("ZEXPIRETIME", RespCommand.ZEXPIRETIME);
            Add("ZPEXPIRETIME", RespCommand.ZPEXPIRETIME);
            Add("ZPERSIST", RespCommand.ZPERSIST);
            Add("ZCOLLECT", RespCommand.ZCOLLECT);

            // Geo commands
            Add("GEOADD", RespCommand.GEOADD);
            Add("GEOPOS", RespCommand.GEOPOS);
            Add("GEOHASH", RespCommand.GEOHASH);
            Add("GEODIST", RespCommand.GEODIST);
            Add("GEOSEARCH", RespCommand.GEOSEARCH);
            Add("GEOSEARCHSTORE", RespCommand.GEOSEARCHSTORE);
            Add("GEORADIUS", RespCommand.GEORADIUS);
            Add("GEORADIUS_RO", RespCommand.GEORADIUS_RO);
            Add("GEORADIUSBYMEMBER", RespCommand.GEORADIUSBYMEMBER);
            Add("GEORADIUSBYMEMBER_RO", RespCommand.GEORADIUSBYMEMBER_RO);

            // Scripting
            Add("EVAL", RespCommand.EVAL);
            Add("EVALSHA", RespCommand.EVALSHA);

            // Pub/Sub
            Add("PUBLISH", RespCommand.PUBLISH);
            Add("SUBSCRIBE", RespCommand.SUBSCRIBE);
            Add("PSUBSCRIBE", RespCommand.PSUBSCRIBE);
            Add("UNSUBSCRIBE", RespCommand.UNSUBSCRIBE);
            Add("PUNSUBSCRIBE", RespCommand.PUNSUBSCRIBE);
            Add("SPUBLISH", RespCommand.SPUBLISH);
            Add("SSUBSCRIBE", RespCommand.SSUBSCRIBE);

            // Custom object scan
            Add("CUSTOMOBJECTSCAN", RespCommand.COSCAN);

            // ===== Control / admin commands =====
            Add("PING", RespCommand.PING);
            Add("ECHO", RespCommand.ECHO);
            Add("QUIT", RespCommand.QUIT);
            Add("AUTH", RespCommand.AUTH);
            Add("HELLO", RespCommand.HELLO);
            Add("INFO", RespCommand.INFO);
            Add("TIME", RespCommand.TIME);
            Add("ROLE", RespCommand.ROLE);
            Add("SAVE", RespCommand.SAVE);
            Add("LASTSAVE", RespCommand.LASTSAVE);
            Add("BGSAVE", RespCommand.BGSAVE);
            Add("COMMITAOF", RespCommand.COMMITAOF);
            Add("FLUSHALL", RespCommand.FLUSHALL);
            Add("FLUSHDB", RespCommand.FLUSHDB);
            Add("FORCEGC", RespCommand.FORCEGC);
            Add("PURGEBP", RespCommand.PURGEBP);
            Add("FAILOVER", RespCommand.FAILOVER);
            Add("MONITOR", RespCommand.MONITOR);
            Add("REGISTERCS", RespCommand.REGISTERCS);
            Add("ASYNC", RespCommand.ASYNC);
            Add("DEBUG", RespCommand.DEBUG);
            Add("EXPDELSCAN", RespCommand.EXPDELSCAN);
            Add("WATCH", RespCommand.WATCH);
            Add("WATCHMS", RespCommand.WATCHMS);
            Add("WATCHOS", RespCommand.WATCHOS);
            Add("MULTI", RespCommand.MULTI);
            Add("EXEC", RespCommand.EXEC);
            Add("DISCARD", RespCommand.DISCARD);
            Add("UNWATCH", RespCommand.UNWATCH);
            Add("RUNTXP", RespCommand.RUNTXP);
            Add("ASKING", RespCommand.ASKING);
            Add("READONLY", RespCommand.READONLY);
            Add("READWRITE", RespCommand.READWRITE);
            Add("REPLICAOF", RespCommand.REPLICAOF);
            Add("SECONDARYOF", RespCommand.SECONDARYOF);
            Add("SLAVEOF", RespCommand.SECONDARYOF);

            // Parent commands with subcommands
            Add("SCRIPT", RespCommand.SCRIPT, hasSub: true);
            Add("CONFIG", RespCommand.CONFIG, hasSub: true);
            Add("CLIENT", RespCommand.CLIENT, hasSub: true);
            Add("CLUSTER", RespCommand.CLUSTER, hasSub: true);
            Add("ACL", RespCommand.ACL, hasSub: true);
            Add("COMMAND", RespCommand.COMMAND, hasSub: true);
            Add("LATENCY", RespCommand.LATENCY, hasSub: true);
            Add("SLOWLOG", RespCommand.SLOWLOG, hasSub: true);
            Add("MODULE", RespCommand.MODULE, hasSub: true);
            Add("PUBSUB", RespCommand.PUBSUB, hasSub: true);
            Add("MEMORY", RespCommand.MEMORY, hasSub: true);
        }

        #endregion

        #region Subcommand Definitions

        private static readonly (string Name, RespCommand Command)[] ClusterSubcommands =
        [
            ("ADDSLOTS", RespCommand.CLUSTER_ADDSLOTS),
            ("ADDSLOTSRANGE", RespCommand.CLUSTER_ADDSLOTSRANGE),
            ("AOFSYNC", RespCommand.CLUSTER_AOFSYNC),
            ("APPENDLOG", RespCommand.CLUSTER_APPENDLOG),
            ("ATTACH_SYNC", RespCommand.CLUSTER_ATTACH_SYNC),
            ("BANLIST", RespCommand.CLUSTER_BANLIST),
            ("BEGIN_REPLICA_RECOVER", RespCommand.CLUSTER_BEGIN_REPLICA_RECOVER),
            ("BUMPEPOCH", RespCommand.CLUSTER_BUMPEPOCH),
            ("COUNTKEYSINSLOT", RespCommand.CLUSTER_COUNTKEYSINSLOT),
            ("DELKEYSINSLOT", RespCommand.CLUSTER_DELKEYSINSLOT),
            ("DELKEYSINSLOTRANGE", RespCommand.CLUSTER_DELKEYSINSLOTRANGE),
            ("DELSLOTS", RespCommand.CLUSTER_DELSLOTS),
            ("DELSLOTSRANGE", RespCommand.CLUSTER_DELSLOTSRANGE),
            ("ENDPOINT", RespCommand.CLUSTER_ENDPOINT),
            ("FAILOVER", RespCommand.CLUSTER_FAILOVER),
            ("FAILREPLICATIONOFFSET", RespCommand.CLUSTER_FAILREPLICATIONOFFSET),
            ("FAILSTOPWRITES", RespCommand.CLUSTER_FAILSTOPWRITES),
            ("FLUSHALL", RespCommand.CLUSTER_FLUSHALL),
            ("FORGET", RespCommand.CLUSTER_FORGET),
            ("GETKEYSINSLOT", RespCommand.CLUSTER_GETKEYSINSLOT),
            ("GOSSIP", RespCommand.CLUSTER_GOSSIP),
            ("HELP", RespCommand.CLUSTER_HELP),
            ("INFO", RespCommand.CLUSTER_INFO),
            ("INITIATE_REPLICA_SYNC", RespCommand.CLUSTER_INITIATE_REPLICA_SYNC),
            ("KEYSLOT", RespCommand.CLUSTER_KEYSLOT),
            ("MEET", RespCommand.CLUSTER_MEET),
            ("MIGRATE", RespCommand.CLUSTER_MIGRATE),
            ("MTASKS", RespCommand.CLUSTER_MTASKS),
            ("MYID", RespCommand.CLUSTER_MYID),
            ("MYPARENTID", RespCommand.CLUSTER_MYPARENTID),
            ("NODES", RespCommand.CLUSTER_NODES),
            ("PUBLISH", RespCommand.CLUSTER_PUBLISH),
            ("SPUBLISH", RespCommand.CLUSTER_SPUBLISH),
            ("REPLICAS", RespCommand.CLUSTER_REPLICAS),
            ("REPLICATE", RespCommand.CLUSTER_REPLICATE),
            ("RESET", RespCommand.CLUSTER_RESET),
            ("SEND_CKPT_FILE_SEGMENT", RespCommand.CLUSTER_SEND_CKPT_FILE_SEGMENT),
            ("SEND_CKPT_METADATA", RespCommand.CLUSTER_SEND_CKPT_METADATA),
            ("SET-CONFIG-EPOCH", RespCommand.CLUSTER_SETCONFIGEPOCH),
            ("SETSLOT", RespCommand.CLUSTER_SETSLOT),
            ("SETSLOTSRANGE", RespCommand.CLUSTER_SETSLOTSRANGE),
            ("SHARDS", RespCommand.CLUSTER_SHARDS),
            ("SLOTS", RespCommand.CLUSTER_SLOTS),
            ("SLOTSTATE", RespCommand.CLUSTER_SLOTSTATE),
            ("SYNC", RespCommand.CLUSTER_SYNC),
        ];

        private static readonly (string Name, RespCommand Command)[] ClientSubcommands =
        [
            ("ID", RespCommand.CLIENT_ID),
            ("INFO", RespCommand.CLIENT_INFO),
            ("LIST", RespCommand.CLIENT_LIST),
            ("KILL", RespCommand.CLIENT_KILL),
            ("GETNAME", RespCommand.CLIENT_GETNAME),
            ("SETNAME", RespCommand.CLIENT_SETNAME),
            ("SETINFO", RespCommand.CLIENT_SETINFO),
            ("UNBLOCK", RespCommand.CLIENT_UNBLOCK),
        ];

        private static readonly (string Name, RespCommand Command)[] AclSubcommands =
        [
            ("CAT", RespCommand.ACL_CAT),
            ("DELUSER", RespCommand.ACL_DELUSER),
            ("GENPASS", RespCommand.ACL_GENPASS),
            ("GETUSER", RespCommand.ACL_GETUSER),
            ("LIST", RespCommand.ACL_LIST),
            ("LOAD", RespCommand.ACL_LOAD),
            ("SAVE", RespCommand.ACL_SAVE),
            ("SETUSER", RespCommand.ACL_SETUSER),
            ("USERS", RespCommand.ACL_USERS),
            ("WHOAMI", RespCommand.ACL_WHOAMI),
        ];

        private static readonly (string Name, RespCommand Command)[] CommandSubcommands =
        [
            ("COUNT", RespCommand.COMMAND_COUNT),
            ("DOCS", RespCommand.COMMAND_DOCS),
            ("INFO", RespCommand.COMMAND_INFO),
            ("GETKEYS", RespCommand.COMMAND_GETKEYS),
            ("GETKEYSANDFLAGS", RespCommand.COMMAND_GETKEYSANDFLAGS),
        ];

        private static readonly (string Name, RespCommand Command)[] ConfigSubcommands =
        [
            ("GET", RespCommand.CONFIG_GET),
            ("REWRITE", RespCommand.CONFIG_REWRITE),
            ("SET", RespCommand.CONFIG_SET),
        ];

        private static readonly (string Name, RespCommand Command)[] ScriptSubcommands =
        [
            ("LOAD", RespCommand.SCRIPT_LOAD),
            ("FLUSH", RespCommand.SCRIPT_FLUSH),
            ("EXISTS", RespCommand.SCRIPT_EXISTS),
        ];

        private static readonly (string Name, RespCommand Command)[] LatencySubcommands =
        [
            ("HELP", RespCommand.LATENCY_HELP),
            ("HISTOGRAM", RespCommand.LATENCY_HISTOGRAM),
            ("RESET", RespCommand.LATENCY_RESET),
        ];

        private static readonly (string Name, RespCommand Command)[] SlowlogSubcommands =
        [
            ("HELP", RespCommand.SLOWLOG_HELP),
            ("GET", RespCommand.SLOWLOG_GET),
            ("LEN", RespCommand.SLOWLOG_LEN),
            ("RESET", RespCommand.SLOWLOG_RESET),
        ];

        private static readonly (string Name, RespCommand Command)[] ModuleSubcommands =
        [
            ("LOADCS", RespCommand.MODULE_LOADCS),
        ];

        private static readonly (string Name, RespCommand Command)[] PubsubSubcommands =
        [
            ("CHANNELS", RespCommand.PUBSUB_CHANNELS),
            ("NUMSUB", RespCommand.PUBSUB_NUMSUB),
            ("NUMPAT", RespCommand.PUBSUB_NUMPAT),
        ];

        private static readonly (string Name, RespCommand Command)[] MemorySubcommands =
        [
            ("USAGE", RespCommand.MEMORY_USAGE),
        ];

        private static readonly (string Name, RespCommand Command)[] BitopSubcommands =
        [
            ("AND", RespCommand.BITOP_AND),
            ("OR", RespCommand.BITOP_OR),
            ("XOR", RespCommand.BITOP_XOR),
            ("NOT", RespCommand.BITOP_NOT),
            ("DIFF", RespCommand.BITOP_DIFF),
        ];

        #endregion
    }
}