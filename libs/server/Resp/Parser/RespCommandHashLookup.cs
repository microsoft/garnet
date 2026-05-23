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
    ///
    /// Design:
    /// - 32-byte entries (half a cache line) with open addressing and linear probing
    /// - CRC32 hardware hash (single instruction) with multiply-shift software fallback
    /// - Table fits in L1 cache (~8-16KB)
    /// - Single-threaded read-only access after static init
    /// </summary>
    internal static unsafe partial class RespCommandHashLookup
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

            // Validate all subcommand tables are round-trip correct
            ValidateSubTable(RespCommand.CLUSTER, ClusterSubcommands, clusterSubTable, clusterSubTableMask);
            ValidateSubTable(RespCommand.CLIENT, ClientSubcommands, clientSubTable, clientSubTableMask);
            ValidateSubTable(RespCommand.ACL, AclSubcommands, aclSubTable, aclSubTableMask);
            ValidateSubTable(RespCommand.COMMAND, CommandSubcommands, commandSubTable, commandSubTableMask);
            ValidateSubTable(RespCommand.CONFIG, ConfigSubcommands, configSubTable, configSubTableMask);
            ValidateSubTable(RespCommand.SCRIPT, ScriptSubcommands, scriptSubTable, scriptSubTableMask);
            ValidateSubTable(RespCommand.LATENCY, LatencySubcommands, latencySubTable, latencySubTableMask);
            ValidateSubTable(RespCommand.SLOWLOG, SlowlogSubcommands, slowlogSubTable, slowlogSubTableMask);
            ValidateSubTable(RespCommand.MODULE, ModuleSubcommands, moduleSubTable, moduleSubTableMask);
            ValidateSubTable(RespCommand.PUBSUB, PubsubSubcommands, pubsubSubTable, pubsubSubTableMask);
            ValidateSubTable(RespCommand.MEMORY, MemorySubcommands, memorySubTable, memorySubTableMask);
            ValidateSubTable(RespCommand.BITOP, BitopSubcommands, bitopSubTable, bitopSubTableMask);

            // Validate primary table: every inserted command must round-trip via Lookup
            ValidatePrimaryTable();
        }

        /// <summary>
        /// Validate that every command inserted into the primary table can be looked up.
        /// Called once during static init; throws on any mismatch to catch registration bugs early.
        /// </summary>
        private static void ValidatePrimaryTable()
        {
            // Scan all occupied slots and verify each round-trips via Lookup
            Span<byte> word0Bytes = stackalloc byte[8];
            Span<byte> word1Bytes = stackalloc byte[8];
            Span<byte> word2Bytes = stackalloc byte[8];

            for (int i = 0; i < primaryTable.Length; i++)
            {
                ref CommandEntry entry = ref primaryTable[i];
                if (entry.NameLength == 0) continue;

                // Reconstruct the name from the stored words
                var nameBytes = new byte[entry.NameLength];
                MemoryMarshal.Write(word0Bytes, in entry.NameWord0);
                MemoryMarshal.Write(word1Bytes, in entry.NameWord1);
                MemoryMarshal.Write(word2Bytes, in entry.NameWord2);

                if (entry.NameLength <= 8)
                {
                    word0Bytes.Slice(0, entry.NameLength).CopyTo(nameBytes);
                }
                else if (entry.NameLength <= 16)
                {
                    word0Bytes.CopyTo(nameBytes);
                    // Word1 stores last 8 bytes (overlapping for lengths 9-16)
                    word1Bytes.CopyTo(nameBytes.AsSpan(entry.NameLength - 8));
                }
                else
                {
                    word0Bytes.CopyTo(nameBytes);
                    word1Bytes.CopyTo(nameBytes.AsSpan(8));
                    word2Bytes.CopyTo(nameBytes.AsSpan(entry.NameLength - 8));
                }

                fixed (byte* p = nameBytes)
                {
                    var found = Lookup(p, nameBytes.Length);
                    if (found != entry.Command)
                    {
                        throw new InvalidOperationException(
                            $"Primary hash table validation failed: '{System.Text.Encoding.ASCII.GetString(nameBytes)}' expected {entry.Command} but Lookup returned {found}");
                    }
                }
            }
        }

        /// <summary>
        /// Validate that every entry in a subcommand definition array can be looked up in the hash table.
        /// Called once during static init; throws on any mismatch to catch typos early.
        /// </summary>
        private static void ValidateSubTable(RespCommand parent, ReadOnlySpan<(string Name, RespCommand Command)> subcommands,
            CommandEntry[] table, int mask)
        {
            foreach (var (name, expectedCmd) in subcommands)
            {
                var nameBytes = System.Text.Encoding.ASCII.GetBytes(name);
                fixed (byte* p = nameBytes)
                {
                    var found = LookupInTable(table, mask, p, nameBytes.Length);
                    if (found != expectedCmd)
                    {
                        throw new InvalidOperationException(
                            $"Hash table validation failed: {parent} subcommand '{name}' expected {expectedCmd} but got {found}");
                    }
                }
            }
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
        /// Look up a primary command name and return whether it has subcommands, in a single probe.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static RespCommand Lookup(byte* name, int length, out bool hasSubcommands)
        {
            hasSubcommands = false;
            if ((uint)length - 1 > 23)
                return RespCommand.NONE;

            uint hash = ComputeHash(name, length);
            int idx = (int)(hash & (uint)PrimaryTableMask);

            for (int probe = 0; probe < MaxProbes; probe++)
            {
                ref CommandEntry entry = ref primaryTable[idx];
                if (entry.NameLength == 0) return RespCommand.NONE;
                if (entry.NameLength == (byte)length && MatchName(ref entry, name, length))
                {
                    hasSubcommands = (entry.Flags & FlagHasSubcommands) != 0;
                    return entry.Command;
                }
                idx = (idx + 1) & PrimaryTableMask;
            }
            return RespCommand.NONE;
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
            Debug.Assert(length > 0, "MatchName: length must be positive");

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
            // CommandEntry stores at most 24 bytes of name; empty or oversized names can never match.
            if ((uint)length - 1 > 23) return RespCommand.NONE;

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
            if (name.Length == 0 || name.Length > 24)
                throw new ArgumentException($"Command name must be 1-24 bytes, got {name.Length}: {System.Text.Encoding.ASCII.GetString(name)}");

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

                    // Store name words to match the layout expected by MatchName:
                    //   length <= 8:  Word0 = bytes 0..len-1 (zero-padded)
                    //   length 9-16:  Word0 = bytes 0..7, Word1 = bytes len-8..len-1 (overlapping)
                    //   length 17-24: Word0 = bytes 0..7, Word1 = bytes 8..15, Word2 = bytes len-8..len-1
                    entry.NameWord0 = GetWordFromSpan(name, 0);
                    entry.NameWord1 = 0;
                    entry.NameWord2 = 0;

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

    }
}