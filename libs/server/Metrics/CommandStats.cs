// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Garnet.server
{
    /// <summary>
    /// Per-command statistics entry tracking calls, failures, rejections, and latency.
    /// Follows the Redis COMMANDSTATS convention.
    /// </summary>
    public struct CommandStatsEntry
    {
        /// <summary>
        /// Total number of times this command was called.
        /// </summary>
        public ulong Calls;

        /// <summary>
        /// Total number of times this command failed (returned an error response).
        /// </summary>
        public ulong FailedCalls;

        /// <summary>
        /// Total number of times this command was rejected before execution (e.g., ACL denied, OOM).
        /// </summary>
        public ulong RejectedCalls;
    }

    /// <summary>
    /// Tracks per-command usage statistics for built-in commands.
    /// Array-indexed by RespCommand enum value for O(1) access.
    /// Each session owns its own instance (single-writer, no locking needed).
    /// </summary>
    public class CommandStats
    {
        /// <summary>
        /// Number of entries in the stats array, sized to hold all valid RespCommand values.
        /// </summary>
        internal static readonly int EntryCount = (int)RespCommandExtensions.LastValidCommand + 1;

        /// <summary>
        /// Per-command statistics entries indexed by (int)RespCommand.
        /// </summary>
        internal CommandStatsEntry[] entries;

        /// <summary>
        /// Creates a new CommandStats instance with zeroed entries.
        /// </summary>
        public CommandStats()
        {
            entries = new CommandStatsEntry[EntryCount];
        }

        /// <summary>
        /// Increment the calls counter for the given command.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementCalls(RespCommand cmd)
        {
            int idx = (int)cmd;
            if ((uint)idx < (uint)entries.Length)
                entries[idx].Calls++;
        }

        /// <summary>
        /// Increment the failed calls counter for the given command.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementFailed(RespCommand cmd)
        {
            int idx = (int)cmd;
            if ((uint)idx < (uint)entries.Length)
                entries[idx].FailedCalls++;
        }

        /// <summary>
        /// Increment the rejected calls counter for the given command.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementRejected(RespCommand cmd)
        {
            int idx = (int)cmd;
            if ((uint)idx < (uint)entries.Length)
                entries[idx].RejectedCalls++;
        }

        /// <summary>
        /// Get the stats entry for the given command.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CommandStatsEntry GetEntry(RespCommand cmd)
        {
            int idx = (int)cmd;
            if ((uint)idx < (uint)entries.Length)
                return entries[idx];
            return default;
        }

        /// <summary>
        /// Add another CommandStats instance into this one (for aggregation).
        /// </summary>
        internal void Add(CommandStats other)
        {
            if (other?.entries == null)
                return;

            int len = Math.Min(entries.Length, other.entries.Length);
            for (int i = 0; i < len; i++)
            {
                entries[i].Calls += other.entries[i].Calls;
                entries[i].FailedCalls += other.entries[i].FailedCalls;
                entries[i].RejectedCalls += other.entries[i].RejectedCalls;
            }
        }

        /// <summary>
        /// Reset all entries to zero.
        /// </summary>
        internal void Reset()
        {
            Array.Clear(entries, 0, entries.Length);
        }

    }
}