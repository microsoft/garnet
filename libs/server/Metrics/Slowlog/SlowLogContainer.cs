// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Garnet.server
{
    /// <summary>
    /// Thread-safe container for the slow log
    /// </summary>
    class SlowLogContainer
    {
        readonly int size;
        readonly ConcurrentQueue<SlowLogEntry> logEntries;
        int id = 0;

        /// <summary>
        /// Create new instance of slow log container
        /// </summary>
        /// <param name="size"></param>
        public SlowLogContainer(int size)
        {
            this.size = size;
            logEntries = new ConcurrentQueue<SlowLogEntry>();
        }

        /// <summary>
        /// Count number of entries in the slow log
        /// </summary>
        public int Count => logEntries.Count;

        /// <summary>
        /// Add entry to the slow log with auto-assigned id
        /// </summary>
        /// <param name="entry"></param>
        public void Add(SlowLogEntry entry)
        {
            entry.Id = Interlocked.Increment(ref id) - 1;
            logEntries.Enqueue(entry);
            while (logEntries.Count > size)
            {
                logEntries.TryDequeue(out _);
            }
        }

        /// <summary>
        /// Clear the slow log buffer
        /// </summary>
        public void Clear()
            => logEntries.Clear();

        /// <summary>
        /// Get a snapshot of the latest count entries in the slow log (-1 returns all entries)
        /// </summary>
        public List<SlowLogEntry> GetEntries(int count)
        {
            List<SlowLogEntry> entries = [.. logEntries];
            if (count < 0 || count >= entries.Count)
                return entries;
            return entries.GetRange(entries.Count - count, count);
        }
    }
}