// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Faults in fresh direct-VM log pages ahead of the appenders that will write them.
    /// <para>
    /// The log is append-only, so the appenders that cross into a fresh page all touch the same unmapped addresses at once. With transparent
    /// huge pages the kernel allocates and zeroes a whole 2 MB page for each faulting thread before it takes the page-table lock, and every
    /// thread but one then discards its page, so most of the fault work is wasted.
    /// </para>
    /// <para>
    /// Instead, the appenders populate the page allocated ahead of the tail as they fill the current one, one 2 MB chunk (one huge page) at a
    /// time: an allocation that crosses into chunk <c>k</c> of the current page makes sure the first <c>k + </c><see cref="Lead"/> chunks of
    /// the next page are claimed, populating any it claims itself. A chunk is claimed by exactly one thread, so no two threads fault in the same
    /// huge page, and the work is spread over the appenders as they cross chunk boundaries, with no thread or allocation of its own. The thread
    /// that turns the tail onto a page populates any chunk not yet claimed, and waits for any still being populated, before appenders use it.
    /// </para>
    /// <para>
    /// Each circular-buffer slot has a state word: the number of chunks not yet claimed in the upper half and the number being populated in the
    /// lower half. Zero means nothing is pending.
    /// </para>
    /// </summary>
    internal sealed class LogPagePrefaulter
    {
        /// <summary>Log2 of the bytes populated at once: one transparent huge page.</summary>
        internal const int ChunkBits = 21;
        const long ChunkSize = 1L << ChunkBits;

        /// <summary>
        /// Chunks of the next page claimed ahead of the chunk of the current page the appenders have reached, so that the last ones have finished
        /// populating by the time the current page is full.
        /// </summary>
        internal const int Lead = 2;

        const long OneUnclaimed = 1L << 32;
        const long RunningMask = OneUnclaimed - 1;

        readonly long[] state;
        readonly long[] address;
        readonly long length;
        readonly int chunkCount;

        /// <param name="pageCount">Number of circular-buffer slots.</param>
        /// <param name="length">Bytes to populate per page.</param>
        internal LogPagePrefaulter(int pageCount, long length)
        {
            state = new long[pageCount];
            address = new long[pageCount];
            this.length = length;
            chunkCount = (int)((length + ChunkSize - 1) >> ChunkBits);
        }

        /// <summary>
        /// Whether pages of <paramref name="pageSizeBytes"/> should be prefaulted: they are direct-VM pages large enough to be backed by
        /// transparent huge pages, and the kernel supports populating them.
        /// </summary>
        internal static bool IsUseful(long pageSizeBytes) => pageSizeBytes >= DirectVirtualMemory.HugePageSize && DirectVirtualMemory.CanPopulate;

        /// <summary>
        /// Record that the fresh page at <paramref name="pageAddress"/>, installed in slot <paramref name="index"/>, is unpopulated. Any earlier
        /// page in the slot has been cancelled or populated.
        /// </summary>
        internal void MarkPending(int index, long pageAddress)
        {
            address[index] = pageAddress;
            Volatile.Write(ref state[index], (long)chunkCount * OneUnclaimed);
        }

        /// <summary>
        /// Populate chunks of the page in slot <paramref name="index"/>, in order, until at least <paramref name="target"/> of them have been
        /// claimed. Returns at once if they have been, or nothing is pending.
        /// </summary>
        internal void Help(int index, int target)
        {
            while (true)
            {
                var current = Volatile.Read(ref state[index]);
                var unclaimed = (int)(current >> 32);
                if (unclaimed == 0 || chunkCount - unclaimed >= target)
                    return;
                if (Interlocked.CompareExchange(ref state[index], current - OneUnclaimed + 1, current) != current)
                    continue;

                var offset = (long)(chunkCount - unclaimed) << ChunkBits;
                DirectVirtualMemory.Populate((nint)(Volatile.Read(ref address[index]) + offset), Math.Min(ChunkSize, length - offset));
                _ = Interlocked.Decrement(ref state[index]);
            }
        }

        /// <summary>Return once slot <paramref name="index"/> is populated, populating here any chunk not yet claimed.</summary>
        internal void EnsurePopulated(int index)
        {
            Help(index, chunkCount);
            WaitForRunning(index);
        }

        /// <summary>Drop the chunks of slot <paramref name="index"/> not yet claimed, and wait out those being populated, before the page is freed.</summary>
        internal void Cancel(int index)
        {
            var current = Volatile.Read(ref state[index]);
            while ((current >> 32) != 0)
            {
                var seen = Interlocked.CompareExchange(ref state[index], current & RunningMask, current);
                if (seen == current)
                    break;
                current = seen;
            }
            WaitForRunning(index);
        }

        /// <summary>Cancel every slot, before the pages are handed off at teardown.</summary>
        internal void CancelAll()
        {
            for (var index = 0; index < state.Length; index++)
                Cancel(index);
        }

        /// <summary>Whether slot <paramref name="index"/> has chunks not yet claimed or being populated.</summary>
        internal bool IsPending(int index) => Volatile.Read(ref state[index]) != 0;

        /// <summary>
        /// Wait for the chunks of slot <paramref name="index"/> that other threads are populating, once none is left unclaimed. Each takes well
        /// under a millisecond, and the appenders are themselves waiting for the page turn, so this yields but never sleeps a whole millisecond.
        /// </summary>
        void WaitForRunning(int index)
        {
            var spinWait = new SpinWait();
            while (Volatile.Read(ref state[index]) != 0)
                spinWait.SpinOnce(sleep1Threshold: -1);
        }
    }
}