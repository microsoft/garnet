// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Faults in fresh direct-VM log pages ahead of the appenders that will write them.
    /// <para>
    /// The log is append-only, so the appenders that cross into a fresh page all touch the same unmapped addresses at once. With transparent
    /// huge pages the kernel allocates and zeroes a whole 2 MB page for each faulting thread before it takes the page-table lock, and every
    /// thread but one then discards its page, so most of the fault work is wasted. Populating a page before appenders reach it makes them find
    /// it mapped; if it is not yet populated when appenders need it, they wait for the populate instead of racing.
    /// </para>
    /// <para>
    /// Each circular-buffer slot has a state word: zero when nothing is pending; the page's address while its populate is pending; and the
    /// address with <see cref="RunningBit"/> set while it is being populated. A pending populate is claimed with a compare-and-swap, so it runs
    /// once: on the shared workers for a page allocated ahead of the tail, or on the thread that turns the tail onto the page if no worker has
    /// started it. Page addresses are sector-aligned, so the low bit is free.
    /// </para>
    /// </summary>
    internal sealed class LogPagePrefaulter
    {
        const long RunningBit = 1;

        readonly long[] state;
        readonly long length;

        /// <param name="pageCount">Number of circular-buffer slots.</param>
        /// <param name="length">Bytes to populate per page.</param>
        internal LogPagePrefaulter(int pageCount, long length)
        {
            state = new long[pageCount];
            this.length = length;
        }

        /// <summary>
        /// Whether pages of <paramref name="pageSizeBytes"/> should be prefaulted: they are direct-VM pages large enough to be backed by
        /// transparent huge pages, and the kernel supports populating them.
        /// </summary>
        internal static bool IsUseful(long pageSizeBytes) => pageSizeBytes >= DirectVirtualMemory.HugePageSize && DirectVirtualMemory.CanPopulate;

        /// <summary>Record that the fresh page at <paramref name="address"/>, installed in slot <paramref name="index"/>, is unpopulated.</summary>
        internal void MarkPending(int index, long address) => Volatile.Write(ref state[index], address);

        /// <summary>Hand the pending populate of slot <paramref name="index"/>, if any, to the workers.</summary>
        internal void Queue(int index)
        {
#if DEBUG
            testOnQueue?.Invoke();
#endif
            var current = Volatile.Read(ref state[index]);
            if (current != 0 && (current & RunningBit) == 0)
                Workers.Enqueue(this, index, current);
        }

#if DEBUG
        /// <summary>Test only, DEBUG builds only: if set, <see cref="Queue"/> invokes it first.</summary>
        internal static Action testOnQueue;
#endif

        /// <summary>Return once slot <paramref name="index"/> is populated, populating it here if no worker has started it.</summary>
        internal void EnsurePopulated(int index) => Settle(index, populate: true);

        /// <summary>Drop a pending populate of slot <paramref name="index"/>, or wait out a running one, before the page is freed.</summary>
        internal void Cancel(int index) => Settle(index, populate: false);

        /// <summary>Cancel every slot, before the pages are handed off at teardown.</summary>
        internal void CancelAll()
        {
            for (var index = 0; index < state.Length; index++)
                Cancel(index);
        }

        /// <summary>Whether slot <paramref name="index"/> has a populate pending or running.</summary>
        internal bool IsPending(int index) => Volatile.Read(ref state[index]) != 0;

        void Settle(int index, bool populate)
        {
            var spinWait = new SpinWait();
            while (true)
            {
                var current = Volatile.Read(ref state[index]);
                if (current == 0)
                    return;
                if ((current & RunningBit) == 0)
                {
                    if (populate)
                    {
                        if (TryPopulate(index, current))
                            return;
                    }
                    else if (Interlocked.CompareExchange(ref state[index], 0, current) == current)
                        return;
                    continue;
                }

                // Another thread is populating the page; it finishes in a few milliseconds.
                spinWait.SpinOnce();
            }
        }

        /// <summary>Claim the pending populate of the page at <paramref name="address"/>; false if it is no longer pending.</summary>
        bool TryClaim(int index, long address) => Interlocked.CompareExchange(ref state[index], address | RunningBit, address) == address;

        /// <summary>Mark slot <paramref name="index"/> populated, once its claimed populate has finished.</summary>
        void Complete(int index) => Volatile.Write(ref state[index], 0);

        /// <summary>Claim and run, on this thread, the pending populate of the page at <paramref name="address"/>; false if it is no longer pending.</summary>
        bool TryPopulate(int index, long address)
        {
            if (!TryClaim(index, address))
                return false;
            try
            {
                DirectVirtualMemory.Populate((nint)address, length);
            }
            finally
            {
                Complete(index);
            }
            return true;
        }

        /// <summary>
        /// Background threads, shared by all allocators, that populate queued pages. Zeroing the memory is most of the work, and appenders can
        /// fill a page faster than one thread zeroes it, so each page is split into a part per thread.
        /// </summary>
        static class Workers
        {
            /// <summary>Worker threads; they block on the queue while no page is being filled.</summary>
            static readonly int Count = Math.Clamp(Environment.ProcessorCount / 8, 1, 4);
            static readonly BlockingCollection<Work> queue = new();
            static int started;

            /// <summary>A queued page (<see cref="Job"/> null), or a part of a page another worker has claimed.</summary>
            readonly record struct Work(LogPagePrefaulter Prefaulter, int Index, long Address, long Length, PageJob Job);

            /// <summary>Counts down the parts of a claimed page; the last to finish marks the page populated.</summary>
            sealed class PageJob(LogPagePrefaulter prefaulter, int index, int parts)
            {
                int remaining = parts;

                internal void PartDone()
                {
                    if (Interlocked.Decrement(ref remaining) == 0)
                        prefaulter.Complete(index);
                }
            }

            internal static void Enqueue(LogPagePrefaulter prefaulter, int index, long address)
            {
                if (Volatile.Read(ref started) == 0 && Interlocked.CompareExchange(ref started, 1, 0) == 0)
                {
                    for (var i = 0; i < Count; i++)
                        new Thread(Run) { IsBackground = true, Name = "Tsavorite log page prefault" }.Start();
                }
                queue.Add(new(prefaulter, index, address, 0, null));
            }

            static void Run()
            {
                foreach (var work in queue.GetConsumingEnumerable())
                {
                    if (work.Job is not null)
                    {
                        DirectVirtualMemory.Populate((nint)work.Address, work.Length);
                        work.Job.PartDone();
                        continue;
                    }

                    // A stale item, for a page since populated, cancelled or replaced, fails the claim and is skipped.
                    var prefaulter = work.Prefaulter;
                    if (!prefaulter.TryClaim(work.Index, work.Address))
                        continue;

                    // Parts are whole huge pages, so no two threads fault in the same one.
                    var partLength = (prefaulter.length / Count + DirectVirtualMemory.HugePageSize - 1) & ~(DirectVirtualMemory.HugePageSize - 1);
                    var parts = (int)((prefaulter.length + partLength - 1) / partLength);
                    var job = new PageJob(prefaulter, work.Index, parts);
                    for (var part = 1; part < parts; part++)
                    {
                        var offset = part * partLength;
                        queue.Add(new(prefaulter, work.Index, work.Address + offset, Math.Min(partLength, prefaulter.length - offset), job));
                    }
                    DirectVirtualMemory.Populate((nint)work.Address, Math.Min(partLength, prefaulter.length));
                    job.PartDone();
                }
            }
        }
    }
}