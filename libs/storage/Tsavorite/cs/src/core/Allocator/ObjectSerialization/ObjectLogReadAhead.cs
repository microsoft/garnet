// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace Tsavorite.core
{
    /// <summary>
    /// Reads ahead through the range of an <see cref="ObjectLog"/> that a walk over a main-log page's records will read, in a few large
    /// windows instead of an IO per extent.
    /// </summary>
    /// <remarks>
    /// <para>A flush writes a page's records to the object log in page order, so the extents a walk of the page reads mostly lie in one
    /// range, which <see cref="ObjectLogReadAheadRange"/> chooses, and the walk reads the records in that range in ascending order. Within a
    /// record it reads the top extent first (the extent the record's position addresses, and its highest; see <see cref="ObjectLogExtent"/>),
    /// then any spine records, which lie below the top, and then the data entries in ascending order; all of them lie at or above the end of
    /// the previous record's top extent. The read-ahead keeps up to <c>maxWindows</c> windows of the range in flight or resident, from the
    /// lowest address the walk may still read, and slides forward as the walk reports, through <see cref="Release"/>, that it is done with
    /// the bytes below an address. The range may have a hole that none of its records reads, such as the bytes between a page's records and
    /// recovery's copies that end the page; no window reads it.</para>
    /// <para>The walk also reads records outside the range, such as captures and recovery's copies, whose extents were written at other
    /// times. Their reads go direct. A release beyond the range or within its hole is ignored, and one below what was already released does
    /// nothing, so such a record leaves the windows where the range's records need them, except as the remarks on
    /// <see cref="ObjectLogReadAheadRange"/> describe.</para>
    /// <para>It is only an optimization. A read it cannot serve, because the bytes lie outside the range or in its hole, below what was
    /// released, beyond the windows read so far, or in a window whose read failed, reports false and the caller reads the bytes directly.
    /// So bytes out of that order cost IOs, never correctness, and a device error surfaces through the direct read.</para>
    /// <para>Used by one thread at a time, as <see cref="ObjectLogReader{TStoreFunctions}"/> is.</para>
    /// </remarks>
    internal sealed class ObjectLogReadAhead : IDisposable
    {
        readonly ObjectLog log;
        readonly long endAddress;
        readonly int windowSize;
        readonly int maxWindows;

        /// <summary>The start of the range's hole, which no window reads; equal to <see cref="holeEnd"/> when the range has none.</summary>
        readonly long holeStart;

        /// <summary>One past the end of the range's hole.</summary>
        readonly long holeEnd;

        /// <summary>The windows in flight or resident, in ascending order, contiguous except across the hole.</summary>
        readonly List<ObjectLog.RangeRead> windows;

        /// <summary>Where the next window starts.</summary>
        long nextWindowAddress;

        /// <summary>The walk reads nothing below this address again.</summary>
        long releasedUntilAddress;

        bool disposed;

        /// <summary>Start reading ahead through [<paramref name="startAddress"/>, <paramref name="endAddress"/>) of <paramref name="log"/>.</summary>
        /// <param name="log">The log to read.</param>
        /// <param name="startAddress">The lowest address the walk will read from the windows.</param>
        /// <param name="endAddress">One past the highest address the walk will read; clamped to the log's tail.</param>
        /// <param name="windowSize">The largest read a window makes. A window never spans a log page.</param>
        /// <param name="maxWindows">The most windows in flight or resident at once.</param>
        /// <param name="holeStart">The start of a hole within the range that the walk reads nothing from; no window reads it.</param>
        /// <param name="holeEnd">One past the end of the hole; at or below <paramref name="holeStart"/> for none.</param>
        internal ObjectLogReadAhead(ObjectLog log, long startAddress, long endAddress, int windowSize, int maxWindows, long holeStart = 0, long holeEnd = 0)
        {
            if (windowSize <= 0 || maxWindows <= 0)
                throw new ArgumentOutOfRangeException(windowSize <= 0 ? nameof(windowSize) : nameof(maxWindows));
            this.log = log;
            this.endAddress = Math.Min(endAddress, log.TailAddress);
            this.windowSize = windowSize;
            this.maxWindows = maxWindows;
            var hasHole = holeStart >= startAddress && holeEnd > holeStart && holeEnd <= this.endAddress;
            this.holeStart = hasHole ? holeStart : this.endAddress;
            this.holeEnd = hasHole ? holeEnd : this.endAddress;
            windows = new(maxWindows);
            nextWindowAddress = releasedUntilAddress = startAddress;
            Fill();
        }

        /// <summary>Number of windows in flight or resident, for tests.</summary>
        internal int WindowCount => windows.Count;

        /// <summary>
        /// Copy [<paramref name="logicalAddress"/>, <paramref name="logicalAddress"/> + <paramref name="length"/>) to
        /// <paramref name="destination"/> from the windows, waiting for their reads if they are in flight.
        /// </summary>
        /// <returns>False if the windows do not hold those bytes, so the caller must read them directly.</returns>
        internal bool TryRead(long logicalAddress, int length, Span<byte> destination)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            var end = logicalAddress + length;
            if (windows.Count == 0 || logicalAddress < windows[0].StartAddress || end > windows[^1].EndAddress)
                return false;

            // The windows skip the hole, so check that they hold every byte requested before copying any.
            var covered = logicalAddress;
            foreach (var window in windows)
            {
                if (window.EndAddress <= covered)
                    continue;
                if (window.StartAddress > covered)
                    return false;
                covered = window.EndAddress;
                if (covered >= end)
                    break;
            }

            foreach (var window in windows)
            {
                if (window.EndAddress <= logicalAddress)
                    continue;
                if (window.StartAddress >= end)
                    break;
                var from = Math.Max(logicalAddress, window.StartAddress);
                var until = Math.Min(end, window.EndAddress);
                if (!window.TryCopyTo(from, destination.Slice((int)(from - logicalAddress), (int)(until - from))))
                    return false;
            }
            return true;
        }

        /// <summary>
        /// Record that the walk reads nothing below <paramref name="logicalAddress"/> again, so the windows below it can go and new ones can
        /// start beyond the last.
        /// </summary>
        /// <remarks>An address beyond the range or within its hole comes from a record outside the range, which was read directly. It is
        /// ignored, since records of the range that the walk has yet to read may lie below it.</remarks>
        internal void Release(long logicalAddress)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            if (logicalAddress <= releasedUntilAddress || logicalAddress > endAddress || (logicalAddress > holeStart && logicalAddress <= holeEnd))
                return;
            releasedUntilAddress = logicalAddress;

            var released = 0;
            while (released < windows.Count && windows[released].EndAddress <= logicalAddress)
                windows[released++].Dispose();
            windows.RemoveRange(0, released);

            // The walk has passed every window read so far, as after reading a large record directly, so continue from where it is.
            if (nextWindowAddress < logicalAddress)
                nextWindowAddress = logicalAddress;
            Fill();
        }

        void Fill()
        {
            while (windows.Count < maxWindows && nextWindowAddress < endAddress)
            {
                if (nextWindowAddress >= holeStart && nextWindowAddress < holeEnd)
                {
                    nextWindowAddress = holeEnd;
                    continue;
                }
                var limit = nextWindowAddress < holeStart ? holeStart : endAddress;
                var length = (int)Math.Min(Math.Min(windowSize, limit - nextWindowAddress), log.RemainingOnPage(nextWindowAddress));
                var window = log.TryBeginRangeRead(nextWindowAddress, length);
                if (window is null)
                {
                    // The range is no longer within the log, as after a truncation; stop reading ahead and let reads go direct.
                    nextWindowAddress = endAddress;
                    return;
                }
                windows.Add(window);
                nextWindowAddress = window.EndAddress;
            }
        }

        /// <summary>Wait for the windows' reads and return their buffers.</summary>
        public void Dispose()
        {
            if (disposed)
                return;
            disposed = true;
            foreach (var window in windows)
                window.Dispose();
            windows.Clear();
        }
    }
}