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
    /// <para>A flush writes a page's records to the object log in address order, so the extents a walk of the page reads lie in one range,
    /// from its first record's extent to its last one's, and the walk reads them in ascending order. Within a record it reads the top
    /// extent first, then any spine records, which lie below the top, and then the data entries in ascending order; all of them lie at or
    /// above the end of the previous record's top extent. The read-ahead keeps up to <c>maxWindows</c> windows of the range in flight or
    /// resident, from the lowest address the walk may still read, and slides forward as the walk reports, through <see cref="Release"/>,
    /// that it is done with the bytes below an address.</para>
    /// <para>It is only an optimization. A read it cannot serve, because the bytes lie outside the range, below what was released, beyond
    /// the windows read so far, or in a window whose read failed, reports false and the caller reads the bytes directly. So bytes out of
    /// that order cost IOs, never correctness, and a device error surfaces through the direct read.</para>
    /// <para>Used by one thread at a time, as <see cref="ObjectLogReader{TStoreFunctions}"/> is.</para>
    /// </remarks>
    internal sealed class ObjectLogReadAhead : IDisposable
    {
        readonly ObjectLog log;
        readonly long endAddress;
        readonly int windowSize;
        readonly int maxWindows;

        /// <summary>The windows in flight or resident, contiguous and in ascending order.</summary>
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
        internal ObjectLogReadAhead(ObjectLog log, long startAddress, long endAddress, int windowSize, int maxWindows)
        {
            if (windowSize <= 0 || maxWindows <= 0)
                throw new ArgumentOutOfRangeException(windowSize <= 0 ? nameof(windowSize) : nameof(maxWindows));
            this.log = log;
            this.endAddress = Math.Min(endAddress, log.TailAddress);
            this.windowSize = windowSize;
            this.maxWindows = maxWindows;
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
        internal void Release(long logicalAddress)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            if (logicalAddress <= releasedUntilAddress)
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
                var length = (int)Math.Min(Math.Min(windowSize, endAddress - nextWindowAddress), log.RemainingOnPage(nextWindowAddress));
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