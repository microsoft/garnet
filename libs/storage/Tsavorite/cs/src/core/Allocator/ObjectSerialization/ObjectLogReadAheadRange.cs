// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Chooses the range of an object log that an <see cref="ObjectLogReadAhead"/> reads for a walk over a page's records, from the
    /// positions of the records the walk will read, added in page order.
    /// </summary>
    /// <remarks>
    /// <para>A flush writes a page's records to an object log in page order, so their positions mostly ascend and one range serves them.
    /// Records whose extents were written at another time lie elsewhere:</para>
    /// <list type="bullet">
    /// <item>A capture is written to a Snapshot checkpoint's object log before the checkpoint's flush reaches its page, so it lies below
    /// the page's other records there. A main-log image of a captured record holds a position in that log, not in the main object log.
    /// Both carry the <see cref="RecordInfo.HasSnapshotObjectLogReference"/> marker, and the caller does not add them.</item>
    /// <item>Recovery copies captures into the main object log at its tail, so on their pages the copies lie above every other record.
    /// The records of a page whose upper part recovery copied from a Snapshot checkpoint's object log jump to the tail there too.</item>
    /// </list>
    /// <para>So the records form an ascending run, which the read-ahead reads through, and outliers, which are read directly. The state is
    /// constant-size, and each record added does one of the following:</para>
    /// <list type="bullet">
    /// <item>A record above the run's last member joins the run. If it lies beyond the end of every member's first read, it is a jump:
    /// the bytes below it are its own chunks, an unused object-log page tail, or other records' extents. The run remembers its latest
    /// jump, with its own state before that jump.</item>
    /// <item>A record below the latest jump but above the run's last member before it shows that the jump and the members since are
    /// outliers, such as recovery's copies: the run reverts to its state before the jump, and the record joins it.</item>
    /// <item>A record below the run's first member shows that the whole run is outliers, and the run starts again at the record.</item>
    /// <item>Any other record lies among the run's members, written out of order, and is left out.</item>
    /// </list>
    /// <para>Outliers removed from the run are kept as a second run, since copies ascend too, and the read-ahead reads through whichever
    /// run has more records. The run's range has a hole: its widest gap below a jump that is not a directory record, whose chunks would
    /// fill the gap. So copies that end a page are read ahead without reading the bytes between them and the page's other records.</para>
    /// <para>The rules see only the latest jump. A block of copies with a jump of its own, at an object-log page boundary or a directory
    /// copy, hides the come-back below it, so the records after it are left out and read directly. And a page whose last jump is a directory
    /// record has no hole there, so a copy below that jump lies within the range; when the walk reads it, its release drops the windows that
    /// the page's later records need.</para>
    /// <para>The choice affects only how many reads a walk takes: a read the read-ahead cannot serve is made directly.</para>
    /// </remarks>
    internal struct ObjectLogReadAheadRange
    {
        /// <summary>The position of the run's first member.</summary>
        long runStart;

        /// <summary>The highest end of a run member's first read.</summary>
        long runEnd;

        /// <summary>The position of the run's last member, which is its highest.</summary>
        long runLast;

        /// <summary>The number of records in the run; zero until the first record is added.</summary>
        int runCount;

        /// <summary>Whether the run has a jump: a member that lay beyond <see cref="runEnd"/> when it joined.</summary>
        bool hasJump;

        /// <summary>The position of the run's latest jump.</summary>
        long jumpPosition;

        /// <summary><see cref="runEnd"/> before the latest jump joined.</summary>
        long jumpBaseEnd;

        /// <summary><see cref="runLast"/> before the latest jump joined.</summary>
        long jumpBaseLast;

        /// <summary><see cref="runCount"/> before the latest jump joined.</summary>
        int jumpBaseCount;

        /// <summary>The start of the run's hole: <see cref="runEnd"/> before the jump at <see cref="holeEnd"/> joined.</summary>
        long holeStart;

        /// <summary>The position of the jump that ends the run's hole; equal to <see cref="holeStart"/> when the run has no hole.</summary>
        long holeEnd;

        /// <summary>The position of the first outlier removed from the run.</summary>
        long outlierStart;

        /// <summary>The highest end of an outlier's first read.</summary>
        long outlierEnd;

        /// <summary>The position of the last outlier removed from the run, which is its highest.</summary>
        long outlierLast;

        /// <summary>The number of outliers removed from the run, which form a run of their own.</summary>
        int outlierCount;

        /// <summary>Add the next record the walk reads, whose extents are in <paramref name="log"/>.</summary>
        /// <remarks>A top extent never spans an object-log page, so the record's first read ends at the end of its page at most.</remarks>
        internal void Add(in ObjectLogRecordRef record, ObjectLog log)
            => Add(record.Address, Math.Min(record.TopExtentInitialRead, log.RemainingOnPage(record.Address)),
                ObjectLogPositionInfo.WordHasDirectory(record.PositionWord));

        /// <summary>Add the next record the walk reads.</summary>
        /// <param name="position">The address of the extent the record's position addresses: its single extent or its top directory record.</param>
        /// <param name="initialRead">The length of the record's first read of that extent, <see cref="ObjectLogRecordRef.TopExtentInitialRead"/>.</param>
        /// <param name="hasDirectory">Whether that extent is a directory record, whose data chunks lie below it.</param>
        internal void Add(long position, long initialRead, bool hasDirectory)
        {
            // An empty extent reads nothing, and may share its position with the next record.
            if (initialRead <= 0)
                return;

            var end = position + initialRead;
            if (runCount == 0)
            {
                StartRun(position, end);
                return;
            }

            if (position > runLast)
            {
                Join(position, end, hasDirectory);
                return;
            }

            if (hasJump && position > jumpBaseLast && position < jumpPosition)
            {
                // The record continues the run below its latest jump, so the jump and the members since are outliers. The jump's gap, if it
                // is the run's hole, goes with them.
                AddOutliers(jumpPosition, runEnd, runLast, runCount - jumpBaseCount);
                if (holeEnd >= jumpPosition)
                    holeStart = holeEnd = 0;
                runEnd = jumpBaseEnd;
                runLast = jumpBaseLast;
                runCount = jumpBaseCount;
                hasJump = false;
                Join(position, end, hasDirectory);
                return;
            }

            if (position < runStart)
            {
                // The whole run lies above the record.
                AddOutliers(runStart, runEnd, runLast, runCount);
                StartRun(position, end);
            }
        }

        void StartRun(long position, long end)
        {
            runStart = runLast = position;
            runEnd = end;
            runCount = 1;
            hasJump = false;
            holeStart = holeEnd = 0;
        }

        void Join(long position, long end, bool hasDirectory)
        {
            if (position > runEnd)
            {
                hasJump = true;
                jumpPosition = position;
                jumpBaseEnd = runEnd;
                jumpBaseLast = runLast;
                jumpBaseCount = runCount;
                if (!hasDirectory && position - runEnd > holeEnd - holeStart)
                {
                    holeStart = runEnd;
                    holeEnd = position;
                }
            }
            runEnd = Math.Max(runEnd, end);
            runLast = position;
            runCount++;
        }

        void AddOutliers(long start, long end, long last, int count)
        {
            if (outlierCount > 0 && start > outlierLast)
            {
                // Recovery copies a page's captures in page order, so later outliers continue the earlier ones.
                outlierEnd = Math.Max(outlierEnd, end);
                outlierLast = last;
                outlierCount += count;
            }
            else if (count > outlierCount)
            {
                outlierStart = start;
                outlierEnd = end;
                outlierLast = last;
                outlierCount = count;
            }
        }

        /// <summary>Get the range to read ahead through, and the hole within it that none of the range's records reads.</summary>
        /// <param name="start">The lowest address to read.</param>
        /// <param name="end">One past the highest address to read.</param>
        /// <param name="holeStart">The start of the hole; equal to <paramref name="holeEnd"/> when there is none.</param>
        /// <param name="holeEnd">One past the end of the hole.</param>
        /// <returns>False if fewer than two records would be read through the range, since a single record's reads gain nothing from it.</returns>
        internal readonly bool TryGetRange(out long start, out long end, out long holeStart, out long holeEnd)
        {
            int count;
            if (outlierCount > runCount)
            {
                start = outlierStart;
                end = outlierEnd;
                holeStart = holeEnd = end;
                count = outlierCount;
            }
            else
            {
                start = runStart;
                end = runEnd;
                holeStart = this.holeStart;
                holeEnd = this.holeEnd;
                if (holeEnd <= holeStart)
                    holeStart = holeEnd = end;
                count = runCount;
            }
            return count >= 2 && end > start;
        }
    }
}