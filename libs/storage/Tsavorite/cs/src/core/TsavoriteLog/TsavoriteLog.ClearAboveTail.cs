// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Clearing what the device holds above the tail, so that records appended at the tail never sit next to older data.
    /// </summary>
    /// <remarks>
    /// Recovery, <see cref="Initialize"/> and <see cref="Reset"/> set the tail without changing the device, so data written
    /// before (the rest of a torn write, the commits after an earlier commit that was recovered, or a previous history) stays
    /// above it. Fast-commit recovery scans forward from the newest commit-metadata file and adopts every intact commit record
    /// it reaches; once records are appended at the tail, that scan can step from them into the older data and adopt its commits.
    /// </remarks>
    public sealed partial class TsavoriteLog
    {
        /// <summary>Largest device read or write issued while clearing above the tail</summary>
        const int ClearIoChunkSize = 1 << 20;

        readonly object clearAboveTailLock = new();

        /// <summary>The tail when the log was last positioned by construction, recovery, Initialize or Reset</summary>
        long positionedTailAddress;

        /// <summary>The cookie of the commit the log was positioned at, if any</summary>
        byte[] positionedCookie;

        /// <summary>Whether the device has been cleared above the tail since the log was positioned</summary>
        bool clearedAboveTail;

        /// <summary>Whether the log was positioned by a recovery that could not determine what the log holds</summary>
        bool positionUnknown;

        /// <summary>
        /// Prepare the log for appending by clearing everything the device holds above the tail. Call this after recovery, or
        /// after <see cref="Initialize"/>, <see cref="SafeInitialize"/> or <see cref="Reset"/>, and before anything is appended.
        /// Records above the tail are lost, including commits after an earlier commit that was recovered.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Recovery leaves the device unchanged, so a log can be recovered to inspect it or to choose a commit. Once records are
        /// appended at the tail, though, a later recovery can scan from them into the data above and adopt its commits. Clearing
        /// that data first prevents this. <see cref="RecoverAsync(long, bool, System.Threading.CancellationToken)"/> can do the
        /// same as part of recovery.
        /// </para>
        /// <para>
        /// This also removes commit-metadata files for commits above the tail and writes one for the tail. It does nothing if the
        /// log has already been cleared since it was positioned. It must not run concurrently with appends.
        /// </para>
        /// </remarks>
        public void PrepareToAppend()
        {
            lock (clearAboveTailLock)
            {
                if (clearedAboveTail)
                    return;
                if (readOnlyMode)
                    throw new TsavoriteException("Cannot append to a read-only log");
                if (LogCompleted)
                    throw new TsavoriteException("Cannot append to a completed log");
                if (positionUnknown)
                    throw new TsavoriteException("Recovery did not determine what the log holds, so appending would discard it. Recover the log, or Reset or Initialize it, first.");
                if (TailAddress != positionedTailAddress)
                    throw new TsavoriteException($"{nameof(PrepareToAppend)} must be called before anything is appended to the log");

                ClearAboveTail(positionedTailAddress);
                clearedAboveTail = true;
            }
        }

        /// <summary>
        /// Record that the log has been positioned at its current tail, without having cleared the device above it
        /// </summary>
        /// <param name="cookie">The cookie of the commit the log was positioned at, if any</param>
        /// <param name="unknown">Whether the position was set by a recovery that could not determine what the log holds</param>
        void RecordCurrentPosition(byte[] cookie, bool unknown = false)
        {
            positionedTailAddress = TailAddress;
            positionedCookie = cookie;
            positionUnknown = unknown;
            clearedAboveTail = false;
        }

        void ClearAboveTail(long tailAddress)
        {
            // Remove the metadata of commits above the tail before clearing their data, so that a crash in between recovers an
            // earlier commit over unchanged data
            RemoveCommitMetadataAboveTail(tailAddress);

            if (!allocator.IsNullDevice)
                ClearDeviceAboveTail(tailAddress);

            // Recovery starts from the newest commit-metadata file, which is now this one
            var info = new TsavoriteLogRecoveryInfo { BeginAddress = beginAddress, UntilAddress = tailAddress, CommitNum = commitNum, Cookie = positionedCookie };
            logCommitManager.Commit(info.BeginAddress, info.UntilAddress, info.ToByteArray(), info.CommitNum, forceWriteMetadata: true);
        }

        /// <summary>
        /// Remove the commit-metadata files that describe data above the tail, which clearing erases, so that recovery cannot
        /// start from one
        /// </summary>
        /// <remarks>
        /// <para>
        /// A file numbered above the current commit always goes: its commit is above the tail, or it belongs to a history that
        /// <see cref="Initialize"/> or <see cref="Reset"/> replaced, since both restart commit numbering.
        /// </para>
        /// <para>
        /// A file numbered at or below the current commit usually describes this history up to the tail, and stays, so that an
        /// earlier commit can still be recovered. After Initialize with a lastCommitNum above 0, though, it can belong to the
        /// replaced history and end above the tail, and recovering its commit would set the tail inside cleared data. Its number
        /// cannot tell the two apart, so the file is loaded, and kept only if its commit ends at or below the tail.
        /// </para>
        /// </remarks>
        void RemoveCommitMetadataAboveTail(long tailAddress)
        {
            foreach (var metadataCommit in (long[])[.. logCommitManager.ListCommits()])
            {
                var remove = metadataCommit > commitNum;
                if (!remove)
                {
                    try
                    {
                        remove = LoadCommitMetadata(metadataCommit, out var info) && info.UntilAddress > tailAddress;
                    }
                    catch (Exception ex)
                    {
                        // Recovery skips a file it cannot read, and the file written for the tail outranks this one
                        logger?.LogWarning(ex, "Unable to load commit metadata for commit {metadataCommit} while clearing above tail {tailAddress}; leaving it", metadataCommit, tailAddress);
                    }
                }

                if (remove)
                    logCommitManager.RemoveCommit(metadataCommit);
            }
        }

        /// <summary>
        /// Zero or remove everything the device holds above <paramref name="tailAddress"/>
        /// </summary>
        /// <remarks>
        /// <para>
        /// <b>Segments above the tail's.</b> The device removes these first, if it can (<see cref="IDevice.TryRemoveSegmentsAbove"/>):
        /// every one it holds, however far above the tail. That includes the old segments that a replica keeps when a full sync
        /// positions its log far below them, which no search from the tail can find. The search for data below then stays within
        /// the tail's segment, so it cannot create segments above it by reading them. <see cref="IDevice.RemoveSegment(int)"/> is
        /// not used: it removes only the segment named, so each would have to be found first, and older native libraries for
        /// <see cref="NativeStorageDevice"/> remove every segment up to the one named, which here would include the tail's.
        /// </para>
        /// <para>
        /// <b>Finding the data.</b> The device cannot report how far its data extends: <see cref="IDevice.GetFileSize(int)"/> may
        /// return the configured segment size. So this reads pages forward from the one after the tail's until it encounters a run
        /// of consecutive empty pages as long as the allocator's buffer (the number of pages the log holds in memory), or until the
        /// end of the tail's segment if the device removed the segments above it. A page is empty if every byte of it reads as
        /// zero, including bytes past the end of a file or in a segment that does not exist. The data ends with the last page that
        /// holds data, and everything from the tail to there is zeroed.
        /// </para>
        /// <para>
        /// <b>Why that run.</b> Its length bounds the gaps within the data, not the amount of data, which can be most of the log
        /// after recovering an old commit or after Initialize or Reset. The data above the tail is what the log held before, so
        /// it is contiguous except where a crash stopped writes partway. A page's buffer is reused only after the page is
        /// flushed, so no more pages than the buffer holds are being written at once, and a crash cannot leave a run of empty
        /// pages that long between written ones. The first such run is therefore past all the data, however much there is. Data
        /// beyond a longer gap is not found: in the tail's own segment on any device, and anywhere on a device that cannot remove
        /// its segments above the tail's. Such a gap is left by a crash while the log had a larger buffer, or by addresses that
        /// were never written or were truncated, when Initialize or Reset positions the log below them.
        /// </para>
        /// <para>
        /// <b>Cost.</b> A page that holds data almost always has data in its first sector, so that is read first, and the rest of
        /// the page only if it is empty: a torn write can lose a page's first sector and keep later ones. When the device removes
        /// the segments above the tail's, reads are thus about one sector per page of data in the tail's segment, and writes zero
        /// that data, including any empty pages among it, since nearly every page there holds data. On a device that cannot, the
        /// reads and writes cover all the data above the tail, which can be most of the log, and on a file device reading past the
        /// data can leave empty segment files, which hold nothing.
        /// </para>
        /// <para>
        /// <b>Order.</b> Data is removed from the highest address down: the segments above the tail's from the highest, then
        /// everything else from the end of the data, ending with the sector that holds the tail. If this is interrupted, what
        /// remains above the tail is the lower part of what was there, so the next attempt finds it, and recovery can adopt only a
        /// commit that was already there.
        /// </para>
        /// </remarks>
        void ClearDeviceAboveTail(long tailAddress)
        {
            var device = allocator.Device;
            var pageSizeBits = allocator.LogPageSizeBits;
            var sectorSize = (int)device.SectorSize;
            var segmented = device.SegmentSize > 0;
            var segmentSize = segmented ? device.SegmentSize : long.MaxValue;
            var region = new DeviceZeroRegion(device, sectorSize, (int)Math.Min(ClearIoChunkSize, segmentSize));

            long PageStart(long page) => page << pageSizeBits;

            var tailPage = tailAddress >> pageSizeBits;
            var tailSegment = tailAddress / segmentSize;

            // Remove the segments above the tail's first. Then the search need not leave the tail's segment.
            var removedSegments = 0;
            var removedAbove = segmented && device.TryRemoveSegmentsAbove((int)tailSegment, out removedSegments);
            var searchEnd = removedAbove ? (tailSegment + 1) * segmentSize : long.MaxValue;

            // Find the last page above the tail's page that holds data
            var lastDataPage = tailPage;
            for (long page = tailPage + 1, emptyPages = 0; emptyPages < allocator.BufferSize && PageStart(page) < searchEnd; page++)
            {
                var pageStart = PageStart(page);
                if (region.HasData(pageStart, pageStart + sectorSize) || region.HasData(pageStart + sectorSize, PageStart(page + 1)))
                {
                    emptyPages = 0;
                    lastDataPage = page;
                }
                else
                {
                    emptyPages++;
                }
            }

            var hasData = lastDataPage > tailPage || region.HasData(tailAddress, PageStart(tailPage + 1));
            var dataEnd = PageStart(lastDataPage + 1);
            if (removedSegments > 0 || hasData)
            {
                logger?.LogInformation("Clearing log data above tail {tailAddress}: removed {removedSegments} segments above segment {tailSegment}, and zeroing {zeroedBytes} bytes from the tail",
                    tailAddress, removedSegments, tailSegment, hasData ? dataEnd - tailAddress : 0);
            }

            if (hasData)
                region.Zero(tailAddress, dataEnd);
        }

        /// <summary>
        /// Zeroes ranges of a device: <see cref="HasData"/> finds the data to zero, and <see cref="Zero"/> zeroes it. It writes
        /// only zeros, except that <see cref="Zero"/> rewrites the sector holding the start of a range with its bytes below the
        /// start unchanged.
        /// </summary>
        /// <remarks>
        /// Each IO is issued and waited for in turn, and covers at most one IO chunk. IO chunks are aligned to their size, which
        /// divides the segment size, so no IO crosses a segment; <see cref="IDevice"/> maps each IO to the segment of its first
        /// byte. One thread uses an instance: each clearing creates its own, under <c>clearAboveTailLock</c>.
        /// </remarks>
        sealed unsafe class DeviceZeroRegion
        {
            /// <summary>The device read and zeroed</summary>
            readonly IDevice device;

            /// <summary>
            /// The device's sector size. Every IO starts and ends on a sector boundary, as unbuffered file IO requires.
            /// </summary>
            readonly int sectorSize;

            /// <summary>
            /// The largest IO, and the alignment that keeps each IO within one segment: a power of two, a multiple of the sector
            /// size, that divides the segment size
            /// </summary>
            readonly int ioChunkSize;

            /// <summary>
            /// Where reads land: one IO chunk, sector-aligned. <see cref="Zero"/> also writes from it the sector it rewrites.
            /// </summary>
            readonly SectorAlignedMemory readBuffer;

            /// <summary>
            /// One IO chunk of zeros, sector-aligned: the source of every write except the sector that <see cref="Zero"/> rewrites
            /// </summary>
            readonly SectorAlignedMemory zeroBuffer;

            /// <summary>
            /// Allocate the buffers for reading and zeroing <paramref name="device"/>. They are pinned arrays that the garbage
            /// collector frees, so nothing needs disposing.
            /// </summary>
            /// <param name="device">The device to read and zero</param>
            /// <param name="sectorSize">The device's sector size</param>
            /// <param name="ioChunkSize">The largest IO to issue: a power of two, a multiple of <paramref name="sectorSize"/>, that
            /// divides the device's segment size if it has segments</param>
            internal DeviceZeroRegion(IDevice device, int sectorSize, int ioChunkSize)
            {
                this.device = device;
                this.sectorSize = sectorSize;
                this.ioChunkSize = ioChunkSize;
                readBuffer = new SectorAlignedMemory(ioChunkSize, sectorSize);
                zeroBuffer = new SectorAlignedMemory(ioChunkSize, sectorSize);
                new Span<byte>(zeroBuffer.aligned_pointer, ioChunkSize).Clear();
            }

            /// <summary>
            /// Whether any byte in [<paramref name="from"/>, <paramref name="until"/>) is non-zero. <paramref name="until"/> must
            /// be sector-aligned.
            /// </summary>
            internal bool HasData(long from, long until)
            {
                for (var address = AlignDown(from, sectorSize); address < until;)
                {
                    var length = (int)(Math.Min(until, AlignDown(address, ioChunkSize) + ioChunkSize) - address);
                    Read(address, length);
                    var skip = (int)Math.Max(0, from - address);
                    if (new ReadOnlySpan<byte>(readBuffer.aligned_pointer + skip, length - skip).IndexOfAnyExcept((byte)0) >= 0)
                        return true;
                    address += length;
                }
                return false;
            }

            /// <summary>
            /// Zero [<paramref name="from"/>, <paramref name="until"/>), highest address first. <paramref name="until"/> must be
            /// sector-aligned. The sector holding <paramref name="from"/> keeps its bytes below it, so it is read and rewritten,
            /// last.
            /// </summary>
            internal void Zero(long from, long until)
            {
                var wholeSectorsFrom = AlignDown(from + sectorSize - 1, sectorSize);
                for (var end = until; end > wholeSectorsFrom;)
                {
                    var start = Math.Max(wholeSectorsFrom, AlignDown(end - 1, ioChunkSize));
                    Write(start, zeroBuffer.aligned_pointer, (int)(end - start));
                    end = start;
                }

                if (wholeSectorsFrom != from)
                {
                    var sector = wholeSectorsFrom - sectorSize;
                    Read(sector, sectorSize);
                    new Span<byte>(readBuffer.aligned_pointer + (from - sector), (int)(wholeSectorsFrom - from)).Clear();
                    Write(sector, readBuffer.aligned_pointer, sectorSize);
                }
            }

            /// <summary>
            /// Round <paramref name="address"/> down to a multiple of <paramref name="alignment"/>, which must be a power of two
            /// </summary>
            static long AlignDown(long address, long alignment) => address & ~(alignment - 1);

            /// <summary>
            /// Read <paramref name="length"/> bytes at <paramref name="address"/> into <see cref="readBuffer"/>, and wait for the
            /// read. Bytes that were never written read as zero, whether the device reports them as a short read, as the end of a
            /// file, or as a segment that does not exist. Any other failure throws <see cref="TsavoriteIOException"/>.
            /// </summary>
            /// <param name="address">Where to read: sector-aligned</param>
            /// <param name="length">How many bytes: whole sectors, within one IO chunk, so they fit in <see cref="readBuffer"/></param>
            void Read(long address, int length)
            {
                new Span<byte>(readBuffer.aligned_pointer, length).Clear();
                var completion = new IoCompletion(isRead: true, address);
                device.ReadAsync((ulong)address, (IntPtr)readBuffer.aligned_pointer, (uint)length, IoCompletion.Callback, completion);
                completion.Wait();

                // The device holds only a raw pointer into readBuffer, so this keeps the buffer's array alive until the IO completes
                GC.KeepAlive(this);
            }

            /// <summary>
            /// Write <paramref name="length"/> bytes from <paramref name="source"/> to the device at <paramref name="address"/>,
            /// and wait for the write. A failure throws <see cref="TsavoriteIOException"/>.
            /// </summary>
            /// <param name="address">Where to write: sector-aligned</param>
            /// <param name="source">The bytes to write: <see cref="zeroBuffer"/>, or <see cref="readBuffer"/> for the sector that
            /// <see cref="Zero"/> rewrites</param>
            /// <param name="length">How many bytes: whole sectors, within one IO chunk</param>
            void Write(long address, byte* source, int length)
            {
                var completion = new IoCompletion(isRead: false, address);
                device.WriteAsync((IntPtr)source, (ulong)address, (uint)length, IoCompletion.Callback, completion);
                completion.Wait();

                // The device holds only a raw pointer into one of this instance's buffers, so this keeps the buffer's array alive
                // until the IO completes
                GC.KeepAlive(this);
            }

            /// <summary>
            /// One device read or write that the issuing thread waits for: the device calls <see cref="Callback"/> when the IO
            /// completes, which records the outcome, and <see cref="Wait"/> blocks until then
            /// </summary>
            /// <param name="isRead">Whether the IO is a read</param>
            /// <param name="address">The device address the IO starts at</param>
            sealed class IoCompletion(bool isRead, long address)
            {
                /// <summary>
                /// Completed by <see cref="Callback"/>, or faulted if the IO failed. RunContinuationsAsynchronously keeps any
                /// continuation off the device's completion thread.
                /// </summary>
                readonly TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);

                /// <summary>Whether the IO is a read: for a read, finding nothing written where it starts is not an error</summary>
                readonly bool isRead = isRead;

                /// <summary>The device address the IO starts at, reported if it fails</summary>
                readonly long address = address;

                /// <summary>
                /// The completion callback passed with every IO, with the IO's <see cref="IoCompletion"/> as its context. It
                /// completes <see cref="done"/>, or faults it with a <see cref="TsavoriteIOException"/> if the IO failed. A read
                /// that failed only because nothing was written where it started (<see cref="TsavoriteLogScanIterator.IsEndOfFile"/>)
                /// succeeds, since <see cref="DeviceZeroRegion.Read"/> clears the buffer first and those bytes read as zero.
                /// </summary>
                /// <remarks>
                /// The byte count is not checked: a short read leaves zeros in the rest of the buffer for the same reason, and a
                /// write is complete when the device reports no error, as the log's page flushes assume. The delegate is static, so
                /// issuing an IO allocates none.
                /// </remarks>
                internal static readonly DeviceIOCompletionCallback Callback = static (errorCode, numBytes, context, ioException) =>
                {
                    var completion = (IoCompletion)context;
                    if (errorCode == 0 || (completion.isRead && TsavoriteLogScanIterator.IsEndOfFile(errorCode)))
                        _ = completion.done.TrySetResult();
                    else
                        _ = completion.done.TrySetException(new TsavoriteIOException(
                            $"Device {(completion.isRead ? "read" : "write")} at address {completion.address} failed with error code {errorCode} while clearing above the tail", ioException));
                };

                /// <summary>
                /// Block until the IO completes. If it failed, throws its <see cref="TsavoriteIOException"/>.
                /// </summary>
                internal void Wait() => done.Task.GetAwaiter().GetResult();
            }
        }
    }
}