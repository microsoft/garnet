// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// 
    /// </summary>
    public abstract class StorageDeviceBase : IDevice
    {
        /// <summary>
        /// Required alignment (in bytes) of buffers, offsets, and lengths for this device's
        /// direct I/O. Computed once at device construction (typically from a kernel probe of
        /// the target filesystem's <c>logical_block_size</c> / <c>STATX_DIOALIGN</c>) and
        /// passed to the base ctor, so the value is immutable for the lifetime of the device.
        /// </summary>
        public uint SectorSize { get; }

        /// <summary>
        /// The largest sector size of <paramref name="devices"/>, and at least <see cref="IDevice.MinDeviceSectorSize"/>.
        /// A device that forwards its requests to <paramref name="devices"/> reports this, so that every request it
        /// accepts is aligned for each of them.
        /// </summary>
        internal static uint GetLargestSectorSize(IEnumerable<IDevice> devices)
        {
            uint sectorSize = IDevice.MinDeviceSectorSize;
            foreach (var device in devices)
                sectorSize = Math.Max(sectorSize, device.SectorSize);
            return sectorSize;
        }

        /// <summary>
        /// 
        /// </summary>
        public string FileName { get; }

        /// <summary>
        /// <see cref="IDevice.Capacity"/>
        /// </summary>
        public long Capacity { get; }

        /// <summary>
        /// <see cref="IDevice.StartSegment"/>
        /// </summary>
        public int StartSegment { get { return startSegment; } }

        /// <summary>
        /// <see cref="IDevice.EndSegment"/>
        /// </summary>
        public int EndSegment { get { return endSegment; } }

        /// <summary>
        /// <see cref="IDevice.SegmentSize"/>
        /// </summary>
        public long SegmentSize { get { return segmentSize; } }

        /// <summary>
        /// Segment size
        /// </summary>
        protected long segmentSize;

        /// <summary>
        /// Segment size in bits
        /// </summary>
        protected int segmentSizeBits;
        private ulong segmentSizeMask;

        /// <summary>
        /// Throttle limit (max number of pending I/Os) for this device instance
        /// </summary>
        public int ThrottleLimit { get; set; } = int.MaxValue;

        /// <summary>
        /// Instance of the epoch protection framework in the current system.
        /// A device may have internal in-memory data structure that requires epoch protection under concurrent access.
        /// </summary>
        protected LightEpoch epoch;

        /// <summary>
        /// start and end segment corresponding to <see cref="StartSegment"/> and <see cref="EndSegment"/>. Subclasses are
        /// allowed to modify these as needed.
        /// </summary>
        protected int startSegment = 0, endSegment = -1;

        /// <summary>
        /// If true, skip adding the segmentId to the filename.
        /// </summary>
        /// <remarks>If true, SegmentSize must be -1</remarks>
        protected bool OmitSegmentIdFromFileName;

        /// <summary>
        /// Initializes a new StorageDeviceBase
        /// </summary>
        /// <param name="filename">Name of the file to use</param>
        /// <param name="sectorSize">The smallest unit of write of the underlying storage device (e.g. 512 bytes for a disk) </param>
        /// <param name="capacity">The maximal number of bytes this storage device can accommodate, or CAPAPCITY_UNSPECIFIED if there is no such limit </param>
        /// <param name="readOnly">Open file in readOnly mode </param>
        /// <remarks>
        /// <para>
        /// The ctor establishes <c>segmentSize = -1</c> / <c>segmentSizeBits = 64</c> /
        /// <c>segmentSizeMask = ~0UL</c> as defaults, which is functionally identical to having
        /// called <c>Initialize(-1)</c>: the segment computation in IO entry points
        /// (<c>address &gt;&gt; segmentSizeBits</c>) routes every address to segment 0, which is
        /// unbounded single-segment mode. Callers may issue IO immediately after construction
        /// without an explicit <see cref="Initialize"/> call; <see cref="Initialize"/> exists only
        /// to override these defaults (e.g. switch to multi-segment mode by passing a positive
        /// power-of-two segment size).
        /// </para>
        /// </remarks>
        public StorageDeviceBase(string filename, uint sectorSize, long capacity, bool readOnly = false)
        {
            FileName = filename;
            SectorSize = sectorSize;

            // Defaults are equivalent to Initialize(-1): unbounded single-segment routing.
            // Initialize() is optional and only needed when the caller wants a different segment
            // size (multi-segment mode).
            segmentSize = -1;
            segmentSizeBits = 64;
            segmentSizeMask = ~0UL;

            Capacity = capacity;
        }

        /// <summary>
        /// Override device defaults. The ctor establishes a valid baseline (unbounded single
        /// segment, equivalent to passing <c>segmentSize = -1</c>); callers only need to invoke
        /// <see cref="Initialize"/> if they want a different segment size (multi-segment mode)
        /// or want to opt into <paramref name="omitSegmentIdFromFilename"/>. Passing
        /// <c>segmentSize = -1</c> is a no-op equivalent to relying on the ctor defaults; any
        /// other value must be a positive power of two.
        /// </summary>
        /// <param name="segmentSize">Segment size in bytes, or -1 for unbounded single segment.</param>
        /// <param name="epoch"></param>
        /// <param name="omitSegmentIdFromFilename">When true, the segment index is not appended
        /// to the filename. Only permitted with <c>segmentSize = -1</c>.</param>
        public virtual void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
            if (segmentSize != -1)
            {
                if (Capacity != -1 && Capacity % segmentSize != 0)
                    throw new TsavoriteException("capacity must be a multiple of segment sizes");
                if (omitSegmentIdFromFilename)
                    throw new TsavoriteException("omitSegmentIdInFilename requires a segment size of -1");
            }
            this.segmentSize = segmentSize;
            this.epoch = epoch;
            OmitSegmentIdFromFileName = omitSegmentIdFromFilename;
            if (!Utility.IsPowerOfTwo(segmentSize))
            {
                if (segmentSize != -1)
                    throw new TsavoriteException("Invalid segment size: " + segmentSize);
                segmentSizeBits = 64;
                segmentSizeMask = ~0UL;
            }
            else
            {
                segmentSizeBits = Utility.GetLogBase2((ulong)segmentSize);
                segmentSizeMask = (ulong)segmentSize - 1;
            }
        }

        /// <summary>
        /// Create a filename that may or may not include the segmentId
        /// </summary>
        protected internal string GetSegmentFilename(string filename, int segmentId) => GetSegmentFilename(filename, segmentId, OmitSegmentIdFromFileName);

        /// <summary>
        /// Create a filename that may or may not include the segmentId
        /// </summary>
        protected internal static string GetSegmentFilename(string filename, int segmentId, bool omitSegmentId)
            => omitSegmentId ? filename : $"{filename}.{segmentId}";

        /// <summary>
        /// Whether device should be throttled
        /// </summary>
        /// <returns></returns>
        public virtual bool Throttle() => false;

        /// <summary>
        /// Write operation; compute the segment id and call the IDevice implementation.
        /// </summary>
        /// <param name="alignedSourceAddress"></param>
        /// <param name="alignedDestinationAddress"></param>
        /// <param name="numBytesToWrite"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public void WriteAsync(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            WriteAsync(
                alignedSourceAddress,
                (int)(segmentSizeBits < 64 ? alignedDestinationAddress >> segmentSizeBits : 0),
                alignedDestinationAddress & segmentSizeMask,
                numBytesToWrite, callback, context);
        }

        /// <summary>
        /// Read operation; compute the segment id and call the IDevice implementation.
        /// </summary>
        /// <param name="alignedSourceAddress"></param>
        /// <param name="alignedDestinationAddress"></param>
        /// <param name="aligned_read_length"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public void ReadAsync(ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint aligned_read_length, DeviceIOCompletionCallback callback, object context)
        {
            var segment = segmentSizeBits < 64 ? alignedSourceAddress >> segmentSizeBits : 0;

            ReadAsync(
                (int)segment,
                alignedSourceAddress & segmentSizeMask,
                alignedDestinationAddress,
                aligned_read_length, callback, context);
        }

        /// <summary>
        /// <see cref="IDevice.RemoveSegmentAsync(int, AsyncCallback, IAsyncResult)"/>
        /// </summary>
        /// <param name="segment"></param>
        /// <param name="callback"></param>
        /// <param name="result"></param>
        public abstract void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result);

        /// <summary>
        /// <see cref="IDevice.RemoveSegment(int)"/>
        /// By default the implementation calls into <see cref="RemoveSegmentAsync(int, AsyncCallback, IAsyncResult)"/>
        /// </summary>
        /// <param name="segment"></param>
        public virtual void RemoveSegment(int segment)
        {
            ManualResetEventSlim completionEvent = new(false);
            RemoveSegmentAsync(segment, r => completionEvent.Set(), null);
            Debug.Assert(!epoch.ThisInstanceProtected());
            bool isProtected = epoch.ThisInstanceProtected();
            if (isProtected)
                epoch.Suspend();
            try
            {
                completionEvent.Wait();
            }
            finally
            {
                if (isProtected)
                    epoch.Resume();
            }
        }

        /// <inheritdoc/>
        public virtual bool TryRemoveSegmentsAbove(int segment, out int removed)
        {
            removed = 0;
            return false;
        }

        /// <summary>
        /// Remove every segment file above <paramref name="segment"/>, for a device that stores segment n in the file named
        /// <see cref="FileName"/>.n and whose <see cref="RemoveSegment(int)"/> deletes exactly that segment's file
        /// </summary>
        /// <param name="segment">The highest segment to keep</param>
        /// <param name="removed">The number of removed files that held data</param>
        /// <returns>Whether the device names its files with segment ids, and so can list them</returns>
        /// <remarks>
        /// Files are removed from the highest segment down. RemoveSegment ignores a failure to delete a file, so a file that is
        /// still there afterwards fails this, rather than leaving data that the caller believes is gone.
        /// </remarks>
        protected bool TryRemoveSegmentFilesAbove(int segment, out int removed)
        {
            removed = 0;
            if (!TryListSegmentFiles(out var segments))
                return false;

            foreach (var above in segments.Where(listed => listed > segment).OrderDescending())
            {
                var file = new FileInfo(GetSegmentFilename(FileName, above));
                var heldData = file.Exists && file.Length > 0;
                RemoveSegment(above);
                file.Refresh();
                if (file.Exists)
                    throw new TsavoriteIOException($"Unable to remove segment file {file.FullName}", null);
                if (heldData)
                    removed++;
            }
            return true;
        }

        /// <summary>
        /// List the segment files of a device that stores segment n in the file named <see cref="FileName"/>.n
        /// </summary>
        /// <param name="segments">The ids of the segment files in <see cref="FileName"/>'s directory, or null if the device names its
        /// only file without a segment id</param>
        /// <returns>Whether the device names its files with segment ids, and so can list them</returns>
        /// <remarks>
        /// Only names that <see cref="GetSegmentFilename(string, int)"/> produces are listed: the file name, a dot and a segment id
        /// written without a sign or leading zeros. A name such as "c.log.007" is not segment 7's file, which RemoveSegment(7)
        /// would not delete. The directory is read when this is called; a missing directory holds no segments.
        /// </remarks>
        protected bool TryListSegmentFiles(out List<int> segments)
        {
            segments = null;
            if (OmitSegmentIdFromFileName)
                return false;

            var file = new FileInfo(FileName);
            var prefix = file.Name + ".";
            List<int> found = [];
            if (file.Directory.Exists)
            {
                // Simple matching avoids the Windows wildcard rules under which "c.log.*" also matches "c.log"
                var options = new EnumerationOptions { MatchType = MatchType.Simple, AttributesToSkip = 0 };
                foreach (var path in Directory.EnumerateFiles(file.DirectoryName, prefix + "*", options))
                {
                    var id = Path.GetFileName(path.AsSpan())[prefix.Length..];
                    if (int.TryParse(id, NumberStyles.None, CultureInfo.InvariantCulture, out var segment) && (id.Length == 1 || id[0] != '0'))
                        found.Add(segment);
                }
            }
            segments = found;
            return true;
        }

        /// <summary>
        /// <see cref="IDevice.TruncateUntilSegmentAsync(int, AsyncCallback, IAsyncResult)"/>
        /// </summary>
        /// <param name="toSegment"></param>
        /// <param name="callback"></param>
        /// <param name="result"></param>
        public void TruncateUntilSegmentAsync(int toSegment, AsyncCallback callback, IAsyncResult result)
        {
            // Reset begin range to at least toSegment
            if (!Utility.MonotonicUpdate(ref startSegment, toSegment, out int oldStart))
            {
                // If no-op, invoke callback and return immediately
                callback(result);
                return;
            }

            // This action needs to be epoch-protected because readers may be issuing reads to the deleted segment, unaware of the delete.
            // Because of earlier compare-and-swap, the caller has exclusive access to the range [oldStartSegment, newStartSegment), and there will
            // be no double deletes.
            var suspendEpochWhenDone = epoch.ResumeIfNotProtected();
            try
            {
                epoch.BumpCurrentEpoch(() => RemoveSegmentsThroughAsync(oldStart, toSegment - 1, callback, result));
            }
            finally
            {
                if (suspendEpochWhenDone)
                    epoch.Suspend();
            }
        }

        /// <summary>
        /// Remove the segments from <paramref name="fromSegment"/> through <paramref name="throughSegment"/> for truncation, once no
        /// reader can still be using them
        /// </summary>
        /// <param name="fromSegment">The lowest segment to remove</param>
        /// <param name="throughSegment">The highest segment to remove</param>
        /// <param name="callback">Called once every segment is removed</param>
        /// <param name="result">Passed to <paramref name="callback"/></param>
        /// <remarks>
        /// The default calls <see cref="RemoveSegmentAsync(int, AsyncCallback, IAsyncResult)"/> for each segment, lowest first,
        /// without waiting for one to finish before starting the next. A device can override this to remove the range in one operation.
        /// </remarks>
        protected virtual void RemoveSegmentsThroughAsync(int fromSegment, int throughSegment, AsyncCallback callback, IAsyncResult result)
        {
            // We will delete segments in parallel; create a countdown event that will signal when all are completed.
            CountdownEvent countdown = new(throughSegment - fromSegment + 1);
            for (int i = fromSegment; i <= throughSegment; i++)
            {
                RemoveSegmentAsync(i, r =>
                {
                    if (countdown.Signal())
                    {
                        callback(r);
                        countdown.Dispose();
                    }
                }, result);
            }
        }

        /// <summary>
        /// <see cref="IDevice.TruncateUntilSegment(int)"/>
        /// </summary>
        /// <param name="toSegment"></param>
        public void TruncateUntilSegment(int toSegment)
        {
            using (ManualResetEventSlim completionEvent = new(false))
            {
                TruncateUntilSegmentAsync(toSegment, r => completionEvent.Set(), null);
                bool isProtected = epoch.ThisInstanceProtected();
                if (isProtected)
                    epoch.Suspend();
                try
                {
                    completionEvent.Wait();
                }
                finally
                {
                    if (isProtected)
                        epoch.Resume();
                }
            }
        }

        /// <summary>
        /// <see cref="IDevice.TruncateUntilAddressAsync(long, AsyncCallback, IAsyncResult)"/>
        /// </summary>
        /// <param name="toAddress"></param>
        /// <param name="callback"></param>
        /// <param name="result"></param>
        public virtual void TruncateUntilAddressAsync(long toAddress, AsyncCallback callback, IAsyncResult result)
        {
            if ((int)(toAddress >> segmentSizeBits) <= startSegment)
            {
                callback(result);
                return;
            }
            // Truncate only up to segment boundary if address is not aligned
            TruncateUntilSegmentAsync((int)(toAddress >> segmentSizeBits), callback, result);
        }

        /// <summary>
        /// <see cref="IDevice.TruncateUntilAddress(long)"/>
        /// </summary>
        /// <param name="toAddress"></param>
        public virtual void TruncateUntilAddress(long toAddress)
        {
            if ((int)(toAddress >> segmentSizeBits) <= startSegment)
                return;

            using (ManualResetEventSlim completionEvent = new(false))
            {
                TruncateUntilAddressAsync(toAddress, r => completionEvent.Set(), null);
                bool isProtected = epoch.ThisInstanceProtected();
                if (isProtected)
                    epoch.Suspend();
                try
                {
                    completionEvent.Wait();
                }
                finally
                {
                    if (isProtected)
                        epoch.Resume();
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sourceAddress"></param>
        /// <param name="segmentId"></param>
        /// <param name="destinationAddress"></param>
        /// <param name="numBytesToWrite"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public abstract void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="segmentId"></param>
        /// <param name="sourceAddress"></param>
        /// <param name="destinationAddress"></param>
        /// <param name="readLength"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public abstract void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, DeviceIOCompletionCallback callback, object context);

        /// <summary>
        /// 
        /// </summary>
        public abstract void Dispose();

        /// <summary>
        /// Handle space utilization of limited capacity devices by invoking segment truncation if necessary
        /// </summary>
        /// <param name="segment">Segment being written to</param>
        protected void HandleCapacity(int segment)
        {
            // If the device has bounded space, and we are writing a new segment, need to check whether an existing segment needs to be evicted. 
            if (Capacity != Devices.CAPACITY_UNSPECIFIED && Utility.MonotonicUpdate(ref endSegment, segment, out _))
            {
                // Attempt to update the stored range until there is enough space on the tier to accomodate the current segment
                int newStartSegment = endSegment - (int)(Capacity >> segmentSizeBits);
                // Assuming that we still have enough physical capacity to write another segment, even if delete does not immediately free up space.
                TruncateUntilSegmentAsync(newStartSegment, r => { }, null);
            }
        }

        /// <inheritdoc/>
        public virtual bool TryComplete()
        {
            return true;
        }

        /// <inheritdoc/>
        public virtual bool TryCompleteMine() => TryComplete();

        /// <inheritdoc/>
        public virtual long GetFileSize(int segment)
        {
            if (segmentSize > 0) return segmentSize;
            return long.MaxValue;
        }

        /// <inheritdoc/>
        /// <remarks>
        /// This forgets which segments have been truncated, since a log that is reset writes from segment 0 again and must be able
        /// to truncate those segments. Overrides must call it.
        /// </remarks>
        public virtual void Reset()
        {
            startSegment = 0;
            endSegment = -1;
        }
    }
}