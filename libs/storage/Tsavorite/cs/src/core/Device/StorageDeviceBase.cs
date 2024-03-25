// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// 
    /// </summary>
    public abstract class StorageDeviceBase : IDevice
    {

        /// <summary>
        /// 
        /// </summary>
        public uint SectorSize { get; }

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
        /// <param name="capacity">The maximal number of bytes this storage device can accommondate, or CAPAPCITY_UNSPECIFIED if there is no such limit </param>
        public StorageDeviceBase(string filename, uint sectorSize, long capacity)
        {
            FileName = filename;
            SectorSize = sectorSize;

            segmentSize = -1;
            segmentSizeBits = 64;
            segmentSizeMask = ~0UL;

            Capacity = capacity;
        }

        /// <summary>
        /// Initialize device
        /// </summary>
        /// <param name="segmentSize"></param>
        /// <param name="epoch"></param>
        /// <param name="omitSegmentIdFromFilename"></param>
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
        /// Write operation
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
        /// Read operation
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

        /// <summary>
        /// <see cref="IDevice.TruncateUntilSegmentAsync(int, AsyncCallback, IAsyncResult)"/>
        /// </summary>
        /// <param name="toSegment"></param>
        /// <param name="callback"></param>
        /// <param name="result"></param>
        public void TruncateUntilSegmentAsync(int toSegment, AsyncCallback callback, IAsyncResult result)
        {
            // Reset begin range to at least toAddress
            if (!Utility.MonotonicUpdate(ref startSegment, toSegment, out int oldStart))
            {
                // If no-op, invoke callback and return immediately
                callback(result);
                return;
            }
            CountdownEvent countdown = new(toSegment - oldStart);
            // This action needs to be epoch-protected because readers may be issuing reads to the deleted segment, unaware of the delete.
            // Because of earlier compare-and-swap, the caller has exclusive access to the range [oldStartSegment, newStartSegment), and there will
            // be no double deletes.

            bool isProtected = epoch.ThisInstanceProtected();
            if (!isProtected)
                epoch.Resume();
            try
            {
                epoch.BumpCurrentEpoch(() =>
                {
                    for (int i = oldStart; i < toSegment; i++)
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
                });
            }
            finally
            {
                if (!isProtected) epoch.Suspend();
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
        public virtual long GetFileSize(int segment)
        {
            if (segmentSize > 0) return segmentSize;
            return long.MaxValue;
        }

        /// <inheritdoc/>
        public virtual void Reset()
        {
        }
    }
}