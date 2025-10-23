// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Delegate for callback on IO completion
    /// </summary>
    /// <param name="errorCode"></param>
    /// <param name="numBytes"></param>
    /// <param name="context"></param>
    public delegate void DeviceIOCompletionCallback(uint errorCode, uint numBytes, object context);

    /// <summary>
    /// Interface for devices
    /// </summary>
    public interface IDevice : IDisposable
    {
        /// <summary>
        /// Size of sector
        /// </summary>
        uint SectorSize { get; }

        /// <summary>
        /// Name of device
        /// </summary>
        string FileName { get; }

        /// <summary>
        /// Returns the maximum capacity of the storage device, in number of bytes. 
        /// If returned CAPACITY_UNSPECIFIED, the storage device has no specfied capacity limit. 
        /// </summary>
        long Capacity { get; }

        /// <summary>
        /// A device breaks up each logical log into multiple self-contained segments that are of the same size.
        /// It is an atomic unit of data that cannot be partially present on a device (i.e. either the entire segment
        /// is present or no data from the segment is present). Examples of this include files or named blobs. This
        /// property returns the size of each segment.
        /// </summary>
        long SegmentSize { get; }

        /// <summary>
        /// The index of the first segment present on this device
        /// </summary>
        int StartSegment { get; }

        /// <summary>
        /// The index of the last segment present on this device
        /// </summary>
        int EndSegment { get; }

        /// <summary>
        /// Throttle limit (max number of pending I/Os) for this device instance. Device needs
        /// to implement Throttle() in order to use this limit.
        /// </summary>
        int ThrottleLimit { get; set; }

        /// <summary>
        /// Initialize device. This function is used to pass optional information that may only be known after
        /// Tsavorite initialization (whose constructor takes in IDevice upfront). Implementation are free to ignore
        /// information if it does not need the supplied information.
        /// 
        /// This is a bit of a hack. 
        /// </summary>
        /// <param name="segmentSize"></param>
        /// <param name="epoch">
        /// <param name="omitSegmentIdFromFilename"></param>
        /// The instance of the epoch protection framework to use, if needed
        /// </param>
        void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false);

        /// <summary>
        /// Try complete async IO completions
        /// </summary>
        /// <returns></returns>
        bool TryComplete();

        /// <summary>
        /// Whether device should be throttled at this instant (i.e., caller should stop issuing new I/Os)
        /// </summary>
        /// <returns></returns>
        bool Throttle();

        /* Segmented addressing API */
        /// <summary>
        /// Write to the file. The alignedSourceAddress must be pinned.
        /// </summary>
        /// <param name="sourceAddress"></param>
        /// <param name="segmentId"></param>
        /// <param name="destinationAddress"></param>
        /// <param name="numBytesToWrite"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        /// <remarks>While this supports concurrent writes, the caller should try as much as possible to sequentialize the writes, as the IDevice implementation
        ///     may require append-only behavior and thus will have to buffer. For similar reasons, do not back up and re-write; depending on the IDevice implementation,
        ///     this may fail or be inefficient.</remarks>
        void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context);

        /// <summary>
        /// Read from the file. The alignedSourceAddress must be pinned.
        /// </summary>
        /// <param name="segmentId"></param>
        /// <param name="sourceAddress"></param>
        /// <param name="destinationAddress"></param>
        /// <param name="readLength"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, DeviceIOCompletionCallback callback, object context);

        /* Direct addressing API */

        /// <summary>
        /// Write to the file. The alignedSourceAddress must be pinned. If inheriting from <see cref="StorageDeviceBase"/>, that provides an implementation of this that calculates the segmentId
        /// and then invokes the overload with that segmentId.
        /// </summary>
        /// <param name="alignedSourceAddress"></param>
        /// <param name="alignedDestinationAddress"></param>
        /// <param name="numBytesToWrite"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        /// <remarks>While this supports concurrent writes, the caller should try as much as possible to sequentialize the writes, as the IDevice implementation
        ///     may require append-only behavior and thus will have to buffer. For similar reasons, do not back up and re-write; depending on the IDevice implementation,
        ///     this may fail or be inefficient.</remarks>
        void WriteAsync(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context);

        /// <summary>
        /// Read from the file. The alignedDestinationAddress must be pinned. If inheriting from <see cref="StorageDeviceBase"/>, that provides an implementation of this that calculates the segmentId
        /// and then invokes the overload with that segmentId.
        /// </summary>
        /// <param name="alignedSourceAddress"></param>
        /// <param name="alignedDestinationAddress"></param>
        /// <param name="aligned_read_length"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        void ReadAsync(ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint aligned_read_length, DeviceIOCompletionCallback callback, object context);

        /// <summary>
        /// Truncates the log until the given address. The truncated portion should no longer be accessed as the device is no longer responsible for 
        /// its maintenance, but physical deletion may not happen immediately.
        /// </summary>
        /// <param name="toAddress">upper bound of truncated address</param>
        /// <param name="callback">callback to invoke when truncation is complete</param>
        /// <param name="result">result to be passed to the callback</param>
        void TruncateUntilAddressAsync(long toAddress, AsyncCallback callback, IAsyncResult result);

        /// <summary>
        /// Truncates the log until the given address. The truncated portion should no longer be accessed as the device is no longer responsible for 
        /// its maintenance, but physical deletion may not happen immediately. This version of the function can block.
        /// </summary>
        /// <param name="toAddress">upper bound of truncated address</param>
        void TruncateUntilAddress(long toAddress);

        /// <summary>
        /// Truncates the log until the given segment. Physical deletion of the given segments are guaranteed to have happened when the callback is invoked.
        /// </summary>
        /// <param name="toSegment">the largest (in index) segment to truncate</param>
        /// <param name="callback">callback to invoke when truncation is complete</param>
        /// <param name="result">result to be passed to the callback</param>
        void TruncateUntilSegmentAsync(int toSegment, AsyncCallback callback, IAsyncResult result);

        /// <summary>
        /// Truncates the log until the given segment. Physical deletion of the given segments are guaranteed to have happened when the function returns.
        /// This version of the function can block.
        /// </summary>
        /// <param name="toSegment">the largest (in index) segment to truncate</param>
        void TruncateUntilSegment(int toSegment);

        /// <summary>
        /// Removes a single segment from the device. This function should not normally be called.
        /// Instead, use <see cref="TruncateUntilAddressAsync(long, AsyncCallback, IAsyncResult)"/>
        /// </summary>
        /// <param name="segment">index of the segment to remov</param>
        /// <param name="callback">callback to invoke when removal is complete</param>
        /// <param name="result">result to be passed to the callback</param>
        void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result);

        /// <summary>
        /// Removes a single segment from the device. This function should not normally be called.
        /// Instead, use <see cref="TruncateUntilAddressAsync(long, AsyncCallback, IAsyncResult)"/>
        /// </summary>
        /// <param name="segment">index of the segment to remov</param>
        void RemoveSegment(int segment);

        /// <summary>
        /// Get file size for given segment
        /// </summary>
        /// <param name="segment"></param>
        /// <returns></returns>
        long GetFileSize(int segment);

        /// <summary>
        /// Reset the device (close and forget open handles)
        /// </summary>
        void Reset();
    }

    /// <summary>
    /// Extension for backward compatibility
    /// </summary>
    public static class Device
    {
        /// <summary>
        /// Close log - use IDevice.Dispose instead
        /// </summary>
        /// <param name="device"></param>
        [Obsolete("Use IDevice.Dispose() instead.")]
        public static void Close(this IDevice device)
        {
            device.Dispose();
        }
    }
}