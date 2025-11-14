// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace ReadFileServer
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
        /// Segment size in bits
        /// </summary>
        protected int segmentSizeBits;
        private ulong segmentSizeMask;

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
        /// <param name="readOnly">Open file in readOnly mode </param>
        public StorageDeviceBase(string filename, uint sectorSize, bool readOnly = false)
        {
            FileName = filename;
            SectorSize = sectorSize;

            segmentSizeBits = 64;
            segmentSizeMask = ~0UL;
        }

        /// <summary>
        /// Create a filename that may or may not include the segmentId
        /// </summary>
        protected internal static string GetSegmentFilename(string filename, int segmentId, bool omitSegmentId)
            => omitSegmentId ? filename : $"{filename}.{segmentId}";

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
    }
}