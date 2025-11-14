// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace ReadFileServer
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
        uint SectorSize { get; }
        string FileName { get; }
        void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context);
        void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, DeviceIOCompletionCallback callback, object context);
        void WriteAsync(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context);
        void ReadAsync(ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint aligned_read_length, DeviceIOCompletionCallback callback, object context);
    }
}