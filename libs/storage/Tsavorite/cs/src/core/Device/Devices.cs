// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Factory to create Tsavorite objects
    /// </summary>
    public static class Devices
    {
        /// <summary>
        /// This value is supplied for capacity when the device does not have a specified limit.
        /// </summary>
        public const long CAPACITY_UNSPECIFIED = -1;

        /// <summary>
        /// Create a storage device for the log
        /// </summary>
        /// <param name="logPath">Path to file that will store the log (empty for null device)</param>
        /// <param name="deviceType">Device type</param>
        /// <param name="preallocateFile">Whether we try to pre-allocate the file on creation</param>
        /// <param name="deleteOnClose">Delete files on close</param>
        /// <param name="capacity">The maximal number of bytes this storage device can accommodate, or CAPACITY_UNSPECIFIED if there is no such limit. Required (must be &gt; 0) for <see cref="DeviceType.LocalMemory"/>.</param>
        /// <param name="recoverDevice">Whether to recover device metadata from existing files</param>
        /// <param name="useIoCompletionPort">Whether we use IO completion port with polling</param>
        /// <param name="disableFileBuffering">Whether file buffering (during write) is disabled (default of true requires aligned writes)</param>
        /// <param name="readOnly">Open file in readOnly mode</param>
        /// <param name="ioBackend">For DeviceType.Native on Linux: which IO backend (libaio or io_uring) to use. Ignored otherwise.</param>
        /// <param name="numCompletionThreads">Number of background IO completion drain threads. For DeviceType.Native on Linux: each drainer is bound 1:1 to its own kernel io_context (libaio) or io_uring ring, and submitters distribute across rings via per-thread affinity. For DeviceType.LocalMemory: each drainer owns one SPSC ring fed by one submitter via per-thread routing. In both cases, raise this value when submitter concurrency exceeds the single-ring drain rate. Ignored otherwise.</param>
        /// <param name="localMemorySegmentSize">For DeviceType.LocalMemory: segment size in bytes (must divide <paramref name="capacity"/>). Default 1 GB. Ignored otherwise.</param>
        /// <param name="localMemoryLatencyMs">For DeviceType.LocalMemory: simulated per-IO wall-clock latency in milliseconds (0 = none). Ignored otherwise.</param>
        /// <param name="logger"></param>
        /// <returns>Device instance</returns>
        public static IDevice CreateLogDevice(string logPath = null, DeviceType deviceType = DeviceType.Default, bool preallocateFile = false, bool deleteOnClose = false, long capacity = CAPACITY_UNSPECIFIED, bool recoverDevice = false, bool useIoCompletionPort = false, bool disableFileBuffering = true, bool readOnly = false, NativeStorageDevice.IoBackend ioBackend = NativeStorageDevice.IoBackend.Default, int numCompletionThreads = 1, long localMemorySegmentSize = 1L << 30, int localMemoryLatencyMs = 0, ILogger logger = null)
        {
            if (deviceType == DeviceType.Default)
            {
                deviceType = GetDefaultDeviceType();
            }

            if (deviceType != DeviceType.Null && deviceType != DeviceType.LocalMemory && logPath == null)
            {
                throw new TsavoriteException("logPath must be specified for non-null devices");
            }

            return deviceType switch
            {
                DeviceType.Native when RuntimeInformation.IsOSPlatform(OSPlatform.Linux) => new NativeStorageDevice(logPath, deleteOnClose, disableFileBuffering, capacity, numCompletionThreads: numCompletionThreads, ioBackend: ioBackend, logger: logger),
                DeviceType.Native when RuntimeInformation.IsOSPlatform(OSPlatform.Windows) => new LocalStorageDevice(logPath, preallocateFile, deleteOnClose, disableFileBuffering, capacity, recoverDevice, useIoCompletionPort, readOnly: readOnly, logger: logger),
                DeviceType.RandomAccess => new RandomAccessLocalStorageDevice(logPath, preallocateFile, deleteOnClose, disableFileBuffering, capacity, recoverDevice, readOnly: readOnly, logger: logger),
                DeviceType.FileStream => new ManagedLocalStorageDevice(logPath, preallocateFile, deleteOnClose, disableFileBuffering, capacity, recoverDevice, readOnly: readOnly, logger: logger),
                DeviceType.Null => new NullDevice(),
                DeviceType.LocalMemory => new LocalMemoryDevice(
                    capacity: capacity > 0 ? capacity : throw new TsavoriteException("LocalMemory requires a positive capacity"),
                    sz_segment: localMemorySegmentSize,
                    parallelism: numCompletionThreads > 0 ? numCompletionThreads : System.Environment.ProcessorCount,
                    latencyMs: localMemoryLatencyMs,
                    fileName: logPath ?? "/userspace/ram/storage"),
                _ => throw new TsavoriteException($"Unsupported local device {deviceType}"),
            };
        }

        /// <summary>
        /// Get default device type for the current platform
        /// </summary>
        /// <returns></returns>
        public static DeviceType GetDefaultDeviceType()
        {
            return RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? DeviceType.Native : DeviceType.RandomAccess;
        }
    }
}