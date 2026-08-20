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
        /// <param name="capacity">The maximal number of bytes this storage device can accommodate, or CAPACITY_UNSPECIFIED if there is no such limit. For <see cref="DeviceType.LocalMemory"/> a value &lt;= 0 (or CAPACITY_UNSPECIFIED) defaults to a large bounded capacity (segments are allocated lazily).</param>
        /// <param name="recoverDevice">Whether to recover device metadata from existing files</param>
        /// <param name="useIoCompletionPort">Whether we use IO completion port with polling</param>
        /// <param name="disableFileBuffering">Whether file buffering (during write) is disabled (default of true requires aligned writes)</param>
        /// <param name="readOnly">Open file in readOnly mode</param>
        /// <param name="numCompletionThreads">Number of background IO completion drain threads. For DeviceType.Native on Linux: a small pool of drainers that range-drain the device's kernel io_contexts (libaio) / io_uring rings; submitters distribute across rings via per-thread affinity. For DeviceType.LocalMemory: each drainer owns one SPSC ring fed by one submitter via per-thread routing; pass 0 for inline completion (copy + callback run on the submitting thread, no rings/threads) or a negative value to default to <see cref="System.Environment.ProcessorCount"/>. In both cases, raise this value when submitter concurrency exceeds the single-ring drain rate. Ignored otherwise.</param>
        /// <param name="logger">Optional logger for device diagnostics.</param>
        /// <param name="nativeDeviceOptions">For DeviceType.Native on Linux: libaio / io_uring backend tuning (IO backend, ring count, per-ring queue depth, SQPOLL). Null (default) uses the backend-specific defaults. Ignored otherwise. See <see cref="NativeDeviceOptions"/>.</param>
        /// <param name="localMemoryDeviceOptions">For DeviceType.LocalMemory: segment size and per-ring capacity. Null (default) uses the defaults. Ignored otherwise. See <see cref="LocalMemoryDeviceOptions"/>.</param>
        /// <returns>Device instance</returns>
        public static IDevice CreateLogDevice(string logPath = null, DeviceType deviceType = DeviceType.Default, bool preallocateFile = false, bool deleteOnClose = false, long capacity = CAPACITY_UNSPECIFIED, bool recoverDevice = false, bool useIoCompletionPort = false, bool disableFileBuffering = true, bool readOnly = false, int numCompletionThreads = 1, ILogger logger = null, NativeDeviceOptions nativeDeviceOptions = null, LocalMemoryDeviceOptions localMemoryDeviceOptions = null)
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
                DeviceType.Native when RuntimeInformation.IsOSPlatform(OSPlatform.Linux) => new NativeStorageDevice(logPath, deleteOnClose, disableFileBuffering, capacity, numCompletionThreads: numCompletionThreads, ioBackend: nativeDeviceOptions?.IoBackend ?? NativeStorageDevice.IoBackend.Default, logger: logger, numIoContexts: nativeDeviceOptions?.NumIoContexts ?? 0, queueDepth: nativeDeviceOptions?.QueueDepth ?? 0, uringSqPoll: nativeDeviceOptions?.UringSqPoll ?? false, uringSqPollIdleMs: nativeDeviceOptions?.UringSqPollIdleMs ?? 0),
                DeviceType.Native when RuntimeInformation.IsOSPlatform(OSPlatform.Windows) => new LocalStorageDevice(logPath, preallocateFile, deleteOnClose, disableFileBuffering, capacity, recoverDevice, useIoCompletionPort, readOnly: readOnly, logger: logger),
                DeviceType.RandomAccess => new RandomAccessLocalStorageDevice(logPath, preallocateFile, deleteOnClose, disableFileBuffering, capacity, recoverDevice, readOnly: readOnly, logger: logger),
                DeviceType.FileStream => new ManagedLocalStorageDevice(logPath, preallocateFile, deleteOnClose, disableFileBuffering, capacity, recoverDevice, readOnly: readOnly, logger: logger),
                DeviceType.Null => new NullDevice(),
                DeviceType.LocalMemory => new LocalMemoryDevice(
                    capacity: capacity,
                    segmentSize: localMemoryDeviceOptions?.SegmentSize ?? (1L << 30),
                    parallelism: numCompletionThreads < 0 ? System.Environment.ProcessorCount : numCompletionThreads,
                    ringCapacity: (localMemoryDeviceOptions?.RingCapacity ?? 0) > 0 ? localMemoryDeviceOptions.RingCapacity : 1024,
                    fileName: logPath ?? "/userspace/ram/storage"),
                _ => throw new TsavoriteException($"Unsupported local device {deviceType}"),
            };
        }

        /// <summary>
        /// Get default device type for the current platform. <see cref="DeviceType.Native"/> maps to the
        /// OS-optimized backend: <see cref="LocalStorageDevice"/> on Windows and <see cref="NativeStorageDevice"/>
        /// (libaio / io_uring) on Linux. Prebuilt Linux native libraries are shipped for x64 for both glibc
        /// (<c>linux-x64</c>) and musl (<c>linux-musl-x64</c>, e.g. Alpine). Linux architectures without a shipped
        /// prebuilt (e.g. arm64) and platforms without a Native implementation fall back to the managed
        /// <see cref="DeviceType.RandomAccess"/> device.
        /// </summary>
        /// <returns></returns>
        public static DeviceType GetDefaultDeviceType()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return DeviceType.Native;

            // NativeStorageDevice on Linux loads a prebuilt native library. We ship x64 builds for both glibc
            // (runtimes/linux-x64) and musl (runtimes/linux-musl-x64, e.g. Alpine); the C# loader selects the right
            // one via the RID. On Linux architectures without a shipped prebuilt (e.g. arm64) the library is
            // absent/unloadable, so fall back to the managed RandomAccess device rather than failing on the first IO.
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)
                && RuntimeInformation.ProcessArchitecture == Architecture.X64)
                return DeviceType.Native;

            return DeviceType.RandomAccess;
        }
    }
}