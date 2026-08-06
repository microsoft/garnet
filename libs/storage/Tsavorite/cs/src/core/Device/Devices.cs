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
        /// <param name="ioBackend">For DeviceType.Native on Linux: which IO backend (libaio or io_uring) to use. Ignored otherwise.</param>
        /// <param name="numCompletionThreads">Number of background IO completion drain threads. For DeviceType.Native on Linux: a small pool of drainers that range-drain the device's kernel io_contexts (libaio) / io_uring rings; submitters distribute across rings via per-thread affinity. For DeviceType.LocalMemory: each drainer owns one SPSC ring fed by one submitter via per-thread routing; pass 0 for inline completion (copy + callback run on the submitting thread, no rings/threads) or a negative value to default to <see cref="System.Environment.ProcessorCount"/>. In both cases, raise this value when submitter concurrency exceeds the single-ring drain rate. Ignored otherwise.</param>
        /// <param name="numIoContexts">For DeviceType.Native on Linux: number of independent kernel io_contexts / io_uring rings, decoupled from <paramref name="numCompletionThreads"/>. Set >= submitter concurrency to make io_submit contention-free and spread completion posting across more rings; the drainers each range-drain a contiguous slice. 0 (default) = one ring per drainer. Clamped up to <paramref name="numCompletionThreads"/>. Ignored otherwise.</param>
        /// <param name="localMemorySegmentSize">For DeviceType.LocalMemory: segment size in bytes (must divide <paramref name="capacity"/>). Default 1 GB. Ignored otherwise.</param>
        /// <param name="localMemoryRingCapacity">For DeviceType.LocalMemory: per-submitter ring capacity (power of two), which is the device's in-flight bound (the producer blocks when its ring is full). 0 = default. This is how an in-flight throttle is applied to LocalMemory: its per-ring SPSC backpressure caps in-flight with no device-wide counter. Ignored otherwise.</param>
        /// <param name="logger">Optional logger for device diagnostics.</param>
        /// <param name="queueDepth">For DeviceType.Native on Linux: per-ring kernel submission depth D (maxEvents for io_uring_queue_init / libaio io_setup). Orthogonal to <paramref name="numIoContexts"/> (ring count) and the aggregate throttle. 0 (default) = the device default depth. Ignored otherwise.</param>
        /// <param name="uringSqPoll">For DeviceType.Native on Linux with the io_uring backend: enable IORING_SETUP_SQPOLL so a kernel thread polls the submission queue (syscall-free submits). Each ring gets its own poll thread. Ignored for libaio / on Windows. Off by default.</param>
        /// <param name="uringSqPollIdleMs">io_uring SQPOLL poll-thread idle window in milliseconds (sq_thread_idle). Only used when <paramref name="uringSqPoll"/> is true; 0 = native default. Ignored otherwise.</param>
        /// <param name="uringSqPollCpus">io_uring SQPOLL poll-thread CPU pin list (comma-separated CPU ids). Only used when <paramref name="uringSqPoll"/> is true; ring i pins its poll thread to cpus[i % count] via IORING_SETUP_SQ_AFF. Null/empty leaves them unpinned.</param>
        /// <returns>Device instance</returns>
        public static IDevice CreateLogDevice(string logPath = null, DeviceType deviceType = DeviceType.Default, bool preallocateFile = false, bool deleteOnClose = false, long capacity = CAPACITY_UNSPECIFIED, bool recoverDevice = false, bool useIoCompletionPort = false, bool disableFileBuffering = true, bool readOnly = false, NativeStorageDevice.IoBackend ioBackend = NativeStorageDevice.IoBackend.Default, int numCompletionThreads = 1, long localMemorySegmentSize = 1L << 30, int localMemoryRingCapacity = 0, ILogger logger = null, int numIoContexts = 0, int queueDepth = 0, bool uringSqPoll = false, int uringSqPollIdleMs = 0, string uringSqPollCpus = null)
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
                DeviceType.Native when RuntimeInformation.IsOSPlatform(OSPlatform.Linux) => new NativeStorageDevice(logPath, deleteOnClose, disableFileBuffering, capacity, numCompletionThreads: numCompletionThreads, ioBackend: ioBackend, logger: logger, numIoContexts: numIoContexts, queueDepth: queueDepth, uringSqPoll: uringSqPoll, uringSqPollIdleMs: uringSqPollIdleMs, uringSqPollCpus: uringSqPollCpus),
                DeviceType.Native when RuntimeInformation.IsOSPlatform(OSPlatform.Windows) => new LocalStorageDevice(logPath, preallocateFile, deleteOnClose, disableFileBuffering, capacity, recoverDevice, useIoCompletionPort, readOnly: readOnly, logger: logger),
                DeviceType.RandomAccess => new RandomAccessLocalStorageDevice(logPath, preallocateFile, deleteOnClose, disableFileBuffering, capacity, recoverDevice, readOnly: readOnly, logger: logger),
                DeviceType.FileStream => new ManagedLocalStorageDevice(logPath, preallocateFile, deleteOnClose, disableFileBuffering, capacity, recoverDevice, readOnly: readOnly, logger: logger),
                DeviceType.Null => new NullDevice(),
                DeviceType.LocalMemory => new LocalMemoryDevice(
                    capacity: capacity,
                    segmentSize: localMemorySegmentSize,
                    parallelism: numCompletionThreads < 0 ? System.Environment.ProcessorCount : numCompletionThreads,
                    ringCapacity: localMemoryRingCapacity > 0 ? localMemoryRingCapacity : 1024,
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