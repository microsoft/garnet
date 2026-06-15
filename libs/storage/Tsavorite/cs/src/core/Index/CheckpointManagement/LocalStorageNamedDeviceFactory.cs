// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Local storage device factory
    /// </summary>
    public class LocalStorageNamedDeviceFactory : INamedDeviceFactory
    {
        readonly string baseName;
        readonly bool deleteOnClose;
        readonly int? throttleLimit;
        readonly bool preallocateFile;
        readonly bool disableFileBuffering;
        readonly DeviceType deviceType;
        readonly NativeStorageDevice.IoBackend ioBackend;
        readonly int numCompletionThreads;
        readonly bool readOnly;
        readonly ILogger logger;

        /// <summary>
        /// Create instance of factory
        /// </summary>
        /// <param name="preallocateFile">Whether files should be preallocated</param>
        /// <param name="deleteOnClose">Whether file should be deleted on close</param>
        /// <param name="disableFileBuffering">Whether file buffering (during write) is disabled (default of true requires aligned writes)</param>
        /// <param name="throttleLimit">Throttle limit (max number of pending I/Os) for this device instance. For DeviceType.LocalMemory (which has no device-wide throttle) it instead sets the per-ring capacity, rounded up to a power of two.</param>
        /// <param name="deviceType">Device type to use</param>
        /// <param name="ioBackend">For DeviceType.Native on Linux: which IO backend (libaio or io_uring) to use. Ignored otherwise.</param>
        /// <param name="numCompletionThreads">For DeviceType.Native on Linux: number of IO completion drain threads (default 1). Ignored otherwise.</param>
        /// <param name="readOnly">Whether files are opened as readonly</param>
        /// <param name="baseName">Base name</param>
        /// <param name="logger">Logger</param>
        public LocalStorageNamedDeviceFactory(bool preallocateFile = false, bool deleteOnClose = false, bool disableFileBuffering = true, int? throttleLimit = null, DeviceType deviceType = DeviceType.Default, NativeStorageDevice.IoBackend ioBackend = NativeStorageDevice.IoBackend.Default, int numCompletionThreads = 1, bool readOnly = false, string baseName = null, ILogger logger = null)
        {
            this.preallocateFile = preallocateFile;
            this.deleteOnClose = deleteOnClose;
            this.disableFileBuffering = disableFileBuffering;
            this.throttleLimit = throttleLimit;
            this.deviceType = deviceType;
            this.ioBackend = ioBackend;
            this.numCompletionThreads = numCompletionThreads;
            this.readOnly = readOnly;
            this.baseName = baseName;
            this.logger = logger;
        }

        /// <inheritdoc />
        public IDevice Get(FileDescriptor fileInfo)
        {
            // LocalMemory has no device-wide Throttle()/numPending counter; its per-ring SPSC backpressure
            // (the producer blocks on a full ring) IS the in-flight bound. So a user-specified throttle maps
            // to the per-ring capacity (rounded up to a power of two, capped at the largest power-of-two int),
            // mirroring KV.benchmark. Other device types honor the throttle via ThrottleLimit (set below).
            int localMemoryRingCapacity = 0;
            if (deviceType == DeviceType.LocalMemory && throttleLimit is > 0)
            {
                const int MaxRing = 1 << 30;
                localMemoryRingCapacity = throttleLimit.Value >= MaxRing ? MaxRing : (int)Utility.NextPowerOf2(throttleLimit.Value);
            }

            var device = Devices.CreateLogDevice(
                logPath: Path.Combine(baseName, fileInfo.directoryName, fileInfo.fileName),
                deviceType: deviceType,
                preallocateFile: preallocateFile,
                deleteOnClose: deleteOnClose,
                disableFileBuffering: disableFileBuffering,
                readOnly: readOnly,
                ioBackend: ioBackend,
                numCompletionThreads: numCompletionThreads,
                localMemoryRingCapacity: localMemoryRingCapacity,
                logger: logger);
            if (throttleLimit.HasValue)
            {
                device.ThrottleLimit = throttleLimit.Value;
            }
            // Checkpoint/commit metadata devices are used as a single growing segment file
            // ("<base>.0" only); the IDevice ctor defaults already establish unbounded
            // single-segment mode (segmentSize = -1, all addresses route to segment 0), so no
            // explicit Initialize call is needed. Consumers that want multi-segment routing
            // can still call device.Initialize(segmentSize) on the returned device.
            return device;
        }

        /// <inheritdoc />
        public IEnumerable<FileDescriptor> ListContents(string path)
        {
            var pathInfo = new DirectoryInfo(Path.Combine(baseName, path));

            if (pathInfo.Exists)
            {
                foreach (var folder in pathInfo.GetDirectories().OrderByDescending(f => f.LastWriteTime))
                {
                    yield return new FileDescriptor(folder.Name, "");
                }

                foreach (var file in pathInfo.GetFiles().OrderByDescending(f => f.LastWriteTime))
                {
                    yield return new FileDescriptor("", file.Name);
                }
            }
        }

        /// <inheritdoc />
        public void Delete(FileDescriptor fileInfo)
        {
            long startTime = DateTimeOffset.UtcNow.Ticks;
            if (fileInfo.fileName != null)
            {
                var file = new FileInfo(Path.Combine(baseName, fileInfo.directoryName, fileInfo.fileName + ".0"));
                while (true)
                {
                    try
                    {
                        if (file.Exists) file.Delete();
                        break;
                    }
                    catch { }
                    Thread.Yield();
                    // Retry until timeout
                    if (DateTimeOffset.UtcNow.Ticks - startTime > TimeSpan.FromSeconds(5).Ticks) break;
                }
            }
            else
            {
                var dir = new DirectoryInfo(Path.Combine(baseName, fileInfo.directoryName));
                while (true)
                {
                    try
                    {
                        if (dir.Exists) dir.Delete(true);
                        break;
                    }
                    catch { }
                    Thread.Yield();
                    // Retry until timeout
                    if (DateTimeOffset.UtcNow.Ticks - startTime > TimeSpan.FromSeconds(5).Ticks) break;
                }
            }
        }
    }
}