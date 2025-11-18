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
        readonly bool readOnly;
        readonly ILogger logger;

        /// <summary>
        /// Create instance of factory
        /// </summary>
        /// <param name="preallocateFile">Whether files should be preallocated</param>
        /// <param name="deleteOnClose">Whether file should be deleted on close</param>
        /// <param name="disableFileBuffering">Whether file buffering (during write) is disabled (default of true requires aligned writes)</param>
        /// <param name="throttleLimit">Throttle limit (max number of pending I/Os) for this device instance</param>
        /// <param name="deviceType">Device type to use</param>
        /// <param name="readOnly">Whether files are opened as readonly</param>
        /// <param name="baseName">Base name</param>
        /// <param name="logger">Logger</param>
        public LocalStorageNamedDeviceFactory(bool preallocateFile = false, bool deleteOnClose = false, bool disableFileBuffering = true, int? throttleLimit = null, DeviceType deviceType = DeviceType.Default, bool readOnly = false, string baseName = null, ILogger logger = null)
        {
            this.preallocateFile = preallocateFile;
            this.deleteOnClose = deleteOnClose;
            this.disableFileBuffering = disableFileBuffering;
            this.throttleLimit = throttleLimit;
            this.deviceType = deviceType;
            this.readOnly = readOnly;
            this.baseName = baseName;
            this.logger = logger;
        }

        /// <inheritdoc />
        public IDevice Get(FileDescriptor fileInfo)
        {
            var device = Devices.CreateLogDevice(
                logPath: Path.Combine(baseName, fileInfo.directoryName, fileInfo.fileName),
                deviceType: deviceType,
                preallocateFile: preallocateFile,
                deleteOnClose: deleteOnClose,
                disableFileBuffering: disableFileBuffering,
                readOnly: readOnly,
                logger: logger);
            if (throttleLimit.HasValue)
            {
                device.ThrottleLimit = throttleLimit.Value;
            }
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