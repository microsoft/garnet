// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Local storage named device factory creator
    /// </summary>
    public class LocalStorageNamedDeviceFactoryCreator : INamedDeviceFactoryCreator
    {
        readonly bool preallocateFile;
        readonly bool deleteOnClose;
        readonly int? throttleLimit;
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
        /// <param name="deviceType">Device type</param>
        /// <param name="ioBackend">For DeviceType.Native on Linux: which IO backend (libaio or io_uring) to use. Ignored otherwise.</param>
        /// <param name="numCompletionThreads">For DeviceType.Native on Linux: number of IO completion drain threads (default 1). Ignored otherwise.</param>
        /// <param name="readOnly">Whether files are opened as readonly</param>
        /// <param name="logger">Logger</param>
        public LocalStorageNamedDeviceFactoryCreator(bool preallocateFile = false, bool deleteOnClose = false, bool disableFileBuffering = true, int? throttleLimit = null, DeviceType deviceType = DeviceType.Default, NativeStorageDevice.IoBackend ioBackend = NativeStorageDevice.IoBackend.Default, int numCompletionThreads = 1, bool readOnly = false, ILogger logger = null)
        {
            this.preallocateFile = preallocateFile;
            this.deleteOnClose = deleteOnClose;
            this.disableFileBuffering = disableFileBuffering;
            this.throttleLimit = throttleLimit;
            this.deviceType = deviceType;
            this.ioBackend = ioBackend;
            this.numCompletionThreads = numCompletionThreads;
            this.readOnly = readOnly;
            this.logger = logger;
        }

        public INamedDeviceFactory Create(string baseName)
        {
            return new LocalStorageNamedDeviceFactory(preallocateFile, deleteOnClose, disableFileBuffering, throttleLimit, deviceType, ioBackend, numCompletionThreads, readOnly, baseName, logger);
        }
    }
}