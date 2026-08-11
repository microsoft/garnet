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
        readonly int numCompletionThreads;
        readonly NativeDeviceOptions nativeDeviceOptions;
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
        /// <param name="numCompletionThreads">For DeviceType.Native on Linux: number of IO completion drain threads (default 1). Ignored otherwise.</param>
        /// <param name="readOnly">Whether files are opened as readonly</param>
        /// <param name="logger">Logger</param>
        /// <param name="nativeDeviceOptions">For DeviceType.Native on Linux: libaio / io_uring backend tuning (IO backend, ring count, per-ring queue depth, SQPOLL). Null (default) uses the backend-specific defaults. Ignored otherwise. See <see cref="NativeDeviceOptions"/>.</param>
        public LocalStorageNamedDeviceFactoryCreator(bool preallocateFile = false, bool deleteOnClose = false, bool disableFileBuffering = true, int? throttleLimit = null, DeviceType deviceType = DeviceType.Default, int numCompletionThreads = 1, bool readOnly = false, ILogger logger = null, NativeDeviceOptions nativeDeviceOptions = null)
        {
            this.preallocateFile = preallocateFile;
            this.deleteOnClose = deleteOnClose;
            this.disableFileBuffering = disableFileBuffering;
            this.throttleLimit = throttleLimit;
            this.deviceType = deviceType;
            this.numCompletionThreads = numCompletionThreads;
            this.nativeDeviceOptions = nativeDeviceOptions;
            this.readOnly = readOnly;
            this.logger = logger;
        }

        public INamedDeviceFactory Create(string baseName)
        {
            return new LocalStorageNamedDeviceFactory(preallocateFile, deleteOnClose, disableFileBuffering, throttleLimit, deviceType, numCompletionThreads, readOnly, baseName, logger, nativeDeviceOptions);
        }
    }
}