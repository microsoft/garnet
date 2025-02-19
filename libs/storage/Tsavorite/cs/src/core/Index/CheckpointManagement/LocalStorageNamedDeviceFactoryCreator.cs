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
        readonly bool useNativeDeviceLinux;
        readonly bool readOnly;
        readonly ILogger logger;

        /// <summary>
        /// Create instance of factory
        /// </summary>
        /// <param name="preallocateFile">Whether files should be preallocated</param>
        /// <param name="deleteOnClose">Whether file should be deleted on close</param>
        /// <param name="disableFileBuffering">Whether file buffering (during write) is disabled (default of true requires aligned writes)</param>
        /// <param name="throttleLimit">Throttle limit (max number of pending I/Os) for this device instance</param>
        /// <param name="useNativeDeviceLinux">Use native device on Linux</param>
        /// <param name="readOnly">Whether files are opened as readonly</param>
        /// <param name="logger">Logger</param>
        public LocalStorageNamedDeviceFactoryCreator(bool preallocateFile = false, bool deleteOnClose = false, bool disableFileBuffering = true, int? throttleLimit = null, bool useNativeDeviceLinux = false, bool readOnly = false, ILogger logger = null)
        {
            this.preallocateFile = preallocateFile;
            this.deleteOnClose = deleteOnClose;
            this.disableFileBuffering = disableFileBuffering;
            this.throttleLimit = throttleLimit;
            this.useNativeDeviceLinux = useNativeDeviceLinux;
            this.readOnly = readOnly;
            this.logger = logger;
        }

        public INamedDeviceFactory Create(string baseName)
        {
            return new LocalStorageNamedDeviceFactory(preallocateFile, deleteOnClose, disableFileBuffering, throttleLimit, useNativeDeviceLinux, readOnly, baseName, logger);
        }
    }
}