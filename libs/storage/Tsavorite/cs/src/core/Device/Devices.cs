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
        /// <param name="capacity">The maximal number of bytes this storage device can accommodate, or CAPACITY_UNSPECIFIED if there is no such limit</param>
        /// <param name="recoverDevice">Whether to recover device metadata from existing files</param>
        /// <param name="useIoCompletionPort">Whether we use IO completion port with polling</param>
        /// <param name="disableFileBuffering">Whether file buffering (during write) is disabled (default of true requires aligned writes)</param>
        /// <param name="readOnly">Open file in readOnly mode</param>
        /// <param name="logger"></param>
        /// <returns>Device instance</returns>
        public static IDevice CreateLogDevice(string logPath = null, DeviceType deviceType = DeviceType.Default, bool preallocateFile = false, bool deleteOnClose = false, long capacity = CAPACITY_UNSPECIFIED, bool recoverDevice = false, bool useIoCompletionPort = false, bool disableFileBuffering = true, bool readOnly = false, ILogger logger = null)
        {
            if (deviceType == DeviceType.Default)
            {
                deviceType = GetDefaultDeviceType();
            }

            if (deviceType != DeviceType.Null && logPath == null)
            {
                throw new TsavoriteException("logPath must be specified for non-null devices");
            }

            return deviceType switch
            {
                DeviceType.Native when RuntimeInformation.IsOSPlatform(OSPlatform.Linux) => new NativeStorageDevice(logPath, deleteOnClose, disableFileBuffering, capacity, logger: logger),
                DeviceType.Native when RuntimeInformation.IsOSPlatform(OSPlatform.Windows) => new LocalStorageDevice(logPath, preallocateFile, deleteOnClose, disableFileBuffering, capacity, recoverDevice, useIoCompletionPort, readOnly: readOnly),
                DeviceType.RandomAccess => new RandomAccessLocalStorageDevice(logPath, preallocateFile, deleteOnClose, disableFileBuffering, capacity, recoverDevice, readOnly: readOnly),
                DeviceType.FileStream => new ManagedLocalStorageDevice(logPath, preallocateFile, deleteOnClose, disableFileBuffering, capacity, recoverDevice, readOnly: readOnly),
                DeviceType.Null => new NullDevice(),
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