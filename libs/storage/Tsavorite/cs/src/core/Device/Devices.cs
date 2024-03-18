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
        /// <param name="preallocateFile">Whether we try to preallocate the file on creation</param>
        /// <param name="deleteOnClose">Delete files on close</param>
        /// <param name="capacity">The maximal number of bytes this storage device can accommondate, or CAPACITY_UNSPECIFIED if there is no such limit</param>
        /// <param name="recoverDevice">Whether to recover device metadata from existing files</param>
        /// <param name="useIoCompletionPort">Whether we use IO completion port with polling</param>
        /// <param name="disableFileBuffering">Whether file buffering (during write) is disabled (default of true requires aligned writes)</param>
        /// <param name="useNativeDeviceLinux">Use native device on Linux, instead of managed device based on FileStream</param>
        /// <param name="logger"></param>
        /// <returns>Device instance</returns>
        public static IDevice CreateLogDevice(string logPath, bool preallocateFile = false, bool deleteOnClose = false, long capacity = CAPACITY_UNSPECIFIED, bool recoverDevice = false, bool useIoCompletionPort = false, bool disableFileBuffering = true, bool useNativeDeviceLinux = false, ILogger logger = null)
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) && useNativeDeviceLinux)
                    return new NativeStorageDevice(logPath, deleteOnClose, disableFileBuffering, capacity, logger: logger);
                else
                    return new ManagedLocalStorageDevice(logPath, preallocateFile, deleteOnClose, disableFileBuffering, capacity, recoverDevice);
            }
            else
                return new LocalStorageDevice(logPath, preallocateFile, deleteOnClose, disableFileBuffering, capacity, recoverDevice, useIoCompletionPort);
        }
    }
}