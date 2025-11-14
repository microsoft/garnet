// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace ReadFileServer
{
    public enum DeviceType
    {
        LocalStorage,
        ManagedLocalStorage,
        RandomAccessLocalStorage,
    }

    /// <summary>
    /// Factory to create Tsavorite objects
    /// </summary>
    public static class Devices
    {
        /// <summary>
        /// Create a storage device for the log
        /// </summary>
        /// <param name="logPath">Path to file that will store the log (empty for null device)</param>
        /// <param name="deleteOnClose">Delete files on close</param>
        /// <param name="readOnly">Open file in readOnly mode</param>
        /// <param name="logger"></param>
        /// <returns>Device instance</returns>
        public static IDevice CreateLogDevice(string logPath, bool deleteOnClose = false, bool readOnly = false, DeviceType deviceType = DeviceType.LocalStorage)
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                throw new Exception("Non-Windows platforms are not supported in this build.");
            }
            else
            {
                switch (deviceType)
                {
                    case DeviceType.LocalStorage:
                        return new LocalStorageDevice(logPath,
                            deleteOnClose: deleteOnClose,
                            useIoCompletionPort: false, // setting this to true does not repro the bug
                            readOnly: readOnly);
                    case DeviceType.RandomAccessLocalStorage:
                        return new RandomAccessLocalStorageDevice(logPath, deleteOnClose, readOnly);
                }
                return null;
            }
        }
    }
}