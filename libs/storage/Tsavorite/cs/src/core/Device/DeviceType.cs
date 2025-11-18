// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Type of device
    /// </summary>
    public enum DeviceType : byte
    {
        /// <summary>
        /// Let the system choose the device type for the current platform. This is the default.
        /// </summary>
        Default = 0,

        /// <summary>
        /// Use Native device if available for the current platform.
        /// </summary>
        Native = 1,

        /// <summary>
        /// Use device based on .NET RandomAccess (this is the default on non-Windows).
        /// </summary>
        RandomAccess = 2,

        /// <summary>
        /// Use device based on .NET FileStream.
        /// </summary>
        FileStream = 3,

        /// <summary>
        /// Use device based on Azure Storage. You need to provide the Azure Storage configuration separately.
        /// </summary>
        AzureStorage = 4,

        /// <summary>
        /// Use null device.
        /// </summary>
        Null = byte.MaxValue,
    }
}