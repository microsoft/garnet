// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Type of device
    /// </summary>
    public enum DeviceType
    {
        /// <summary>
        /// Native (this is the default on Windows). On Linux, you need to
        /// enable the UseNativeDeviceLinux option to use this.
        /// </summary>
        Native = 0,

        /// <summary>
        /// Based on .NET RandomAccess (this is the default on non-Windows).
        /// </summary>
        RandomAccess,

        /// <summary>
        /// Based on .NET FileStream.
        /// </summary>
        FileStream,

        /// <summary>
        /// Based on Azure Storage. You need to provide the Azure Storage configuration separately.
        /// </summary>
        AzureStorage,
    }
}