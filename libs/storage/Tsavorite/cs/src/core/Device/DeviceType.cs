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
        /// Use device based on .NET RandomAccess. This is the default on platforms that have no
        /// <see cref="Native"/> implementation for the current OS/architecture: macOS, and non-x64 Linux
        /// (e.g. arm64), since the Linux native library is currently shipped only for x64. Windows and
        /// x64 Linux default to <see cref="Native"/>.
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
        /// In-process RAM-backed device with async submit/complete on dedicated processor threads. Useful for
        /// unit tests and benchmarks where the goal is to characterize the upper bound of Tsavorite throughput
        /// without paying any real disk or kernel-syscall cost. The number of IO processor threads, simulated
        /// per-IO latency, and segment size can be passed to <see cref="Devices.CreateLogDevice"/>.
        /// </summary>
        LocalMemory = 5,

        /// <summary>
        /// Use null device.
        /// </summary>
        Null = byte.MaxValue,
    }
}