// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace Tsavorite.core
{
    /// <summary>
    /// Factory for getting IDevice instances for checkpointing. The factory is specific to a particular base path or container.
    /// </summary>
    public interface INamedDeviceFactory
    {
        /// <summary>
        /// Get IDevice instance for given file info
        /// </summary>
        /// <param name="fileInfo">File info</param>
        /// <returns></returns>
        IDevice Get(FileDescriptor fileInfo);

        /// <summary>
        /// Delete IDevice for given file info
        /// </summary>
        /// <param name="fileInfo">File info</param>
        /// <returns></returns>
        void Delete(FileDescriptor fileInfo);

        /// <summary>
        /// List path contents, in order of preference
        /// </summary>
        /// <returns></returns>
        IEnumerable<FileDescriptor> ListContents(string path);

        /// <summary>
        /// Get the sector size, in bytes, of the device <see cref="Get(FileDescriptor)"/> would return for
        /// <paramref name="fileInfo"/>, without creating that device, its file, or any directory.
        /// </summary>
        /// <param name="fileInfo">File info</param>
        /// <returns>The sector size; the default implementation returns <see cref="IDevice.MinDeviceSectorSize"/>.</returns>
        uint GetSectorSize(FileDescriptor fileInfo) => IDevice.MinDeviceSectorSize;
    }
}