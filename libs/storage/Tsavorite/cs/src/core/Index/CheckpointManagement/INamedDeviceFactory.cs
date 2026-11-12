// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;

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
        /// Open the existing first segment of a named file for reading metadata, without creating or modifying
        /// files, directories, or blobs. This API is independent of the create-capable <see cref="Get"/> API.
        /// </summary>
        /// <param name="fileInfo">Named file whose segment zero is opened.</param>
        /// <returns>A readable stream owned by the caller.</returns>
        /// <exception cref="NotSupportedException">The factory does not support non-creating metadata reads.</exception>
        Stream OpenRead(FileDescriptor fileInfo)
            => throw new NotSupportedException("This factory does not support non-creating metadata reads");

        /// <summary>
        /// Identify missing-file errors from <see cref="OpenRead"/> or its stream, including backend-specific errors.
        /// </summary>
        /// <param name="exception">Error raised while opening or reading metadata.</param>
        /// <returns>Whether the named file or its parent no longer exists.</returns>
        bool IsFileNotFound(Exception exception)
            => exception is FileNotFoundException or DirectoryNotFoundException;

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