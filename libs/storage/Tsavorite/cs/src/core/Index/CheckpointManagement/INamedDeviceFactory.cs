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
    }
}