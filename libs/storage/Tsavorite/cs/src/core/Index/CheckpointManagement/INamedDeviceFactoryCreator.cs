// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Factory creator for getting IDevice instances for checkpointing
    /// </summary>
    public interface INamedDeviceFactoryCreator
    {
        /// <summary>
        /// Create factory for creating IDevice instances, for the given base name or container
        /// </summary>
        /// <param name="baseName">Base name or container</param>
        /// <returns></returns>
        INamedDeviceFactory Create(string baseName);
    }
}