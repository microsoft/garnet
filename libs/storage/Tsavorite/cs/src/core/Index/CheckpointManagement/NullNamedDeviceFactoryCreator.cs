// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Creator of factory for getting null device instances
    /// </summary>
    public class NullNamedDeviceFactoryCreator : INamedDeviceFactoryCreator
    {
        static readonly NullNamedDeviceFactory nullNamedDeviceFactory = new();

        public INamedDeviceFactory Create(string baseName)
        {
            return nullNamedDeviceFactory;
        }
    }
}