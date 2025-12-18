// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Represents a result from an operation against VectorManager.
    /// </summary>
    public enum VectorManagerResult
    {
        /// <summary>
        /// Invalid default value.
        /// </summary>
        Invalid = 0,

        /// <summary>
        /// Everything worked.
        /// </summary>
        OK,
        /// <summary>
        /// Response from DiskANN suggests invalid parameters were passed.
        /// </summary>
        BadParams,
        /// <summary>
        /// Vector already exists.
        /// </summary>
        Duplicate,
        /// <summary>
        /// Vector was not found.
        /// </summary>
        MissingElement,
    }
}
