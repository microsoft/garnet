// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.cluster
{
    /// <summary>
    /// MigrateState
    /// </summary>
    internal enum MigrateState : byte
    {
        /// <summary>
        /// SUCCESS
        /// </summary>
        SUCCESS = 0x0,
        /// <summary>
        /// FAIL
        /// </summary>
        FAIL,
        /// <summary>
        /// PENDING
        /// </summary>
        PENDING,
        /// <summary>
        /// IMPORT
        /// </summary>
        IMPORT,
        /// <summary>
        /// STABLE
        /// </summary>
        STABLE,
        /// <summary>
        /// NODE
        /// </summary>
        NODE,
    }
}