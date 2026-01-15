// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    /// <summary>
    /// Specifies the placement constraints for background tasks.
    /// </summary>
    [Flags]
    public enum TaskPlacementCategory
    {
        /// <summary>
        /// Indicates that this task can safely run on all node types.
        /// </summary>
        All = Primary | Replica,
        /// <summary>
        /// Indicates that this task can safely run only on primary nodes.
        /// </summary>
        Primary = 1 << 0,
        /// <summary>
        /// Indicates that this task can safely run only on replica nodes.
        /// </summary>
        Replica = 1 << 1,
    }
}