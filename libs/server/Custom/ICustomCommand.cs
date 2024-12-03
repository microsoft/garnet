// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Interface for custom commands
    /// </summary>
    interface ICustomCommand
    {
        /// <summary>
        /// Name of command
        /// </summary>
        byte[] Name { get; }
    }
}
