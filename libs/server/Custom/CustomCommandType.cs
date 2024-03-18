// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Type of custom command
    /// </summary>
    public enum CustomCommandType : byte
    {
        /// <summary>
        /// Read
        /// </summary>
        Read,
        /// <summary>
        /// Read-modify-write
        /// </summary>
        ReadModifyWrite
    }
}