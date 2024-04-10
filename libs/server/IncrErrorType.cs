// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Type of error when executing Increment/Decrement commands
    /// </summary>
    public enum IncrErrorType : byte
    {
        /// <summary>
        /// Increment/Decrement Success
        /// </summary>
        SUCCESS,
        /// <summary>
        /// Increment/Decrement Invalid Numeric Value
        /// </summary>
        INVALID_NUMBER
    }
}