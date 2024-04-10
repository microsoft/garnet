// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
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
