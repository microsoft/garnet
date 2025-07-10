// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Operation error type
    /// </summary>
    public enum OperationError : byte
    {
        /// <summary>
        /// Operation on data type succeeded
        /// </summary>
        SUCCESS,
        /// <summary>
        /// Operation failed due to incompatible type
        /// </summary>
        INVALID_TYPE,
        /// <summary>
        /// Operation failed due to NaN/infinity
        /// </summary>
        NAN_OR_INFINITY
    }
}