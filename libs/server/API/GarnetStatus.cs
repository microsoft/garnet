// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Garnet API call return status
    /// </summary>
    public enum GarnetStatus : byte
    {
        /// <summary>
        /// OK
        /// </summary>
        OK,
        /// <summary>
        /// Not Found
        /// </summary>
        NOTFOUND,
        /// <summary>
        /// Moved
        /// </summary>
        MOVED
    }
}