// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Expire option
    /// </summary>
    public enum ExpireOption : byte
    {
        /// <summary>
        /// None
        /// </summary>
        None,
        /// <summary>
        /// Set expiry only when the key has no expiry
        /// </summary>
        NX,
        /// <summary>
        /// Set expiry only when the key has an existing expiry 
        /// </summary>
        XX,
        /// <summary>
        /// Set expiry only when the new expiry is greater than current one
        /// </summary>
        GT,
        /// <summary>
        /// Set expiry only when the new expiry is less than current one
        /// </summary>
        LT
    }
}