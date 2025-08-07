// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    /// <summary>
    /// Expire option
    /// </summary>
    [Flags]
    public enum ExpireOption : byte
    {
        /// <summary>
        /// None
        /// </summary>
        None = 0,
        /// <summary>
        /// Set expiry only when the key has no expiry
        /// </summary>
        NX = 1 << 0,
        /// <summary>
        /// Set expiry only when the key has an existing expiry 
        /// </summary>
        XX = 1 << 1,
        /// <summary>
        /// Set expiry only when the new expiry is greater than current one
        /// </summary>
        GT = 1 << 2,
        /// <summary>
        /// Set expiry only when the new expiry is less than current one
        /// </summary>
        LT = 1 << 3,
        /// <summary>
        /// Set expiry only when the key has an existing expiry and the new expiry is greater than current one
        /// </summary>
        XXGT = XX | GT,
        /// <summary>
        /// Set expiry only when the key has an existing expiry and the new expiry is less than current one
        /// </summary>
        XXLT = XX | LT,

        // Important: Any addition to this enum beyond 4 bytes will break ExpirationWithOption struct
    }
}