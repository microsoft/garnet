// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.cluster
{
    /// <summary>
    /// Transfer option supported by MIGRATE
    /// </summary>
    internal enum TransferOption : byte
    {
        NONE,
        /// <summary>
        /// Transfer all provided keys
        /// </summary>
        KEYS,
        /// <summary>
        /// Transfer all keys in a single slot
        /// </summary>
        SLOTS,
    }
}