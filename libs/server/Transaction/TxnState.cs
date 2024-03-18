// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Transaction state enum
    /// </summary>
    public enum TxnState : byte
    {
        /// <summary>
        /// None
        /// </summary>
        None,
        /// <summary>
        /// Started
        /// </summary>
        Started,
        /// <summary>
        /// Running
        /// </summary>
        Running,
        /// <summary>
        /// Aborted
        /// </summary>
        Aborted,
    }
}