// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.networking
{
    /// <summary>
    /// TLS reader status
    /// </summary>
    enum TlsReaderStatus
    {
        /// <summary>
        /// Rest phase, no reader task or work running
        /// </summary>
        Rest,
        /// <summary>
        /// Reader is active, processing TLS data on some thread
        /// </summary>
        Active,
        /// <summary>
        /// Reader is waiting on a semaphore for data to be available
        /// </summary>
        Waiting
    }
}