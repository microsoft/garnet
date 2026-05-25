// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace Garnet.cluster
{
    /// <summary>
    /// Interface for a checkpoint reader that provides an enumeration of transmit sources.
    /// </summary>
    internal interface ISnapshotReader : IDisposable
    {
        /// <summary>
        /// Returns an enumeration of transmit sources with initialized data sources.
        /// The caller is responsible for disposing each transmit source after use.
        /// </summary>
        IEnumerable<ISnapshotTransmitSource> GetTransmitSources();
    }
}