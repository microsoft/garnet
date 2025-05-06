// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Interface to provide a batch of ReadOnlySpan[byte] data to Tsavorite
    /// </summary>
    public interface IReadOnlySpanBatch
    {
        /// <summary>
        /// Number of entries in provided batch
        /// </summary>
        /// <returns>Number of entries</returns>
        int TotalEntries();

        /// <summary>
        /// Retrieve batch entry at specified index
        /// </summary>
        /// <param name="index">Index</param>
        /// <returns></returns>
        ReadOnlySpan<byte> Get(int index);
    }
}