// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace Tsavorite.core
{
    /// <summary>
    /// Consumes a TsavoriteLog entry without copying 
    /// </summary>
    public interface ILogEntryConsumer
    {
        /// <summary>
        /// Consumes the given entry.
        /// </summary>
        /// <param name="payloadPtr"></param>
        /// <param name="payloadLength"></param>
        /// <param name="currentAddress">Address of the consumed entry (excluding log header)</param>
        /// <param name="nextAddress">Expected address of the next log entry</param>
        /// <param name="isProtected">Whether call is under epoch protection</param>
        unsafe void Consume(byte* payloadPtr, int payloadLength, long currentAddress, long nextAddress, bool isProtected);
    }

    /// <summary>
    /// Consumes TsavoriteLog entries in bulk (raw data) without copying 
    /// </summary>
    public interface IBulkLogEntryConsumer
    {
        /// <summary>
        /// Consumes the given bulk entries (raw data) under epoch protection - do not block.
        /// </summary>
        /// <param name="payloadPtr"></param>
        /// <param name="payloadLength"></param>
        /// <param name="currentAddress"> address of the consumed entry </param>
        /// <param name="nextAddress"> (predicted) address of the next entry </param>
        /// <param name="isProtected"> If call is under epoch protection </param>
        unsafe void Consume(byte* payloadPtr, int payloadLength, long currentAddress, long nextAddress, bool isProtected);

        /// <summary>
        /// Throttle the iteration if needed, outside epoch protection - blocking here is fine.
        /// </summary>
        void Throttle();

        /// <summary>
        /// Waits, outside epoch protection and after <see cref="Throttle"/>, until the consumer can take more entries.
        /// Only <see cref="TsavoriteLogScanIterator.BulkConsumeAllAsync"/> calls it: the scan stops to await a wait that does
        /// not complete synchronously, so a consumer that has to wait holds no thread. A wait that completes synchronously
        /// is not awaited, so return <c>default</c> rather than a completed wait backed by an <see cref="IValueTaskSource"/>.
        /// </summary>
        /// <param name="token">Cancellation token of the scan</param>
        ValueTask ThrottleAsync(CancellationToken token) => default;
    }

}