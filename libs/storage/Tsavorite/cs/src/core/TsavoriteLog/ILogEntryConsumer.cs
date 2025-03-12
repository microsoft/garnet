// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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
    }

}