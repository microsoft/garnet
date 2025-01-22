// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Consumes a TsavoriteAof entry without copying 
    /// </summary>
    public interface IAofEntryConsumer
    {
        /// <summary>
        /// Consumes the given entry.
        /// </summary>
        /// <param name="entry"> the entry to consume </param>
        /// <param name="currentAddress"> address of the consumed entry </param>
        /// <param name="nextAddress"> (predicted) address of the next entry </param>
        public void Consume(ReadOnlySpan<byte> entry, long currentAddress, long nextAddress);
    }

    /// <summary>
    /// Consumes TsavoriteAof entries in bulk (raw data) without copying 
    /// </summary>
    public interface IBulkAofEntryConsumer
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