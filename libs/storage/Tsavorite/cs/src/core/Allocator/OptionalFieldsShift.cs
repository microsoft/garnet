// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Saves optional fields ETag and Expiration during a record-resizing operation and restores them when done.
    /// </summary>
    /// <remarks>
    /// We don't save ObjectLogPosition; that's only used during Serialization. The caller (TrySetValueLength) adjusts filler
    /// address and length by the growth (positive or negative) of the object value, so no address adjustment or zeroing of
    /// space is needed.
    /// </remarks>
    internal unsafe struct OptionalFieldsShift
    {
        long eTag;
        long expiration;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Save(long address, RecordInfo recordInfo)
        {
            if (recordInfo.HasETag)
            {
                eTag = *(long*)address;
                address += LogRecord.ETagSize;
            }
            if (recordInfo.HasExpiration)
            {
                expiration = *(long*)address;
                address += LogRecord.ExpirationSize;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Restore(long address, RecordInfo recordInfo)
        {
            if (recordInfo.HasETag)
            {
                *(long*)address = eTag;
                address += LogRecord.ETagSize;
            }
            if (recordInfo.HasExpiration)
            {
                *(long*)address = expiration;
                address += LogRecord.ExpirationSize;
            }
        }
    }
}