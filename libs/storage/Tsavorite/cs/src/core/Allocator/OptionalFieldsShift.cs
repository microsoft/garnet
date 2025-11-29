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
        long eTag = LogRecord.NoETag;
        long expiration = LogRecord.NoExpiration;

        public OptionalFieldsShift() { }

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
        internal void Restore(long address, in RecordSizeInfo sizeInfo, ref RecordInfo recordInfo)
        {
            if (sizeInfo.FieldInfo.HasETag)
            {
                *(long*)address = eTag;
                address += LogRecord.ETagSize;
                recordInfo.SetHasETag();
            }
            else
                recordInfo.ClearHasETag();

            if (sizeInfo.FieldInfo.HasExpiration)
            {
                *(long*)address = expiration;
                address += LogRecord.ExpirationSize;
                recordInfo.SetHasExpiration();
            }
            else
                recordInfo.ClearHasExpiration();
        }
    }
}