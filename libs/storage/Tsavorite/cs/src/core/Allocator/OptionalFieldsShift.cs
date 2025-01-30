// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    internal unsafe struct OptionalFieldsShift
    {
        internal long ETag;
        internal long Expiration;

        internal static OptionalFieldsShift SaveAndClear(long address, ref RecordInfo recordInfo) 
            => new OptionalFieldsShift(address, ref recordInfo);

        private OptionalFieldsShift(long address, ref RecordInfo recordInfo) => GetAndZero(address, ref recordInfo);

        internal void GetAndZero(long address, ref RecordInfo recordInfo)
        {
            // We are calling this when we are shifting, so zero out the old value. We do not need to spend cycles clearing the Has* bits in
            // RecordInfo because we will not be doing data operations during this shift, and we have already verified they will be within 
            // range of the record's allocated size, and here we are zeroing them so zero-init is maintained.
            if (recordInfo.HasETag)
            {
                ETag = *(long*)address;
                *(long*)address = 0;
                address += LogRecord.ETagSize;
            }
            if (recordInfo.HasExpiration)
            {
                Expiration = *(long*)address;
                *(long*)address = 0;
                address += LogRecord.ExpirationSize;
            }

            // For Filler we do need to clear the bit, as we may end up with no filler. We don't preserve the existing value; it will be calculated in Restore().
            if (recordInfo.HasFiller)
            {
                *(int*)address = 0;
                recordInfo.ClearHasFiller();
            }
        }

        internal void Restore(long address, ref RecordInfo recordInfo, int optionalPlusFillerLen)
        {
            // Restore after shift. See comments in GetAndZero for more details.
            if (recordInfo.HasETag)
            {
                *(long*)address = ETag;
                optionalPlusFillerLen -= LogRecord.ETagSize;
                address += LogRecord.ETagSize;
            }
            if (recordInfo.HasExpiration)
            {
                *(long*)address = Expiration;
                optionalPlusFillerLen -= LogRecord.ExpirationSize;
                address += LogRecord.ExpirationSize;
            }
            if (optionalPlusFillerLen >= sizeof(int))
            {
                *(long*)address = optionalPlusFillerLen;
                recordInfo.SetHasFiller();
            }
        }
    }
}
