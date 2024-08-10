// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        /// <summary>
        /// if reset is true it simply resets the modified bit for the key
        /// if reset is false it only checks whether the key is modified or not
        /// </summary>
        /// <param name="key">key of the record.</param>
        /// <param name="modifiedInfo">RecordInfo of the key for checkModified.</param>
        /// <param name="reset">Operation Type, whether it is reset or check</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalModifiedBitOperation(ref TKey key, out RecordInfo modifiedInfo, bool reset = true)
        {
            Debug.Assert(kernel.epoch.ThisInstanceProtected());

            HashEntryInfo hei = new(storeFunctions.GetKeyHashCode64(ref key)); ;

            #region Trace back for record in in-memory HybridLog
            _ = FindTag(ref hei);
            var logicalAddress = hei.Address;
            var physicalAddress = hlog.GetPhysicalAddress(logicalAddress);

            if (logicalAddress >= hlogBase.HeadAddress)
            {
                ref RecordInfo recordInfo = ref hlog.GetInfo(physicalAddress);
                if (recordInfo.Invalid || !storeFunctions.KeysEqual(ref key, ref hlog.GetKey(physicalAddress)))
                {
                    logicalAddress = recordInfo.PreviousAddress;
                    TraceBackForKeyMatch(ref key, logicalAddress, hlogBase.HeadAddress, out logicalAddress, out physicalAddress);
                }
            }
            #endregion

            modifiedInfo = default;
            if (logicalAddress >= hlogBase.HeadAddress)
            {
                ref RecordInfo recordInfo = ref hlog.GetInfo(physicalAddress);
                if (reset)
                {
                    if (!recordInfo.TryResetModifiedAtomic())
                        return OperationStatus.RETRY_LATER;
                }
                else if (!recordInfo.Tombstone)
                    modifiedInfo = recordInfo;
                return OperationStatus.SUCCESS;
            }

            // If the record does not exist we return unmodified; if it is on the disk we return modified
            modifiedInfo.Modified = logicalAddress >= hlogBase.BeginAddress;

            // It is not in memory so we return success
            return OperationStatus.SUCCESS;
        }
    }
}