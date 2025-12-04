// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>
        /// if reset is true it simply resets the modified bit for the key
        /// if reset is false it only checks whether the key is modified or not
        /// </summary>
        /// <param name="key">key of the record.</param>
        /// <param name="modifiedInfo">RecordInfo of the key for checkModified.</param>
        /// <param name="reset">Operation Type, whether it is reset or check</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalModifiedBitOperation(ReadOnlySpan<byte> key, out RecordInfo modifiedInfo, bool reset = true)
        {
            Debug.Assert(epoch.ThisInstanceProtected());

            HashEntryInfo hei = new(storeFunctions.GetKeyHashCode64(key)); ;

            #region Trace back for record in in-memory HybridLog
            _ = FindTag(ref hei);
            var logicalAddress = hei.Address;

            if (logicalAddress >= hlogBase.HeadAddress)
            {
                var logRecord = hlog.CreateLogRecord(logicalAddress);
                if (logRecord.Info.Invalid || !storeFunctions.KeysEqual(key, logRecord.Key))
                {
                    logicalAddress = logRecord.Info.PreviousAddress;
                    TraceBackForKeyMatch(key, logicalAddress, hlogBase.HeadAddress, out logicalAddress, out _);
                }
            }
            #endregion

            modifiedInfo = default;
            if (logicalAddress >= hlogBase.HeadAddress)
            {
                ref var recordInfo = ref LogRecord.GetInfoRef(hlogBase.GetPhysicalAddress(logicalAddress));
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