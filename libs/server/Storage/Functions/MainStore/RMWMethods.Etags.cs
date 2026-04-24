// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;
using static Garnet.server.SessionFunctionsUtils;

namespace Garnet.server
{
    /// <summary>
    /// ETag-specific RMW callback methods for main store, kept in a separate file
    /// with NoInlining to minimize hot-path method footprint.
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<StringInput, StringOutput, long>
    {
        #region InPlaceUpdater helpers

        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly IPUResult HandleDelIfGreaterInPlaceUpdate(ref StringInput input, ref RMWInfo rmwInfo)
        {
            var etagFromClient = input.parseState.GetLong(0);
            rmwInfo.Action = etagFromClient > functionsState.etagState.ETag ? RMWAction.ExpireAndStop : RMWAction.CancelOperation;
            ETagState.ResetState(ref functionsState.etagState);
            return IPUResult.Failed;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly IPUResult HandleSetIfMatchInPlaceUpdate(RespCommand cmd, ref LogRecord logRecord, ref StringInput input, ref StringOutput output, ref RMWInfo rmwInfo, ref bool shouldUpdateEtag)
        {
            var etagFromClient = input.parseState.GetLong(1);
            // in IFMATCH we check for equality, in IFGREATER we are checking for sent etag being strictly greater
            var comparisonResult = etagFromClient.CompareTo(functionsState.etagState.ETag);
            var expectedResult = cmd is RespCommand.SETIFMATCH ? 0 : 1;

            if (comparisonResult != expectedResult)
            {
                if (input.header.CheckSetGetFlag())
                    CopyRespWithEtagData(logRecord.ValueSpan, ref output, shouldUpdateEtag, functionsState.memoryPool);
                else
                {
                    // write back array of the format [etag, nil]
                    var nilResponse = functionsState.nilResp;
                    // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                    WriteValAndEtagToDst(
                        4 + 1 + NumUtils.CountDigits(functionsState.etagState.ETag) + 2 + nilResponse.Length,
                        nilResponse,
                        functionsState.etagState.ETag,
                        ref output,
                        functionsState.memoryPool,
                        writeDirect: true
                    );
                }
                // reset etag state after done using
                ETagState.ResetState(ref functionsState.etagState);
                return IPUResult.NotUpdated;
            }

            // If we're here we know we have a valid ETag for update. Get the value to update. We'll need to return false for CopyUpdate if no space for new value.
            var inputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
            if (logRecord.Info.ValueIsInline)
            {
                // We are going to set ETag and possibly Expiration--but we won't remove either. Precheck adequate length before making any changes.
                if (!logRecord.CanGrowPinnedValue(inputValue.Length, newETagLen: LogRecord.ETagSize,
                        newExpirationLen: input.arg1 != 0 ? LogRecord.ExpirationSize : logRecord.ExpirationLen, out var valueAddress, out var valueLength))
                    return IPUResult.Failed;
                if (!logRecord.TrySetPinnedValueSpan(inputValue, valueAddress, ref valueLength))
                {
                    Debug.Fail("Should have succeeded in growing the value as we have ensured there was space there already");
                    return IPUResult.Failed;
                }
            }
            else
            {
                // Create local sizeInfo
                var sizeInfo = new RecordSizeInfo() { FieldInfo = GetRMWModifiedFieldInfo(logRecord, ref input) };
                functionsState.storeWrapper.store.Log.PopulateRecordSizeInfo(ref sizeInfo);
                if (!logRecord.TrySetValueSpanAndPrepareOptionals(inputValue, in sizeInfo))
                    return IPUResult.Failed;
            }

            var newEtag = cmd is RespCommand.SETIFMATCH ? (functionsState.etagState.ETag + 1) : etagFromClient;
            if (!logRecord.TrySetETag(newEtag))
            {
                Debug.Fail("Should have succeeded in setting ETag as we should have ensured there was space there already");
                return IPUResult.Failed;
            }

            // Need to check for input.arg1 != 0 because GetRMWModifiedFieldInfo shares its logic with CopyUpdater and thus may set sizeInfo.FieldInfo.Expiration true
            // due to srcRecordInfo having expiration set; here, that srcRecordInfo is us, so we should do nothing if input.arg1 == 0.
            if (input.arg1 != 0 && !logRecord.TrySetExpiration(input.arg1))
                return IPUResult.Failed;

            // Write Etag and Val back to Client as an array of the format [etag, nil]
            var nilResp = functionsState.nilResp;
            // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
            var numDigitsInEtag = NumUtils.CountDigits(newEtag);
            WriteValAndEtagToDst(4 + 1 + numDigitsInEtag + 2 + nilResp.Length, nilResp, newEtag, ref output, functionsState.memoryPool, writeDirect: true);
            // reset etag state after done using
            ETagState.ResetState(ref functionsState.etagState);
            shouldUpdateEtag = false;   // since we already updated the ETag
            return IPUResult.Succeeded;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly IPUResult HandleSetWithEtagInPlaceUpdate(ref LogRecord logRecord, ref StringInput input, ref StringOutput output, ref RMWInfo rmwInfo, ref bool shouldUpdateEtag)
        {
            // Update value and increment ETag
            var inputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
            if (logRecord.Info.ValueIsInline)
            {
                if (!logRecord.CanGrowPinnedValue(inputValue.Length, newETagLen: LogRecord.ETagSize,
                        newExpirationLen: input.arg1 != 0 ? LogRecord.ExpirationSize : logRecord.ExpirationLen, out var valueAddress, out var valueLength))
                    return IPUResult.Failed;
                if (!logRecord.TrySetPinnedValueSpan(inputValue, valueAddress, ref valueLength))
                {
                    Debug.Fail("Should have succeeded in growing the value as we have ensured there was space there already");
                    return IPUResult.Failed;
                }
            }
            else
            {
                var sizeInfo = new RecordSizeInfo() { FieldInfo = GetRMWModifiedFieldInfo(logRecord, ref input) };
                functionsState.storeWrapper.store.Log.PopulateRecordSizeInfo(ref sizeInfo);
                if (!logRecord.TrySetValueSpanAndPrepareOptionals(inputValue, in sizeInfo))
                    return IPUResult.Failed;
            }

            var newEtag = functionsState.etagState.ETag + 1;
            if (!logRecord.TrySetETag(newEtag))
            {
                Debug.Fail("Should have succeeded in setting ETag");
                return IPUResult.Failed;
            }

            if (input.arg1 != 0 && !logRecord.TrySetExpiration(input.arg1))
                return IPUResult.Failed;

            // Return the new ETag as integer
            functionsState.CopyRespNumber(newEtag, ref output.SpanByteAndMemory);
            ETagState.ResetState(ref functionsState.etagState);
            shouldUpdateEtag = false;
            return IPUResult.Succeeded;
        }

        #endregion

        #region NeedCopyUpdate helpers

        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly bool HandleDelIfGreaterNeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref StringInput input, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (srcLogRecord.Info.HasETag)
                ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in srcLogRecord);
            long etagFromClient = input.parseState.GetLong(0);
            if (etagFromClient > functionsState.etagState.ETag)
                rmwInfo.Action = RMWAction.ExpireAndStop;

            ETagState.ResetState(ref functionsState.etagState);
            // We always return false because we would rather not create a new record in hybrid log if we don't need to delete the object.
            // Setting no Action and returning false for non-delete case will shortcircuit the InternalRMW code to not run CU, and return SUCCESS.
            // If we want to delete the object setting the Action to ExpireAndStop will add the tombstone in hybrid log for us.
            return false;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly bool HandleSetIfMatchNeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref StringInput input, ref StringOutput output)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (srcLogRecord.Info.HasETag)
                ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in srcLogRecord);

            long etagToCheckWith = input.parseState.GetLong(1);

            // in IFMATCH we check for equality, in IFGREATER we are checking for sent etag being strictly greater
            int comparisonResult = etagToCheckWith.CompareTo(functionsState.etagState.ETag);
            int expectedResult = input.header.cmd is RespCommand.SETIFMATCH ? 0 : 1;

            if (comparisonResult == expectedResult)
                return true;

            if (input.header.CheckSetGetFlag())
            {
                // Copy value to output for the GET part of the command.
                CopyRespWithEtagData(srcLogRecord.ValueSpan, ref output, srcLogRecord.Info.HasETag, functionsState.memoryPool);
            }
            else
            {
                // write back array of the format [etag, nil]
                var nilResponse = functionsState.nilResp;
                // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                WriteValAndEtagToDst(
                    4 + 1 + NumUtils.CountDigits(functionsState.etagState.ETag) + 2 + nilResponse.Length,
                    nilResponse,
                    functionsState.etagState.ETag,
                    ref output,
                    functionsState.memoryPool,
                    writeDirect: true
                );
            }

            ETagState.ResetState(ref functionsState.etagState);
            return false;
        }

        #endregion

        #region InitialUpdater helpers

        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly bool HandleSetIfMatchInitialUpdate(RespCommand cmd, ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref StringInput input, ref StringOutput output)
        {
            // Copy input to value
            var newInputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
            if (!logRecord.TrySetValueSpanAndPrepareOptionals(newInputValue, in sizeInfo))
                return false;
            if (sizeInfo.FieldInfo.HasExpiration)
                _ = logRecord.TrySetExpiration(input.arg1);

            // the increment on initial etag is for satisfying the variant that any key with no etag is the same as a zero'd etag
            Debug.Assert(sizeInfo.FieldInfo.HasETag, "Expected sizeInfo.FieldInfo.HasETag to be true");
            _ = logRecord.TrySetETag(input.parseState.GetLong(1) + (cmd == RespCommand.SETIFMATCH ? 1 : 0));
            ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in logRecord);

            // write back array of the format [etag, nil]
            var nilResponse = functionsState.nilResp;
            // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
            WriteValAndEtagToDst(
                4 + 1 + NumUtils.CountDigits(functionsState.etagState.ETag) + 2 + nilResponse.Length,
                nilResponse,
                functionsState.etagState.ETag,
                ref output,
                functionsState.memoryPool,
                writeDirect: true
            );

            sizeInfo.AssertOptionalsIfSet(logRecord.Info);
            return true;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly bool HandleSetWithEtagInitialUpdate(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref StringInput input, ref StringOutput output)
        {
            // Copy input to value
            var newInputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
            if (!logRecord.TrySetValueSpanAndPrepareOptionals(newInputValue, in sizeInfo))
                return false;

            Debug.Assert(sizeInfo.FieldInfo.HasETag, "Expected sizeInfo.FieldInfo.HasETag to be true");
            _ = logRecord.TrySetETag(LogRecord.NoETag + 1);
            ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in logRecord);

            // Set expiration if provided
            if (sizeInfo.FieldInfo.HasExpiration && !logRecord.TrySetExpiration(input.arg1))
            {
                functionsState.logger?.LogError("Could not set expiration in {methodName}.{caseName}", "InitialUpdater", "SETWITHETAG");
                return false;
            }

            // Return the initial ETag
            functionsState.CopyRespNumber(LogRecord.NoETag + 1, ref output.SpanByteAndMemory);

            sizeInfo.AssertOptionalsIfSet(logRecord.Info);
            return true;
        }

        #endregion

        #region CopyUpdater helpers

        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly bool HandleSetIfMatchCopyUpdate<TSourceLogRecord>(RespCommand cmd, in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref StringInput input, ref StringOutput output, ref bool shouldUpdateEtag)
            where TSourceLogRecord : ISourceLogRecord
        {
            // By now the comparison for etag against existing etag has already been done in NeedCopyUpdate
            shouldUpdateEtag = true;

            var inputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
            if (!dstLogRecord.TrySetValueSpanAndPrepareOptionals(inputValue, in sizeInfo))
                return false;

            // change the current etag to the the etag sent from client since rest remains same
            functionsState.etagState.ETag = input.parseState.GetLong(1);
            if (!dstLogRecord.TrySetETag(functionsState.etagState.ETag + (cmd == RespCommand.SETIFMATCH ? 1 : 0)))
                return false;

            if (sizeInfo.FieldInfo.HasExpiration && !dstLogRecord.TrySetExpiration(input.arg1 != 0 ? input.arg1 : srcLogRecord.Expiration))
                return false;

            // Write Etag and Val back to Client as an array of the format [etag, nil]
            long eTagForResponse = cmd == RespCommand.SETIFMATCH ? functionsState.etagState.ETag + 1 : functionsState.etagState.ETag;
            // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
            var numDigitsInEtag = NumUtils.CountDigits(eTagForResponse);
            WriteValAndEtagToDst(4 + 1 + numDigitsInEtag + 2 + functionsState.nilResp.Length, functionsState.nilResp, eTagForResponse, ref output, functionsState.memoryPool, writeDirect: true);
            shouldUpdateEtag = false;   // since we already updated the ETag

            sizeInfo.AssertOptionalsIfSet(dstLogRecord.Info);
            return true;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly bool HandleSetWithEtagCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref StringInput input, ref StringOutput output, ref bool shouldUpdateEtag)
            where TSourceLogRecord : ISourceLogRecord
        {
            var inputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
            if (!dstLogRecord.TrySetValueSpanAndPrepareOptionals(inputValue, in sizeInfo))
                return false;

            // Increment existing ETag or set to 1 for non-ETag keys
            var setWithEtagNewETag = functionsState.etagState.ETag + 1;
            if (!dstLogRecord.TrySetETag(setWithEtagNewETag))
                return false;

            if (sizeInfo.FieldInfo.HasExpiration && !dstLogRecord.TrySetExpiration(input.arg1 != 0 ? input.arg1 : srcLogRecord.Expiration))
                return false;

            // Return the new ETag as integer
            functionsState.CopyRespNumber(setWithEtagNewETag, ref output.SpanByteAndMemory);
            shouldUpdateEtag = false;

            sizeInfo.AssertOptionalsIfSet(dstLogRecord.Info);
            return true;
        }

        #endregion
    }
}
