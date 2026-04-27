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
    /// All helpers are stateless with respect to ETag — they receive <c>existingEtag</c> as a parameter
    /// and write new ETag values directly to the record, without using any shared mutable state.
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<StringInput, StringOutput, long>
    {
        #region InPlaceUpdater dispatcher

        /// <summary>
        /// Single dispatcher for all ETag commands in InPlaceUpdaterWorker.
        /// Reads the existing ETag from the record, delegates to the appropriate helper,
        /// and returns the result directly — no bottom-level ETag update needed.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly IPUResult HandleEtagInPlaceUpdateWorker(RespCommand cmd, ref LogRecord logRecord, ref StringInput input, ref StringOutput output, ref RMWInfo rmwInfo)
        {
            long existingEtag = logRecord.Info.HasETag ? logRecord.ETag : LogRecord.NoETag;

            return cmd switch
            {
                RespCommand.DELIFGREATER => HandleDelIfGreaterInPlaceUpdate(existingEtag, ref input, ref rmwInfo),
                RespCommand.SETIFMATCH or RespCommand.SETIFGREATER => HandleSetIfMatchInPlaceUpdate(cmd, existingEtag, ref logRecord, ref input, ref output),
                RespCommand.SETWITHETAG => HandleSetWithEtagInPlaceUpdate(existingEtag, ref logRecord, ref input, ref output),
                _ => throw new GarnetException("Unexpected ETag command")
            };
        }

        #endregion

        #region InPlaceUpdater helpers

        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly IPUResult HandleDelIfGreaterInPlaceUpdate(long existingEtag, ref StringInput input, ref RMWInfo rmwInfo)
        {
            var etagFromClient = input.parseState.GetLong(0);
            rmwInfo.Action = etagFromClient > existingEtag ? RMWAction.ExpireAndStop : RMWAction.CancelOperation;
            return IPUResult.Failed;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly IPUResult HandleSetIfMatchInPlaceUpdate(RespCommand cmd, long existingEtag, ref LogRecord logRecord, ref StringInput input, ref StringOutput output)
        {
            var etagFromClient = input.parseState.GetLong(1);
            // in IFMATCH we check for equality, in IFGREATER we are checking for sent etag being strictly greater
            var comparisonResult = etagFromClient.CompareTo(existingEtag);
            var expectedResult = cmd is RespCommand.SETIFMATCH ? 0 : 1;

            if (comparisonResult != expectedResult)
            {
                if (input.header.CheckSetGetFlag())
                    CopyRespWithEtagData(logRecord.ValueSpan, ref output, logRecord.Info.HasETag, existingEtag, functionsState.memoryPool);
                else
                {
                    // write back array of the format [etag, nil]
                    var nilResponse = functionsState.nilResp;
                    // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                    WriteValAndEtagToDst(
                        4 + 1 + NumUtils.CountDigits(existingEtag) + 2 + nilResponse.Length,
                        nilResponse,
                        existingEtag,
                        ref output,
                        functionsState.memoryPool,
                        writeDirect: true
                    );
                }
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

            var newEtag = cmd is RespCommand.SETIFMATCH ? (existingEtag + 1) : etagFromClient;
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
            return IPUResult.Succeeded;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly IPUResult HandleSetWithEtagInPlaceUpdate(long existingEtag, ref LogRecord logRecord, ref StringInput input, ref StringOutput output)
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

            var newEtag = existingEtag + 1;
            if (!logRecord.TrySetETag(newEtag))
            {
                Debug.Fail("Should have succeeded in setting ETag");
                return IPUResult.Failed;
            }

            // Set or clear expiration to match SET semantics
            if (input.arg1 != 0)
            {
                if (!logRecord.TrySetExpiration(input.arg1))
                    return IPUResult.Failed;
            }
            else if (logRecord.Info.HasExpiration)
                _ = logRecord.RemoveExpiration();

            // Return the new ETag as integer
            functionsState.CopyRespNumber(newEtag, ref output.SpanByteAndMemory);
            return IPUResult.Succeeded;
        }

        #endregion

        #region NeedCopyUpdate dispatcher

        /// <summary>
        /// Single dispatcher for all ETag commands in NeedCopyUpdate.
        /// Reads the existing ETag from the source record and delegates to the appropriate helper.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly bool HandleEtagNeedCopyUpdate<TSourceLogRecord>(RespCommand cmd, in TSourceLogRecord srcLogRecord, ref StringInput input, ref StringOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            long existingEtag = srcLogRecord.Info.HasETag ? srcLogRecord.ETag : LogRecord.NoETag;

            return cmd switch
            {
                RespCommand.DELIFGREATER => HandleDelIfGreaterNeedCopyUpdate(existingEtag, ref input, ref rmwInfo),
                RespCommand.SETIFMATCH or RespCommand.SETIFGREATER => HandleSetIfMatchNeedCopyUpdate(cmd, existingEtag, in srcLogRecord, ref input, ref output),
                RespCommand.SETWITHETAG => true,
                _ => throw new GarnetException("Unexpected ETag command")
            };
        }

        #endregion

        #region NeedCopyUpdate helpers

        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly bool HandleDelIfGreaterNeedCopyUpdate(long existingEtag, ref StringInput input, ref RMWInfo rmwInfo)
        {
            long etagFromClient = input.parseState.GetLong(0);
            if (etagFromClient > existingEtag)
                rmwInfo.Action = RMWAction.ExpireAndStop;

            // We always return false because we would rather not create a new record in hybrid log if we don't need to delete the object.
            // Setting no Action and returning false for non-delete case will shortcircuit the InternalRMW code to not run CU, and return SUCCESS.
            // If we want to delete the object setting the Action to ExpireAndStop will add the tombstone in hybrid log for us.
            return false;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly bool HandleSetIfMatchNeedCopyUpdate<TSourceLogRecord>(RespCommand cmd, long existingEtag, in TSourceLogRecord srcLogRecord, ref StringInput input, ref StringOutput output)
            where TSourceLogRecord : ISourceLogRecord
        {
            long etagToCheckWith = input.parseState.GetLong(1);

            // in IFMATCH we check for equality, in IFGREATER we are checking for sent etag being strictly greater
            int comparisonResult = etagToCheckWith.CompareTo(existingEtag);
            int expectedResult = cmd is RespCommand.SETIFMATCH ? 0 : 1;

            if (comparisonResult == expectedResult)
                return true;

            if (input.header.CheckSetGetFlag())
            {
                // Copy value to output for the GET part of the command.
                CopyRespWithEtagData(srcLogRecord.ValueSpan, ref output, srcLogRecord.Info.HasETag, existingEtag, functionsState.memoryPool);
            }
            else
            {
                // write back array of the format [etag, nil]
                var nilResponse = functionsState.nilResp;
                // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                WriteValAndEtagToDst(
                    4 + 1 + NumUtils.CountDigits(existingEtag) + 2 + nilResponse.Length,
                    nilResponse,
                    existingEtag,
                    ref output,
                    functionsState.memoryPool,
                    writeDirect: true
                );
            }

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
            var newEtag = input.parseState.GetLong(1) + (cmd == RespCommand.SETIFMATCH ? 1 : 0);
            _ = logRecord.TrySetETag(newEtag);

            // write back array of the format [etag, nil]
            var nilResponse = functionsState.nilResp;
            // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
            WriteValAndEtagToDst(
                4 + 1 + NumUtils.CountDigits(newEtag) + 2 + nilResponse.Length,
                nilResponse,
                newEtag,
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

        #region CopyUpdater dispatcher

        /// <summary>
        /// Single dispatcher for all ETag commands in CopyUpdater.
        /// Reads the existing ETag from the source record and delegates to the appropriate helper.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly bool HandleEtagCopyUpdateWorker<TSourceLogRecord>(RespCommand cmd, in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref StringInput input, ref StringOutput output)
            where TSourceLogRecord : ISourceLogRecord
        {
            long existingEtag = srcLogRecord.Info.HasETag ? srcLogRecord.ETag : LogRecord.NoETag;

            return cmd switch
            {
                RespCommand.SETIFMATCH or RespCommand.SETIFGREATER => HandleSetIfMatchCopyUpdate(cmd, existingEtag, in srcLogRecord, ref dstLogRecord, in sizeInfo, ref input, ref output),
                RespCommand.SETWITHETAG => HandleSetWithEtagCopyUpdate(existingEtag, in srcLogRecord, ref dstLogRecord, in sizeInfo, ref input, ref output),
                _ => throw new GarnetException("Unexpected ETag command")
            };
        }

        #endregion

        #region CopyUpdater helpers

        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly bool HandleSetIfMatchCopyUpdate<TSourceLogRecord>(RespCommand cmd, long existingEtag, in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref StringInput input, ref StringOutput output)
            where TSourceLogRecord : ISourceLogRecord
        {
            var inputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
            if (!dstLogRecord.TrySetValueSpanAndPrepareOptionals(inputValue, in sizeInfo))
                return false;

            // Use the etag sent from client as the base
            var etagFromClient = input.parseState.GetLong(1);
            var newEtag = etagFromClient + (cmd == RespCommand.SETIFMATCH ? 1 : 0);
            if (!dstLogRecord.TrySetETag(newEtag))
                return false;

            if (sizeInfo.FieldInfo.HasExpiration && !dstLogRecord.TrySetExpiration(input.arg1 != 0 ? input.arg1 : srcLogRecord.Expiration))
                return false;

            // Write Etag and Val back to Client as an array of the format [etag, nil]
            // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
            var numDigitsInEtag = NumUtils.CountDigits(newEtag);
            WriteValAndEtagToDst(4 + 1 + numDigitsInEtag + 2 + functionsState.nilResp.Length, functionsState.nilResp, newEtag, ref output, functionsState.memoryPool, writeDirect: true);

            sizeInfo.AssertOptionalsIfSet(dstLogRecord.Info);
            return true;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly bool HandleSetWithEtagCopyUpdate<TSourceLogRecord>(long existingEtag, in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref StringInput input, ref StringOutput output)
            where TSourceLogRecord : ISourceLogRecord
        {
            var inputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
            if (!dstLogRecord.TrySetValueSpanAndPrepareOptionals(inputValue, in sizeInfo))
                return false;

            // Increment existing ETag or set to 1 for non-ETag keys
            var newEtag = existingEtag + 1;
            if (!dstLogRecord.TrySetETag(newEtag))
                return false;

            // Set or clear expiration to match SET semantics
            if (sizeInfo.FieldInfo.HasExpiration && !dstLogRecord.TrySetExpiration(input.arg1))
                return false;

            // Return the new ETag as integer
            functionsState.CopyRespNumber(newEtag, ref output.SpanByteAndMemory);

            sizeInfo.AssertOptionalsIfSet(dstLogRecord.Info);
            return true;
        }

        #endregion
    }
}