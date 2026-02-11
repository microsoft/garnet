// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;
using static Garnet.server.SessionFunctionsUtils;

namespace Garnet.server
{
    /// <summary>
    /// Unified store functions
    /// </summary>
    public readonly unsafe partial struct UnifiedSessionFunctions : ISessionFunctions<UnifiedInput, UnifiedOutput, long>
    {
        /// <inheritdoc />
        public bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref UnifiedInput input, ref UnifiedOutput output,
            ref RMWInfo rmwInfo)
        {
            return input.header.cmd switch
            {
                RespCommand.DEL or
                RespCommand.DELIFEXPIM or
                RespCommand.PERSIST or
                RespCommand.EXPIRE => false,
                _ => true
            };
        }

        /// <inheritdoc />
        public bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedInput input,
            ref UnifiedOutput output, ref RMWInfo rmwInfo)
        {
            Debug.Assert(logRecord.Info.ValueIsObject || (!logRecord.Info.HasETag && !logRecord.Info.HasExpiration),
                "Should not have Expiration or ETag on InitialUpdater log records");

            var updatedEtag = EtagUtils.GetUpdatedEtag(LogRecord.NoETag, ref input.metaCommandInfo, out _, init: true);

            var result =  input.header.cmd switch
            {
                RespCommand.DELIFEXPIM or
                RespCommand.PERSIST or
                RespCommand.EXPIRE => throw new Exception(),
                _ => true
            };

            // the increment on initial etag is for satisfying the variant that any key with no etag is the same as a zero'd etag
            if (sizeInfo.FieldInfo.HasETag && !logRecord.TrySetETag(updatedEtag))
            {
                functionsState.logger?.LogError("Could not set etag in {methodName}", "InitialUpdater");
                return false;
            }
            ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in logRecord);
            
            return result;
        }

        /// <inheritdoc />
        public void PostInitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedInput input,
            ref UnifiedOutput output, ref RMWInfo rmwInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                input.header.SetExpiredFlag();
                WriteLogRMW(logRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);
            }

            if (logRecord.Info.ValueIsObject)
                functionsState.objectStoreSizeTracker?.AddTrackedSize(logRecord.ValueObject.HeapMemorySize);
        }

        /// <inheritdoc />
        public bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref UnifiedInput input,
            ref UnifiedOutput output, ref RMWInfo rmwInfo) where TSourceLogRecord : ISourceLogRecord
        {
            var cmd = input.header.cmd;
            if (cmd == RespCommand.DELIFEXPIM && srcLogRecord.Info.HasExpiration && input.header.CheckExpiry(srcLogRecord.Expiration))
            {
                rmwInfo.Action = RMWAction.ExpireAndStop;
                return false;
            }

            _ = EtagUtils.GetUpdatedEtag(srcLogRecord.ETag, ref input.metaCommandInfo, out var execCmd);

            switch (input.header.cmd)
            {
                case RespCommand.DEL:
                    if (execCmd)
                        rmwInfo.Action = RMWAction.ExpireAndStop;

                    output.Header.etag = LogRecord.NoETag;
                    ETagState.ResetState(ref functionsState.etagState);
                    // We always return false because we would rather not create a new record in hybrid log if we don't need to delete the object.
                    // Setting no Action and returning false for non-delete case will shortcircuit the InternalRMW code to not run CU, and return SUCCESS.
                    // If we want to delete the object setting the Action to ExpireAndStop will add the tombstone in hybrid log for us.
                    return false;
            }

            return true;
        }

        /// <inheritdoc />
        public bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord,
            in RecordSizeInfo sizeInfo, ref UnifiedInput input, ref UnifiedOutput output,
            ref RMWInfo rmwInfo) where TSourceLogRecord : ISourceLogRecord
        {

            if (srcLogRecord.Info.HasExpiration && input.header.CheckExpiry(srcLogRecord.Expiration))
            {
                if (!srcLogRecord.Info.ValueIsObject)
                {
                    _ = dstLogRecord.RemoveETag();
                    // reset etag state that may have been initialized earlier
                    ETagState.ResetState(ref functionsState.etagState);
                }

                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }

            if (srcLogRecord.Info.ValueIsObject)
            {
                // Defer the actual copying of data to PostCopyUpdater, so we know the record has been successfully CASed into the hash chain before we potentially
                // create large allocations (e.g. if srcLogRecord is from disk, we would have to allocate the overflow byte[]). Because we are doing an update we have
                // and XLock, so nobody will see the unset data even after the CAS. Tsavorite will handle cloning the ValueObject and caching serialized data as needed,
                // based on whether srcLogRecord is in-memory or a DiskLogRecord.
                return true;
            }

            var recordHadEtagPreMutation = srcLogRecord.Info.HasETag;
            var shouldUpdateEtag = recordHadEtagPreMutation;
            if (shouldUpdateEtag)
            {
                // during checkpointing we might skip the inplace calls and go directly to copy update so we need to initialize here if needed
                ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in srcLogRecord);
            }

            var cmd = input.header.cmd;
            var updatedEtag = EtagUtils.GetUpdatedEtag(srcLogRecord.ETag, ref input.metaCommandInfo, out var execCmd);
            Debug.Assert(execCmd);

            var result = cmd switch
            {
                RespCommand.EXPIRE => HandleExpireCopyUpdate(srcLogRecord, ref dstLogRecord, in sizeInfo, ref shouldUpdateEtag, ref input, ref output),
                RespCommand.PERSIST => HandlePersistCopyUpdate(srcLogRecord, ref dstLogRecord, in sizeInfo, ref shouldUpdateEtag, ref output),
                _ => throw new NotImplementedException()
            };

            if (!result)
                return false;

            updatedEtag = shouldUpdateEtag ? updatedEtag : functionsState.etagState.ETag;
            if (recordHadEtagPreMutation || shouldUpdateEtag)
            {
                dstLogRecord.TrySetETag(updatedEtag);
                output.Header.etag = functionsState.etagState.ETag;
                ETagState.ResetState(ref functionsState.etagState);
            }

            sizeInfo.AssertOptionals(dstLogRecord.Info);
            return true;
        }

        /// <inheritdoc />
        public bool PostCopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord,
            in RecordSizeInfo sizeInfo, ref UnifiedInput input, ref UnifiedOutput output,
            ref RMWInfo rmwInfo) where TSourceLogRecord : ISourceLogRecord
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);

            if (srcLogRecord.Info.ValueIsObject)
            {
                var recordHadEtagPreMutation = srcLogRecord.Info.HasETag;
                var shouldUpdateEtag = recordHadEtagPreMutation;
                if (shouldUpdateEtag)
                {
                    // during checkpointing we might skip the inplace calls and go directly to copy update so we need to initialize here if needed
                    ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in srcLogRecord);
                }

                // We're performing the object update here (and not in CopyUpdater) so that we are guaranteed that
                // the record was CASed into the hash chain before it gets modified
                var value = Unsafe.As<IGarnetObject>(srcLogRecord.ValueObject.Clone());
                var oldValueSize = srcLogRecord.ValueObject.HeapMemorySize;

                // First copy the new Value and optionals to the new record. This will also ensure space and set the flag for expiration if it's present.
                // Do not set actually set dstLogRecord.Expiration until we know it is a command for which we allocated length in the LogRecord for it.
                var hasExpiration = dstLogRecord.Info.HasExpiration;
                if (!dstLogRecord.TrySetValueObjectAndPrepareOptionals(value, in sizeInfo))
                    return false;

                var updatedEtag = EtagUtils.GetUpdatedEtag(srcLogRecord.ETag, ref input.metaCommandInfo, out var execCmd);
                Debug.Assert(execCmd);

                var cmd = input.header.cmd;
                switch (cmd)
                {
                    case RespCommand.EXPIRE:
                        if (HandleExpireInPlaceUpdate(ref dstLogRecord, hasExpiration, ref shouldUpdateEtag, ref input, ref output) == IPUResult.Failed)
                            return false;
                        break;

                    case RespCommand.PERSIST:
                        HandlePersistInPlaceUpdate(ref dstLogRecord, hasExpiration, ref shouldUpdateEtag, ref output);
                        break;
                }

                updatedEtag = shouldUpdateEtag ? updatedEtag : functionsState.etagState.ETag;
                if (recordHadEtagPreMutation || shouldUpdateEtag)
                {
                    dstLogRecord.TrySetETag(updatedEtag);
                    output.Header.etag = functionsState.etagState.ETag;
                    ETagState.ResetState(ref functionsState.etagState);
                }

                sizeInfo.AssertOptionals(dstLogRecord.Info);

                // If oldValue has been set to null, subtract its size from the tracked heap size
                var sizeAdjustment = rmwInfo.ClearSourceValueObject ? value.HeapMemorySize - oldValueSize : value.HeapMemorySize;
                functionsState.objectStoreSizeTracker?.AddTrackedSize(sizeAdjustment);
            }

            if (functionsState.appendOnlyFile != null)
                WriteLogRMW(dstLogRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);

            return true;
        }

        /// <inheritdoc />
        public bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedInput input,
            ref UnifiedOutput output, ref RMWInfo rmwInfo)
        {
            var ipuResult = InPlaceUpdaterWorker(ref logRecord, in sizeInfo, ref input, ref output, ref rmwInfo, out var sizeChange);
            switch (ipuResult)
            {
                case IPUResult.Failed:
                    return false;
                case IPUResult.Succeeded:
                    if (!logRecord.Info.Modified)
                        functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
                    if (functionsState.appendOnlyFile != null)
                        WriteLogRMW(logRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);
                    if (logRecord.Info.ValueIsObject)
                        functionsState.objectStoreSizeTracker?.AddTrackedSize(sizeChange);
                    return true;
                case IPUResult.NotUpdated:
                default:
                    return true;
            }
        }

        IPUResult InPlaceUpdaterWorker(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedInput input, ref UnifiedOutput output, ref RMWInfo rmwInfo, out long sizeChange)
        {
            sizeChange = 0;
            var cmd = input.header.cmd;

            // Expired data
            if (logRecord.Info.HasExpiration && input.header.CheckExpiry(logRecord.Expiration))
            {
                if (logRecord.Info.ValueIsObject)
                {
                    functionsState.objectStoreSizeTracker?.AddTrackedSize(-logRecord.ValueObject.HeapMemorySize);

                    // Can't access 'this' in a lambda so dispose directly and pass a no-op lambda.
                    functionsState.storeFunctions.DisposeValueObject(logRecord.ValueObject, DisposeReason.Deleted);
                    logRecord.ClearValueIfHeap(_ => { });
                }
                else
                    logRecord.RemoveETag();

                rmwInfo.Action = cmd == RespCommand.DELIFEXPIM ? RMWAction.ExpireAndStop : RMWAction.ExpireAndResume;
                return IPUResult.Failed;
            }

            var hadETagPreMutation = logRecord.Info.HasETag;
            var shouldUpdateEtag = hadETagPreMutation;
            if (shouldUpdateEtag)
                ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in logRecord);

            var hasExpiration = logRecord.Info.HasExpiration;

            var updatedEtag = EtagUtils.GetUpdatedEtag(logRecord.ETag, ref input.metaCommandInfo, out var execCmd);

            if (!execCmd)
            {
                output.Header.etag = functionsState.etagState.ETag;
                rmwInfo.Action = RMWAction.CancelOperation;
                if (hadETagPreMutation)
                {
                    // reset etag state that may have been initialized earlier
                    ETagState.ResetState(ref functionsState.etagState);
                }

                if (!input.header.CheckSkipRespOutputFlag())
                {
                    using var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
                    writer.WriteNull();
                }

                return IPUResult.Failed;
            }

            var ipuResult = IPUResult.Succeeded;
            switch (cmd)
            {
                case RespCommand.DEL:
                    rmwInfo.Action = execCmd ? RMWAction.ExpireAndStop : RMWAction.CancelOperation;
                    ETagState.ResetState(ref functionsState.etagState);
                    return IPUResult.Failed;
                case RespCommand.EXPIRE:
                    ipuResult = HandleExpireInPlaceUpdate(ref logRecord, hasExpiration, ref shouldUpdateEtag, ref input, ref output);
                    if (ipuResult == IPUResult.Failed)
                        return IPUResult.Failed;
                    break;
                case RespCommand.PERSIST:
                    HandlePersistInPlaceUpdate(ref logRecord, hasExpiration, ref shouldUpdateEtag, ref output);
                    break;
                case RespCommand.DELIFEXPIM:
                    if (!logRecord.Info.ValueIsObject)
                    {
                        // this is the case where it isn't expired
                        shouldUpdateEtag = false;
                    }
                    break;
                default:
                    throw new NotImplementedException();
            }

            // increment the Etag transparently if in place update happened
            if (shouldUpdateEtag)
            {
                logRecord.TrySetETag(updatedEtag);
                output.Header.etag = functionsState.etagState.ETag;
                ETagState.ResetState(ref functionsState.etagState);
            }
            else if (hadETagPreMutation)
            {
                output.Header.etag = functionsState.etagState.ETag;
                // reset etag state that may have been initialized earlier
                ETagState.ResetState(ref functionsState.etagState);
            }

            sizeInfo.AssertOptionals(logRecord.Info);
            return ipuResult;
        }

        private bool HandleExpireCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord,
            in RecordSizeInfo sizeInfo, ref bool shouldUpdateEtag, ref UnifiedInput input, ref UnifiedOutput output) where TSourceLogRecord : ISourceLogRecord
        {
            shouldUpdateEtag = false;
            var expirationWithOption = new ExpirationWithOption(input.arg1);

            // First copy the old Value and non-Expiration optionals to the new record. This will also ensure space for expiration.
            if (!dstLogRecord.TryCopyFrom(in srcLogRecord, in sizeInfo))
                return false;

            return EvaluateExpireCopyUpdate(ref dstLogRecord, in sizeInfo, expirationWithOption.ExpireOption,
                expirationWithOption.ExpirationTimeInTicks, dstLogRecord.ValueSpan, ref output);
        }

        private IPUResult HandleExpireInPlaceUpdate(ref LogRecord logRecord, bool hasExpiration, ref bool shouldUpdateEtag, ref UnifiedInput input, ref UnifiedOutput output)
        {
            shouldUpdateEtag = false;
            var expirationWithOption = new ExpirationWithOption(input.arg1);

            return EvaluateExpireInPlace(ref logRecord, expirationWithOption.ExpireOption, expirationWithOption.ExpirationTimeInTicks, hasExpiration, ref output);
        }

        private bool HandlePersistCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord,
            in RecordSizeInfo sizeInfo, ref bool shouldUpdateEtag, ref UnifiedOutput output) where TSourceLogRecord : ISourceLogRecord
        {
            shouldUpdateEtag = false;
            if (!dstLogRecord.TryCopyFrom(in srcLogRecord, in sizeInfo))
                return false;

            if (srcLogRecord.Info.HasExpiration)
            {
                dstLogRecord.RemoveExpiration();
                functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output.SpanByteAndMemory);
            }
            else
                functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output.SpanByteAndMemory);

            return true;
        }

        private void HandlePersistInPlaceUpdate(ref LogRecord logRecord, bool hasExpiration, ref bool shouldUpdateEtag, ref UnifiedOutput output)
        {
            shouldUpdateEtag = false;

            if (hasExpiration)
            {
                logRecord.RemoveExpiration();
                functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output.SpanByteAndMemory);
            }
            else
                functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output.SpanByteAndMemory);
        }
    }
}