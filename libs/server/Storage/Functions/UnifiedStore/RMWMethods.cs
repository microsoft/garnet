// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Tsavorite.core;
using static Garnet.server.SessionFunctionsUtils;

namespace Garnet.server
{
    /// <summary>
    /// Unified store functions
    /// </summary>
    public readonly partial struct UnifiedSessionFunctions : ISessionFunctions<UnifiedInput, UnifiedOutput, long>
    {
        /// <inheritdoc />
        public bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref UnifiedInput input, ref UnifiedOutput output,
            ref RMWInfo rmwInfo)
        {
            return input.header.cmd switch
            {
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

            return input.header.cmd switch
            {
                RespCommand.DELIFEXPIM or
                RespCommand.PERSIST or
                RespCommand.EXPIRE => throw new Exception(),
                _ => true
            };
        }

        /// <inheritdoc />
        public void PostInitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedInput input,
            ref UnifiedOutput output, ref RMWInfo rmwInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                input.header.SetExpiredFlag();
                rmwInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
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

            var result = cmd switch
            {
                RespCommand.EXPIRE => HandleExpireCopyUpdate(srcLogRecord, ref dstLogRecord, in sizeInfo, ref shouldUpdateEtag, ref input, ref output),
                RespCommand.PERSIST => HandlePersistCopyUpdate(srcLogRecord, ref dstLogRecord, in sizeInfo, ref shouldUpdateEtag, ref output),
                _ => throw new NotImplementedException()
            };

            if (!result)
                return false;

            if (shouldUpdateEtag)
            {
                dstLogRecord.TrySetETag(functionsState.etagState.ETag + 1);
                ETagState.ResetState(ref functionsState.etagState);
            }
            else if (recordHadEtagPreMutation)
            {
                // reset etag state that may have been initialized earlier
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

            var shouldUpdateEtag = false;
            if (srcLogRecord.Info.ValueIsObject)
            {
                // We're performing the object update here (and not in CopyUpdater) so that we are guaranteed that
                // the record was CASed into the hash chain before it gets modified
                var value = Unsafe.As<IGarnetObject>(srcLogRecord.ValueObject.Clone());
                var oldValueSize = srcLogRecord.ValueObject.HeapMemorySize;

                // First copy the new Value and optionals to the new record. This will also ensure space and set the flag for expiration if it's present.
                // Do not set actually set dstLogRecord.Expiration until we know it is a command for which we allocated length in the LogRecord for it.
                var hasExpiration = dstLogRecord.Info.HasExpiration;
                if (!dstLogRecord.TrySetValueObjectAndPrepareOptionals(value, in sizeInfo))
                    return false;

                var cmd = input.header.cmd;
                switch (cmd)
                {
                    case RespCommand.EXPIRE:
                        if (HandleExpireInPlaceUpdate(ref dstLogRecord, hasExpiration, ref input, ref output) == IPUResult.Failed)
                            return false;
                        break;

                    case RespCommand.PERSIST:
                        HandlePersistInPlaceUpdate(ref dstLogRecord, hasExpiration, ref shouldUpdateEtag, ref output);
                        break;
                }

                sizeInfo.AssertOptionals(dstLogRecord.Info);

                // If oldValue has been set to null, subtract its size from the tracked heap size
                var sizeAdjustment = rmwInfo.ClearSourceValueObject ? value.HeapMemorySize - oldValueSize : value.HeapMemorySize;
                functionsState.objectStoreSizeTracker?.AddTrackedSize(sizeAdjustment);
            }

            if (functionsState.appendOnlyFile != null)
                rmwInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF

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
                        rmwInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
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

            var ipuResult = IPUResult.Succeeded;
            switch (cmd)
            {
                case RespCommand.EXPIRE:
                    ipuResult = HandleExpireInPlaceUpdate(ref logRecord, hasExpiration, ref input, ref output);
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

            if (!logRecord.Info.ValueIsObject)
            {
                // increment the Etag transparently if in place update happened
                if (shouldUpdateEtag)
                {
                    logRecord.TrySetETag(this.functionsState.etagState.ETag + 1);
                    ETagState.ResetState(ref functionsState.etagState);
                }
                else if (hadETagPreMutation)
                {
                    // reset etag state that may have been initialized earlier
                    ETagState.ResetState(ref functionsState.etagState);
                }
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

        private IPUResult HandleExpireInPlaceUpdate(ref LogRecord logRecord, bool hasExpiration, ref UnifiedInput input, ref UnifiedOutput output)
        {
            var expirationWithOption = new ExpirationWithOption(input.arg1);

            if (!logRecord.Info.ValueIsObject)   // TODO ETag for unified store
            {
                // reset etag state that may have been initialized earlier, but don't update etag because only the expiration was updated
                ETagState.ResetState(ref functionsState.etagState);
            }

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
            if (hasExpiration)
            {
                logRecord.RemoveExpiration();
                functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output.SpanByteAndMemory);
            }
            else
                functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output.SpanByteAndMemory);

            if (!logRecord.Info.ValueIsObject)
            {
                // reset etag state that may have been initialized earlier, but don't update etag because only the metadata was updated
                ETagState.ResetState(ref functionsState.etagState);
                shouldUpdateEtag = false;
            }
        }


        /// <inheritdoc />
        public void PostRMWOperation<TEpochAccessor>(ReadOnlySpan<byte> key, ref UnifiedInput input, ref RMWInfo rmwInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        {
            if ((rmwInfo.UserData & NeedAofLog) == NeedAofLog) // Check if we need to write to AOF
                WriteLogRMW(key, ref input, rmwInfo.Version, rmwInfo.SessionID, epochAccessor);
        }
    }
}