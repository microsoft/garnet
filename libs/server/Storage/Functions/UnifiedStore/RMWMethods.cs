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
        public bool NeedInitialUpdate<TKey>(TKey key, ref UnifiedInput input, ref UnifiedOutput output,
            ref RMWInfo rmwInfo)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
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

            var cmd = input.header.cmd;
            bool shouldUpdateEtag = false;

            var result = cmd switch
            {
                RespCommand.EXPIRE => HandleExpireCopyUpdate(srcLogRecord, ref dstLogRecord, in sizeInfo, ref shouldUpdateEtag, ref input, ref output),
                RespCommand.PERSIST => HandlePersistCopyUpdate(srcLogRecord, ref dstLogRecord, in sizeInfo, ref shouldUpdateEtag, ref output),
                _ => throw new NotImplementedException()
            };

            if (!result)
                return false;

            sizeInfo.AssertOptionalsIfSet(dstLogRecord.Info);
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
                // We're performing the object update here (and not in CopyUpdater) so that we are guaranteed that
                // the record was CASed into the hash chain before it gets modified
                var value = Unsafe.As<IGarnetObject>(srcLogRecord.ValueObject.Clone());

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
                        HandlePersistInPlaceUpdate(ref dstLogRecord, hasExpiration, ref output);
                        break;
                }

                sizeInfo.AssertOptionalsIfSet(dstLogRecord.Info);

            }

            if (functionsState.appendOnlyFile != null)
                rmwInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF

            return true;
        }

        /// <inheritdoc />
        public bool InPlaceUpdater(ref LogRecord logRecord, ref UnifiedInput input, ref UnifiedOutput output, ref RMWInfo rmwInfo)
        {
            var ipuResult = InPlaceUpdaterWorker(ref logRecord, ref input, ref output, ref rmwInfo);
            switch (ipuResult)
            {
                case IPUResult.Failed:
                    return false;
                case IPUResult.Succeeded:
                    if (!logRecord.Info.Modified)
                        functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
                    if (functionsState.appendOnlyFile != null)
                        rmwInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
                    return true;
                case IPUResult.NotUpdated:
                default:
                    return true;
            }
        }

        IPUResult InPlaceUpdaterWorker(ref LogRecord logRecord, ref UnifiedInput input, ref UnifiedOutput output, ref RMWInfo rmwInfo)
        {
            var cmd = input.header.cmd;

            // Expired data
            if (logRecord.Info.HasExpiration && input.header.CheckExpiry(logRecord.Expiration))
            {
                // Heap disposal and cache size tracking are handled by
                // OnDispose(Deleted) in InternalRMW for both ExpireAndStop and ExpireAndResume.
                rmwInfo.Action = cmd == RespCommand.DELIFEXPIM ? RMWAction.ExpireAndStop : RMWAction.ExpireAndResume;
                return IPUResult.Failed;
            }

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
                    HandlePersistInPlaceUpdate(ref logRecord, hasExpiration, ref output);
                    break;
                case RespCommand.DELIFEXPIM:
                    // Not expired — no-op
                    break;
                default:
                    throw new NotImplementedException();
            }

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

        private void HandlePersistInPlaceUpdate(ref LogRecord logRecord, bool hasExpiration, ref UnifiedOutput output)
        {
            if (hasExpiration)
            {
                logRecord.RemoveExpiration();
                functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output.SpanByteAndMemory);
            }
            else
                functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output.SpanByteAndMemory);
        }


        /// <inheritdoc />
        public void PostRMWOperation<TKey, TEpochAccessor>(TKey key, ref UnifiedInput input, ref RMWInfo rmwInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        {
            if ((rmwInfo.UserData & NeedAofLog) == NeedAofLog) // Check if we need to write to AOF
                WriteLogRMW(key.KeyBytes, ref input, rmwInfo.Version, rmwInfo.SessionID, epochAccessor);
        }
    }
}