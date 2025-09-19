// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Unified store functions
    /// </summary>
    public readonly unsafe partial struct UnifiedSessionFunctions : ISessionFunctions<UnifiedStoreInput, GarnetUnifiedStoreOutput, long>
    {
        public bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref UnifiedStoreInput input, ref GarnetUnifiedStoreOutput output,
            ref RMWInfo rmwInfo)
        {
            return input.header.cmd switch
            {
                RespCommand.PERSIST or
                RespCommand.EXPIRE or
                RespCommand.EXPIREAT or
                RespCommand.PEXPIRE or
                RespCommand.PEXPIREAT => false,
                _ => true
            };
        }

        public bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input,
            ref GarnetUnifiedStoreOutput output, ref RMWInfo rmwInfo)
        {
            return input.header.cmd switch
            {
                RespCommand.PERSIST or
                RespCommand.EXPIRE or
                RespCommand.EXPIREAT or
                RespCommand.PEXPIRE or
                RespCommand.PEXPIREAT => throw new Exception(),
                _ => true
            };
        }

        public void PostInitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input,
            ref GarnetUnifiedStoreOutput output, ref RMWInfo rmwInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                input.header.SetExpiredFlag();
                WriteLogRMW(logRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);
            }

            if (logRecord.Info.ValueIsObject)
            {
                functionsState.objectStoreSizeTracker?.AddTrackedSize(logRecord.ValueObject.MemorySize);
            }
        }

        public bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref UnifiedStoreInput input,
            ref GarnetUnifiedStoreOutput output, ref RMWInfo rmwInfo) where TSourceLogRecord : ISourceLogRecord => true;

        public bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord,
            in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input, ref GarnetUnifiedStoreOutput output,
            ref RMWInfo rmwInfo) where TSourceLogRecord : ISourceLogRecord
        {
            if (srcLogRecord.Info.HasExpiration && input.header.CheckExpiry(srcLogRecord.Expiration))
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }

            if (srcLogRecord.Info.ValueIsObject) return true;

            var cmd = input.header.cmd;

            var result = cmd switch
            {
                RespCommand.EXPIRE => HandleExpire(srcLogRecord, ref dstLogRecord, in sizeInfo, ref input, ref output, ref rmwInfo),
                RespCommand.PERSIST => HandlePersist(srcLogRecord, ref dstLogRecord, in sizeInfo, ref input, ref output, ref rmwInfo),
                _ => throw new NotImplementedException()
            };

            if (!result) 
                return false;

            sizeInfo.AssertOptionals(dstLogRecord.Info);
            return true;
        }

        public bool PostCopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord,
            in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input, ref GarnetUnifiedStoreOutput output,
            ref RMWInfo rmwInfo) where TSourceLogRecord : ISourceLogRecord
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);

            if (srcLogRecord.Info.ValueIsObject)
            {
                // We're performing the object update here (and not in CopyUpdater) so that we are guaranteed that
                // the record was CASed into the hash chain before it gets modified
                var oldValueSize = srcLogRecord.ValueObject.MemorySize;
                var value = ((IGarnetObject)srcLogRecord.ValueObject).CopyUpdate(srcLogRecord.Info.IsInNewVersion,
                    ref rmwInfo);

                // First copy the new Value and optionals to the new record. This will also ensure space for expiration if it's present.
                // Do not set actually set dstLogRecord.Expiration until we know it is a command for which we allocated length in the LogRecord for it.
                if (!dstLogRecord.TrySetValueObject(value, in sizeInfo))
                    return false;

                var cmd = input.header.cmd;
                switch (cmd)
                {
                    case RespCommand.EXPIRE:
                        var expirationWithOption = new ExpirationWithOption(input.arg1);

                        // Expire will have allocated space for the expiration, so copy it over and do the "in-place" logic to replace it in the new record
                        if (srcLogRecord.Info.HasExpiration)
                            dstLogRecord.TrySetExpiration(srcLogRecord.Expiration);
                        if (!EvaluateObjectExpireInPlace(ref dstLogRecord, expirationWithOption.ExpireOption,
                                expirationWithOption.ExpirationTimeInTicks, ref output))
                            return false;
                        break;

                    case RespCommand.PERSIST:
                        if (!dstLogRecord.TryCopyFrom(in srcLogRecord, in sizeInfo))
                            return false;
                        if (srcLogRecord.Info.HasExpiration)
                        {
                            dstLogRecord.RemoveExpiration();
                            functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output.SpanByteAndMemory);
                        }
                        else
                            functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output.SpanByteAndMemory);

                        break;
                }

                sizeInfo.AssertOptionals(dstLogRecord.Info);

                // If oldValue has been set to null, subtract its size from the tracked heap size
                var sizeAdjustment = rmwInfo.ClearSourceValueObject ? value.MemorySize - oldValueSize : value.MemorySize;
                functionsState.objectStoreSizeTracker?.AddTrackedSize(sizeAdjustment);
            }

            if (functionsState.appendOnlyFile != null)
                WriteLogRMW(dstLogRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);

            return true;
        }

        public bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input,
            ref GarnetUnifiedStoreOutput output, ref RMWInfo rmwInfo)
        {

        }

        private bool HandleExpire<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord,
            in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input, ref GarnetUnifiedStoreOutput output,
            ref RMWInfo rmwInfo) where TSourceLogRecord : ISourceLogRecord
        {
            var expirationWithOption = new ExpirationWithOption(input.arg1);

            // First copy the old Value and non-Expiration optionals to the new record. This will also ensure space for expiration.
            if (!dstLogRecord.TryCopyFrom(in srcLogRecord, in sizeInfo))
                return false;

            return EvaluateExpireCopyUpdate(ref dstLogRecord, in sizeInfo, expirationWithOption.ExpireOption,
                expirationWithOption.ExpirationTimeInTicks, dstLogRecord.ValueSpan, ref output);
        }

        private bool HandlePersist<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord,
            in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input, ref GarnetUnifiedStoreOutput output,
            ref RMWInfo rmwInfo) where TSourceLogRecord : ISourceLogRecord
        {
            if (!dstLogRecord.TryCopyFrom(in srcLogRecord, in sizeInfo))
                return false;
            if (srcLogRecord.Info.HasExpiration)
            {
                dstLogRecord.RemoveExpiration();
                using var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
                writer.WriteOne();
            }

            return true;
        }
    }
}
