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
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<ObjectInput, ObjectOutput, long>
    {
        /// <inheritdoc />
        public bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref ObjectInput input, ref ObjectOutput output, ref RMWInfo rmwInfo)
        {
            var type = input.header.type;

            if ((byte)type < CustomCommandManager.CustomTypeIdStartOffset)
                return GarnetObject.NeedToCreate(input.header);

            var customObjectCommand = GetCustomObjectCommand(ref input, type);

            // TODO: Cannot use 'using' statement because writer is passed by ref. Change that to non-ref parameter and convert to 'using'.
            var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
            try
            {
                return customObjectCommand.NeedInitialUpdate(key, ref input, ref writer);
            }
            finally
            {
                writer.Dispose();
            }
        }

        /// <inheritdoc />
        public bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, ref ObjectOutput output, ref RMWInfo rmwInfo)
        {
            Debug.Assert(!logRecord.Info.HasETag && !logRecord.Info.HasExpiration, "Should not have Expiration or ETag on InitialUpdater log records");

            var type = input.header.type;
            IGarnetObject value;
            if ((byte)type < CustomCommandManager.CustomTypeIdStartOffset)
            {
                value = GarnetObject.Create(type);

                var updatedEtag = HandleMetaCommandAndOperate(value, ref input, ref output, LogRecord.NoETag, out _, init: true);
               
                _ = logRecord.TrySetValueObjectAndPrepareOptionals(value, in sizeInfo);

                // the increment on initial etag is for satisfying the variant that any key with no etag is the same as a zero'd etag
                if (sizeInfo.FieldInfo.HasETag && !logRecord.TrySetETag(updatedEtag))
                {
                    functionsState.logger?.LogError("Could not set etag in {methodName}", "InitialUpdater");
                    return false;
                }

                ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in logRecord);

                return true;
            }

            var customObjectCommand = GetCustomObjectCommand(ref input, type);
            value = functionsState.GetCustomObjectFactory((byte)type).Create((byte)type);

            var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
            try
            {
                var result = customObjectCommand.InitialUpdater(logRecord.Key, ref input, value, ref writer, ref rmwInfo);
                _ = logRecord.TrySetValueObjectAndPrepareOptionals(value, in sizeInfo);
                if (result)
                    sizeInfo.AssertOptionals(logRecord.Info);
                return result;
            }
            finally
            {
                writer.Dispose();
            }
        }

        /// <inheritdoc />
        public void PostInitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, ref ObjectOutput output, ref RMWInfo rmwInfo)
        {
            // reset etag state set at need initial update
            ETagState.ResetState(ref functionsState.etagState);

            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                input.header.SetExpiredFlag();
                WriteLogRMW(dstLogRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);
            }

            functionsState.objectStoreSizeTracker?.AddTrackedSize(dstLogRecord.ValueObject.HeapMemorySize);
        }

        /// <inheritdoc />
        public bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, ref ObjectOutput output, ref RMWInfo rmwInfo)
        {
            if (!logRecord.Info.ValueIsObject)
            {
                rmwInfo.Action = RMWAction.WrongType;
                output.OutputFlags |= OutputFlags.WrongType;
                return true;
            }

            if (InPlaceUpdaterWorker(ref logRecord, in sizeInfo, ref input, ref output, ref rmwInfo, out long sizeChange))
            {
                if (!logRecord.Info.Modified)
                    functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
                if (functionsState.appendOnlyFile != null)
                    WriteLogRMW(logRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);
                functionsState.objectStoreSizeTracker?.AddTrackedSize(sizeChange);
                return true;
            }
            return false;
        }

        bool InPlaceUpdaterWorker(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, ref ObjectOutput output, ref RMWInfo rmwInfo, out long sizeChange)
        {
            sizeChange = 0;

            // Expired data
            if (logRecord.Info.HasExpiration && input.header.CheckExpiry(logRecord.Expiration))
            {
                functionsState.objectStoreSizeTracker?.AddTrackedSize(-logRecord.ValueObject.HeapMemorySize);

                // Can't access 'this' in a lambda so dispose directly and pass a no-op lambda.
                functionsState.storeFunctions.DisposeValueObject(logRecord.ValueObject, DisposeReason.Expired);
                logRecord.ClearValueIfHeap(_ => { });
                rmwInfo.Action = RMWAction.ExpireAndResume;
                logRecord.RemoveETag();
                return false;
            }

            // If the user calls withetag then we need to either update an existing etag and set the value or set the value with an etag and increment it.
            var metaCmd = input.header.MetaCmd;
            var inputHeaderHasEtag = metaCmd.IsEtagCommand();
            var hadETagPreMutation = logRecord.Info.HasETag;
            if (!hadETagPreMutation && inputHeaderHasEtag)
                return false;

            var shouldUpdateEtag = hadETagPreMutation;
            if (shouldUpdateEtag)
                ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in logRecord);

            if ((byte)input.header.type < CustomCommandManager.CustomTypeIdStartOffset)
            {
                var updatedEtag = HandleMetaCommandAndOperate((IGarnetObject)logRecord.ValueObject, ref input, ref output, logRecord.ETag, out sizeChange);
                shouldUpdateEtag |= (updatedEtag != logRecord.ETag);

                if (output.HasWrongType)
                    return true;
                if (output.HasRemoveKey)
                {
                    functionsState.objectStoreSizeTracker?.AddTrackedSize(-logRecord.ValueObject.HeapMemorySize);

                    // Can't access 'this' in a lambda so dispose directly and pass a no-op lambda.
                    functionsState.storeFunctions.DisposeValueObject(logRecord.ValueObject, DisposeReason.Deleted);
                    logRecord.ClearValueIfHeap(obj => { });
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    logRecord.RemoveETag();
                    return false;
                }

                if (shouldUpdateEtag)
                {
                    logRecord.TrySetETag(updatedEtag);
                    ETagState.ResetState(ref functionsState.etagState);
                }
                else if (hadETagPreMutation)
                {
                    ETagState.ResetState(ref functionsState.etagState);
                }

                sizeInfo.AssertOptionals(logRecord.Info);
                return true;
            }

            var garnetValueObject = Unsafe.As<IGarnetObject>(logRecord.ValueObject);
            if (IncorrectObjectType(ref input, garnetValueObject, ref output.SpanByteAndMemory))
            {
                output.OutputFlags |= OutputFlags.WrongType;
                return true;
            }

            var customObjectCommand = GetCustomObjectCommand(ref input, input.header.type);
            var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
            try
            {
                var result = customObjectCommand.Updater(logRecord.Key, ref input, garnetValueObject, ref writer, ref rmwInfo);
                if (!result)
                    return false;
            }
            finally
            {
                writer.Dispose();
            }

            sizeInfo.AssertOptionals(logRecord.Info);
            return true;
        }

        /// <inheritdoc />
        public bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref ObjectInput input,
            ref ObjectOutput output, ref RMWInfo rmwInfo) where TSourceLogRecord : ISourceLogRecord
            => true;

        /// <inheritdoc />
        public bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, ref ObjectOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            // Expired data
            if (srcLogRecord.Info.HasExpiration && input.header.CheckExpiry(srcLogRecord.Expiration))
            {
                _ = dstLogRecord.RemoveETag();
                rmwInfo.Action = RMWAction.ExpireAndResume;
                // reset etag state that may have been initialized earlier
                ETagState.ResetState(ref functionsState.etagState);
                return false;
            }
            // Defer the actual copying of data to PostCopyUpdater, so we know the record has been successfully CASed into the hash chain before we potentially
            // create large allocations (e.g. if srcLogRecord is from disk, we would have to allocate the overflow byte[]). Because we are doing an update we have
            // and XLock, so nobody will see the unset data even after the CAS. Tsavorite will handle cloning the ValueObject and caching serialized data as needed,
            // based on whether srcLogRecord is in-memory or a DiskLogRecord.
            return true;
        }

        /// <inheritdoc />
        public bool PostCopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, ref ObjectOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            // We're performing the object update here (and not in CopyUpdater) so that we are guaranteed that
            // the record was CASed into the hash chain before it gets modified
            var value = Unsafe.As<IGarnetObject>(srcLogRecord.ValueObject.Clone());
            var oldValueSize = srcLogRecord.ValueObject.HeapMemorySize;
            _ = dstLogRecord.TrySetValueObject(value);

            // Do not set actually set dstLogRecord.Expiration until we know it is a command for which we allocated length in the LogRecord for it.
            // TODO: Object store ETags

            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);

            var recordHadEtagPreMutation = srcLogRecord.Info.HasETag;
            var shouldUpdateEtag = recordHadEtagPreMutation;
            if (shouldUpdateEtag)
            {
                // during checkpointing we might skip the inplace calls and go directly to copy update so we need to initialize here if needed
                ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in srcLogRecord);
            }

            if ((byte)input.header.type < CustomCommandManager.CustomTypeIdStartOffset)
            {
                var updatedEtag = HandleMetaCommandAndOperate(value, ref input, ref output, srcLogRecord.ETag, out _);
                shouldUpdateEtag |= (srcLogRecord.ETag != updatedEtag);

                if (output.HasWrongType)
                    return true;
                if (output.HasRemoveKey)
                {
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return false;
                }

                if (shouldUpdateEtag)
                {
                    dstLogRecord.TrySetETag(updatedEtag);
                    ETagState.ResetState(ref functionsState.etagState);
                }
                else if (recordHadEtagPreMutation)
                {
                    ETagState.ResetState(ref functionsState.etagState);
                }
            }
            else
            {
                // TODO: Update to invoke CopyUpdater of custom object command without creating a new object
                // using Clone. Currently, expire and persist commands are performed on the new copy of the object.
                if (IncorrectObjectType(ref input, value, ref output.SpanByteAndMemory))
                {
                    output.OutputFlags |= OutputFlags.WrongType;
                    return true;
                }

                var customObjectCommand = GetCustomObjectCommand(ref input, input.header.type);
                var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
                try
                {
                    var result = customObjectCommand.Updater(srcLogRecord.Key, ref input, value, ref writer, ref rmwInfo);
                    return result;
                }
                finally
                {
                    writer.Dispose();
                }
            }

            sizeInfo.AssertOptionals(dstLogRecord.Info);

            // If oldValue has been set to null, subtract its size from the tracked heap size
            var sizeAdjustment = rmwInfo.ClearSourceValueObject ? value.HeapMemorySize - oldValueSize : value.HeapMemorySize;
            functionsState.objectStoreSizeTracker?.AddTrackedSize(sizeAdjustment);

            if (functionsState.appendOnlyFile != null)
                WriteLogRMW(srcLogRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);
            return true;
        }

        private long HandleMetaCommandAndOperate(IGarnetObject value, ref ObjectInput input, ref ObjectOutput output,
            long currEtag, out long sizeChange, bool init = false, bool readOnly = false)
        {
            sizeChange = 0;

            var metaCmd = input.header.MetaCmd;
            var updatedEtag = GetUpdatedEtag(currEtag, metaCmd, ref input.parseState, out var execCmd, init, readOnly);

            var isEtagCmd = metaCmd.IsEtagCommand();
            var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);

            try
            {
                if (isEtagCmd)
                    writer.WriteArrayLength(2);

                if (execCmd)
                {
                    value.Operate(ref input, ref output, ref writer, out sizeChange);

                    if (!readOnly && (currEtag != LogRecord.NoETag) && (output.OutputFlags & OutputFlags.ValueUnchanged) == OutputFlags.ValueUnchanged)
                        updatedEtag = currEtag;
                }
                else
                    writer.WriteNull();

                if (isEtagCmd)
                    writer.WriteInt64(updatedEtag);
            }
            finally
            {
                writer.Dispose();
            }

            return updatedEtag;
        }
    }
}