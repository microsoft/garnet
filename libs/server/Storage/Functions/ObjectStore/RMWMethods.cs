// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly partial struct ObjectSessionFunctions : ISessionFunctions<ObjectInput, ObjectOutput, long>
    {
        /// <inheritdoc />
        public bool NeedInitialUpdate<TKey>(TKey key, ref ObjectInput input, ref ObjectOutput output, ref RMWInfo rmwInfo)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            var type = input.header.type;

            if ((byte)type < CustomCommandManager.CustomTypeIdStartOffset)
                return GarnetObject.NeedToCreate(input.header);

            var customObjectCommand = GetCustomObjectCommand(ref input, type);

            // TODO: Cannot use 'using' statement because writer is passed by ref. Change that to non-ref parameter and convert to 'using'.
            var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
            try
            {
                // Deliberately hiding key type complexity from custom object commands
                return customObjectCommand.NeedInitialUpdate(key.KeyBytes, ref input, ref writer);
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

                var isETagCmd = input.metaCommandInfo.MetaCommand.IsETagCommand();
                long updatedETag = LogRecord.NoETag;
                if (isETagCmd)
                    // Conditional execution should pass in the InitUpdater context, calling this method to get the updated ETag
                    _ = input.metaCommandInfo.CheckConditionalExecution(LogRecord.NoETag, out updatedETag, initContext: true);

                value.Operate(ref input, ref output, functionsState.respProtocolVersion, out _);

                _ = logRecord.TrySetValueObjectAndPrepareOptionals(value, in sizeInfo);

                // Increment on initial etag is for satisfying the variant that any key with no etag is the same as a zeroed etag
                if (sizeInfo.FieldInfo.HasETag && !logRecord.TrySetETag(updatedETag))
                {
                    functionsState.logger?.LogError("Could not set etag in {methodName}", "InitialUpdater");
                    return false;
                }

                output.ETag = logRecord.ETag;

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
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                input.header.SetExpiredFlag();
                rmwInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
            }

            functionsState.cacheSizeTracker?.AddHeapSize(dstLogRecord.ValueObject.HeapMemorySize);
        }

        /// <inheritdoc />
        public bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, ref ObjectOutput output, ref RMWInfo rmwInfo)
        {
            if (!logRecord.Info.ValueIsObject)
            {
                rmwInfo.Action = RMWAction.WrongType;
                output.OutputFlags |= ObjectOutputFlags.WrongType;
                return true;
            }

            if (InPlaceUpdaterWorker(ref logRecord, in sizeInfo, ref input, ref output, ref rmwInfo, out long sizeChange))
            {
                if (!logRecord.Info.Modified)
                    functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
                if (functionsState.appendOnlyFile != null)
                    rmwInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
                functionsState.cacheSizeTracker?.AddHeapSize(sizeChange);
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
                functionsState.cacheSizeTracker?.AddHeapSize(-logRecord.ValueObject.HeapMemorySize);

                // Can't access 'this' in a lambda so dispose directly and pass a no-op lambda.
                functionsState.storeFunctions.DisposeValueObject(logRecord.ValueObject, DisposeReason.Expired);
                logRecord.ClearValueIfHeap(_ => { });
                rmwInfo.Action = RMWAction.ExpireAndResume;
                logRecord.RemoveETag();
                return false;
            }

            var hadETagPreMutation = logRecord.Info.HasETag;
            var isETagCmd = input.metaCommandInfo.MetaCommand.IsETagCommand();
            var updatedETag = logRecord.ETag;

            // Check if we should skip execution of this command based on the eTag meta-command (if exists) and the current etag
            if ((isETagCmd || hadETagPreMutation) &&
                !input.metaCommandInfo.CheckConditionalExecution(logRecord.ETag, out updatedETag))
            {
                // Handle skipped execution based on eTag meta-command and current eTag value
                output.ETag = logRecord.ETag;
                output.OutputFlags |= ObjectOutputFlags.OperationSkipped;
                return functionsState.HandleSkippedExecution(in input.header, ref output.SpanByteAndMemory);
            }

            // If we need to add an ETag and log record has no space for adding it in-place, continue to CU
            if (!hadETagPreMutation && isETagCmd &&
                !logRecord.CanAddETagInPlace(out _, out _, out _))
                return false;

            var shouldUpdateETag = !hadETagPreMutation && isETagCmd;

            if ((byte)input.header.type < CustomCommandManager.CustomTypeIdStartOffset)
            {
                ((IGarnetObject)logRecord.ValueObject).Operate(ref input, ref output, functionsState.respProtocolVersion, out sizeChange);

                // Do not update the eTag if the operation explicitly marked the record value as unchanged
                shouldUpdateETag |= hadETagPreMutation && (output.OutputFlags & ObjectOutputFlags.ValueUnchanged) == 0;

                if (output.HasWrongType)
                    return true;
                if (output.HasRemoveKey)
                {
                    functionsState.cacheSizeTracker?.AddHeapSize(-logRecord.ValueObject.HeapMemorySize);

                    // Can't access 'this' in a lambda so dispose directly and pass a no-op lambda.
                    functionsState.storeFunctions.DisposeValueObject(logRecord.ValueObject, DisposeReason.Deleted);
                    logRecord.ClearValueIfHeap(_ => { });
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    logRecord.RemoveETag();
                    return false;
                }

                // Update the record's eTag, if necessary
                if (shouldUpdateETag)
                {
                    // Should always succeed since we checked CanAddETagInPlace
                    logRecord.TrySetETag(updatedETag);
                    output.ETag = updatedETag;
                }
                else if (hadETagPreMutation)
                {
                    output.ETag = logRecord.ETag;
                }

                sizeInfo.AssertOptionals(logRecord.Info);
                return true;
            }

            var garnetValueObject = Unsafe.As<IGarnetObject>(logRecord.ValueObject);
            if (IncorrectObjectType(ref input, garnetValueObject, ref output.SpanByteAndMemory))
            {
                output.OutputFlags |= ObjectOutputFlags.WrongType;
                return true;
            }

            var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);

            try
            {
                // Disallow custom commands on records with eTags
                if (hadETagPreMutation)
                {
                    writer.WriteError(CmdStrings.RESP_ERR_ETAG_ON_CUSTOM_PROC);
                    return true;
                }

                var customObjectCommand = GetCustomObjectCommand(ref input, input.header.type);

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

            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);

            var hadETagPreMutation = srcLogRecord.Info.HasETag;
            var isETagCmd = input.metaCommandInfo.MetaCommand.IsETagCommand();
            var shouldUpdateETag = !hadETagPreMutation && isETagCmd;

            if ((byte)input.header.type < CustomCommandManager.CustomTypeIdStartOffset)
            {
                var updatedETag = srcLogRecord.ETag;
                if (isETagCmd || hadETagPreMutation)
                {
                    // Conditional execution should pass in the CU context (otherwise we would have cancelled the operation in IPU)
                    var execOp = input.metaCommandInfo.CheckConditionalExecution(srcLogRecord.ETag, out updatedETag);
                    Debug.Assert(execOp);
                }

                value.Operate(ref input, ref output, functionsState.respProtocolVersion, out _);

                // Do not update the eTag if the operation explicitly marked the record value as unchanged
                shouldUpdateETag |= hadETagPreMutation && (output.OutputFlags & ObjectOutputFlags.ValueUnchanged) == 0;

                if (output.HasWrongType)
                    return true;
                if (output.HasRemoveKey)
                {
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return false;
                }

                // Update the record's eTag, if necessary
                if (shouldUpdateETag)
                {
                    dstLogRecord.TrySetETag(updatedETag);
                    output.ETag = updatedETag;
                }
                // Set the existing eTag in the new record if previous record had an eTag and we did not update it
                else if (hadETagPreMutation)
                {
                    dstLogRecord.TrySetETag(srcLogRecord.ETag);
                    output.ETag = srcLogRecord.ETag;
                }
            }
            else
            {
                // TODO: Update to invoke CopyUpdater of custom object command without creating a new object
                // using Clone. Currently, expire and persist commands are performed on the new copy of the object.
                if (IncorrectObjectType(ref input, value, ref output.SpanByteAndMemory))
                {
                    output.OutputFlags |= ObjectOutputFlags.WrongType;
                    return true;
                }

                var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);

                try
                {
                    // Disallow custom commands on records with eTags
                    if (hadETagPreMutation)
                    {
                        writer.WriteError(CmdStrings.RESP_ERR_ETAG_ON_CUSTOM_PROC);
                        return true;
                    }

                    var customObjectCommand = GetCustomObjectCommand(ref input, input.header.type);

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
            functionsState.cacheSizeTracker?.AddHeapSize(sizeAdjustment);

            if (functionsState.appendOnlyFile != null)
                rmwInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
            return true;
        }

        /// <inheritdoc />
        public void PostRMWOperation<TKey, TEpochAccessor>(TKey key, ref ObjectInput input, ref RMWInfo rmwInfo, TEpochAccessor epochAccessor)
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