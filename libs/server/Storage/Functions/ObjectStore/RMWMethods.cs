// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly partial struct ObjectSessionFunctions : ISessionFunctions<ObjectInput, ObjectOutput, long>
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
                _ = value.Operate(ref input, ref output, functionsState.respProtocolVersion, out _);
                _ = logRecord.TrySetValueObjectAndPrepareOptionals(value, in sizeInfo);
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
                logRecord.ClearValueIfHeap(obj => { });
                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }

            if ((byte)input.header.type < CustomCommandManager.CustomTypeIdStartOffset)
            {
                var operateSuccessful = ((IGarnetObject)logRecord.ValueObject).Operate(ref input, ref output, functionsState.respProtocolVersion, out sizeChange);
                if (output.HasWrongType)
                    return true;
                if (output.HasRemoveKey)
                {
                    functionsState.cacheSizeTracker?.AddHeapSize(-logRecord.ValueObject.HeapMemorySize);

                    // Can't access 'this' in a lambda so dispose directly and pass a no-op lambda.
                    functionsState.storeFunctions.DisposeValueObject(logRecord.ValueObject, DisposeReason.Deleted);
                    logRecord.ClearValueIfHeap(obj => { });
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return false;
                }

                sizeInfo.AssertOptionals(logRecord.Info);
                return operateSuccessful;
            }

            var garnetValueObject = Unsafe.As<IGarnetObject>(logRecord.ValueObject);
            if (IncorrectObjectType(ref input, garnetValueObject, ref output.SpanByteAndMemory))
            {
                output.OutputFlags |= ObjectOutputFlags.WrongType;
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
            // TODO: Object store ETags

            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);

            if ((byte)input.header.type < CustomCommandManager.CustomTypeIdStartOffset)
            {
                value.Operate(ref input, ref output, functionsState.respProtocolVersion, out _);
                if (output.HasWrongType)
                    return true;
                if (output.HasRemoveKey)
                {
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return false;
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
            functionsState.cacheSizeTracker?.AddHeapSize(sizeAdjustment);

            if (functionsState.appendOnlyFile != null)
                rmwInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
            return true;
        }


        /// <inheritdoc />
        public void PostRMWOperation<TEpochAccessor>(ReadOnlySpan<byte> key, ref ObjectInput input, ref RMWInfo rmwInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        {
            if ((rmwInfo.UserData & NeedAofLog) == NeedAofLog) // Check if we need to write to AOF
                WriteLogRMW(key, ref input, rmwInfo.Version, rmwInfo.SessionID, epochAccessor);
        }
    }
}