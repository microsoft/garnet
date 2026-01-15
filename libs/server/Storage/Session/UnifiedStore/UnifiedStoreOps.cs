// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{

    sealed partial class StorageSession : IDisposable
    {
        /// <summary>
        /// GET a value in the unified store context (value is serialized to the <see cref="UnifiedOutput"/>'s <see cref="SpanByteAndMemory"/>).
        /// </summary>
        /// <typeparam name="TUnifiedContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public GarnetStatus GET<TUnifiedContext>(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output, ref TUnifiedContext context)
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            long ctx = default;
            var status = context.Read(key.ReadOnlySpan, ref input, ref output, ctx);

            if (status.IsPending)
            {
                StartPendingMetrics();
                CompletePendingForUnifiedStoreSession(ref status, ref output, ref context);
                StopPendingMetrics();
            }

            if (status.Found)
            {
                incr_session_found();
                return GarnetStatus.OK;
            }
            incr_session_notfound();
            return GarnetStatus.NOTFOUND;
        }

        /// <summary>
        /// SET a log record in the unified store context.
        /// </summary>
        /// <typeparam name="TUnifiedContext"></typeparam>
        /// <typeparam name="TSourceLogRecord"></typeparam>
        /// <param name="srcLogRecord">The log record</param>
        /// <param name="unifiedContext">Basic unifiedContext for the unified store.</param>
        /// <returns></returns>
        public GarnetStatus SET<TUnifiedContext, TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TUnifiedContext unifiedContext)
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
            where TSourceLogRecord : ISourceLogRecord
        {
            _ = unifiedContext.Upsert(in srcLogRecord);
            return GarnetStatus.OK;
        }

        /// <summary>
        /// SET a log record in the unified store context.
        /// </summary>
        /// <typeparam name="TUnifiedContext"></typeparam>
        /// <typeparam name="TSourceLogRecord"></typeparam>
        /// <param name="key">The key to override the one in <paramref name="srcLogRecord"/>, e.g. if from RENAME.</param>
        /// <param name="input"></param>
        /// <param name="srcLogRecord">The log record</param>
        /// <param name="unifiedContext">Basic unifiedContext for the unified store.</param>
        /// <returns></returns>
        public GarnetStatus SET<TUnifiedContext, TSourceLogRecord>(ReadOnlySpan<byte> key, ref UnifiedInput input, in TSourceLogRecord srcLogRecord, ref TUnifiedContext unifiedContext)
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
            where TSourceLogRecord : ISourceLogRecord
        {
            _ = unifiedContext.Upsert(key, ref input, in srcLogRecord);
            return GarnetStatus.OK;
        }

        /// <summary>
        /// Checks if a key exists in the unified store context.
        /// </summary>
        /// <typeparam name="TUnifiedContext"></typeparam>
        /// <param name="key">The name of the key to use in the operation</param>
        /// <param name="unifiedContext">Basic unifiedContext for the unified store.</param>
        /// <returns></returns>
        public GarnetStatus EXISTS<TUnifiedContext>(PinnedSpanByte key, ref TUnifiedContext unifiedContext)
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            // Prepare input
            var input = new UnifiedInput(RespCommand.EXISTS);

            // Prepare UnifiedOutput output
            var output = new UnifiedOutput();

            // TODO: The output is unused so optimize ReadMethods to not copy it.
            return Read_UnifiedStore(key, ref input, ref output, ref unifiedContext);
        }

        public unsafe GarnetStatus DEL_Conditional<TStringContext>(PinnedSpanByte key, ref UnifiedInput input, ref TStringContext context)
            where TStringContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var output = new UnifiedOutput();

            var status = context.RMW(key, ref input, ref output);

            if (status.IsPending)
            {
                StartPendingMetrics();
                CompletePendingForUnifiedStoreSession(ref status, ref output, ref context);
                StopPendingMetrics();
            }

            // Deletions in RMW are done by expiring the record, hence we use expiration as the indicator of success.
            if (status.IsExpired)
            {
                incr_session_found();
                return GarnetStatus.OK;
            }
            else
            {
                if (status.NotFound)
                    incr_session_notfound();

                return GarnetStatus.NOTFOUND;
            }
        }

        /// <summary>
        /// Deletes a key from the unified store context.
        /// </summary>
        /// <param name="key">The name of the key to use in the operation</param>
        /// <param name="unifiedContext">Basic unifiedContext for the unified store.</param>
        /// <returns></returns>
        public GarnetStatus DELETE<TUnifiedContext>(PinnedSpanByte key, ref TUnifiedContext unifiedContext)
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = unifiedContext.Delete(key.ReadOnlySpan);
            Debug.Assert(!status.IsPending);
            return status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        /// <summary>
        /// Deletes a key if it is in memory and expired.
        /// </summary>
        /// <param name="key">The name of the key to use in the operation</param>
        /// <param name="unifiedContext">Basic unifiedContext for the unified store.</param>
        /// <returns></returns>
        public GarnetStatus DELIFEXPIM<TUnifiedContext>(PinnedSpanByte key, ref TUnifiedContext unifiedContext)
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long,
                UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var input = new UnifiedInput(RespCommand.DELIFEXPIM);
            var status = unifiedContext.RMW(key.ReadOnlySpan, ref input);
            return status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        /// <summary>
        /// Set a timeout on key
        /// </summary>
        /// <typeparam name="TUnifiedContext"></typeparam>
        /// <param name="key">The key to set the timeout on.</param>
        /// <param name="expiryMs">Milliseconds value for the timeout.</param>
        /// <param name="timeoutSet">True when the timeout was properly set.</param>
        /// <param name="expireOption">>Flags to use for the operation.</param>
        /// <param name="unifiedContext">Basic context for the unified store.</param>
        /// <returns></returns>
        public unsafe GarnetStatus EXPIRE<TUnifiedContext>(PinnedSpanByte key, PinnedSpanByte expiryMs, out bool timeoutSet, ExpireOption expireOption, ref TUnifiedContext unifiedContext)
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
            => EXPIRE(key, TimeSpan.FromMilliseconds(NumUtils.ReadInt64(expiryMs.Length, expiryMs.ToPointer())), out timeoutSet, expireOption, ref unifiedContext);

        /// <summary>
        /// Set a timeout on key using absolute Unix timestamp (seconds since January 1, 1970).
        /// </summary>
        /// <typeparam name="TUnifiedContext"></typeparam>
        /// <param name="key">The key to set the timeout on.</param>
        /// <param name="expiryTimestamp">Absolute Unix timestamp</param>
        /// <param name="timeoutSet">True when the timeout was properly set.</param>
        /// <param name="expireOption">Flags to use for the operation.</param>
        /// <param name="unifiedContext">Basic context for the unified store.</param>
        /// <param name="milliseconds">When true, <paramref name="expiryTimestamp"/> is treated as milliseconds else seconds</param>
        /// <returns>Return GarnetStatus.OK when key found, else GarnetStatus.NOTFOUND</returns>
        public unsafe GarnetStatus EXPIREAT<TUnifiedContext>(PinnedSpanByte key, long expiryTimestamp, out bool timeoutSet, ExpireOption expireOption, ref TUnifiedContext unifiedContext, bool milliseconds = false)
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
            => EXPIRE(key, expiryTimestamp, out timeoutSet, expireOption, ref unifiedContext, milliseconds ? RespCommand.PEXPIREAT : RespCommand.EXPIREAT);

        /// <summary>
        /// Set a timeout on key.
        /// </summary>
        /// <typeparam name="TUnifiedContext"></typeparam>
        /// <param name="key">The key to set the timeout on.</param>
        /// <param name="expiry">The timespan value to set the expiration for.</param>
        /// <param name="timeoutSet">True when the timeout was properly set.</param>
        /// <param name="expireOption">Flags to use for the operation.</param>
        /// <param name="unifiedContext">Basic context for the unified store.</param>
        /// <param name="milliseconds">When true the command executed is PEXPIRE, expire by default.</param>
        /// <returns>Return GarnetStatus.OK when key found, else GarnetStatus.NOTFOUND</returns>
        public unsafe GarnetStatus EXPIRE<TUnifiedContext>(PinnedSpanByte key, TimeSpan expiry, out bool timeoutSet, ExpireOption expireOption, ref TUnifiedContext unifiedContext, bool milliseconds = false)
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
            => EXPIRE(key, (long)(milliseconds ? expiry.TotalMilliseconds : expiry.TotalSeconds), out timeoutSet, expireOption,
                ref unifiedContext, milliseconds ? RespCommand.PEXPIRE : RespCommand.EXPIRE);

        /// <summary>
        /// Set a timeout on key.
        /// </summary>
        /// <typeparam name="TUnifiedContext"></typeparam>
        /// <param name="key">The key to set the timeout on.</param>
        /// <param name="expiration">The timespan value to set the expiration for.</param>
        /// <param name="timeoutSet">True when the timeout was properly set.</param>
        /// <param name="expireOption">Flags to use for the operation.</param>
        /// <param name="unifiedContext">Basic context for the main store</param>
        /// <param name="respCommand">The current RESP command</param>
        /// <returns></returns>
        public unsafe GarnetStatus EXPIRE<TUnifiedContext>(PinnedSpanByte key, long expiration, out bool timeoutSet, ExpireOption expireOption, ref TUnifiedContext unifiedContext, RespCommand respCommand)
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            Span<byte> rmwOutput = stackalloc byte[OutputHeader.Size];
            var unifiedOutput = new UnifiedOutput(SpanByteAndMemory.FromPinnedSpan(rmwOutput));

            // Convert to expiration time in ticks
            var expirationTimeInTicks = respCommand switch
            {
                RespCommand.EXPIRE => DateTimeOffset.UtcNow.AddSeconds(expiration).UtcTicks,
                RespCommand.PEXPIRE => DateTimeOffset.UtcNow.AddMilliseconds(expiration).UtcTicks,
                RespCommand.EXPIREAT => ConvertUtils.UnixTimestampInSecondsToTicks(expiration),
                _ => ConvertUtils.UnixTimestampInMillisecondsToTicks(expiration)
            };

            var expirationWithOption = new ExpirationWithOption(expirationTimeInTicks, expireOption);

            var input = new UnifiedInput(RespCommand.EXPIRE, arg1: expirationWithOption.Word);
            var status = unifiedContext.RMW(key.ReadOnlySpan, ref input, ref unifiedOutput);

            if (status.IsPending)
                CompletePendingForUnifiedStoreSession(ref status, ref unifiedOutput, ref unifiedContext);

            timeoutSet = status.Found &&
                         unifiedOutput.SpanByteAndMemory.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.RESP_RETURN_VAL_1);

            return status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        /// <summary>
        /// RENAME a key in the unified store context
        /// </summary>
        /// <param name="key">The key to rename</param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        public unsafe GarnetStatus RENAME(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output)
        {
            using var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);

            var newKeySlice = input.parseState.GetArgSliceByRef(0);

            var isNx = input.header.cmd == RespCommand.RENAMENX;

            // If same name check return early.
            if (key.ReadOnlySpan.SequenceEqual(newKeySlice.ReadOnlySpan))
            {
                if (isNx)
                    writer.WriteInt32(1);
                return GarnetStatus.OK;
            }

            // Note: RespServerSession.CanServeSlot has already verified the keys are in the same slot

            var createTransaction = false;
            if (txnManager.state != TxnState.Running)
            {
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Main | TransactionStoreTypes.Object);
                txnManager.SaveKeyEntryToLock(key, LockType.Exclusive);
                txnManager.SaveKeyEntryToLock(newKeySlice, LockType.Exclusive);
                _ = txnManager.Run(true);
            }

            var context = txnManager.UnifiedTransactionalContext;
            var oldKey = key;
            var newKey = newKeySlice;

            var returnStatus = GarnetStatus.NOTFOUND;
            var abortTransaction = false;

            var recordOutput = new UnifiedOutput();

            try
            {
                // Check if new key exists. This extra query isn't ideal, but it should be a rare operation and there's nowhere in Input to 
                // pass the srcLogRecord or even the ValueObject to RMW. TODO: Optimize this to return only the ETag, or set functionsState.etagState.ETag directly.
                // Set the input so Read knows to do the special "serialization" into output
                var status = GET(newKey, ref input, ref recordOutput, ref context);
                if (isNx && status != GarnetStatus.NOTFOUND)
                {
                    abortTransaction = true;
                    writer.WriteInt32(0);
                    return GarnetStatus.OK;
                }

                // Try to get the new key's etag, if exists
                if (status != GarnetStatus.NOTFOUND)
                {
                    fixed (byte* recordPtr = recordOutput.SpanByteAndMemory.ReadOnlySpan)
                    {
                        // We have a record in in-memory, unserialized format, with its objects (if any) resolved to the TransientObjectIdMap.
                        var logRecord = new LogRecord(recordPtr, functionsState.transientObjectIdMap);
                        if (logRecord.Info.HasETag)
                            functionsState.etagState.ETag = logRecord.ETag;
                    }
                }

                status = GET(oldKey, ref input, ref recordOutput, ref context);
                if (status != GarnetStatus.OK)
                {
                    abortTransaction = true;
                    return status;
                }

                fixed (byte* recordPtr = recordOutput.SpanByteAndMemory.ReadOnlySpan)
                {
                    // We have a record in in-memory, unserialized format, with its objects (if any) resolved to the TransientObjectIdMap.
                    var logRecord = new LogRecord(recordPtr, functionsState.transientObjectIdMap);

                    status = SET(newKey, ref input, in logRecord, ref context);
                    if (status == GarnetStatus.OK)
                    {
                        if (isNx)
                            writer.WriteInt32(1);

                        // Delete the old key
                        _ = DELETE(oldKey, ref context);
                        return GarnetStatus.OK;
                    }
                }
            }
            finally
            {
                if (createTransaction)
                {
                    if (abortTransaction)
                        txnManager.Reset();
                    else
                        txnManager.Commit(true);
                }
                recordOutput.Dispose();
            }
            return returnStatus;
        }
    }
}