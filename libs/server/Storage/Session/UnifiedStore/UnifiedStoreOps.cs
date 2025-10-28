// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
#pragma warning disable IDE0065 // Misplaced using directive
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    sealed partial class StorageSession : IDisposable
    {
        public GarnetStatus GET<TContext>(PinnedSpanByte key, ref UnifiedStoreInput input, ref GarnetUnifiedStoreOutput output, ref TContext context)
            where TContext : ITsavoriteContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
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
            where TUnifiedContext : ITsavoriteContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
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
        /// <param name="srcLogRecord">The log record</param>
        /// <param name="unifiedContext">Basic unifiedContext for the unified store.</param>
        /// <returns></returns>
        public GarnetStatus SET<TUnifiedContext, TSourceLogRecord>(ReadOnlySpan<byte> key, in TSourceLogRecord srcLogRecord, ref TUnifiedContext unifiedContext)
            where TUnifiedContext : ITsavoriteContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
            where TSourceLogRecord : ISourceLogRecord
        {
            _ = unifiedContext.Upsert(key, in srcLogRecord);
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
            where TUnifiedContext : ITsavoriteContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            // Prepare input
            var input = new UnifiedStoreInput(RespCommand.EXISTS);

            // Prepare GarnetUnifiedStoreOutput output
            var output = new GarnetUnifiedStoreOutput();

            // TODO: The output is unused so optimize ReadMethods to not copy it.
            var status = Read_UnifiedStore(key, ref input, ref output, ref unifiedContext);

            return status;
        }

        /// <summary>
        /// Deletes a key from the unified store context.
        /// </summary>
        /// <param name="key">The name of the key to use in the operation</param>
        /// <param name="unifiedContext">Basic unifiedContext for the unified store.</param>
        /// <returns></returns>
        public GarnetStatus DELETE<TUnifiedContext>(PinnedSpanByte key, ref TUnifiedContext unifiedContext)
            where TUnifiedContext : ITsavoriteContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
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
            where TUnifiedContext : ITsavoriteContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long,
                UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var input = new UnifiedStoreInput(RespCommand.DELIFEXPIM);
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
            where TUnifiedContext : ITsavoriteContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
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
            where TUnifiedContext : ITsavoriteContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            return EXPIRE(key, expiryTimestamp, out timeoutSet, expireOption, ref unifiedContext, milliseconds ? RespCommand.PEXPIREAT : RespCommand.EXPIREAT);
        }

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
            where TUnifiedContext : ITsavoriteContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            return EXPIRE(key, (long)(milliseconds ? expiry.TotalMilliseconds : expiry.TotalSeconds), out timeoutSet, expireOption,
                ref unifiedContext, milliseconds ? RespCommand.PEXPIRE : RespCommand.EXPIRE);
        }

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
            where TUnifiedContext : ITsavoriteContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            Span<byte> rmwOutput = stackalloc byte[OutputHeader.Size];
            var unifiedOutput = new GarnetUnifiedStoreOutput(SpanByteAndMemory.FromPinnedSpan(rmwOutput));

            // Convert to expiration time in ticks
            var expirationTimeInTicks = respCommand switch
            {
                RespCommand.EXPIRE => DateTimeOffset.UtcNow.AddSeconds(expiration).UtcTicks,
                RespCommand.PEXPIRE => DateTimeOffset.UtcNow.AddMilliseconds(expiration).UtcTicks,
                RespCommand.EXPIREAT => ConvertUtils.UnixTimestampInSecondsToTicks(expiration),
                _ => ConvertUtils.UnixTimestampInMillisecondsToTicks(expiration)
            };

            var expirationWithOption = new ExpirationWithOption(expirationTimeInTicks, expireOption);

            var input = new UnifiedStoreInput(RespCommand.EXPIRE, arg1: expirationWithOption.Word);
            var status = unifiedContext.RMW(key.ReadOnlySpan, ref input, ref unifiedOutput);

            if (status.IsPending)
                CompletePendingForUnifiedStoreSession(ref status, ref unifiedOutput, ref unifiedContext);

            timeoutSet = status.Found && ((OutputHeader*)unifiedOutput.SpanByteAndMemory.SpanByte.ToPointer())->result1 == 1;

            return status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        /// <inheritdoc/>
        public unsafe GarnetStatus RENAME(PinnedSpanByte oldKeySlice, PinnedSpanByte newKeySlice, bool withEtag)
            => RENAME(oldKeySlice, newKeySlice, false, out _, withEtag);

        /// <inheritdoc/>
        public unsafe GarnetStatus RENAMENX(PinnedSpanByte oldKeySlice, PinnedSpanByte newKeySlice, out int result, bool withEtag)
            => RENAME(oldKeySlice, newKeySlice, true, out result, withEtag);

        private unsafe GarnetStatus RENAME(PinnedSpanByte oldKeySlice, PinnedSpanByte newKeySlice, bool isNX, out int result, bool withEtag)
        {
            result = -1;

            // If same name check return early.
            if (oldKeySlice.ReadOnlySpan.SequenceEqual(newKeySlice.ReadOnlySpan))
            {
                result = 1;
                return GarnetStatus.OK;
            }

            // TODO verify the keys are in the same slot

            var createTransaction = false;
            if (txnManager.state != TxnState.Running)
            {
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Main | TransactionStoreTypes.Object);
                txnManager.SaveKeyEntryToLock(oldKeySlice, LockType.Exclusive);
                txnManager.SaveKeyEntryToLock(newKeySlice, LockType.Exclusive);
                _ = txnManager.Run(true);
            }

            var context = txnManager.UnifiedStoreTransactionalContext;
            var oldKey = oldKeySlice;
            var newKey = newKeySlice;

            var returnStatus = GarnetStatus.NOTFOUND;
            var abortTransaction = false;

            var output = new GarnetUnifiedStoreOutput();
            try
            {
                // Check if new key exists. This extra query isn't ideal, but it should be a rare operation and there's nowhere in Input to 
                // pass the srcLogRecord or even the ValueObject to RMW.
                if (isNX && EXISTS(newKey, ref context) != GarnetStatus.NOTFOUND)
                {
                    result = 0;             // This is the "oldkey was found" return
                    abortTransaction = true;
                    return GarnetStatus.OK;
                }

                // Set the input so Read knows to do the special "serialization" into output
                UnifiedStoreInput input = new(RespCommand.RENAME);
                var status = GET(oldKey, ref input, ref output, ref context);
                if (status != GarnetStatus.OK)
                {
                    abortTransaction = true;
                    return status;
                }

                if (status == GarnetStatus.OK)
                {
                    fixed (byte* recordPtr = output.SpanByteAndMemory.ReadOnlySpan)
                    {
                        // We have a record in in-memory, unserialized format, with its objects (if any) resolved to the TransientObjectIdMap.
                        var logRecord = new LogRecord(recordPtr, functionsState.transientObjectIdMap);

                        // The spec is that Expiration does not change. Set input ETag flag if requested.
                        input = new();
                        if (withEtag)
                            input.header.SetWithETagFlag();

                        status = SET(newKey, in logRecord, ref context);
                        if (status == GarnetStatus.OK)
                        {
                            result = 1;

                            // Delete the old key
                            _ = DELETE(oldKey, ref context);
                            return GarnetStatus.OK;
                        }
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
                output.Dispose();
            }
            return returnStatus;
        }
    }
}