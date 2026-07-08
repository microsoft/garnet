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
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            long ctx = default;
            var status = context.Read((FixedSpanByteKey)key, ref input, ref output, ctx);

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
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
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
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
            where TSourceLogRecord : ISourceLogRecord
        {
            _ = unifiedContext.Upsert((FixedSpanByteKey)key, ref input, in srcLogRecord);
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
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            // Prepare input
            var input = new UnifiedInput(RespCommand.EXISTS);

            // Prepare UnifiedOutput output
            var output = new UnifiedOutput();

            // TODO: The output is unused so optimize ReadMethods to not copy it.
            return Read_UnifiedStore(key, ref input, ref output, ref unifiedContext);
        }

        /// <summary>
        /// Deletes a key from the unified store context.
        /// </summary>
        /// <param name="key">The name of the key to use in the operation</param>
        /// <param name="unifiedContext">Basic unifiedContext for the unified store.</param>
        /// <returns></returns>
        public GarnetStatus DELETE<TUnifiedContext>(PinnedSpanByte key, ref TUnifiedContext unifiedContext)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = unifiedContext.Delete((FixedSpanByteKey)key);

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
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long,
                UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var input = new UnifiedInput(RespCommand.DELIFEXPIM);
            var status = unifiedContext.RMW((FixedSpanByteKey)key, ref input);
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
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
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
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
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
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
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
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            Span<byte> rmwOutput = stackalloc byte[sizeof(int)];
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
            var status = unifiedContext.RMW((FixedSpanByteKey)key, ref input, ref unifiedOutput);

            if (status.IsPending)
                CompletePendingForUnifiedStoreSession(ref status, ref unifiedOutput, ref unifiedContext);

            timeoutSet = status.Found &&
                         unifiedOutput.SpanByteAndMemory.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.RESP_RETURN_VAL_1);

            return status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        /// <summary>
        /// RENAME a key in the unified store context
        /// </summary>
        /// <param name="oldKeySlice">The key to rename</param>
        /// <param name="newKeySlice">The new key name</param>
        /// <returns></returns>
        public unsafe GarnetStatus RENAME(PinnedSpanByte oldKeySlice, PinnedSpanByte newKeySlice)
            => RENAME(oldKeySlice, newKeySlice, false, out _);

        /// <summary>
        /// RENAME a key in the unified store context - if the new key does not exist
        /// </summary>
        /// <param name="oldKeySlice">The key to rename</param>
        /// <param name="newKeySlice">The new key name</param>
        /// <param name="result">Number of renamed records</param>
        /// <returns></returns>
        public unsafe GarnetStatus RENAMENX(PinnedSpanByte oldKeySlice, PinnedSpanByte newKeySlice, out int result)
            => RENAME(oldKeySlice, newKeySlice, true, out result);

        /// <summary>
        /// RENAME a key in the unified store context
        /// </summary>
        /// <param name="oldKeySlice">The key to rename</param>
        /// <param name="newKeySlice">The new key name</param>
        /// <param name="isNX">If true, rename only if the new key does not exist</param>
        /// <param name="result">Number of renamed records</param>
        /// <returns></returns>
        private unsafe GarnetStatus RENAME(PinnedSpanByte oldKeySlice, PinnedSpanByte newKeySlice, bool isNX, out int result)
        {
            result = -1;

            // If same name check return early.
            if (oldKeySlice.ReadOnlySpan.SequenceEqual(newKeySlice.ReadOnlySpan))
            {
                result = 1;
                return GarnetStatus.OK;
            }

            // Note: RespServerSession.CanServeSlot has already verified the keys are in the same slot

            // Use transaction to prevent modifications to old and new keys while rename is in progress
            //
            // If we're already in a transaction, we get that for free
            var createTransaction = false;
            if (txnManager.state != TxnState.Running)
            {
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Main | TransactionStoreTypes.Object);
                txnManager.SaveKeyEntryToLock(oldKeySlice, LockType.Exclusive);
                txnManager.SaveKeyEntryToLock(newKeySlice, LockType.Exclusive);
                _ = txnManager.Run(true);
            }

            // Vector Sets complicate renames, there are 4 cases to consider:
            //  1. old and new key are NOT Vector Sets
            //     * We can copy old's record into new and then delete old
            //  2. old IS a Vector Set and new IS NOT a Vector Set
            //     * We mark old with suppress cleanup before we copy it into new key,
            //       then delete after the rename
            //  3. old IS NOT a Vector Set and new IS a Vector Set
            //     * We must explicitly delete new before copying old into new
            //       and then delete old
            //  4. old IS a Vector Set and new IS a Vector Set
            //     * First we perform an explicit delete of new (like in #3)
            //       then we do a suppress cleanup + copy as in #2
            //
            // One final subtlety: if the RecordType changes between old and new, we must
            // DELETE new before SET'ing no matter what

            var context = txnManager.UnifiedTransactionalContext;
            var oldKey = oldKeySlice;
            var newKey = newKeySlice;

            var abortTransaction = false;

            var output = new UnifiedOutput();
            try
            {
                // Check if new key exists.
                UnifiedInput input = new(RespCommand.RENAME);
                var status = GET(newKey, ref input, ref output, ref context);

                var newExists = status != GarnetStatus.NOTFOUND;
                if (isNX && newExists)
                {
                    result = 0;             // This is the "oldkey was found" return
                    abortTransaction = true;
                    return GarnetStatus.OK;
                }

                bool newIsVectorSet;
                if (newExists)
                {
                    fixed (byte* recordPtr = output.SpanByteAndMemory.ReadOnlySpan)
                    {
                        // We have a record in in-memory, unserialized format, with its objects (if any) resolved to the TransientObjectIdMap.
                        var logRecord = new LogRecord(recordPtr, functionsState.transientObjectIdMap);

                        newIsVectorSet = logRecord.RecordType == VectorManager.RecordType;
                    }
                }
                else
                {
                    newIsVectorSet = false;
                }

                status = GET(oldKey, ref input, ref output, ref context);
                if (status != GarnetStatus.OK)
                {
                    abortTransaction = true;
                    return status;
                }

                fixed (byte* recordPtr = output.SpanByteAndMemory.ReadOnlySpan)
                {
                    // We have a record in in-memory, unserialized format, with its objects (if any) resolved to the TransientObjectIdMap.
                    var logRecord = new LogRecord(recordPtr, functionsState.transientObjectIdMap);

                    var oldIsVectorSet = logRecord.RecordType == VectorManager.RecordType;

                    var needDeleteNewKey =
                        newIsVectorSet ||
                        (newExists && newIsVectorSet != oldIsVectorSet);

                    if (needDeleteNewKey)
                    {
                        // Case #3 or #4 - new key is a Vector Set and needs to be explicitly deleted
                        // OR
                        // The RecordType is changing, and so we can't just copy values and optionals over
                        _ = DELETE(newKey, ref context);
                    }

                    var suppressedCleanup = false;

                    if (oldIsVectorSet)
                    {
                        // Case #2 or #4 - old key is a Vector Set, so suppress cleanups on the coming delete

                        VectorManager.MarkSuppressCleanup(oldKey, ref stringTransactionalContext);
                        suppressedCleanup = true;
                    }

                    // Copy old record into new key - this happens in all 4 cases
                    status = SET(newKey, ref input, in logRecord, ref context);
                    if (status == GarnetStatus.OK)
                    {
                        result = 1;

                        if (oldIsVectorSet)
                        {
                            // Update hash slot after rename occurs - in cluster mode this is a no-op
                            // but in single node mode this data does need to be synced
                            functionsState.vectorManager.UpdateHashSlot(oldKey, newKey, logRecord.ValueSpan, ref vectorBasicContext);
                        }

                        // Delete the old key
                        _ = DELETE(oldKey, ref context);
                        return GarnetStatus.OK;
                    }

                    // If set failed for whatever reason, attempt to recover
                    if (suppressedCleanup)
                    {
                        VectorManager.ClearSuppressCleanup(oldKey, ref stringTransactionalContext);
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

            return GarnetStatus.NOTFOUND;
        }
    }
}