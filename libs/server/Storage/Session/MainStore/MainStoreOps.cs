﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    sealed partial class StorageSession : IDisposable
    {
        public GarnetStatus GET<TContext>(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            long ctx = default;
            var status = context.Read(key.ReadOnlySpan, ref input, ref output, ctx);

            if (status.IsPending)
            {
                StartPendingMetrics();
                CompletePendingForSession(ref status, ref output, ref context);
                StopPendingMetrics();
            }

            if (status.Found)
            {
                incr_session_found();
                return GarnetStatus.OK;
            }
            else
            {
                incr_session_notfound();
                return GarnetStatus.NOTFOUND;
            }
        }

        public unsafe GarnetStatus ReadWithUnsafeContext<TContext>(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, long localHeadAddress, out bool epochChanged, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>, IUnsafeContext
        {
            epochChanged = false;
            var status = context.Read(key.ReadOnlySpan, ref Unsafe.AsRef(in input), ref output, userContext: default);

            if (status.IsPending)
            {
                context.EndUnsafe();
                StartPendingMetrics();
                CompletePendingForSession(ref status, ref output, ref context);
                StopPendingMetrics();
                context.BeginUnsafe();
                // Start read of pointers from beginning if epoch changed
                if (HeadAddress == localHeadAddress)
                {
                    context.EndUnsafe();
                    epochChanged = true;
                }
            }
            else if (status.NotFound)
            {
                incr_session_notfound();
                return GarnetStatus.NOTFOUND;
            }
            else
            {
                incr_session_found();
            }

            return GarnetStatus.OK;
        }

        public unsafe GarnetStatus GET<TContext>(PinnedSpanByte key, out PinnedSpanByte value, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var input = new RawStringInput(RespCommand.GET);
            value = default;

            var _output = new SpanByteAndMemory { SpanByte = scratchBufferBuilder.ViewRemainingArgSlice() };

            var ret = GET(key, ref input, ref _output, ref context);
            if (ret == GarnetStatus.OK)
            {
                if (!_output.IsSpanByte)
                {
                    value = scratchBufferBuilder.FormatScratch(0, _output.ReadOnlySpan);
                    _output.Memory.Dispose();
                }
                else
                {
                    value = scratchBufferBuilder.CreateArgSlice(_output.Length);
                }
            }
            return ret;
        }

        public unsafe GarnetStatus GET<TContext>(PinnedSpanByte key, out MemoryResult<byte> value, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var input = new RawStringInput(RespCommand.GET);

            var _output = new SpanByteAndMemory();

            var ret = GET(key, ref input, ref _output, ref context);
            value = new MemoryResult<byte>(_output.Memory, _output.Length);
            return ret;
        }

        public GarnetStatus GET<TObjectContext>(PinnedSpanByte key, out GarnetObjectStoreOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            ObjectInput input = default;
            output = default;
            var status = objectContext.Read(key.ReadOnlySpan, ref input, ref output, userContext: default);

            if (status.IsPending)
            {
                StartPendingMetrics();
                CompletePendingForObjectStoreSession(ref status, ref output, ref objectContext);
                StopPendingMetrics();
            }

            if (status.Found)
            {
                incr_session_found();
                return GarnetStatus.OK;
            }
            else
            {
                incr_session_notfound();
                return GarnetStatus.NOTFOUND;
            }
        }

        public unsafe GarnetStatus GETEX<TContext>(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = context.RMW(key.ReadOnlySpan, ref input, ref output);

            if (status.IsPending)
            {
                StartPendingMetrics();
                CompletePendingForSession(ref status, ref output, ref context);
                StopPendingMetrics();
            }

            if (status.Found)
            {
                incr_session_found();
                return GarnetStatus.OK;
            }
            else
            {
                incr_session_notfound();
                return GarnetStatus.NOTFOUND;
            }
        }

        /// <summary>
        /// GETDEL command - Gets the value corresponding to the given key and deletes the key.
        /// </summary>
        /// <param name="key">The key to get the value for.</param>
        /// <param name="output">Span to allocate the output of the operation</param>
        /// <param name="context">Basic Context of the store</param>
        /// <returns> Operation status </returns>
        public unsafe GarnetStatus GETDEL<TContext>(PinnedSpanByte key, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var input = new RawStringInput(RespCommand.GETDEL);

            var status = context.RMW(key.ReadOnlySpan, ref input, ref output);
            Debug.Assert(output.IsSpanByte);

            if (status.IsPending)
                CompletePendingForSession(ref status, ref output, ref context);

            return status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        public unsafe GarnetStatus GETRANGE<TContext>(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = context.Read(key.ReadOnlySpan, ref input, ref output);

            if (status.IsPending)
            {
                StartPendingMetrics();
                CompletePendingForSession(ref status, ref output, ref context);
                StopPendingMetrics();
            }

            if (status.Found)
            {
                incr_session_found();
                return GarnetStatus.OK;
            }
            else
            {
                incr_session_notfound();
                return GarnetStatus.NOTFOUND;
            }
        }


        /// <summary>
        /// Returns the remaining time to live of a key that has a timeout.
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key">The key to get the remaining time to live in the store.</param>
        /// <param name="storeType">The store to operate on</param>
        /// <param name="output">Span to allocate the output of the operation</param>
        /// <param name="context">Basic Context of the store</param>
        /// <param name="objectContext">Object Context of the store</param>
        /// <param name="milliseconds">when true the command to execute is PTTL.</param>
        /// <returns></returns>
        public unsafe GarnetStatus TTL<TContext, TObjectContext>(PinnedSpanByte key, StoreType storeType, ref SpanByteAndMemory output, ref TContext context, ref TObjectContext objectContext, bool milliseconds = false)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var cmd = milliseconds ? RespCommand.PTTL : RespCommand.TTL;
            var input = new RawStringInput(cmd);

            if (storeType == StoreType.Main || storeType == StoreType.All)
            {
                var status = context.Read(key.ReadOnlySpan, ref input, ref output);

                if (status.IsPending)
                {
                    StartPendingMetrics();
                    CompletePendingForSession(ref status, ref output, ref context);
                    StopPendingMetrics();
                }

                if (status.Found)
                    return GarnetStatus.OK;
            }

            if ((storeType == StoreType.Object || storeType == StoreType.All) && !objectStoreBasicContext.IsNull)
            {
                var header = new RespInputHeader(milliseconds ? GarnetObjectType.PTtl : GarnetObjectType.Ttl);
                var objInput = new ObjectInput(header);

                var objO = new GarnetObjectStoreOutput(output);
                var status = objectContext.Read(key.ReadOnlySpan, ref objInput, ref objO);

                if (status.IsPending)
                    CompletePendingForObjectStoreSession(ref status, ref objO, ref objectContext);

                if (status.Found)
                {
                    output = objO.SpanByteAndMemory;
                    return GarnetStatus.OK;
                }
            }
            return GarnetStatus.NOTFOUND;
        }

        public GarnetStatus SET<TContext>(PinnedSpanByte key, PinnedSpanByte value, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            context.Upsert(key.ReadOnlySpan, value.ReadOnlySpan);
            return GarnetStatus.OK;
        }

        public GarnetStatus SET<TContext>(PinnedSpanByte key, ref RawStringInput input, PinnedSpanByte value, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var output = new SpanByteAndMemory();
            context.Upsert(key.ReadOnlySpan, ref input, value.ReadOnlySpan, ref output);
            return GarnetStatus.OK;
        }

        public unsafe GarnetStatus SET_Conditional<TContext>(PinnedSpanByte key, ref RawStringInput input, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            byte* pbOutput = stackalloc byte[8];
            var o = SpanByteAndMemory.FromPinnedPointer(pbOutput, 8);

            var status = context.RMW(key.ReadOnlySpan, ref input, ref o);

            if (status.IsPending)
            {
                StartPendingMetrics();
                CompletePendingForSession(ref status, ref o, ref context);
                StopPendingMetrics();
            }

            if (status.NotFound)
            {
                incr_session_notfound();
                return GarnetStatus.NOTFOUND;
            }
            else
            {
                incr_session_found();
                return GarnetStatus.OK;
            }
        }


        public unsafe GarnetStatus DEL_Conditional<TContext>(PinnedSpanByte key, ref RawStringInput input, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            Debug.Assert(input.header.cmd is RespCommand.DELIFGREATER or RespCommand.DELIFEXPIM);

            Span<byte> outputSpan = stackalloc byte[8];
            var output = SpanByteAndMemory.FromPinnedSpan(outputSpan);
            var status = context.RMW(key, ref input, ref output);

            if (status.IsPending)
            {
                StartPendingMetrics();
                CompletePendingForSession(ref status, ref output, ref context);
                StopPendingMetrics();
            }

            // Deletions in RMW are done by expiring the record, hence we use expiration as the indicator of success.
            if (status.Expired)
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

        public unsafe GarnetStatus SET_Conditional<TContext>(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = context.RMW(key.ReadOnlySpan, ref input, ref output);

            if (status.IsPending)
            {
                StartPendingMetrics();
                CompletePendingForSession(ref status, ref output, ref context);
                StopPendingMetrics();
            }

            if (status.NotFound)
            {
                incr_session_notfound();
                return GarnetStatus.NOTFOUND;
            }
            else
            {
                incr_session_found();
                return GarnetStatus.OK;
            }
        }

        internal GarnetStatus MSET_Conditional<TContext>(ref RawStringInput input, ref TContext ctx)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var error = false;
            var count = input.parseState.Count;

            var createTransaction = false;
            if (txnManager.state != TxnState.Running)
            {
                createTransaction = true;
                for (var i = 0; i < count; i += 2)
                {
                    var srcKey = input.parseState.GetArgSliceByRef(i);
                    txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Main | TransactionStoreTypes.Unified);
                    txnManager.SaveKeyEntryToLock(srcKey, LockType.Exclusive);
                }
                txnManager.Run(true);
            }

            var context = txnManager.TransactionalContext;
            var unifiedContext = txnManager.UnifiedStoreTransactionalContext;

            try
            {
                for (var i = 0; i < count; i += 2)
                {
                    var srcKey = input.parseState.GetArgSliceByRef(i);
                    var status = EXISTS(srcKey, ref unifiedContext);
                    if (status != GarnetStatus.NOTFOUND)
                    {
                        count = 0;
                        error = true;
                    }
                }

                for (var i = 0; i < count; i += 2)
                {
                    var srcKey = input.parseState.GetArgSliceByRef(i);
                    var srcVal = input.parseState.GetArgSliceByRef(i + 1);
                    SET(srcKey, srcVal, ref context);
                }
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }

            return error ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        public GarnetStatus SET<TObjectContext>(PinnedSpanByte key, IGarnetObject value, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            objectContext.Upsert(key.ReadOnlySpan, value);
            return GarnetStatus.OK;
        }

        public GarnetStatus SET<TContext>(PinnedSpanByte key, Memory<byte> value, ref TContext context)   // TODO are memory<byte> overloads needed?
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            unsafe
            {
                fixed (byte* ptr = value.Span)
                    context.Upsert(key.ReadOnlySpan, new ReadOnlySpan<byte>(ptr, value.Length));
            }
            return GarnetStatus.OK;
        }

        public GarnetStatus SET_Main<TContext, TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            where TSourceLogRecord : ISourceLogRecord
        {
            context.Upsert(in srcLogRecord);
            return GarnetStatus.OK;
        }

        public GarnetStatus SET_Object<TObjectContext, TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            where TSourceLogRecord : ISourceLogRecord
        {
            objectContext.Upsert(in srcLogRecord);
            return GarnetStatus.OK;
        }

        public unsafe GarnetStatus SETEX<TContext>(PinnedSpanByte key, PinnedSpanByte value, PinnedSpanByte expiryMs, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            => SETEX(key, value, TimeSpan.FromMilliseconds(NumUtils.ReadInt64(expiryMs.Length, expiryMs.ToPointer())), ref context);

        public GarnetStatus SETEX<TContext>(PinnedSpanByte key, PinnedSpanByte value, TimeSpan expiry, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var input = new RawStringInput(RespCommand.APPEND, ref parseState, arg1: DateTimeOffset.UtcNow.Ticks + expiry.Ticks);
            return SET(key, ref input, value, ref context);
        }

        /// <summary>
        /// APPEND command - appends value at the end of existing string
        /// </summary>
        /// <typeparam name="TContext">Context type</typeparam>
        /// <param name="key">Key whose value is to be appended</param>
        /// <param name="value">Value to be appended</param>
        /// <param name="output">Length of updated value</param>
        /// <param name="context">Store context</param>
        /// <returns>Operation status</returns>
        public unsafe GarnetStatus APPEND<TContext>(PinnedSpanByte key, PinnedSpanByte value, ref PinnedSpanByte output, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var _output = new SpanByteAndMemory(output);

            parseState.InitializeWithArgument(value);
            var input = new RawStringInput(RespCommand.APPEND, ref parseState);

            return APPEND(key, ref input, ref _output, ref context);
        }

        /// <summary>
        /// APPEND command - appends value at the end of existing string
        /// </summary>
        /// <typeparam name="TContext">Context type</typeparam>
        /// <param name="key">Key whose value is to be appended</param>
        /// <param name="input">Input for main store</param>
        /// <param name="output">Length of updated value</param>
        /// <param name="context">Store context</param>
        /// <returns>Operation status</returns>
        public unsafe GarnetStatus APPEND<TContext>(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = context.RMW(key.ReadOnlySpan, ref input, ref output);
            if (status.IsPending)
            {
                StartPendingMetrics();
                CompletePendingForSession(ref status, ref output, ref context);
                StopPendingMetrics();
            }

            Debug.Assert(output.IsSpanByte);

            return GarnetStatus.OK;
        }

        /// <summary>
        /// Deletes a key from the main store context.
        /// </summary>
        /// <param name="key">The name of the key to use in the operation</param>
        /// <param name="context">Basic context for the main store.</param>
        /// <returns></returns>
        public GarnetStatus DELETE_MainStore<TContext>(PinnedSpanByte key, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = context.Delete(key.ReadOnlySpan);
            Debug.Assert(!status.IsPending);
            return status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        public unsafe GarnetStatus RENAME(PinnedSpanByte oldKeySlice, PinnedSpanByte newKeySlice, StoreType storeType, bool withEtag)
            => RENAME(oldKeySlice, newKeySlice, storeType, false, out _, withEtag);

        /// <summary>
        /// Renames key to newkey if newkey does not yet exist. It returns an error when key does not exist.
        /// </summary>
        /// <param name="oldKeySlice">The old key to be renamed.</param>
        /// <param name="newKeySlice">The new key name.</param>
        /// <param name="storeType">The type of store to perform the operation on.</param>
        /// <returns></returns>
        public unsafe GarnetStatus RENAMENX(PinnedSpanByte oldKeySlice, PinnedSpanByte newKeySlice, StoreType storeType, out int result, bool withEtag)
            => RENAME(oldKeySlice, newKeySlice, storeType, true, out result, withEtag);

        private unsafe GarnetStatus RENAME(PinnedSpanByte oldKeySlice, PinnedSpanByte newKeySlice, StoreType storeType, bool isNX, out int result, bool withEtag)
        {
            RawStringInput input = default;
            var returnStatus = GarnetStatus.NOTFOUND;
            result = -1;

            // If same name check return early.
            if (oldKeySlice.ReadOnlySpan.SequenceEqual(newKeySlice.ReadOnlySpan))
            {
                result = 1;
                return GarnetStatus.OK;
            }

            var createTransaction = false;
            if (txnManager.state != TxnState.Running)
            {
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Main | TransactionStoreTypes.Object);
                txnManager.SaveKeyEntryToLock(oldKeySlice, LockType.Exclusive);
                txnManager.SaveKeyEntryToLock(newKeySlice, LockType.Exclusive);
                _ = txnManager.Run(true);
            }

            var context = txnManager.TransactionalContext;
            var objectContext = txnManager.ObjectStoreTransactionalContext;
            var oldKey = oldKeySlice;

            // TODO: This needs to be converted to a form of GET that returns all information in the (Disk)LogRecord, perhaps serializing it to the output, and then
            // inserts with that record.

            if (storeType == StoreType.Main || storeType == StoreType.All)
            {
                try
                {
                    var newKey = newKeySlice;

                    var o = new SpanByteAndMemory();
                    var status = GET(oldKey, ref input, ref o, ref context);

                    if (status == GarnetStatus.OK)
                    {
                        Debug.Assert(!o.IsSpanByte);
                        var memoryHandle = o.Memory.Memory.Pin();
                        var ptrVal = (byte*)memoryHandle.Pointer;

                        _ = RespReadUtils.TryReadUnsignedLengthHeader(out var headerLength, ref ptrVal, ptrVal + o.Length);

                        // Find expiration time of the old key
                        var expireSpan = new SpanByteAndMemory();
                        var ttlStatus = TTL(oldKey, storeType, ref expireSpan, ref context, ref objectContext, true);
                         
                        if (ttlStatus == GarnetStatus.OK && !expireSpan.IsSpanByte)
                        {
                            var newValSlice = PinnedSpanByte.FromPinnedPointer(ptrVal, headerLength);

                            using var expireMemoryHandle = expireSpan.Memory.Memory.Pin();
                            var expirePtrVal = (byte*)expireMemoryHandle.Pointer;
                            _ = RespReadUtils.TryReadInt64(out var expireTimeMs, ref expirePtrVal, expirePtrVal + expireSpan.Length, out var _);

                            input = isNX ? new RawStringInput(RespCommand.SETEXNX) : new RawStringInput(RespCommand.SET);

                            // If the key has an expiration, set the new key with the expiration
                            if (expireTimeMs > 0)
                            {
                                if (!withEtag && !isNX)
                                {
                                    SETEX(newKeySlice, newValSlice, TimeSpan.FromMilliseconds(expireTimeMs), ref context);
                                }
                                else
                                {
                                    // Move payload forward to make space for RespInputHeader and Metadata
                                    parseState.InitializeWithArgument(newValSlice);
                                    input.parseState = parseState;
                                    input.arg1 = DateTimeOffset.UtcNow.Ticks + TimeSpan.FromMilliseconds(expireTimeMs).Ticks;

                                    if (withEtag)
                                        input.header.SetWithETagFlag();

                                    var setStatus = SET_Conditional(newKey, ref input, ref context);
                                    if (isNX)
                                    {
                                        // For SET NX `NOTFOUND` means the operation succeeded
                                        result = setStatus == GarnetStatus.NOTFOUND ? 1 : 0;
                                        returnStatus = GarnetStatus.OK;
                                    }
                                    SETEX(newKeySlice, newValSlice, TimeSpan.FromMilliseconds(expireTimeMs), ref context);
                                }
                            }
                            else if (expireTimeMs == -1) // Its possible to have expireTimeMs as 0 (Key expired or will be expired now) or -2 (Key does not exist), in those cases we don't SET the new key
                            {
                                if (!withEtag && !isNX)
                                    SET(newKey, newValSlice, ref context);
                                else
                                {
                                    // Build parse state
                                    parseState.InitializeWithArgument(newValSlice);
                                    input.parseState = parseState;

                                    if (withEtag)
                                        input.header.SetWithETagFlag();

                                    var setStatus = SET_Conditional(newKey, ref input, ref context);

                                    if (isNX)
                                    {
                                        // For SET NX `NOTFOUND` means the operation succeeded
                                        result = setStatus == GarnetStatus.NOTFOUND ? 1 : 0;
                                        returnStatus = GarnetStatus.OK;
                                    }
                                }
                            }

                            expireSpan.Memory.Dispose();
                            memoryHandle.Dispose();
                            o.Memory.Dispose();

                            // Delete the old key only when SET NX succeeded
                            if (isNX && result == 1)
                            {
                                DELETE_MainStore(oldKey, ref context);
                            }
                            else if (!isNX)
                            {
                                // Delete the old key
                                DELETE_MainStore(oldKey, ref context);
                                returnStatus = GarnetStatus.OK;
                            }
                        }
                    }
                }
                finally
                {
                    if (createTransaction)
                        txnManager.Commit(true);
                }
            }

            if ((storeType == StoreType.Object || storeType == StoreType.All) && !objectStoreBasicContext.IsNull)
            {
                createTransaction = false;
                if (txnManager.state != TxnState.Running)
                {
                    txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Object);
                    txnManager.SaveKeyEntryToLock(oldKeySlice, LockType.Exclusive);
                    txnManager.SaveKeyEntryToLock(newKeySlice, LockType.Exclusive);
                    txnManager.Run(true);
                    createTransaction = true;
                }

                try
                {
                    var status = GET(oldKeySlice, out var value, ref objectContext);

                    if (status == GarnetStatus.OK)
                    {
                        var valObj = value.GarnetObject;

                        returnStatus = GarnetStatus.OK;
                        var canSetAndDelete = true;
                        if (isNX)
                        {
                            // Not using EXISTS method to avoid new allocation of Array for key
                            var getNewStatus = GET(newKeySlice, out _, ref objectContext);
                            canSetAndDelete = getNewStatus == GarnetStatus.NOTFOUND;
                        }

                        if (canSetAndDelete)
                        {
                            // valObj already has expiration time, so no need to write expiration logic here. TODO: No longer true; this is now a LogRecord attribute and must be SETEX'd
                            SET(newKeySlice, valObj, ref objectContext);

                            // Delete the old key
                            DELETE_ObjectStore(oldKeySlice, ref objectContext);

                            result = 1;
                        }
                        else
                        {
                            result = 0;
                        }
                    }
                }
                finally
                {
                    if (createTransaction)
                        txnManager.Commit(true);
                }
            }
            return returnStatus;
        }

        /// <summary>
        /// For existing keys - overwrites part of the value at a specified offset (in-place if possible)
        /// For non-existing keys - creates a new string with the value at a specified offset (padded with '\0's)
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <param name="key">The key for which to set the range</param>
        /// <param name="input">Input for the main store</param>
        /// <param name="output">The length of the updated string</param>
        /// <param name="context">Basic context for the main store</param>
        /// <returns></returns>
        public unsafe GarnetStatus SETRANGE<TContext>(PinnedSpanByte key, ref RawStringInput input, ref PinnedSpanByte output, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            SpanByteAndMemory sbmOut = new(output);

            var status = context.RMW(key.ReadOnlySpan, ref input, ref sbmOut);
            if (status.IsPending)
                CompletePendingForSession(ref status, ref sbmOut, ref context);

            Debug.Assert(sbmOut.IsSpanByte);
            output.Length = sbmOut.Length;

            return GarnetStatus.OK;
        }

        public GarnetStatus Increment<TContext>(PinnedSpanByte key, ref RawStringInput input, ref PinnedSpanByte output, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            SpanByteAndMemory _output = new(output);

            var status = context.RMW(key.ReadOnlySpan, ref input, ref _output);
            if (status.IsPending)
                CompletePendingForSession(ref status, ref _output, ref context);
            Debug.Assert(_output.IsSpanByte);
            output.Length = _output.Length;
            return GarnetStatus.OK;
        }

        public unsafe GarnetStatus Increment<TContext>(PinnedSpanByte key, out long output, long increment, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var cmd = RespCommand.INCRBY;
            if (increment < 0)
            {
                cmd = RespCommand.DECRBY;
                increment = -increment;
            }

            var input = new RawStringInput(cmd, 0, increment);

            const int outputBufferLength = NumUtils.MaximumFormatInt64Length + 1;
            var outputBuffer = stackalloc byte[outputBufferLength];

            var _output = SpanByteAndMemory.FromPinnedPointer(outputBuffer, outputBufferLength);

            var status = context.RMW(key.ReadOnlySpan, ref input, ref _output);
            if (status.IsPending)
                CompletePendingForSession(ref status, ref _output, ref context);

            Debug.Assert(_output.IsSpanByte);

            output = NumUtils.ReadInt64(_output.Length, outputBuffer);
            return GarnetStatus.OK;
        }

        public void WATCH(PinnedSpanByte key, StoreType type)
        {
            txnManager.AddTransactionStoreType(type);
            txnManager.Watch(key);
        }

        public unsafe GarnetStatus SCAN<TContext>(long cursor, PinnedSpanByte match, long count, ref TContext context) => GarnetStatus.OK;

        /// <summary>
        /// Computes the Longest Common Subsequence (LCS) of two keys.
        /// </summary>
        /// <param name="key1">The first key to compare.</param>
        /// <param name="key2">The second key to compare.</param>
        /// <param name="output">The output span to store the result.</param>
        /// <param name="lenOnly">If true, only the length of the LCS is returned.</param>
        /// <param name="withIndices">If true, the indices of the LCS in both keys are returned.</param>
        /// <param name="withMatchLen">If true, the length of each match is returned.</param>
        /// <param name="minMatchLen">The minimum length of a match to be considered.</param>
        /// <returns>The status of the operation.</returns>
        public unsafe GarnetStatus LCS(PinnedSpanByte key1, PinnedSpanByte key2, ref SpanByteAndMemory output, bool lenOnly = false, bool withIndices = false, bool withMatchLen = false, int minMatchLen = 0)
        {
            var createTransaction = false;
            if (txnManager.state != TxnState.Running)
            {
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Main);
                txnManager.SaveKeyEntryToLock(key1, LockType.Shared);
                txnManager.SaveKeyEntryToLock(key2, LockType.Shared);
                txnManager.Run(true);
                createTransaction = true;
            }

            var context = txnManager.TransactionalContext;
            try
            {
                var status = LCSInternal(key1, key2, ref output, ref context, lenOnly, withIndices, withMatchLen, minMatchLen);
                return status;
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }
        }

        private unsafe GarnetStatus LCSInternal<TContext>(PinnedSpanByte key1, PinnedSpanByte key2, ref SpanByteAndMemory output, ref TContext context, bool lenOnly = false, bool withIndices = false, bool withMatchLen = false, int minMatchLen = 0)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            PinnedSpanByte val1, val2;
            var status1 = GET(key1, out val1, ref context);
            var status2 = GET(key2, out val2, ref context);

            var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output);

            try
            {
                if (lenOnly)
                {
                    if (status1 != GarnetStatus.OK || status2 != GarnetStatus.OK)
                    {
                        writer.WriteInt32(0);
                        return GarnetStatus.OK;
                    }

                    var len = ComputeLCSLength(val1.ReadOnlySpan, val2.ReadOnlySpan, minMatchLen);
                    writer.WriteInt32(len);
                }
                else if (withIndices)
                {
                    List<LCSMatch> matches;
                    int len;
                    if (status1 != GarnetStatus.OK || status2 != GarnetStatus.OK)
                    {
                        matches = [];
                        len = 0;
                    }
                    else
                    {
                        matches = ComputeLCSWithIndices(val1.ReadOnlySpan, val2.ReadOnlySpan, minMatchLen, out len);
                    }

                    WriteLCSMatches(matches, withMatchLen, len, ref writer);
                }
                else
                {
                    if (status1 != GarnetStatus.OK || status2 != GarnetStatus.OK)
                    {
                        writer.WriteDirect(CmdStrings.RESP_EMPTY);
                        return GarnetStatus.OK;
                    }

                    var lcs = ComputeLCS(val1.ReadOnlySpan, val2.ReadOnlySpan, minMatchLen);
                    writer.WriteBulkString(lcs);
                }

                return GarnetStatus.OK;
            }
            finally
            {
                writer.Dispose();
            }
        }

        private static int ComputeLCSLength(ReadOnlySpan<byte> str1, ReadOnlySpan<byte> str2, int minMatchLen)
        {
            var m = str1.Length;
            var n = str2.Length;
            var dp = GetLcsDpTable(str1, str2);

            return dp[m, n] >= minMatchLen ? dp[m, n] : 0;
        }

        private static List<LCSMatch> ComputeLCSWithIndices(ReadOnlySpan<byte> str1, ReadOnlySpan<byte> str2, int minMatchLen, out int lcsLength)
        {
            var m = str1.Length;
            var n = str2.Length;
            var dp = GetLcsDpTable(str1, str2);

            lcsLength = dp[m, n];

            var matches = new List<LCSMatch>();
            // Backtrack to find matches
            if (dp[m, n] >= minMatchLen)
            {
                int i = m, j = n;
                var currentMatch = new List<(int, int)>();

                while (i > 0 && j > 0)
                {
                    if (str1[i - 1] == str2[j - 1])
                    {
                        currentMatch.Insert(0, (i - 1, j - 1));
                        i--; j--;
                    }
                    else if (dp[i - 1, j] > dp[i, j - 1])
                        i--;
                    else
                        j--;
                }

                // Convert consecutive matches into LCSMatch objects
                if (currentMatch.Count > 0)
                {
                    int start = 0;
                    for (int k = 1; k <= currentMatch.Count; k++)
                    {
                        if (k == currentMatch.Count ||
                            currentMatch[k].Item1 != currentMatch[k - 1].Item1 + 1 ||
                            currentMatch[k].Item2 != currentMatch[k - 1].Item2 + 1)
                        {
                            int length = k - start;
                            if (length >= minMatchLen)
                            {
                                matches.Add(new LCSMatch
                                {
                                    Start1 = currentMatch[start].Item1,
                                    Start2 = currentMatch[start].Item2,
                                    Length = length
                                });
                            }
                            start = k;
                        }
                    }
                }
            }

            matches.Reverse();

            return matches;
        }

        private static unsafe void WriteLCSMatches(List<LCSMatch> matches, bool withMatchLen, int lcsLength,
                                                   ref RespMemoryWriter writer)
        {
            writer.WriteMapLength(2);

            // Write "matches" section identifier
            writer.WriteBulkString(CmdStrings.matches);

            // Write matches array
            writer.WriteArrayLength(matches.Count);

            foreach (var match in matches)
            {
                writer.WriteArrayLength(withMatchLen ? 3 : 2);

                writer.WriteArrayLength(2);

                writer.WriteInt32(match.Start1);
                writer.WriteInt32(match.Start1 + match.Length - 1);

                writer.WriteArrayLength(2);

                writer.WriteInt32(match.Start2);
                writer.WriteInt32(match.Start2 + match.Length - 1);

                if (withMatchLen)
                {
                    writer.WriteInt32(match.Length);
                }
            }

            // Write "len" section identifier
            writer.WriteBulkString(CmdStrings.len);

            // Write LCS length
            writer.WriteInt32(lcsLength);
        }

        private static byte[] ComputeLCS(ReadOnlySpan<byte> str1, ReadOnlySpan<byte> str2, int minMatchLen)
        {
            var m = str1.Length;
            var n = str2.Length;
            var dp = GetLcsDpTable(str1, str2);

            // If result is shorter than minMatchLen, return empty array
            if (dp[m, n] < minMatchLen)
                return [];

            // Backtrack to build the LCS
            var result = new byte[dp[m, n]];
            int index = dp[m, n] - 1;
            int k = m, l = n;

            while (k > 0 && l > 0)
            {
                if (str1[k - 1] == str2[l - 1])
                {
                    result[index] = str1[k - 1];
                    k--; l--; index--;
                }
                else if (dp[k - 1, l] > dp[k, l - 1])
                    k--;
                else
                    l--;
            }

            return result;
        }

        private static int[,] GetLcsDpTable(ReadOnlySpan<byte> str1, ReadOnlySpan<byte> str2)
        {
            var m = str1.Length;
            var n = str2.Length;
            var dp = new int[m + 1, n + 1];
            for (int i = 1; i <= m; i++)
            {
                for (int j = 1; j <= n; j++)
                {
                    if (str1[i - 1] == str2[j - 1])
                        dp[i, j] = dp[i - 1, j - 1] + 1;
                    else
                        dp[i, j] = Math.Max(dp[i - 1, j], dp[i, j - 1]);
                }
            }
            return dp;
        }

        private struct LCSMatch
        {
            public int Start1;
            public int Start2;
            public int Length;
        }
    }
}