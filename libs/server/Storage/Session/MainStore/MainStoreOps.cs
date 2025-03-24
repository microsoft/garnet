﻿d// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using ObjectStoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    sealed partial class StorageSession : IDisposable
    {
        public GarnetStatus GET<TContext>(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
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
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>, IUnsafeContext
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
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            var input = new RawStringInput(RespCommand.GET);
            value = default;

            var _output = new SpanByteAndMemory { SpanByte = scratchBufferManager.ViewRemainingArgSlice().SpanByte };

            var ret = GET(key, ref input, ref _output, ref context);
            if (ret == GarnetStatus.OK)
            {
                if (!_output.IsSpanByte)
                {
                    value = scratchBufferManager.FormatScratch(0, _output.ReadOnlySpan());
                    _output.Memory.Dispose();
                }
                else
                {
                    value = scratchBufferManager.CreateArgSlice(_output.Length);
                }
            }
            return ret;
        }

        public unsafe GarnetStatus GET<TContext>(PinnedSpanByte key, out MemoryResult<byte> value, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            var input = new RawStringInput(RespCommand.GET);

            var _output = new SpanByteAndMemory();

            var ret = GET(key, ref input, ref _output, ref context);
            value = new MemoryResult<byte>(_output.Memory, _output.Length);
            return ret;
        }

        public GarnetStatus GET<TObjectContext>(PinnedSpanByte key, out GarnetObjectStoreOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
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
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
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
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            var input = new RawStringInput(RespCommand.GETDEL);

            var status = context.RMW(key.ReadOnlySpan, ref input, ref output);
            Debug.Assert(output.IsSpanByte);

            if (status.IsPending)
                CompletePendingForSession(ref status, ref output, ref context);

            return status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        public unsafe GarnetStatus GETRANGE<TContext>(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
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
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
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

                var objO = new GarnetObjectStoreOutput { SpanByteAndMemory = output };
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

        /// <summary>
        /// Get the absolute Unix timestamp at which the given key will expire.
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key">The key to get the Unix timestamp.</param>
        /// <param name="storeType">The store to operate on</param>
        /// <param name="output">Span to allocate the output of the operation</param>
        /// <param name="context">Basic Context of the store</param>
        /// <param name="objectContext">Object Context of the store</param>
        /// <param name="milliseconds">when true the command to execute is PEXPIRETIME.</param>
        /// <returns>Returns the absolute Unix timestamp (since January 1, 1970) in seconds or milliseconds at which the given key will expire.</returns>
        public unsafe GarnetStatus EXPIRETIME<TContext, TObjectContext>(PinnedSpanByte key, StoreType storeType, ref SpanByteAndMemory output, ref TContext context, ref TObjectContext objectContext, bool milliseconds = false)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            if (storeType == StoreType.Main || storeType == StoreType.All)
            {
                var cmd = milliseconds ? RespCommand.PEXPIRETIME : RespCommand.EXPIRETIME;
                var input = new RawStringInput(cmd);
                var status = context.Read(key.ReadOnlySpan, ref input, ref output);

                if (status.IsPending)
                {
                    StartPendingMetrics();
                    CompletePendingForSession(ref status, ref output, ref context);
                    StopPendingMetrics();
                }

                if (status.Found) return GarnetStatus.OK;
            }

            if ((storeType == StoreType.Object || storeType == StoreType.All) && !objectStoreBasicContext.IsNull)
            {
                var type = milliseconds ? GarnetObjectType.PExpireTime : GarnetObjectType.ExpireTime;
                var header = new RespInputHeader(type);
                var input = new ObjectInput(header);

                var objO = new GarnetObjectStoreOutput { SpanByteAndMemory = output };
                var status = objectContext.Read(key.ReadOnlySpan, ref input, ref objO);

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
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            context.Upsert(key.ReadOnlySpan, value.ReadOnlySpan);
            return GarnetStatus.OK;
        }

        public GarnetStatus SET<TContext>(PinnedSpanByte key, ref RawStringInput input, PinnedSpanByte value, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            var output = new SpanByteAndMemory();
            context.Upsert(key.ReadOnlySpan, ref input, value.ReadOnlySpan, ref output);
            return GarnetStatus.OK;
        }

        public unsafe GarnetStatus SET_Conditional<TContext>(PinnedSpanByte key, ref RawStringInput input, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            byte* pbOutput = stackalloc byte[8];
            var o = new SpanByteAndMemory(pbOutput, 8);

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

        public unsafe GarnetStatus SET_Conditional<TContext>(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
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
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
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
                    txnManager.SaveKeyEntryToLock(srcKey, false, LockType.Exclusive);
                    txnManager.SaveKeyEntryToLock(srcKey, true, LockType.Exclusive);
                }
                txnManager.Run(true);
            }

            var context = txnManager.TransactionalContext;
            var objContext = txnManager.ObjectStoreTransactionalContext;

            try
            {
                for (var i = 0; i < count; i += 2)
                {
                    var srcKey = input.parseState.GetArgSliceByRef(i);
                    var status = EXISTS(srcKey, StoreType.All, ref context, ref objContext);
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
            where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            objectContext.Upsert(key.ReadOnlySpan, value);
            return GarnetStatus.OK;
        }

        public GarnetStatus SET<TContext>(PinnedSpanByte key, Memory<byte> value, ref TContext context)   // TODO are memory<byte> overloads needed?
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            unsafe
            {
                fixed (byte* ptr = value.Span)
                    context.Upsert(key.ReadOnlySpan, new ReadOnlySpan<byte>(ptr, value.Length));
            }
            return GarnetStatus.OK;
        }

        public unsafe GarnetStatus SETEX<TContext>(PinnedSpanByte key, PinnedSpanByte value, PinnedSpanByte expiryMs, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            => SETEX(key, value, TimeSpan.FromMilliseconds(NumUtils.ReadInt64(expiryMs.Length, expiryMs.ToPointer())), ref context);

        public GarnetStatus SETEX<TContext>(PinnedSpanByte key, PinnedSpanByte value, TimeSpan expiry, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            parseState.InitializeWithArgument(value);
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
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
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
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
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

        public GarnetStatus DELETE<TContext, TObjectContext>(PinnedSpanByte key, StoreType storeType, ref TContext context, ref TObjectContext objectContext)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            var found = false;

            if (storeType == StoreType.Main || storeType == StoreType.All)
            {
                var status = context.Delete(key.ReadOnlySpan);
                Debug.Assert(!status.IsPending);
                if (status.Found) found = true;
            }

            if (!objectStoreBasicContext.IsNull && (storeType == StoreType.Object || storeType == StoreType.All))
            {
                var status = objectContext.Delete(key.ReadOnlySpan);
                Debug.Assert(!status.IsPending);
                if (status.Found) found = true;
            }
            return found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
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
                txnManager.SaveKeyEntryToLock(oldKeySlice, false, LockType.Exclusive);
                txnManager.SaveKeyEntryToLock(newKeySlice, false, LockType.Exclusive);
                _ = txnManager.Run(true);
            }

            var context = txnManager.TransactionalContext;
            var objectContext = txnManager.ObjectStoreTransactionalContext;
            var oldKey = oldKeySlice.ReadOnlySpan;

            if (storeType == StoreType.Main || storeType == StoreType.All)
            {
                try
                {
                    var newKey = newKeySlice.ReadOnlySpan;

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
                                    SET(newKey, newValSlice.ReadOnlySpan, ref context);
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
                                DELETE(oldKey, StoreType.Main, ref context, ref objectContext);
                            }
                            else if (!isNX)
                            {
                                // Delete the old key
                                DELETE(oldKey, StoreType.Main, ref context, ref objectContext);
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
                    txnManager.SaveKeyEntryToLock(oldKeySlice, true, LockType.Exclusive);
                    txnManager.SaveKeyEntryToLock(newKeySlice, true, LockType.Exclusive);
                    txnManager.Run(true);
                    createTransaction = true;
                }

                try
                {
                    var status = GET(oldKeySlice.ReadOnlySpan, out var value, ref objectContext);

                    if (status == GarnetStatus.OK)
                    {
                        var valObj = value.GarnetObject;

                        returnStatus = GarnetStatus.OK;
                        var canSetAndDelete = true;
                        if (isNX)
                        {
                            // Not using EXISTS method to avoid new allocation of Array for key
                            var getNewStatus = GET(newKeySlice.ReadOnlySpan, out _, ref objectContext);
                            canSetAndDelete = getNewStatus == GarnetStatus.NOTFOUND;
                        }

                        if (canSetAndDelete)
                        {
                            // valObj already has expiration time, so no need to write expiration logic here. TODO: No longer true; this is now a LogRecord attribute and must be SETEX'd
                            SET(newKeySlice.ReadOnlySpan, valObj, ref objectContext);

                            // Delete the old key
                            DELETE(oldKeySlice.ReadOnlySpan, StoreType.Object, ref context, ref objectContext);

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
        /// Returns if key is an existing one in the store.
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key">The name of the key to use in the operation</param>
        /// <param name="storeType">The store to operate on.</param>
        /// <param name="context">Basic context for the main store.</param>
        /// <param name="objectContext">Object context for the object store.</param>
        /// <returns></returns>
        public GarnetStatus EXISTS<TContext, TObjectContext>(PinnedSpanByte key, StoreType storeType, ref TContext context, ref TObjectContext objectContext)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            var status = GarnetStatus.NOTFOUND;
            RawStringInput input = default;

            if (storeType == StoreType.Main || storeType == StoreType.All)
            {
                var _output = new SpanByteAndMemory { SpanByte = scratchBufferManager.ViewRemainingArgSlice().SpanByte };
                status = GET(key, ref input, ref _output, ref context);

                if (status == GarnetStatus.OK)
                {
                    if (!_output.IsSpanByte)
                        _output.Memory.Dispose();
                    return status;
                }
            }

            if ((storeType == StoreType.Object || storeType == StoreType.All) && !objectStoreBasicContext.IsNull)
            {
                status = GET(key, out _, ref objectContext);
            }

            return status;
        }

        /// <summary>
        /// Set a timeout on key
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key">The key to set the timeout on.</param>
        /// <param name="expiryMs">Milliseconds value for the timeout.</param>
        /// <param name="timeoutSet">True when the timeout was properly set.</param>
        /// <param name="storeType">The store to operate on.</param>
        /// <param name="expireOption">>Flags to use for the operation.</param>
        /// <param name="context">Basic context for the main store.</param>
        /// <param name="objectStoreContext">Object context for the object store.</param>
        /// <returns></returns>
        public unsafe GarnetStatus EXPIRE<TContext, TObjectContext>(PinnedSpanByte key, PinnedSpanByte expiryMs, out bool timeoutSet, StoreType storeType, ExpireOption expireOption, ref TContext context, ref TObjectContext objectStoreContext)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => EXPIRE(key, TimeSpan.FromMilliseconds(NumUtils.ReadInt64(expiryMs.Length, expiryMs.ToPointer())), out timeoutSet, storeType, expireOption, ref context, ref objectStoreContext);

        /// <summary>
        /// Set a timeout on key.
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key">The key to set the timeout on.</param>
        /// <param name="input">Input for the main store</param>
        /// <param name="timeoutSet">True when the timeout was properly set.</param>
        /// <param name="storeType">The store to operate on.</param>
        /// <param name="context">Basic context for the main store</param>
        /// <param name="objectStoreContext">Object context for the object store</param>
        /// <returns></returns>
        public unsafe GarnetStatus EXPIRE<TContext, TObjectContext>(PinnedSpanByte key, ref RawStringInput input, out bool timeoutSet, StoreType storeType, ref TContext context, ref TObjectContext objectStoreContext)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            Span<byte> rmwOutput = stackalloc byte[ObjectOutputHeader.Size];
            var output = SpanByteAndMemory.FromPinnedSpan(rmwOutput);
            timeoutSet = false;

            var found = false;

            if (storeType == StoreType.Main || storeType == StoreType.All)
            {
                var status = context.RMW(key.ReadOnlySpan, ref input, ref output);

                if (status.IsPending)
                    CompletePendingForSession(ref status, ref output, ref context);
                if (status.Found) found = true;
            }

            if (!found && (storeType == StoreType.Object || storeType == StoreType.All) &&
                !objectStoreBasicContext.IsNull)
            {
                var respCommand = input.header.cmd;

                var type = (respCommand == RespCommand.PEXPIRE || respCommand == RespCommand.PEXPIREAT)
                    ? GarnetObjectType.PExpire
                    : GarnetObjectType.Expire;

                var expiryAt = respCommand == RespCommand.PEXPIREAT || respCommand == RespCommand.EXPIREAT;

                var header = new RespInputHeader(type);

                var objInput = new ObjectInput(header, ref input.parseState, arg1: (int)input.arg1, arg2: expiryAt ? 1 : 0);

                // Retry on object store
                var objOutput = new GarnetObjectStoreOutput { SpanByteAndMemory = output };
                var status = objectStoreContext.RMW(key.ReadOnlySpan, ref objInput, ref objOutput);

                if (status.IsPending)
                    CompletePendingForObjectStoreSession(ref status, ref objOutput, ref objectStoreContext);
                if (status.Found) found = true;

                output = objOutput.SpanByteAndMemory;
            }

            Debug.Assert(output.IsSpanByte);
            if (found) timeoutSet = ((ObjectOutputHeader*)output.SpanByte.ToPointer())->result1 == 1;

            return found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }



        /// <summary>
        /// Set a timeout on key using absolute Unix timestamp (seconds since January 1, 1970).
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key">The key to set the timeout on.</param>
        /// <param name="expiryTimestamp">Absolute Unix timestamp</param>
        /// <param name="timeoutSet">True when the timeout was properly set.</param>
        /// <param name="storeType">The store to operate on.</param>
        /// <param name="expireOption">Flags to use for the operation.</param>
        /// <param name="context">Basic context for the main store</param>
        /// <param name="objectStoreContext">Object context for the object store</param>
        /// <param name="milliseconds">When true, <paramref name="expiryTimestamp"/> is treated as milliseconds else seconds</param>
        /// <returns>Return GarnetStatus.OK when key found, else GarnetStatus.NOTFOUND</returns>
        public unsafe GarnetStatus EXPIREAT<TContext, TObjectContext>(PinnedSpanByte key, long expiryTimestamp, out bool timeoutSet, StoreType storeType, ExpireOption expireOption, ref TContext context, ref TObjectContext objectStoreContext, bool milliseconds = false)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            return EXPIRE(key, expiryTimestamp, out timeoutSet, storeType, expireOption, ref context, ref objectStoreContext, milliseconds ? RespCommand.PEXPIREAT : RespCommand.EXPIREAT);
        }

        /// <summary>
        /// Set a timeout on key.
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key">The key to set the timeout on.</param>
        /// <param name="expiry">The timespan value to set the expiration for.</param>
        /// <param name="timeoutSet">True when the timeout was properly set.</param>
        /// <param name="storeType">The store to operate on.</param>
        /// <param name="expireOption">Flags to use for the operation.</param>
        /// <param name="context">Basic context for the main store</param>
        /// <param name="objectStoreContext">Object context for the object store</param>
        /// <param name="milliseconds">When true the command executed is PEXPIRE, expire by default.</param>
        /// <returns>Return GarnetStatus.OK when key found, else GarnetStatus.NOTFOUND</returns>
        public unsafe GarnetStatus EXPIRE<TContext, TObjectContext>(PinnedSpanByte key, TimeSpan expiry, out bool timeoutSet, StoreType storeType, ExpireOption expireOption, ref TContext context, ref TObjectContext objectStoreContext, bool milliseconds = false)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            return EXPIRE(key, (long)(milliseconds ? expiry.TotalMilliseconds : expiry.TotalSeconds), out timeoutSet, storeType, expireOption,
                ref context, ref objectStoreContext, milliseconds ? RespCommand.PEXPIRE : RespCommand.EXPIRE);
        }

        /// <summary>
        /// Set a timeout on key.
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key">The key to set the timeout on.</param>
        /// <param name="expiry">The timespan value to set the expiration for.</param>
        /// <param name="timeoutSet">True when the timeout was properly set.</param>
        /// <param name="storeType">The store to operate on.</param>
        /// <param name="expireOption">Flags to use for the operation.</param>
        /// <param name="context">Basic context for the main store</param>
        /// <param name="objectStoreContext">Object context for the object store</param>
        /// <param name="respCommand">The current RESP command</param>
        /// <returns></returns>
        public unsafe GarnetStatus EXPIRE<TContext, TObjectContext>(PinnedSpanByte key, long expiry, out bool timeoutSet, StoreType storeType, ExpireOption expireOption, ref TContext context, ref TObjectContext objectStoreContext, RespCommand respCommand)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            Span<byte> rmwOutput = stackalloc byte[ObjectOutputHeader.Size];
            var output = SpanByteAndMemory.FromPinnedSpan(rmwOutput);
            timeoutSet = false;
            var found = false;

            // Serialize expiry + expiry options to parse state
            var expiryLength = NumUtils.CountDigits(expiry);
            var expirySlice = scratchBufferManager.CreateArgSlice(expiryLength);
            var expirySpan = expirySlice.Span;
            NumUtils.WriteInt64(expiry, expirySpan);

            if (storeType == StoreType.Main || storeType == StoreType.All)
            {
                // Build parse state
                parseState.InitializeWithArgument(expirySlice);

                var input = new RawStringInput(respCommand, ref parseState, arg1: (byte)expireOption);
                var status = context.RMW(key.ReadOnlySpan, ref input, ref output);

                if (status.IsPending)
                    CompletePendingForSession(ref status, ref output, ref context);
                if (status.Found) found = true;
            }

            if (!found && (storeType == StoreType.Object || storeType == StoreType.All) &&
                !objectStoreBasicContext.IsNull)
            {
                // Build parse state

                var type = (respCommand == RespCommand.PEXPIRE || respCommand == RespCommand.PEXPIREAT)
                    ? GarnetObjectType.PExpire
                    : GarnetObjectType.Expire;
                parseState.InitializeWithArgument(expirySlice);

                var expiryAt = respCommand == RespCommand.PEXPIREAT || respCommand == RespCommand.EXPIREAT;

                var header = new RespInputHeader(type);
                var objInput = new ObjectInput(header, ref parseState, arg1: (byte)expireOption, arg2: expiryAt ? 1 : 0);

                // Retry on object store
                var objOutput = new GarnetObjectStoreOutput { SpanByteAndMemory = output };
                var keyBytes = key.ToArray();
                var status = objectStoreContext.RMW(key.ReadOnlySpan, ref objInput, ref objOutput);

                if (status.IsPending)
                    CompletePendingForObjectStoreSession(ref status, ref objOutput, ref objectStoreContext);
                if (status.Found) found = true;

                output = objOutput.SpanByteAndMemory;
            }

            scratchBufferManager.RewindScratchBuffer(ref expirySlice);

            Debug.Assert(output.IsSpanByte);
            if (found) timeoutSet = ((ObjectOutputHeader*)output.SpanByte.ToPointer())->result1 == 1;

            return found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        public unsafe GarnetStatus PERSIST<TContext, TObjectContext>(PinnedSpanByte key, StoreType storeType, ref TContext context, ref TObjectContext objectStoreContext)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            GarnetStatus status = GarnetStatus.NOTFOUND;

            var inputHeader = new RawStringInput(RespCommand.PERSIST);

            var pbOutput = stackalloc byte[8];
            var o = new SpanByteAndMemory(pbOutput, 8);

            if (storeType == StoreType.Main || storeType == StoreType.All)
            {
                var _status = context.RMW(key.ReadOnlySpan, ref inputHeader, ref o);

                if (_status.IsPending)
                    CompletePendingForSession(ref _status, ref o, ref context);

                Debug.Assert(o.IsSpanByte);
                if (o.SpanByte.ReadOnlySpan[0] == 1)
                    status = GarnetStatus.OK;
            }

            if (status == GarnetStatus.NOTFOUND && (storeType == StoreType.Object || storeType == StoreType.All) && !objectStoreBasicContext.IsNull)
            {
                // Retry on object store
                var header = new RespInputHeader(GarnetObjectType.Persist);
                var objInput = new ObjectInput(header);

                var objO = new GarnetObjectStoreOutput { SpanByteAndMemory = o };
                var _key = key.ToArray();
                var _status = objectStoreContext.RMW(key.ReadOnlySpan, ref objInput, ref objO);

                if (_status.IsPending)
                    CompletePendingForObjectStoreSession(ref _status, ref objO, ref objectStoreContext);

                Debug.Assert(o.IsSpanByte);
                if (o.SpanByte.ReadOnlySpan.Slice(0, CmdStrings.RESP_RETURN_VAL_1.Length)
                    .SequenceEqual(CmdStrings.RESP_RETURN_VAL_1))
                    status = GarnetStatus.OK;
            }

            return status;
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
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
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
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
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
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
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

            var _output = new SpanByteAndMemory(outputBuffer, outputBufferLength);

            var status = context.RMW(key.ReadOnlySpan, ref input, ref _output);
            if (status.IsPending)
                CompletePendingForSession(ref status, ref _output, ref context);

            Debug.Assert(_output.IsSpanByte);

            output = NumUtils.ReadInt64(_output.Length, outputBuffer);
            return GarnetStatus.OK;
        }

        public void WATCH(PinnedSpanByte key, StoreType type) => txnManager.Watch(key, type);

        public unsafe void WATCH(SpanByte key, StoreType type) => WATCH(key, type);

        public unsafe GarnetStatus SCAN<TContext>(long cursor, PinnedSpanByte match, long count, ref TContext context) => GarnetStatus.OK;

        public GarnetStatus GetKeyType<TContext, TObjectContext>(PinnedSpanByte key, out string keyType, ref TContext context, ref TObjectContext objectContext)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            keyType = "string";
            // Check if key exists in Main store
            var status = EXISTS(key, StoreType.Main, ref context, ref objectContext);

            // If key was not found in the main store then it is an object
            if (status != GarnetStatus.OK && !objectStoreBasicContext.IsNull)
            {
                status = GET(key, out GarnetObjectStoreOutput output, ref objectContext);
                if (status == GarnetStatus.OK)
                {
                    if ((output.GarnetObject as SortedSetObject) != null)
                    {
                        keyType = "zset";
                    }
                    else if ((output.GarnetObject as ListObject) != null)
                    {
                        keyType = "list";
                    }
                    else if ((output.GarnetObject as SetObject) != null)
                    {
                        keyType = "set";
                    }
                    else if ((output.GarnetObject as HashObject) != null)
                    {
                        keyType = "hash";
                    }
                }
                else
                {
                    keyType = "none";
                    status = GarnetStatus.NOTFOUND;
                }
            }
            return status;
        }

        public GarnetStatus MemoryUsageForKey<TContext, TObjectContext>(PinnedSpanByte key, out long memoryUsage, ref TContext context, ref TObjectContext objectContext, int samples = 0)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            memoryUsage = -1;

            // Check if key exists in Main store
            var status = GET(key, out PinnedSpanByte keyValue, ref context);

            if (status == GarnetStatus.NOTFOUND)
            {
                status = GET(key, out GarnetObjectStoreOutput objectValue, ref objectContext);
                if (status != GarnetStatus.NOTFOUND)
                {
                    memoryUsage = RecordInfo.GetLength() + (2 * IntPtr.Size) + // Log record length
                        Utility.RoundUp(key.Length, IntPtr.Size) + MemoryUtils.ByteArrayOverhead + // Key allocation in heap with overhead
                        objectValue.GarnetObject.Size; // Value allocation in heap
                }
            }
            else
            {
                memoryUsage = RecordInfo.GetLength() + Utility.RoundUp(key.TotalSize(), RecordInfo.GetLength()) + Utility.RoundUp(keyValue.TotalSize(), RecordInfo.GetLength());
            }

            return status;
        }

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
                txnManager.SaveKeyEntryToLock(key1, false, LockType.Shared);
                txnManager.SaveKeyEntryToLock(key2, false, LockType.Shared);
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
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();
            var curr = ptr;
            var end = curr + output.Length;

            try
            {
                PinnedSpanByte val1, val2;
                var status1 = GET(key1, out val1, ref context);
                var status2 = GET(key2, out val2, ref context);

                if (lenOnly)
                {
                    if (status1 != GarnetStatus.OK || status2 != GarnetStatus.OK)
                    {
                        while (!RespWriteUtils.TryWriteInt32(0, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        return GarnetStatus.OK;
                    }

                    var len = ComputeLCSLength(val1.ReadOnlySpan, val2.ReadOnlySpan, minMatchLen);
                    while (!RespWriteUtils.TryWriteInt32(len, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                else if (withIndices)
                {
                    List<LCSMatch> matches;
                    int len;
                    if (status1 != GarnetStatus.OK || status2 != GarnetStatus.OK)
                    {
                        matches = new List<LCSMatch>();
                        len = 0;
                    }
                    else
                    {
                        matches = ComputeLCSWithIndices(val1.ReadOnlySpan, val2.ReadOnlySpan, minMatchLen, out len);
                    }

                    WriteLCSMatches(matches, withMatchLen, len, ref curr, end, ref output, ref isMemory, ref ptr, ref ptrHandle);
                }
                else
                {
                    if (status1 != GarnetStatus.OK || status2 != GarnetStatus.OK)
                    {
                        while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_EMPTY, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        return GarnetStatus.OK;
                    }

                    var lcs = ComputeLCS(val1.ReadOnlySpan, val2.ReadOnlySpan, minMatchLen);
                    while (!RespWriteUtils.TryWriteBulkString(lcs, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
            }
            finally
            {
                if (isMemory)
                    ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }

            return GarnetStatus.OK;
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
            ref byte* curr, byte* end, ref SpanByteAndMemory output,
            ref bool isMemory, ref byte* ptr, ref MemoryHandle ptrHandle)
        {
            while (!RespWriteUtils.TryWriteArrayLength(4, ref curr, end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

            // Write "matches" section identifier
            while (!RespWriteUtils.TryWriteBulkString(CmdStrings.matches, ref curr, end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

            // Write matches array
            while (!RespWriteUtils.TryWriteArrayLength(matches.Count, ref curr, end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

            foreach (var match in matches)
            {
                while (!RespWriteUtils.TryWriteArrayLength(withMatchLen ? 3 : 2, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                while (!RespWriteUtils.TryWriteArrayLength(2, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                while (!RespWriteUtils.TryWriteInt32(match.Start1, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                while (!RespWriteUtils.TryWriteInt32(match.Start1 + match.Length - 1, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                while (!RespWriteUtils.TryWriteArrayLength(2, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                while (!RespWriteUtils.TryWriteInt32(match.Start2, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                while (!RespWriteUtils.TryWriteInt32(match.Start2 + match.Length - 1, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (withMatchLen)
                {
                    while (!RespWriteUtils.TryWriteInt32(match.Length, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
            }

            // Write "len" section identifier
            while (!RespWriteUtils.TryWriteBulkString(CmdStrings.len, ref curr, end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

            // Write LCS length
            while (!RespWriteUtils.TryWriteInt32(lcsLength, ref curr, end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
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