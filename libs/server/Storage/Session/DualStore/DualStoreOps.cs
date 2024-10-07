// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    using MainStoreDualContext = DualContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions,
        /* MainStoreFunctions */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
        /* MainStoreAllocator */ SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>;
    using ObjectStoreDualContext = DualContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions,
        /* ObjectStoreFunctions */ StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>,
        /* ObjectStoreAllocator */ GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>>;

    sealed partial class StorageSession : IDisposable
    {
        private ushort MainStorePartitionId = 0;
        private ushort ObjectStorePartitionId = 1;

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
        public unsafe GarnetStatus TTL<TContext, TObjectContext>(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output, ref TContext context, ref TObjectContext objectContext, bool milliseconds = false)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            Debug.Assert(typeof(TContext) == typeof(MainStoreDualContext), "Expected Dual Context");
            Debug.Assert(typeof(TObjectContext) == typeof(ObjectStoreDualContext), "Expected Dual ObjectContext");
            int inputSize = sizeof(int) + RespInputHeader.Size;
            byte* pbCmdInput = stackalloc byte[inputSize];

            byte* pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            (*(RespInputHeader*)pcurr).cmd = milliseconds ? RespCommand.PTTL : RespCommand.TTL;
            (*(RespInputHeader*)pcurr).flags = 0;

            var locker = new DualClientLocker();
            var epochThread = new SafeEpochThread<KernelSession>();
            HashEntryInfo hei = default;

            try
            {
                // TODO: Lock or unlock based on IsInTransaction; propagate to retry loop
                Status status = Kernel.EnterForRead(ref kernelSession, locker, epochThread, GetMainStoreKeyHashCode64(ref key), MainStorePartitionId, out hei);
                if (!status.IsCompletedSuccessfully)
                    return GarnetStatus.NOTFOUND;

                if (storeType == StoreType.Main || storeType == StoreType.All)
                {
                    status = context.Read(ref key, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref output);

                    if (status.IsPending)
                    {
                        // TODO: Propagate epochThread to retry loop
                        epochThread.EndUnsafe(ref kernelSession);
                        StartPendingMetrics();
                        CompletePendingForSession(ref status, ref output, ref context);
                        StopPendingMetrics();
                        epochThread.BeginUnsafe(ref kernelSession);
                    }

                    if (status.Found)
                        return GarnetStatus.OK;
                }

                if ((storeType == StoreType.Object || storeType == StoreType.All) && !objectStoreBasicContext.IsNull)
                {
                    var objInput = new ObjectInput
                    {
                        header = new RespInputHeader
                        {
                            cmd = milliseconds ? RespCommand.PTTL : RespCommand.TTL,
                            type = milliseconds ? GarnetObjectType.PTtl : GarnetObjectType.Ttl,
                        },
                    };

                    var keyBA = key.ToByteArray();

                    Debug.Assert(hei.HashCodeEquals(GetObjectStoreKeyHashCode64(ref keyBA)), "Main and Object hash codes are not the same");
                    status = Kernel.EnterForReadDual2(ObjectStorePartitionId, ref hei);
                    if (!status.IsCompletedSuccessfully)
                        return GarnetStatus.NOTFOUND;

                    var objO = new GarnetObjectStoreOutput { spanByteAndMemory = output };
                    status = objectContext.Read(ref keyBA, ref objInput, ref objO);

                    if (status.IsPending)
                    { 
                        epochThread.EndUnsafe(ref kernelSession);
                        CompletePendingForObjectStoreSession(ref status, ref objO, ref objectContext);
                        epochThread.BeginUnsafe(ref kernelSession);
                    }

                    if (status.Found)
                    {
                        output = objO.spanByteAndMemory;
                        return GarnetStatus.OK;
                    }
                }
            }
            finally
            {
                Kernel.ExitForRead(ref kernelSession, locker, epochThread, ref hei);
            }
            return GarnetStatus.NOTFOUND;
        }

        public GarnetStatus DELETE<TContext, TObjectContext>(ArgSlice key, StoreType storeType, ref TContext context, ref TObjectContext objectContext)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            var _key = key.SpanByte;
            return DELETE(ref _key, storeType, ref context, ref objectContext);
        }

        public GarnetStatus DELETE<TContext, TObjectContext>(ref SpanByte key, StoreType storeType, ref TContext context, ref TObjectContext objectContext)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            var found = false;

            if (storeType == StoreType.Main || storeType == StoreType.All)
            {
                var status = context.Delete(ref key);
                Debug.Assert(!status.IsPending);
                if (status.Found) found = true;
            }

            if (!objectStoreBasicContext.IsNull && (storeType == StoreType.Object || storeType == StoreType.All))
            {
                var keyBA = key.ToByteArray();
                var status = objectContext.Delete(ref keyBA);
                Debug.Assert(!status.IsPending);
                if (status.Found) found = true;
            }
            return found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        public GarnetStatus DELETE<TContext, TObjectContext>(byte[] key, StoreType storeType, ref TContext context, ref TObjectContext objectContext)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            bool found = false;

            if ((storeType == StoreType.Object || storeType == StoreType.All) && !objectStoreBasicContext.IsNull)
            {
                var status = objectContext.Delete(key);
                Debug.Assert(!status.IsPending);
                if (status.Found) found = true;
            }

            if (!found && (storeType == StoreType.Main || storeType == StoreType.All))
            {
                unsafe
                {
                    fixed (byte* ptr = key)
                    {
                        var keySB = SpanByte.FromPinnedPointer(ptr, key.Length);
                        var status = context.Delete(ref keySB);
                        Debug.Assert(!status.IsPending);
                        if (status.Found) found = true;
                    }
                }
            }
            return found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        public unsafe GarnetStatus RENAME(ArgSlice oldKeySlice, ArgSlice newKeySlice, StoreType storeType)
        {
            return RENAME(oldKeySlice, newKeySlice, storeType, false, out _);
        }

        /// <summary>
        /// Renames key to newkey if newkey does not yet exist. It returns an error when key does not exist.
        /// </summary>
        /// <param name="oldKeySlice">The old key to be renamed.</param>
        /// <param name="newKeySlice">The new key name.</param>
        /// <param name="storeType">The type of store to perform the operation on.</param>
        /// <returns></returns>
        public unsafe GarnetStatus RENAMENX(ArgSlice oldKeySlice, ArgSlice newKeySlice, StoreType storeType, out int result)
        {
            return RENAME(oldKeySlice, newKeySlice, storeType, true, out result);
        }

        private unsafe GarnetStatus RENAME(ArgSlice oldKeySlice, ArgSlice newKeySlice, StoreType storeType, bool isNX, out int result)
        {
            GarnetStatus returnStatus = GarnetStatus.NOTFOUND;
            result = -1;

            // If same name check return early.
            if (oldKeySlice.ReadOnlySpan.SequenceEqual(newKeySlice.ReadOnlySpan))
            {
                result = 1;
                return GarnetStatus.OK;
            }

            bool createTransaction = false;
            if (txnManager.state != TxnState.Running)
            {
                createTransaction = true;
                txnManager.SaveKeyEntryToLock(oldKeySlice, false, LockType.Exclusive);
                txnManager.SaveKeyEntryToLock(newKeySlice, false, LockType.Exclusive);
                txnManager.Run(true);
            }

            var context = txnManager.LockableContext;
            var objectContext = txnManager.ObjectStoreLockableContext;
            SpanByte oldKey = oldKeySlice.SpanByte;

            if (storeType == StoreType.Main || storeType == StoreType.All)
            {
                try
                {
                    SpanByte input = default;
                    var o = new SpanByteAndMemory();
                    var status = GET(ref oldKey, ref input, ref o, ref context);

                    if (status == GarnetStatus.OK)
                    {
                        Debug.Assert(!o.IsSpanByte);
                        var memoryHandle = o.Memory.Memory.Pin();
                        var ptrVal = (byte*)memoryHandle.Pointer;

                        RespReadUtils.ReadUnsignedLengthHeader(out var headerLength, ref ptrVal, ptrVal + o.Length);

                        // Find expiration time of the old key
                        var expireSpan = new SpanByteAndMemory();
                        var ttlStatus = TTL(ref oldKey, storeType, ref expireSpan, ref context, ref objectContext, true);

                        if (ttlStatus == GarnetStatus.OK && !expireSpan.IsSpanByte)
                        {
                            using var expireMemoryHandle = expireSpan.Memory.Memory.Pin();
                            var expirePtrVal = (byte*)expireMemoryHandle.Pointer;
                            RespReadUtils.TryRead64Int(out var expireTimeMs, ref expirePtrVal, expirePtrVal + expireSpan.Length, out var _);

                            // If the key has an expiration, set the new key with the expiration
                            if (expireTimeMs > 0)
                            {
                                if (isNX)
                                {
                                    // Move payload forward to make space for RespInputHeader and Metadata
                                    var setValue = scratchBufferManager.FormatScratch(RespInputHeader.Size + sizeof(long), new ArgSlice(ptrVal, headerLength));
                                    var setValueSpan = setValue.SpanByte;
                                    var setValuePtr = setValueSpan.ToPointerWithMetadata();
                                    setValueSpan.ExtraMetadata = DateTimeOffset.UtcNow.Ticks + TimeSpan.FromMilliseconds(expireTimeMs).Ticks;
                                    ((RespInputHeader*)(setValuePtr + sizeof(long)))->cmd = RespCommand.SETEXNX;
                                    ((RespInputHeader*)(setValuePtr + sizeof(long)))->flags = 0;
                                    var newKey = newKeySlice.SpanByte;
                                    var setStatus = SET_Conditional(ref newKey, ref setValueSpan, ref context);

                                    // For SET NX `NOTFOUND` means the operation succeeded
                                    result = setStatus == GarnetStatus.NOTFOUND ? 1 : 0;
                                    returnStatus = GarnetStatus.OK;
                                }
                                else
                                {
                                    SETEX(newKeySlice, new ArgSlice(ptrVal, headerLength), TimeSpan.FromMilliseconds(expireTimeMs), ref context);
                                }
                            }
                            else if (expireTimeMs == -1) // Its possible to have expireTimeMs as 0 (Key expired or will be expired now) or -2 (Key does not exist), in those cases we don't SET the new key
                            {
                                if (isNX)
                                {
                                    // Move payload forward to make space for RespInputHeader
                                    var setValue = scratchBufferManager.FormatScratch(RespInputHeader.Size, new ArgSlice(ptrVal, headerLength));
                                    var setValueSpan = setValue.SpanByte;
                                    var setValuePtr = setValueSpan.ToPointerWithMetadata();
                                    ((RespInputHeader*)setValuePtr)->cmd = RespCommand.SETEXNX;
                                    ((RespInputHeader*)setValuePtr)->flags = 0;
                                    var newKey = newKeySlice.SpanByte;
                                    var setStatus = SET_Conditional(ref newKey, ref setValueSpan, ref context);

                                    // For SET NX `NOTFOUND` means the operation succeeded
                                    result = setStatus == GarnetStatus.NOTFOUND ? 1 : 0;
                                    returnStatus = GarnetStatus.OK;
                                }
                                else
                                {
                                    SpanByte newKey = newKeySlice.SpanByte;
                                    var value = SpanByte.FromPinnedPointer(ptrVal, headerLength);
                                    SET(ref newKey, ref value, ref context);
                                }
                            }

                            expireSpan.Memory.Dispose();
                            memoryHandle.Dispose();
                            o.Memory.Dispose();

                            // Delete the old key only when SET NX succeeded
                            if (isNX && result == 1)
                            {
                                DELETE(ref oldKey, StoreType.Main, ref context, ref objectContext);
                            }
                            else if (!isNX)
                            {
                                // Delete the old key
                                DELETE(ref oldKey, StoreType.Main, ref context, ref objectContext);

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
                    byte[] oldKeyArray = oldKeySlice.ToArray();
                    var status = GET(oldKeyArray, out var value, ref objectContext);

                    if (status == GarnetStatus.OK)
                    {
                        var valObj = value.garnetObject;
                        byte[] newKeyArray = newKeySlice.ToArray();

                        returnStatus = GarnetStatus.OK;
                        var canSetAndDelete = true;
                        if (isNX)
                        {
                            // Not using EXISTS method to avoid new allocation of Array for key
                            var getNewStatus = GET(newKeyArray, out _, ref objectContext);
                            canSetAndDelete = getNewStatus == GarnetStatus.NOTFOUND;
                        }

                        if (canSetAndDelete)
                        {
                            // valObj already has expiration time, so no need to write expiration logic here
                            SET(newKeyArray, valObj, ref objectContext);

                            // Delete the old key
                            DELETE(oldKeyArray, StoreType.Object, ref context, ref objectContext);

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
        public GarnetStatus EXISTS<TContext, TObjectContext>(ArgSlice key, StoreType storeType, ref TContext context, ref TObjectContext objectContext)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            GarnetStatus status = GarnetStatus.NOTFOUND;

            if (storeType == StoreType.Main || storeType == StoreType.All)
            {
                var _key = key.SpanByte;
                SpanByte input = default;
                var _output = new SpanByteAndMemory { SpanByte = scratchBufferManager.ViewRemainingArgSlice().SpanByte };
                status = GET(ref _key, ref input, ref _output, ref context);

                if (status == GarnetStatus.OK)
                {
                    if (!_output.IsSpanByte)
                        _output.Memory.Dispose();
                    return status;
                }
            }

            if ((storeType == StoreType.Object || storeType == StoreType.All) && !objectStoreBasicContext.IsNull)
            {
                status = GET(key.ToArray(), out _, ref objectContext);
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
        public unsafe GarnetStatus EXPIRE<TContext, TObjectContext>(ArgSlice key, ArgSlice expiryMs, out bool timeoutSet, StoreType storeType, ExpireOption expireOption, ref TContext context, ref TObjectContext objectStoreContext)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => EXPIRE(key, TimeSpan.FromMilliseconds(NumUtils.BytesToLong(expiryMs.Length, expiryMs.ptr)), out timeoutSet, storeType, expireOption, ref context, ref objectStoreContext);

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
        /// <returns></returns>
        public unsafe GarnetStatus EXPIRE<TContext, TObjectContext>(ArgSlice key, TimeSpan expiry, out bool timeoutSet, StoreType storeType, ExpireOption expireOption, ref TContext context, ref TObjectContext objectStoreContext, bool milliseconds = false)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            byte* pbCmdInput = stackalloc byte[sizeof(int) + sizeof(long) + RespInputHeader.Size + sizeof(byte)];
            *(int*)pbCmdInput = sizeof(long) + RespInputHeader.Size;
            ((RespInputHeader*)(pbCmdInput + sizeof(int) + sizeof(long)))->cmd = milliseconds ? RespCommand.PEXPIRE : RespCommand.EXPIRE;
            ((RespInputHeader*)(pbCmdInput + sizeof(int) + sizeof(long)))->flags = 0;

            *(pbCmdInput + sizeof(int) + sizeof(long) + RespInputHeader.Size) = (byte)expireOption;
            ref var input = ref SpanByte.Reinterpret(pbCmdInput);

            input.ExtraMetadata = DateTimeOffset.UtcNow.Ticks + expiry.Ticks;

            var rmwOutput = stackalloc byte[ObjectOutputHeader.Size];
            var output = new SpanByteAndMemory(SpanByte.FromPinnedPointer(rmwOutput, ObjectOutputHeader.Size));
            timeoutSet = false;

            bool found = false;

            if (storeType == StoreType.Main || storeType == StoreType.All)
            {
                var _key = key.SpanByte;
                var status = context.RMW(ref _key, ref input, ref output);

                if (status.IsPending)
                    CompletePendingForSession(ref status, ref output, ref context);
                if (status.Found) found = true;
            }

            if (!found && (storeType == StoreType.Object || storeType == StoreType.All) &&
                !objectStoreBasicContext.IsNull)
            {
                var parseState = new SessionParseState();
                ArgSlice[] parseStateBuffer = default;

                var expireOptionLength = NumUtils.NumDigits((byte)expireOption);
                var expireOptionPtr = stackalloc byte[expireOptionLength];
                NumUtils.IntToBytes((byte)expireOption, expireOptionLength, ref expireOptionPtr);
                expireOptionPtr -= expireOptionLength;
                var expireOptionSlice = new ArgSlice(expireOptionPtr, expireOptionLength);

                var extraMetadataLength = NumUtils.NumDigitsInLong(input.ExtraMetadata);
                var extraMetadataPtr = stackalloc byte[extraMetadataLength];
                NumUtils.LongToBytes(input.ExtraMetadata, extraMetadataLength, ref extraMetadataPtr);
                extraMetadataPtr -= extraMetadataLength;
                var extraMetadataSlice = new ArgSlice(extraMetadataPtr, extraMetadataLength);

                parseState.InitializeWithArguments(ref parseStateBuffer, expireOptionSlice, extraMetadataSlice);

                var objInput = new ObjectInput
                {
                    header = new RespInputHeader
                    {
                        cmd = milliseconds ? RespCommand.PEXPIRE : RespCommand.EXPIRE,
                        type = GarnetObjectType.Expire,
                    },
                    parseState = parseState,
                    parseStateStartIdx = 0,
                };

                // Retry on object store
                var objOutput = new GarnetObjectStoreOutput { spanByteAndMemory = output };
                var keyBytes = key.ToArray();
                var status = objectStoreContext.RMW(ref keyBytes, ref objInput, ref objOutput);

                if (status.IsPending)
                    CompletePendingForObjectStoreSession(ref status, ref objOutput, ref objectStoreContext);
                if (status.Found) found = true;

                output = objOutput.spanByteAndMemory;
            }

            Debug.Assert(output.IsSpanByte);
            if (found) timeoutSet = ((ObjectOutputHeader*)output.SpanByte.ToPointer())->result1 == 1;

            return found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        public unsafe GarnetStatus PERSIST<TContext, TObjectContext>(ArgSlice key, StoreType storeType, ref TContext context, ref TObjectContext objectStoreContext)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            GarnetStatus status = GarnetStatus.NOTFOUND;

            int inputSize = sizeof(int) + RespInputHeader.Size;
            byte* pbCmdInput = stackalloc byte[inputSize];
            byte* pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            (*(RespInputHeader*)pcurr).cmd = RespCommand.PERSIST;
            (*(RespInputHeader*)pcurr).flags = 0;

            byte* pbOutput = stackalloc byte[8];
            var o = new SpanByteAndMemory(pbOutput, 8);

            if (storeType == StoreType.Main || storeType == StoreType.All)
            {
                var _key = key.SpanByte;
                var _status = context.RMW(ref _key, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref o);

                if (_status.IsPending)
                    CompletePendingForSession(ref _status, ref o, ref context);

                Debug.Assert(o.IsSpanByte);
                if (o.SpanByte.AsReadOnlySpan()[0] == 1)
                    status = GarnetStatus.OK;
            }

            if (status == GarnetStatus.NOTFOUND && (storeType == StoreType.Object || storeType == StoreType.All) && !objectStoreBasicContext.IsNull)
            {
                // Retry on object store
                var objInput = new ObjectInput
                {
                    header = new RespInputHeader
                    {
                        cmd = RespCommand.PERSIST,
                        type = GarnetObjectType.Persist,
                    },
                };

                var objO = new GarnetObjectStoreOutput { spanByteAndMemory = o };
                var _key = key.ToArray();
                var _status = objectStoreContext.RMW(ref _key, ref objInput, ref objO);

                if (_status.IsPending)
                    CompletePendingForObjectStoreSession(ref _status, ref objO, ref objectStoreContext);

                Debug.Assert(o.IsSpanByte);
                if (o.SpanByte.AsReadOnlySpan().Slice(0, CmdStrings.RESP_RETURN_VAL_1.Length)
                    .SequenceEqual(CmdStrings.RESP_RETURN_VAL_1))
                    status = GarnetStatus.OK;
            }

            return status;
        }

        public GarnetStatus GetKeyType<TContext, TObjectContext>(ArgSlice key, out string keyType, ref TContext context, ref TObjectContext objectContext)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            keyType = "string";
            // Check if key exists in Main store
            var status = EXISTS(key, StoreType.Main, ref context, ref objectContext);

            // If key was not found in the main store then it is an object
            if (status != GarnetStatus.OK && !objectStoreBasicContext.IsNull)
            {
                status = GET(key.ToArray(), out GarnetObjectStoreOutput output, ref objectContext);
                if (status == GarnetStatus.OK)
                {
                    keyType = output.garnetObject switch
                    {
                        SortedSetObject => "zset",
                        ListObject => "list",
                        SetObject => "set",
                        HashObject => "hash",
                        _ => throw new GarnetException("Unexpected garnetObject type")
                    };
                }
                else
                {
                    keyType = "none";
                    status = GarnetStatus.NOTFOUND;
                }
            }
            return status;
        }

        public GarnetStatus MemoryUsageForKey<TContext, TObjectContext>(ArgSlice key, out long memoryUsage, ref TContext context, ref TObjectContext objectContext, int samples = 0)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            memoryUsage = -1;

            // Check if key exists in Main store
            var status = GET(key, out ArgSlice keyValue, ref context);

            if (status == GarnetStatus.NOTFOUND)
            {
                status = GET(key.ToArray(), out GarnetObjectStoreOutput objectValue, ref objectContext);
                if (status != GarnetStatus.NOTFOUND)
                {
                    memoryUsage = RecordInfo.GetLength() + (2 * IntPtr.Size) + // Log record length
                        Utility.RoundUp(key.SpanByte.Length, IntPtr.Size) + MemoryUtils.ByteArrayOverhead + // Key allocation in heap with overhead
                        objectValue.garnetObject.Size; // Value allocation in heap
                }
            }
            else
            {
                memoryUsage = RecordInfo.GetLength() + Utility.RoundUp(key.SpanByte.TotalSize, RecordInfo.GetLength()) + Utility.RoundUp(keyValue.SpanByte.TotalSize, RecordInfo.GetLength());
            }

            return status;
        }

        public GarnetStatus Watch<TContext, TObjectContext>(ArgSlice key, StoreType storeType, in TContext context, in TObjectContext objectContext)
             where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            if (storeType == StoreType.Main || storeType == StoreType.All)
                basicContext.ResetModified(key.SpanByte);
            if ((storeType == StoreType.Object || storeType == StoreType.All) && !objectStoreBasicContext.IsNull)
                objectStoreBasicContext.ResetModified(key.ToArray());
            return GarnetStatus.OK;
        }
    }
}