// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession : IDisposable
    {
        private const ushort MainStoreId = 0;
        private const ushort ObjectStoreId = 1;

        /// <summary>
        /// Returns the remaining time to live of a key that has a timeout.
        /// </summary>
        /// <param name="key">The key to get the remaining time to live in the store.</param>
        /// <param name="storeType">The store to operate on</param>
        /// <param name="output">Span to allocate the output of the operation</param>
        /// <param name="milliseconds">when true the command to execute is PTTL.</param>
        /// <returns></returns>
        public unsafe GarnetStatus TTL<TKeyLocker, TEpochGuard>(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output, bool milliseconds = false)
            where TKeyLocker: struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            // Try to find the key; if the tag is not found, we have nothing to fetch.
            var status = dualContext.EnterKernelForRead<TKeyLocker, TEpochGuard>(GetMainStoreKeyHashCode64(ref key), MainStoreId, out var hei);
            if (status.NotFound)
            {
                output = default;
                return GarnetStatus.NOTFOUND;
            }

            try
            {
                return InternalTTL<TKeyLocker>(ref hei, ref key, storeType, ref output, milliseconds);
            }
            finally
            {
                dualContext.ExitKernelForRead<TKeyLocker, TEpochGuard>(ref hei);
            }
        }

        private unsafe GarnetStatus InternalTTL<TKeyLocker>(ref HashEntryInfo hei, ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output, bool milliseconds = false)
            where TKeyLocker : struct, IKeyLocker
        {
            var inputSize = sizeof(int) + RespInputHeader.Size;
            var pbCmdInput = stackalloc byte[inputSize];

            var pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            (*(RespInputHeader*)pcurr).cmd = milliseconds ? RespCommand.PTTL : RespCommand.TTL;
            (*(RespInputHeader*)pcurr).flags = 0;

            ref var input1 = ref Unsafe.AsRef<SpanByte>(pbCmdInput);

            // Because we may bring in hei from outside due to RENAME, we do both stores separately here rather than introducing more complexity in DualContext.
            if (storeType is StoreType.Main or StoreType.All)
            {
                ReadOptions readOptions = default;
                var status = MainContext.Read<TKeyLocker>(ref hei, ref key, ref input1, ref output, ref readOptions, recordMetadata: out _, userContext: default);
                if (status.Found)
                    return GarnetStatus.OK;
            }
            if (storeType is StoreType.Object or StoreType.All)
            {
                ReadOptions readOptions = default;
                new GarnetDualInputConverter().ConvertForRead(ref key, ref input1, out var key2, out var input2, out var objOutput);
                var status = ObjectContext.Read<TKeyLocker>(ref hei, ref key2, ref input2, ref objOutput, ref readOptions, recordMetadata: out _, userContext: default);
                if (status.Found)
                {
                    output = objOutput.spanByteAndMemory;
                    return GarnetStatus.OK;
                }
            }

            output = default;
            return GarnetStatus.NOTFOUND;
        }

        public GarnetStatus DELETE<TKeyLocker, TEpochGuard>(ArgSlice key, StoreType storeType)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var _key = key.SpanByte;
            return DELETE<TKeyLocker, TEpochGuard>(ref _key, storeType);
        }

        public GarnetStatus DELETE<TKeyLocker, TEpochGuard>(ref SpanByte key, StoreType storeType)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            if (storeType == StoreType.All && !dualContext.IsDual)
                storeType = StoreType.Main;
            var status = storeType switch
            {
                StoreType.All => dualContext.DualDelete<TKeyLocker, TEpochGuard>(ref key),
                StoreType.Main => dualContext.Delete<TKeyLocker, TEpochGuard>(ref key),
                StoreType.Object => dualContext.Delete<TKeyLocker, TEpochGuard>(key.ToByteArray()),
                _ => throw new GarnetException($"Unknown storeType {storeType}")
            };
            return status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        public GarnetStatus DELETE<TKeyLocker, TEpochGuard>(byte[] key, StoreType storeType)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            if (storeType == StoreType.All && !dualContext.IsDual)
                storeType = StoreType.Main;

            // Status.Found is not reliable because Tsavorite does not go to disk to verify whether the record is found; it deletes blindly.
            // So we have to delete in both stores. We do this on explicit store type because we have the object key instead of the SpanByte.
            var found = false;
            if (storeType is StoreType.Object or StoreType.All)
            {
                var status = dualContext.Delete<TKeyLocker, TEpochGuard>(key);
                Debug.Assert(!status.IsPending);
                found = status.Found;
            }

            if (storeType is StoreType.Main or StoreType.All)
            {
                unsafe
                {
                    fixed (byte* ptr = key)
                    {
                        var keySB = SpanByte.FromPinnedPointer(ptr, key.Length);
                        var status = dualContext.Delete<TKeyLocker, TEpochGuard>(ref keySB);
                        Debug.Assert(!status.IsPending);
                        found = status.Found;
                    }
                }
            }
            return found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        public GarnetStatus DELETE<TKeyLocker>(ref HashEntryInfo hei, ref SpanByte key, StoreType storeType)
            where TKeyLocker : struct, IKeyLocker
        {
            if (storeType == StoreType.All && !dualContext.IsDual)
                storeType = StoreType.Main;
            var status = storeType switch
            {
                StoreType.Main or StoreType.All => dualContext.ItemContext1.Delete<TKeyLocker>(ref hei, ref key),
                StoreType.Object or StoreType.All => dualContext.ItemContext2.Delete<TKeyLocker>(ref hei, key.ToByteArray()),
                _ => throw new GarnetException($"Unknown storeType {storeType}")
            };
            return status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        public GarnetStatus DELETE<TKeyLocker>(ref HashEntryInfo hei, byte[] key, StoreType storeType)
            where TKeyLocker : struct, IKeyLocker
        {
            if (storeType == StoreType.All && !dualContext.IsDual)
                storeType = StoreType.Main;

            // Status.Found is not reliable because Tsavorite does not go to disk to verify whether the record is found; it deletes blindly.
            // So we have to delete in both stores. We do this on explicit store type because we have the object key instead of the SpanByte.
            var found = false;
            if (storeType is StoreType.Object or StoreType.All)
            {
                var status = dualContext.ItemContext2.Delete<TKeyLocker>(ref hei, key);
                Debug.Assert(!status.IsPending);
                found = status.Found;
            }

            if (storeType is StoreType.Main or StoreType.All)
            {
                unsafe
                {
                    fixed (byte* ptr = key)
                    {
                        var keySB = SpanByte.FromPinnedPointer(ptr, key.Length);
                        var status = dualContext.ItemContext1.Delete<TKeyLocker>(ref hei, ref keySB);
                        Debug.Assert(!status.IsPending);
                        found = status.Found;
                    }
                }
            }
            return found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        /// <summary>
        /// Renames key to newkey. It returns an error when key does not exist.
        /// </summary>
        /// <param name="oldKeySlice">The old key to be renamed.</param>
        /// <param name="newKeySlice">The new key name.</param>
        /// <param name="storeType">The type of store to perform the operation on.</param>
        /// <returns></returns>
        public unsafe GarnetStatus RENAME(ArgSlice oldKeySlice, ArgSlice newKeySlice, StoreType storeType)
            => RENAME(oldKeySlice, newKeySlice, storeType, false, out _);

        /// <summary>
        /// Renames key to newkey if newkey does not yet exist. It returns an error when key does not exist.
        /// </summary>
        /// <param name="oldKeySlice">The old key to be renamed.</param>
        /// <param name="newKeySlice">The new key name.</param>
        /// <param name="storeType">The type of store to perform the operation on.</param>
        /// <param name="result">The result indicating whether the key was renamed: 1 if key was renamed to newkey, 0 if newkey already exists</param>
        /// <returns></returns>
        public unsafe GarnetStatus RENAMENX(ArgSlice oldKeySlice, ArgSlice newKeySlice, StoreType storeType, out int result)
            => RENAME(oldKeySlice, newKeySlice, storeType, true, out result);

        /// <summary>
        /// Renames key to newkey if newkey does not yet exist. It returns an error when key does not exist.
        /// </summary>
        /// <param name="oldKeySlice">The old key to be renamed.</param>
        /// <param name="newKeySlice">The new key name.</param>
        /// <param name="storeType">The type of store to perform the operation on.</param>
        /// <param name="isNX">Whether this is a "not exists" operation</param>
        /// <param name="result">The result indicating whether the key was renamed: 1 if key was renamed to newkey, 0 if newkey already exists</param>
        /// <remarks>This takes only the TEpochGuard; it creates a transaction if one does not yet exist, so always uses <see cref="TransactionalKeyLocker"/></remarks>>
        private unsafe GarnetStatus RENAME(ArgSlice oldKeySlice, ArgSlice newKeySlice, StoreType storeType, bool isNX, out int result)
        {
            var returnStatus = GarnetStatus.NOTFOUND;
            result = -1;

            // If same name check return early.
            if (oldKeySlice.ReadOnlySpan.SequenceEqual(newKeySlice.ReadOnlySpan))
            {
                result = 1;
                return GarnetStatus.OK;
            }

            var oldKey = oldKeySlice.SpanByte;
            var newKey = newKeySlice.SpanByte;

            long oldKeyHash, newKeyHash;
            var createTransaction = false;
            if (txnManager.state != TxnState.Running)
            {
                oldKeyHash = txnManager.SaveKeyEntryToLock(oldKeySlice, isObject: false, LockType.Exclusive);
                newKeyHash = txnManager.SaveKeyEntryToLock(newKeySlice, isObject: false, LockType.Exclusive);
                createTransaction = txnManager.Run(internal_txn: true);
            }
            else
            {
                oldKeyHash = dualContext.GetKeyHash(ref oldKey);
                newKeyHash = dualContext.GetKeyHash(ref newKey);
            }

            try
            {
                // We're in a Transaction so use TransactionalSessionLocker. Obtain the epoch once then use GarnetUnsafeEpochGuard for called operations.
                // Acquire HashEntryInfos directly if needed; they won't contain transient lock info, which is correct because we're in a transaction.
                GarnetSafeEpochGuard.BeginUnsafe(ref dualContext.KernelSession);

                var oldHei = dualContext.CreateHei1(oldKeyHash);
                _ = Kernel.FindTag(ref oldHei);
                var newHei = dualContext.CreateHei1(newKeyHash);
                Kernel.FindOrCreateTag(ref newHei, dualContext.Store1.Log.BeginAddress);

                if (storeType is StoreType.Main or StoreType.All)
                {
                    SpanByte input = default;
                    var output = new SpanByteAndMemory();
                    var status = GET<TransactionalKeyLocker>(ref oldHei, ref oldKey, ref input, ref output);

                    if (status == GarnetStatus.OK)
                    {
                        Debug.Assert(!output.IsSpanByte);
                        var memoryHandle = output.Memory.Memory.Pin();
                        var ptrVal = (byte*)memoryHandle.Pointer;

                        _ = RespReadUtils.ReadUnsignedLengthHeader(out var headerLength, ref ptrVal, ptrVal + output.Length);

                        // Find expiration time of the old key
                        var expireSpan = new SpanByteAndMemory();
                        var ttlStatus = InternalTTL<TransactionalKeyLocker>(ref oldHei, ref oldKey, storeType, ref expireSpan, milliseconds:true);

                        if (ttlStatus == GarnetStatus.OK && !expireSpan.IsSpanByte)
                        {
                            using var expireMemoryHandle = expireSpan.Memory.Memory.Pin();
                            var expirePtrVal = (byte*)expireMemoryHandle.Pointer;
                            _ = RespReadUtils.TryRead64Int(out var expireTimeMs, ref expirePtrVal, expirePtrVal + expireSpan.Length, out _);

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
                                    var setStatus = SET_Conditional<TransactionalKeyLocker>(ref newHei, ref newKey, ref setValueSpan);

                                    // For SET NX `NOTFOUND` means the operation succeeded
                                    result = setStatus == GarnetStatus.NOTFOUND ? 1 : 0;
                                    returnStatus = GarnetStatus.OK;
                                }
                                else
                                {
                                    _ = SETEX<TransactionalKeyLocker>(ref newHei, newKeySlice, new ArgSlice(ptrVal, headerLength), TimeSpan.FromMilliseconds(expireTimeMs));
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
                                    var setStatus = SET_Conditional<TransactionalKeyLocker>(ref newHei, ref newKey, ref setValueSpan);

                                    // For SET NX `NOTFOUND` means the operation succeeded
                                    result = setStatus == GarnetStatus.NOTFOUND ? 1 : 0;
                                    returnStatus = GarnetStatus.OK;
                                }
                                else
                                {
                                    var value = SpanByte.FromPinnedPointer(ptrVal, headerLength);
                                    _ = SET<TransactionalKeyLocker>(ref newHei, ref newKey, ref value);
                                }
                            }

                            expireSpan.Memory.Dispose();
                            memoryHandle.Dispose();
                            output.Memory.Dispose();

                            // Delete the old key only when SET NX succeeded
                            if (isNX && result == 1)
                            {
                                _ = DELETE<TransactionalKeyLocker>(ref oldHei, ref oldKey, StoreType.Main);
                            }
                            else if (!isNX)
                            {
                                // Delete the old key
                                _ = DELETE<TransactionalKeyLocker>(ref oldHei, ref oldKey, StoreType.Main);
                                returnStatus = GarnetStatus.OK;
                            }
                        }
                    }
                }

                if ((storeType == StoreType.Object || storeType == StoreType.All) && dualContext.IsDual)
                {
                    oldHei = dualContext.CreateHei2(oldKeyHash);
                    _ = Kernel.FindTag(ref oldHei);
                    newHei = dualContext.CreateHei2(newKeyHash);
                    Kernel.FindOrCreateTag(ref newHei, dualContext.Store2.Log.BeginAddress);

                    {
                        var oldKeyArray = oldKeySlice.ToArray();
                        GarnetObjectStoreOutput value = default;
                        var status = GET<TransactionalKeyLocker>(ref oldHei, oldKeyArray, ref value);

                        if (status == GarnetStatus.OK)
                        {
                            var valObj = value.garnetObject;
                            var newKeyArray = newKeySlice.ToArray();

                            returnStatus = GarnetStatus.OK;
                            var canSetAndDelete = true;
                            if (isNX)
                            {
                                // Not using EXISTS method to avoid new allocation of Array for key
                                GarnetObjectStoreOutput output = default;
                                var getNewStatus = GET<TransactionalKeyLocker>(ref newHei, newKeyArray, ref output);
                                canSetAndDelete = getNewStatus == GarnetStatus.NOTFOUND;
                            }

                            if (canSetAndDelete)
                            {
                                // valObj already has expiration time, so no need to write expiration logic here
                                _ = SET<TransactionalKeyLocker>(ref newHei, newKeyArray, valObj);

                                // Delete the old key
                                _ = DELETE<TransactionalKeyLocker>(ref oldHei, oldKeyArray, StoreType.Object);
                                result = 1;
                            }
                            else
                                result = 0;
                        }
                    }
                }
            }
            finally
            {
                GarnetSafeEpochGuard.EndUnsafe(ref dualContext.KernelSession);
                if (createTransaction)
                    txnManager.Commit(true);
            }
            return returnStatus;
        }

        /// <summary>
        /// Returns if key is an existing one in the store.
        /// </summary>
        /// <param name="key">The name of the key to use in the operation</param>
        /// <param name="storeType">The store to operate on.</param>
        /// <returns></returns>
        public GarnetStatus EXISTS<TKeyLocker, TEpochGuard>(ArgSlice key, StoreType storeType)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var status = GarnetStatus.NOTFOUND;
            if (storeType == StoreType.All && !dualContext.IsDual)
                storeType = StoreType.Main;

            if (storeType is StoreType.Main or StoreType.All)
            {
                var _key = key.SpanByte;
                SpanByte input = default;
                var _output = new SpanByteAndMemory { SpanByte = scratchBufferManager.ViewRemainingArgSlice().SpanByte };
                status = GET<TKeyLocker, TEpochGuard>(ref _key, ref input, ref _output);

                if (status == GarnetStatus.OK)
                {
                    if (!_output.IsSpanByte)
                        _output.Memory.Dispose();
                    return status;
                }
            }

            if (storeType is StoreType.Object or StoreType.All)
            {
                GarnetObjectStoreOutput output = default;
                status = GET<TKeyLocker, TEpochGuard>(key.ToArray(), ref output);
            }

            return status;
        }

        /// <summary>
        /// Set a timeout on key
        /// </summary>
        /// <param name="key">The key to set the timeout on.</param>
        /// <param name="expiryMs">Milliseconds value for the timeout.</param>
        /// <param name="timeoutSet">True when the timeout was properly set.</param>
        /// <param name="storeType">The store to operate on.</param>
        /// <param name="expireOption">>Flags to use for the operation.</param>
        /// <returns></returns>
        public unsafe GarnetStatus EXPIRE<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice expiryMs, out bool timeoutSet, StoreType storeType, ExpireOption expireOption)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => EXPIRE<TKeyLocker, TEpochGuard>(key, TimeSpan.FromMilliseconds(NumUtils.BytesToLong(expiryMs.Length, expiryMs.ptr)), out timeoutSet, storeType, expireOption);

        /// <summary>
        /// Set a timeout on key.
        /// </summary>
        /// <param name="key">The key to set the timeout on.</param>
        /// <param name="expiry">The timespan value to set the expiration for.</param>
        /// <param name="timeoutSet">True when the timeout was properly set.</param>
        /// <param name="storeType">The store to operate on.</param>
        /// <param name="expireOption">Flags to use for the operation.</param>
        /// <param name="milliseconds">When true the command executed is PEXPIRE, expire by default.</param>
        /// <returns></returns>
        public unsafe GarnetStatus EXPIRE<TKeyLocker, TEpochGuard>(ArgSlice key, TimeSpan expiry, out bool timeoutSet, StoreType storeType, ExpireOption expireOption, bool milliseconds = false)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var pbCmdInput = stackalloc byte[sizeof(int) + sizeof(long) + RespInputHeader.Size + sizeof(byte)];
            *(int*)pbCmdInput = sizeof(long) + RespInputHeader.Size;
            ((RespInputHeader*)(pbCmdInput + sizeof(int) + sizeof(long)))->cmd = milliseconds ? RespCommand.PEXPIRE : RespCommand.EXPIRE;
            ((RespInputHeader*)(pbCmdInput + sizeof(int) + sizeof(long)))->flags = 0;

            *(pbCmdInput + sizeof(int) + sizeof(long) + RespInputHeader.Size) = (byte)expireOption;
            ref var input = ref SpanByte.Reinterpret(pbCmdInput);

            input.ExtraMetadata = DateTimeOffset.UtcNow.Ticks + expiry.Ticks;

            var rmwOutput = stackalloc byte[ObjectOutputHeader.Size];
            var output = new SpanByteAndMemory(SpanByte.FromPinnedPointer(rmwOutput, ObjectOutputHeader.Size));
            timeoutSet = false;

            var found = false;
            if (storeType == StoreType.All && !dualContext.IsDual)
                storeType = StoreType.Main;

            if (storeType is StoreType.Main or StoreType.All)
            {
                var _key = key.SpanByte;
                var status = dualContext.RMW<TKeyLocker, TEpochGuard>(ref _key, ref input, ref output);

                if (status.IsPending)
                    CompletePending<TKeyLocker>(out status, out output);
                if (status.Found) found = true;
            }

            if (!found && (storeType is StoreType.Object or StoreType.All))
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
                var status = dualContext.RMW<TKeyLocker, TEpochGuard>(ref keyBytes, ref objInput, ref objOutput);

                if (status.IsPending)
                    CompletePending<TKeyLocker>(out status, out objOutput);
                if (status.Found) found = true;

                output = objOutput.spanByteAndMemory;
            }

            Debug.Assert(output.IsSpanByte);
            if (found) timeoutSet = ((ObjectOutputHeader*)output.SpanByte.ToPointer())->result1 == 1;

            return found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        public unsafe GarnetStatus PERSIST<TKeyLocker, TEpochGuard>(ArgSlice key, StoreType storeType)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var status = GarnetStatus.NOTFOUND;

            var inputSize = sizeof(int) + RespInputHeader.Size;
            var pbCmdInput = stackalloc byte[inputSize];
            var pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            (*(RespInputHeader*)pcurr).cmd = RespCommand.PERSIST;
            (*(RespInputHeader*)pcurr).flags = 0;

            var pbOutput = stackalloc byte[8];
            var output = new SpanByteAndMemory(pbOutput, 8);

            if (storeType is StoreType.Main or StoreType.All)
            {
                var _key = key.SpanByte;
                var _status = dualContext.RMW<TKeyLocker, TEpochGuard>(ref _key, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref output);

                if (_status.IsPending)
                    CompletePending<TKeyLocker>(out _status, out output);

                Debug.Assert(output.IsSpanByte);
                if (output.SpanByte.AsReadOnlySpan()[0] == 1)
                    status = GarnetStatus.OK;
            }

            if (status == GarnetStatus.NOTFOUND && (storeType is StoreType.Object or StoreType.All) && dualContext.IsDual)
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

                var objO = new GarnetObjectStoreOutput { spanByteAndMemory = output };
                var _key = key.ToArray();
                var _status = dualContext.RMW<TKeyLocker, TEpochGuard>(ref _key, ref objInput, ref objO);

                if (_status.IsPending)
                    CompletePending<TKeyLocker>(out _status, out objO);

                Debug.Assert(output.IsSpanByte);
                if (output.SpanByte.AsReadOnlySpan().Slice(0, CmdStrings.RESP_RETURN_VAL_1.Length)
                    .SequenceEqual(CmdStrings.RESP_RETURN_VAL_1))
                    status = GarnetStatus.OK;
            }

            return status;
        }

        public GarnetStatus GetKeyType<TKeyLocker, TEpochGuard>(ArgSlice key, out string keyType)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            keyType = "string";
            // Check if key exists in Main store
            var status = EXISTS<TKeyLocker, TEpochGuard>(key, StoreType.Main);

            // If key was not found in the main store then it is an object
            if (status != GarnetStatus.OK && dualContext.IsDual)
            {
                GarnetObjectStoreOutput output = default;
                status = GET<TKeyLocker, TEpochGuard>(key.ToArray(), ref output);
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

        public GarnetStatus MemoryUsageForKey<TKeyLocker, TEpochGuard>(ArgSlice key, out long memoryUsage, int samples = 0) // TODO: Implement SAMPLES
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            memoryUsage = -1;

            // Check if key exists in Main store
            var status = GET<TKeyLocker, TEpochGuard>(key, out ArgSlice keyValue);

            if (status == GarnetStatus.NOTFOUND)
            {
                GarnetObjectStoreOutput objectValue = new();
                status = GET<TKeyLocker, TEpochGuard>(key.ToArray(), ref objectValue);
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

        public GarnetStatus Watch<TKeyLocker, TEpochGuard>(ArgSlice key, StoreType storeType)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            if (storeType is StoreType.Main or StoreType.All)
                dualContext.ResetModified<TKeyLocker, TEpochGuard>(key.SpanByte, out _);
            if ((storeType is StoreType.Object or StoreType.All) && dualContext.IsDual)
                dualContext.ResetModified<TKeyLocker, TEpochGuard>(key.ToArray(), out _);
            return GarnetStatus.OK;
        }
    }
}