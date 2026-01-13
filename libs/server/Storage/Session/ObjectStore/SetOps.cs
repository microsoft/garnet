// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Tsavorite.core;

namespace Garnet.server
{
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    /// <summary>
    /// Server session for RESP protocol - SET
    /// </summary>
    sealed partial class StorageSession : IDisposable
    {
        /// <summary>
        ///  Adds the specified member to the set at key.
        ///  Specified members that are already a member of this set are ignored.
        ///  If key does not exist, a new set is created.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key">ArgSlice with key</param>
        /// <param name="member"></param>
        /// <param name="saddCount"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        internal unsafe GarnetStatus SetAdd<TObjectContext>(PinnedSpanByte key, PinnedSpanByte member, out int saddCount, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            saddCount = 0;

            // Prepare the parse state
            parseState.InitializeWithArgument(member);

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.Set, RespMetaCommand.None, ref parseState, flags: RespInputFlags.SkipRespOutput) { SetOp = SetOperation.SADD };

            var status = RMWObjectStoreOperation(key.ReadOnlySpan, ref input, out var output, ref objectContext);

            saddCount = output.result1;
            return status;
        }

        /// <summary>
        ///  Adds the specified members to the set at key.
        ///  Specified members that are already a member of this set are ignored.
        ///  If key does not exist, a new set is created.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key">ArgSlice with key</param>
        /// <param name="members"></param>
        /// <param name="saddCount"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        internal unsafe GarnetStatus SetAdd<TObjectContext>(PinnedSpanByte key, PinnedSpanByte[] members, out int saddCount, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            saddCount = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            parseState.InitializeWithArguments(members);

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.Set, RespMetaCommand.None, ref parseState, flags: RespInputFlags.SkipRespOutput) { SetOp = SetOperation.SADD };

            // Iterate through all inputs and add them to the scratch buffer in RESP format


            var status = RMWObjectStoreOperation(key.ReadOnlySpan, ref input, out var output, ref objectContext);
            saddCount = output.result1;

            return status;
        }

        /// <summary>
        /// Removes the specified member from the set.
        /// Members that are not in the set are ignored.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key">ArgSlice with key</param>
        /// <param name="member"></param>
        /// <param name="sremCount"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        internal unsafe GarnetStatus SetRemove<TObjectContext>(PinnedSpanByte key, PinnedSpanByte member, out int sremCount, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            sremCount = 0;

            // Prepare the parse state
            parseState.InitializeWithArgument(member);

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.Set, RespMetaCommand.None, ref parseState, flags: RespInputFlags.SkipRespOutput) { SetOp = SetOperation.SREM };

            var status = RMWObjectStoreOperation(key.ReadOnlySpan, ref input, out var output, ref objectContext);
            sremCount = output.result1;

            return status;
        }


        /// <summary>
        /// Removes the specified members from the set.
        /// Specified members that are not a member of the set are ignored.
        /// If key does not exist, this command returns 0.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key">ArgSlice with key</param>
        /// <param name="members"></param>
        /// <param name="sremCount"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        internal unsafe GarnetStatus SetRemove<TObjectContext>(PinnedSpanByte key, PinnedSpanByte[] members, out int sremCount, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            sremCount = 0;

            if (key.Length == 0 || members.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            parseState.InitializeWithArguments(members);

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.Set, RespMetaCommand.None, ref parseState, flags: RespInputFlags.SkipRespOutput) { SetOp = SetOperation.SREM };

            var status = RMWObjectStoreOperation(key.ReadOnlySpan, ref input, out var output, ref objectContext);

            sremCount = output.result1;
            return status;
        }

        /// <summary>
        /// Returns the number of elements of the set.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="count"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        internal unsafe GarnetStatus SetLength<TObjectContext>(PinnedSpanByte key, out int count, ref TObjectContext objectContext)
                where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            count = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.Set, flags: RespInputFlags.SkipRespOutput) { SetOp = SetOperation.SCARD };

            var status = ReadObjectStoreOperation(key.ReadOnlySpan, ref input, out var output, ref objectContext);

            count = output.result1;
            return status;
        }

        /// <summary>
        /// Returns all members of the set at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="members"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        internal unsafe GarnetStatus SetMembers<TObjectContext>(PinnedSpanByte key, out PinnedSpanByte[] members, ref TObjectContext objectContext)
             where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            members = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.Set) { SetOp = SetOperation.SMEMBERS };

            var output = new ObjectOutput();

            var status = RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

            if (status == GarnetStatus.OK)
                members = ProcessRespArrayOutput(output, out _);

            return status;
        }

        /// <summary>
        /// Removes and returns one random member from the set at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="element"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        internal GarnetStatus SetPop<TObjectContext>(PinnedSpanByte key, out PinnedSpanByte element, ref TObjectContext objectContext)
             where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = SetPop(key, int.MinValue, out var elements, ref objectContext);
            element = default;
            if (status == GarnetStatus.OK && elements != default)
                element = elements[0];

            return status;
        }

        /// <summary>
        /// Removes and returns up to count random members from the set at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="count"></param>
        /// <param name="elements"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        internal unsafe GarnetStatus SetPop<TObjectContext>(PinnedSpanByte key, int count, out PinnedSpanByte[] elements, ref TObjectContext objectContext)
             where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            elements = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.Set, arg1: count) { SetOp = SetOperation.SPOP };

            var output = new ObjectOutput();

            var status = RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

            if (status != GarnetStatus.OK)
                return status;

            //process output
            elements = ProcessRespArrayOutput(output, out _);

            return GarnetStatus.OK;
        }

        /// <summary>
        /// Moves a member from a source set to a destination set.
        /// If the move was performed, this command returns 1.
        /// If the member was not found in the source set, or if no operation was performed, this command returns 0.
        /// </summary>
        /// <param name="sourceKey"></param>
        /// <param name="destinationKey"></param>
        /// <param name="member"></param>
        /// <param name="smoveResult"></param>
        internal unsafe GarnetStatus SetMove(PinnedSpanByte sourceKey, PinnedSpanByte destinationKey, PinnedSpanByte member, out int smoveResult)
        {
            smoveResult = 0;

            var createTransaction = false;
            if (txnManager.state != TxnState.Running)
            {
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Object | TransactionStoreTypes.Unified);
                txnManager.SaveKeyEntryToLock(sourceKey, LockType.Exclusive);
                txnManager.SaveKeyEntryToLock(destinationKey, LockType.Exclusive);
                _ = txnManager.Run(true);
            }

            var objectTransactionalContext = txnManager.ObjectTransactionalContext;
            var unifiedTransactionalContext = txnManager.UnifiedTransactionalContext;

            try
            {
                var srcGetStatus = GET(sourceKey, out var srcObject, ref objectTransactionalContext);

                if (srcGetStatus == GarnetStatus.NOTFOUND)
                    return GarnetStatus.NOTFOUND;

                if (srcObject.GarnetObject is not SetObject srcSetObject)
                    return GarnetStatus.WRONGTYPE;

                // If the keys are the same, no operation is performed.
                var sameKey = sourceKey.ReadOnlySpan.SequenceEqual(destinationKey.ReadOnlySpan);
                if (sameKey)
                    return GarnetStatus.OK;

                var dstGetStatus = GET(destinationKey, out var dstObject, ref objectTransactionalContext);

                SetObject dstSetObject;
                if (dstGetStatus == GarnetStatus.OK)
                {
                    if (dstObject.GarnetObject is not SetObject tmpDstSetObject)
                        return GarnetStatus.WRONGTYPE;

                    dstSetObject = tmpDstSetObject;
                }
                else
                {
                    dstSetObject = new SetObject();
                }

                var arrMember = member.ToArray();

                var removed = srcSetObject.Set.Remove(arrMember);
                if (!removed) return GarnetStatus.OK;

                srcSetObject.UpdateSize(arrMember, false);

                if (srcSetObject.Set.Count == 0)
                {
                    _ = EXPIRE(sourceKey, TimeSpan.Zero, out _, ExpireOption.None, ref unifiedTransactionalContext);
                }

                _ = dstSetObject.Set.Add(arrMember);
                dstSetObject.UpdateSize(arrMember);

                if (dstGetStatus == GarnetStatus.NOTFOUND)
                {
                    var setStatus = SET(destinationKey, dstSetObject, ref objectTransactionalContext);
                    if (setStatus == GarnetStatus.OK)
                        smoveResult = 1;
                }
                else
                {
                    smoveResult = 1;
                }
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }

            return GarnetStatus.OK;
        }


        /// <summary>
        /// Returns the members of the set resulting from the intersection of all the given sets.
        /// Keys that do not exist are considered to be empty sets.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        public GarnetStatus SetIntersect(PinnedSpanByte[] keys, out HashSet<byte[]> output)
        {
            output = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Object);
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, LockType.Shared);
                _ = txnManager.Run(true);
            }

            // SetObject
            var setObjectTransactionalContext = txnManager.ObjectTransactionalContext;

            try
            {
                return SetIntersect(keys, ref setObjectTransactionalContext, out output);
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }
        }

        /// <summary>
        /// This command is equal to SINTER, but instead of returning the resulting set, it is stored in destination.
        /// If destination already exists, it is overwritten.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="keys"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public GarnetStatus SetIntersectStore(PinnedSpanByte key, PinnedSpanByte[] keys, out int count)
        {
            count = default;

            if (keys.Length == 0)
            {
                return GarnetStatus.OK;
            }

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Object | TransactionStoreTypes.Unified);
                txnManager.SaveKeyEntryToLock(key, LockType.Exclusive);
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, LockType.Shared);
                _ = txnManager.Run(true);
            }

            // SetObject
            var setObjectTransactionalContext = txnManager.ObjectTransactionalContext;
            var setUnifiedTransactionalContext = txnManager.UnifiedTransactionalContext;

            try
            {
                var status = SetIntersect(keys, ref setObjectTransactionalContext, out var members);

                if (status == GarnetStatus.OK)
                {
                    if (members.Count > 0)
                    {
                        var newSetObject = new SetObject();
                        foreach (var item in members)
                        {
                            _ = newSetObject.Set.Add(item);
                            newSetObject.UpdateSize(item);
                        }

                        _ = SET(key, newSetObject, ref setObjectTransactionalContext);
                    }
                    else
                    {
                        _ = EXPIRE(key, TimeSpan.Zero, out _, ExpireOption.None, ref setUnifiedTransactionalContext);
                    }

                    count = members.Count;
                }

                return status;
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }
        }


        private GarnetStatus SetIntersect<TObjectContext>(ReadOnlySpan<PinnedSpanByte> keys, ref TObjectContext objectContext, out HashSet<byte[]> output)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            output = new HashSet<byte[]>(ByteArrayComparer.Instance);

            if (keys.Length == 0)
            {
                return GarnetStatus.OK;
            }

            var status = GET(keys[0], out var first, ref objectContext);
            if (status == GarnetStatus.NOTFOUND)
                return GarnetStatus.OK;
            if (status == GarnetStatus.WRONGTYPE)
                return GarnetStatus.WRONGTYPE;

            if (status == GarnetStatus.OK)
            {
                if (first.GarnetObject is not SetObject firstObject)
                {
                    output = default;
                    return GarnetStatus.WRONGTYPE;
                }

                output = new HashSet<byte[]>(firstObject.Set, ByteArrayComparer.Instance);
            }
            else
            {
                return GarnetStatus.OK;
            }


            for (var i = 1; i < keys.Length; i++)
            {
                // intersection of anything with empty set is empty set
                if (output.Count == 0)
                {
                    output.Clear();
                    return GarnetStatus.OK;
                }

                status = GET(keys[i], out var next, ref objectContext);
                if (status == GarnetStatus.WRONGTYPE)
                    return GarnetStatus.WRONGTYPE;
                if (status == GarnetStatus.OK)
                {
                    if (next.GarnetObject is not SetObject nextObject)
                    {
                        output = default;
                        return GarnetStatus.WRONGTYPE;
                    }

                    output.IntersectWith(nextObject.Set);
                }
                else
                {
                    output.Clear();
                    return GarnetStatus.OK;
                }
            }

            return GarnetStatus.OK;
        }

        /// <summary>
        /// Returns the members of the set resulting from the union of all the given sets.
        /// Keys that do not exist are considered to be empty sets.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        public GarnetStatus SetUnion(PinnedSpanByte[] keys, out HashSet<byte[]> output)
        {
            output = new HashSet<byte[]>(ByteArrayComparer.Instance);

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Object);
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, LockType.Shared);
                _ = txnManager.Run(true);
            }

            // SetObject
            var setObjectTransactionalContext = txnManager.ObjectTransactionalContext;

            try
            {
                return SetUnion(keys, ref setObjectTransactionalContext, out output);
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }
        }

        /// <summary>
        /// This command is equal to SUNION, but instead of returning the resulting set, it is stored in destination.
        /// If destination already exists, it is overwritten.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="keys"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public GarnetStatus SetUnionStore(PinnedSpanByte key, PinnedSpanByte[] keys, out int count)
        {
            count = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Object | TransactionStoreTypes.Unified);
                txnManager.SaveKeyEntryToLock(key, LockType.Exclusive);
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, LockType.Shared);
                _ = txnManager.Run(true);
            }

            // SetObject
            var setObjectTransactionalContext = txnManager.ObjectTransactionalContext;
            var setUnifiedTransactionalContext = txnManager.UnifiedTransactionalContext;

            try
            {
                var status = SetUnion(keys, ref setObjectTransactionalContext, out var members);

                if (status == GarnetStatus.OK)
                {
                    if (members.Count > 0)
                    {
                        var newSetObject = new SetObject();
                        foreach (var item in members)
                        {
                            _ = newSetObject.Set.Add(item);
                            newSetObject.UpdateSize(item);
                        }

                        _ = SET(key, newSetObject, ref setObjectTransactionalContext);
                    }
                    else
                    {
                        _ = EXPIRE(key, TimeSpan.Zero, out _, ExpireOption.None, ref setUnifiedTransactionalContext);
                    }

                    count = members.Count;
                }

                return status;
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }
        }

        private GarnetStatus SetUnion<TObjectContext>(PinnedSpanByte[] keys, ref TObjectContext objectContext, out HashSet<byte[]> output)
             where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            output = new HashSet<byte[]>(ByteArrayComparer.Instance);
            if (keys.Length == 0)
                return GarnetStatus.OK;

            foreach (var item in keys)
            {
                if (GET(item, out var currObject, ref objectContext) == GarnetStatus.OK)
                {
                    if (currObject.GarnetObject is not SetObject setObject)
                    {
                        output = default;
                        return GarnetStatus.WRONGTYPE;
                    }

                    output.UnionWith(setObject.Set);
                }
            }

            return GarnetStatus.OK;
        }

        /// <summary>
        ///  Adds the specified members to the set at key.
        ///  Specified members that are already a member of this set are ignored.
        ///  If key does not exist, a new set is created.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SetAdd<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
           => RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Removes the specified members from the set.
        /// Specified members that are not a member of this set are ignored.
        /// If key does not exist, this command returns 0.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SetRemove<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Returns the number of elements of the set.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SetLength<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Returns all members of the set at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SetMembers<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Returns if member is a member of the set stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SetIsMember<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Returns whether each member is a member of the set stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="members"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SetIsMember<TObjectContext>(PinnedSpanByte key, PinnedSpanByte[] members, out int[] result, ref TObjectContext objectContext)
             where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            result = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            parseState.InitializeWithArguments(members);

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.Set, RespMetaCommand.None, ref parseState) { SetOp = members.Length == 1 ? SetOperation.SISMEMBER : SetOperation.SMISMEMBER };

            var output = new ObjectOutput { SpanByteAndMemory = new SpanByteAndMemory(null) };
            var status = ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

            if (status == GarnetStatus.OK)
                result = ProcessRespIntegerArrayOutput(output, out _);

            return status;
        }

        /// <summary>
        /// Removes and returns one or more random members from the set at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SetPop<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// When called with just the key argument, return a random element from the set value stored at key.
        /// If the provided count argument is positive, return an array of distinct elements.
        /// The array's length is either count or the set's cardinality (SCARD), whichever is lower.
        /// If called with a negative count, the behavior changes and the command is allowed to return the same element multiple times.
        /// In this case, the number of returned elements is the absolute value of the specified count.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SetRandomMember<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Returns the members of the set resulting from the difference between the first set at key and all the successive sets at keys.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="members"></param>
        /// <returns></returns>
        public GarnetStatus SetDiff(PinnedSpanByte[] keys, out HashSet<byte[]> members)
        {
            members = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Object);
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, LockType.Shared);
                _ = txnManager.Run(true);
            }

            // SetObject
            var setObjectTransactionalContext = txnManager.ObjectTransactionalContext;

            try
            {
                return SetDiff(keys, ref setObjectTransactionalContext, out members);
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }
        }

        /// <summary>
        /// This command is equal to SDIFF, but instead of returning the resulting set, it is stored in destination.
        /// If destination already exists, it is overwritten.
        /// </summary>
        /// <param name="key">destination</param>
        /// <param name="keys"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public GarnetStatus SetDiffStore(PinnedSpanByte key, PinnedSpanByte[] keys, out int count)
        {
            count = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Object | TransactionStoreTypes.Unified);
                txnManager.SaveKeyEntryToLock(key, LockType.Exclusive);
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, LockType.Shared);
                _ = txnManager.Run(true);
            }

            // SetObject
            var setObjectTransactionalContext = txnManager.ObjectTransactionalContext;
            var setUnifiedTransactionalContext = txnManager.UnifiedTransactionalContext;

            try
            {
                var status = SetDiff(keys, ref setObjectTransactionalContext, out var diffSet);

                if (status == GarnetStatus.OK)
                {
                    if (diffSet.Count > 0)
                    {
                        var newSetObject = new SetObject();
                        foreach (var item in diffSet)
                        {
                            _ = newSetObject.Set.Add(item);
                            newSetObject.UpdateSize(item);
                        }
                        _ = SET(key, newSetObject, ref setObjectTransactionalContext);
                    }
                    else
                    {
                        _ = EXPIRE(key, TimeSpan.Zero, out _, ExpireOption.None, ref setUnifiedTransactionalContext);
                    }

                    count = diffSet.Count;
                }

                return status;
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }
        }

        private GarnetStatus SetDiff<TObjectContext>(PinnedSpanByte[] keys, ref TObjectContext objectContext, out HashSet<byte[]> output)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            output = new HashSet<byte[]>();
            if (keys.Length == 0)
            {
                return GarnetStatus.OK;
            }

            // first SetObject
            var status = GET(keys[0], out var first, ref objectContext);
            if (status == GarnetStatus.NOTFOUND)
                return GarnetStatus.OK;
            if (status == GarnetStatus.WRONGTYPE)
                return GarnetStatus.WRONGTYPE;

            if (status == GarnetStatus.OK)
            {
                if (first.GarnetObject is not SetObject firstObject)
                {
                    output = default;
                    return GarnetStatus.WRONGTYPE;
                }

                output = new HashSet<byte[]>(firstObject.Set, ByteArrayComparer.Instance);
            }
            else
            {
                return GarnetStatus.OK;
            }

            // after SetObjects
            for (var i = 1; i < keys.Length; i++)
            {
                status = GET(keys[i], out var next, ref objectContext);
                if (status == GarnetStatus.WRONGTYPE)
                    return GarnetStatus.WRONGTYPE;
                if (status == GarnetStatus.OK)
                {
                    if (next.GarnetObject is not SetObject nextObject)
                    {
                        output = default;
                        return GarnetStatus.WRONGTYPE;
                    }

                    output.ExceptWith(nextObject.Set);
                }
            }

            return GarnetStatus.OK;
        }

        /// <summary>
        /// Returns the cardinality of the intersection of all the given sets.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="limit">Optional limit for stopping early when reaching this size</param>
        /// <param name="count"></param>
        /// <returns></returns>
        public GarnetStatus SetIntersectLength(ReadOnlySpan<PinnedSpanByte> keys, int? limit, out int count)
        {
            if (txnManager.ObjectTransactionalContext.Session is null)
                ThrowObjectStoreUninitializedException();

            count = 0;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Object);
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, LockType.Shared);
                _ = txnManager.Run(true);
            }

            var setObjectTransactionalContext = txnManager.ObjectTransactionalContext;

            try
            {
                var status = SetIntersect(keys, ref setObjectTransactionalContext, out var result);
                if (status == GarnetStatus.OK && result != null)
                {
                    count = limit > 0 ? Math.Min(result.Count, limit.Value) : result.Count;
                }
                return status;
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }
        }
    }
}