// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Tsavorite.core;

namespace Garnet.server
{
    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

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
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        internal unsafe GarnetStatus SetAdd<TObjectContext>(ArgSlice key, ArgSlice member, out int saddCount, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            saddCount = 0;

            // Prepare the parse state
            parseState.InitializeWithArgument(member);

            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.Set) { SetOp = SetOperation.SADD };
            var input = new ObjectInput(header, ref parseState);

            var status = RMWObjectStoreOperation(key.ToArray(), ref input, out var output, ref objectStoreContext);

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
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        internal unsafe GarnetStatus SetAdd<TObjectContext>(ArgSlice key, ArgSlice[] members, out int saddCount, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            saddCount = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            parseState.InitializeWithArguments(members);

            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.Set) { SetOp = SetOperation.SADD };
            var input = new ObjectInput(header, ref parseState);

            // Iterate through all inputs and add them to the scratch buffer in RESP format


            var status = RMWObjectStoreOperation(key.ToArray(), ref input, out var output, ref objectStoreContext);
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
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        internal unsafe GarnetStatus SetRemove<TObjectContext>(ArgSlice key, ArgSlice member, out int sremCount, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            sremCount = 0;

            // Prepare the parse state
            parseState.InitializeWithArgument(member);

            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.Set) { SetOp = SetOperation.SREM };
            var input = new ObjectInput(header, ref parseState);

            var status = RMWObjectStoreOperation(key.ToArray(), ref input, out var output, ref objectStoreContext);
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
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        internal unsafe GarnetStatus SetRemove<TObjectContext>(ArgSlice key, ArgSlice[] members, out int sremCount, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            sremCount = 0;

            if (key.Length == 0 || members.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            parseState.InitializeWithArguments(members);

            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.Set) { SetOp = SetOperation.SREM };
            var input = new ObjectInput(header, ref parseState);

            var status = RMWObjectStoreOperation(key.ToArray(), ref input, out var output, ref objectStoreContext);

            sremCount = output.result1;
            return status;
        }

        /// <summary>
        /// Returns the number of elements of the set.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="count"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        internal unsafe GarnetStatus SetLength<TObjectContext>(ArgSlice key, out int count, ref TObjectContext objectStoreContext)
                where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            count = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.Set) { SetOp = SetOperation.SCARD };
            var input = new ObjectInput(header);

            var status = ReadObjectStoreOperation(key.ToArray(), ref input, out var output, ref objectStoreContext);

            count = output.result1;
            return status;
        }

        /// <summary>
        /// Returns all members of the set at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="members"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        internal unsafe GarnetStatus SetMembers<TObjectContext>(ArgSlice key, out ArgSlice[] members, ref TObjectContext objectStoreContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            members = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.Set) { SetOp = SetOperation.SMEMBERS };
            var input = new ObjectInput(header);

            var output = new GarnetObjectStoreOutput();

            var status = RMWObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectStoreContext, ref output);

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
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        internal GarnetStatus SetPop<TObjectContext>(ArgSlice key, out ArgSlice element, ref TObjectContext objectStoreContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            var status = SetPop(key, int.MinValue, out var elements, ref objectStoreContext);
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
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        internal unsafe GarnetStatus SetPop<TObjectContext>(ArgSlice key, int count, out ArgSlice[] elements, ref TObjectContext objectStoreContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            elements = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.Set) { SetOp = SetOperation.SPOP };
            var input = new ObjectInput(header, count);

            var output = new GarnetObjectStoreOutput();

            var status = RMWObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectStoreContext, ref output);

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
        internal unsafe GarnetStatus SetMove(ArgSlice sourceKey, ArgSlice destinationKey, ArgSlice member, out int smoveResult)
        {
            smoveResult = 0;

            var createTransaction = false;
            if (txnManager.state != TxnState.Running)
            {
                createTransaction = true;
                txnManager.SaveKeyEntryToLock(sourceKey, true, LockType.Exclusive);
                txnManager.SaveKeyEntryToLock(destinationKey, true, LockType.Exclusive);
                _ = txnManager.Run(true);
            }

            var objectLockableContext = txnManager.ObjectStoreLockableContext;

            try
            {
                var arrDstKey = destinationKey.ToArray();
                var arrSrcKey = sourceKey.ToArray();

                var srcGetStatus = GET(arrSrcKey, out var srcObject, ref objectLockableContext);

                if (srcGetStatus == GarnetStatus.NOTFOUND)
                    return GarnetStatus.NOTFOUND;

                if (srcObject.GarnetObject is not SetObject srcSetObject)
                    return GarnetStatus.WRONGTYPE;

                // If the keys are the same, no operation is performed.
                var sameKey = sourceKey.ReadOnlySpan.SequenceEqual(destinationKey.ReadOnlySpan);
                if (sameKey)
                    return GarnetStatus.OK;

                var dstGetStatus = GET(arrDstKey, out var dstObject, ref objectLockableContext);

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
                    _ = EXPIRE(sourceKey, TimeSpan.Zero, out _, StoreType.Object, ExpireOption.None,
                        ref lockableContext, ref objectLockableContext);
                }

                dstSetObject.Set.Add(arrMember);
                dstSetObject.UpdateSize(arrMember);

                if (dstGetStatus == GarnetStatus.NOTFOUND)
                {
                    var setStatus = SET(arrDstKey, dstSetObject, ref objectLockableContext);
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
        public GarnetStatus SetIntersect(ArgSlice[] keys, out HashSet<byte[]> output)
        {
            output = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, true, LockType.Shared);
                _ = txnManager.Run(true);
            }

            // SetObject
            var setObjectStoreLockableContext = txnManager.ObjectStoreLockableContext;

            try
            {
                return SetIntersect(keys, ref setObjectStoreLockableContext, out output);
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
        public GarnetStatus SetIntersectStore(byte[] key, ArgSlice[] keys, out int count)
        {
            count = default;

            if (keys.Length == 0)
            {
                return GarnetStatus.OK;
            }

            var destination = scratchBufferBuilder.CreateArgSlice(key);

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.SaveKeyEntryToLock(destination, true, LockType.Exclusive);
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, true, LockType.Shared);
                _ = txnManager.Run(true);
            }

            // SetObject
            var setObjectStoreLockableContext = txnManager.ObjectStoreLockableContext;

            try
            {
                var status = SetIntersect(keys, ref setObjectStoreLockableContext, out var members);

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

                        _ = SET(key, newSetObject, ref setObjectStoreLockableContext);
                    }
                    else
                    {
                        _ = EXPIRE(destination, TimeSpan.Zero, out _, StoreType.Object, ExpireOption.None,
                            ref lockableContext, ref setObjectStoreLockableContext);
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


        private GarnetStatus SetIntersect<TObjectContext>(ReadOnlySpan<ArgSlice> keys, ref TObjectContext objectContext, out HashSet<byte[]> output)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            output = new HashSet<byte[]>(ByteArrayComparer.Instance);

            if (keys.Length == 0)
            {
                return GarnetStatus.OK;
            }

            var status = GET(keys[0].ToArray(), out var first, ref objectContext);

            if (status == GarnetStatus.NOTFOUND)
            {
                return GarnetStatus.OK;
            }

            if (status == GarnetStatus.WRONGTYPE)
            {
                return GarnetStatus.WRONGTYPE;
            }

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

                status = GET(keys[i].ToArray(), out var next, ref objectContext);
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
        public GarnetStatus SetUnion(ArgSlice[] keys, out HashSet<byte[]> output)
        {
            output = new HashSet<byte[]>(ByteArrayComparer.Instance);

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, true, LockType.Shared);
                _ = txnManager.Run(true);
            }

            // SetObject
            var setObjectStoreLockableContext = txnManager.ObjectStoreLockableContext;

            try
            {
                return SetUnion(keys, ref setObjectStoreLockableContext, out output);
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
        public GarnetStatus SetUnionStore(byte[] key, ArgSlice[] keys, out int count)
        {
            count = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var destination = scratchBufferBuilder.CreateArgSlice(key);

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.SaveKeyEntryToLock(destination, true, LockType.Exclusive);
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, true, LockType.Shared);
                _ = txnManager.Run(true);
            }

            // SetObject
            var setObjectStoreLockableContext = txnManager.ObjectStoreLockableContext;

            try
            {
                var status = SetUnion(keys, ref setObjectStoreLockableContext, out var members);

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

                        _ = SET(key, newSetObject, ref setObjectStoreLockableContext);
                    }
                    else
                    {
                        _ = EXPIRE(destination, TimeSpan.Zero, out _, StoreType.Object, ExpireOption.None,
                            ref lockableContext, ref setObjectStoreLockableContext);
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

        private GarnetStatus SetUnion<TObjectContext>(ArgSlice[] keys, ref TObjectContext objectContext, out HashSet<byte[]> output)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            output = new HashSet<byte[]>(ByteArrayComparer.Instance);
            if (keys.Length == 0)
            {
                return GarnetStatus.OK;
            }

            foreach (var item in keys)
            {
                if (GET(item.ToArray(), out var currObject, ref objectContext) == GarnetStatus.OK)
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
        public GarnetStatus SetAdd<TObjectContext>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
           => RMWObjectStoreOperation(key, ref input, out output, ref objectContext);

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
        public GarnetStatus SetRemove<TObjectContext>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => RMWObjectStoreOperation(key, ref input, out output, ref objectContext);

        /// <summary>
        /// Returns the number of elements of the set.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SetLength<TObjectContext>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperation(key, ref input, out output, ref objectContext);

        /// <summary>
        /// Returns all members of the set at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SetMembers<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperationWithOutput(key, ref input, ref objectContext, ref output);

        /// <summary>
        /// Returns if member is a member of the set stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SetIsMember<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperationWithOutput(key, ref input, ref objectContext, ref output);

        /// <summary>
        /// Returns whether each member is a member of the set stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="members"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SetIsMember<TObjectContext>(ArgSlice key, ArgSlice[] members, out int[] result, ref TObjectContext objectContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            result = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            parseState.InitializeWithArguments(members);

            // Prepare the input
            var input = new ObjectInput(new RespInputHeader
            {
                type = GarnetObjectType.Set,
                SetOp = SetOperation.SMISMEMBER,
            }, ref parseState);

            var output = new GarnetObjectStoreOutput();
            var status = ReadObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectContext, ref output);

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
        public GarnetStatus SetPop<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => RMWObjectStoreOperationWithOutput(key, ref input, ref objectContext, ref output);

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
        public GarnetStatus SetRandomMember<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperationWithOutput(key, ref input, ref objectContext, ref output);

        /// <summary>
        /// Returns the members of the set resulting from the difference between the first set at key and all the successive sets at keys.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="members"></param>
        /// <returns></returns>
        public GarnetStatus SetDiff(ArgSlice[] keys, out HashSet<byte[]> members)
        {
            members = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, true, LockType.Shared);
                _ = txnManager.Run(true);
            }

            // SetObject
            var setObjectStoreLockableContext = txnManager.ObjectStoreLockableContext;

            try
            {
                return SetDiff(keys, ref setObjectStoreLockableContext, out members);
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
        public GarnetStatus SetDiffStore(byte[] key, ArgSlice[] keys, out int count)
        {
            count = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var destination = scratchBufferBuilder.CreateArgSlice(key);

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.SaveKeyEntryToLock(destination, true, LockType.Exclusive);
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, true, LockType.Shared);
                _ = txnManager.Run(true);
            }

            // SetObject
            var setObjectStoreLockableContext = txnManager.ObjectStoreLockableContext;

            try
            {
                var status = SetDiff(keys, ref setObjectStoreLockableContext, out var diffSet);

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
                        _ = SET(key, newSetObject, ref setObjectStoreLockableContext);
                    }
                    else
                    {
                        _ = EXPIRE(destination, TimeSpan.Zero, out _, StoreType.Object, ExpireOption.None,
                            ref lockableContext, ref setObjectStoreLockableContext);
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

        private GarnetStatus SetDiff<TObjectContext>(ArgSlice[] keys, ref TObjectContext objectContext, out HashSet<byte[]> output)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            output = new HashSet<byte[]>();
            if (keys.Length == 0)
            {
                return GarnetStatus.OK;
            }

            // first SetObject
            var status = GET(keys[0].ToArray(), out var first, ref objectContext);

            if (status == GarnetStatus.NOTFOUND)
            {
                return GarnetStatus.OK;
            }

            if (status == GarnetStatus.WRONGTYPE)
            {
                return GarnetStatus.WRONGTYPE;
            }

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
                status = GET(keys[i].ToArray(), out var next, ref objectContext);
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
        public GarnetStatus SetIntersectLength(ReadOnlySpan<ArgSlice> keys, int? limit, out int count)
        {
            if (txnManager.ObjectStoreLockableContext.Session is null)
                ThrowObjectStoreUninitializedException();

            count = 0;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, true, LockType.Shared);
                _ = txnManager.Run(true);
            }

            var setObjectStoreLockableContext = txnManager.ObjectStoreLockableContext;

            try
            {
                var status = SetIntersect(keys, ref setObjectStoreLockableContext, out var result);
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