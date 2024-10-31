// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Tsavorite.core;

namespace Garnet.server
{
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
        internal GarnetStatus SetAdd<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice member, out int saddCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            // Prepare the parse state
            var parseState = new SessionParseState();
            ArgSlice[] parseStateBuffer = default;
            parseState.InitializeWithArguments(ref parseStateBuffer, member);

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Set,
                    SetOp = SetOperation.SADD,
                },
                parseState = parseState,
                parseStateStartIdx = 0,
            };

            var status = RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, out var output);

            saddCount = output.result1;
            return status;
        }

        /// <summary>
        ///  Adds the specified members to the set at key.
        ///  Specified members that are already a member of this set are ignored. 
        ///  If key does not exist, a new set is created.
        /// </summary>
        internal GarnetStatus SetAdd<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice[] members, out int saddCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            saddCount = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            var parseState = new SessionParseState();
            ArgSlice[] parseStateBuffer = default;
            parseState.InitializeWithArguments(ref parseStateBuffer, members);

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Set,
                    SetOp = SetOperation.SADD,
                },
                parseState = parseState,
                parseStateStartIdx = 0,
            };

            // Iterate through all inputs and add them to the scratch buffer in RESP format


            var status = RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, out var output);
            saddCount = output.result1;

            return status;
        }

        /// <summary>
        /// Removes the specified member from the set.
        /// Members that are not in the set are ignored.
        /// </summary>
        internal GarnetStatus SetRemove<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice member, out int sremCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            // Prepare the parse state
            var parseState = new SessionParseState();
            ArgSlice[] parseStateBuffer = default;
            parseState.InitializeWithArguments(ref parseStateBuffer, member);

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Set,
                    SetOp = SetOperation.SREM,
                },
                parseState = parseState,
                parseStateStartIdx = 0,
            };

            var status = RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, out var output);
            sremCount = output.result1;

            return status;
        }


        /// <summary>
        /// Removes the specified members from the set.
        /// Specified members that are not a member of the set are ignored. 
        /// If key does not exist, this command returns 0.
        /// </summary>
        internal GarnetStatus SetRemove<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice[] members, out int sremCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            sremCount = 0;

            if (key.Length == 0 || members.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            var parseState = new SessionParseState();
            ArgSlice[] parseStateBuffer = default;
            parseState.InitializeWithArguments(ref parseStateBuffer, members);

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Set,
                    SetOp = SetOperation.SREM,
                },
                parseState = parseState,
                parseStateStartIdx = 0,
            };

            var status = RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, out var output);

            sremCount = output.result1;
            return status;
        }

        /// <summary>
        /// Returns the number of elements of the set.
        /// </summary>
        internal GarnetStatus SetLength<TKeyLocker, TEpochGuard>(ArgSlice key, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            count = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Set,
                    SetOp = SetOperation.SCARD,
                },
            };

            var status = ReadObjectStoreOperation<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, out var output);

            count = output.result1;
            return status;
        }

        /// <summary>
        /// Returns all members of the set at key.
        /// </summary>
        internal GarnetStatus SetMembers<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice[] members)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            members = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Set,
                    SetOp = SetOperation.SMEMBERS,
                },
            };

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };
            var status = RMWObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, ref outputFooter);
            if (status == GarnetStatus.OK)
                members = ProcessRespArrayOutput(outputFooter, out _);
            return status;
        }

        /// <summary>
        /// Removes and returns one random member from the set at key.
        /// </summary>
        internal GarnetStatus SetPop<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice element)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var status = SetPop<TKeyLocker, TEpochGuard>(key, int.MinValue, out var elements);
            element = default;
            if (status == GarnetStatus.OK && elements != default)
                element = elements[0];

            return status;
        }

        /// <summary>
        /// Removes and returns up to count random members from the set at key.
        /// </summary>
        internal GarnetStatus SetPop<TKeyLocker, TEpochGuard>(ArgSlice key, int count, out ArgSlice[] elements)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            elements = default;
            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Set,
                    SetOp = SetOperation.SPOP,
                },
                arg1 = count,
            };

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };
            var status = RMWObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, ref outputFooter);
            if (status != GarnetStatus.OK)
                return status;

            elements = ProcessRespArrayOutput(outputFooter, out _);
            return GarnetStatus.OK;
        }

        /// <summary>
        /// Moves a member from a source set to a destination set.
        /// If the move was performed, this command returns 1.
        /// If the member was not found in the source set, or if no operation was performed, this command returns 0.
        /// </summary>
        internal GarnetStatus SetMove(ArgSlice sourceKey, ArgSlice destinationKey, ArgSlice member, out int smoveResult)
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

            try
            {
                // Perform Store operation under unsafe epoch control for pointer safety with speed, and always use TransactionalSessionLocker as we're in a transaction.
                // We have already locked via TransactionManager.Run so we only need to acquire the epoch here; operations within the transaction can use GarnetUnsafeEpochGuard.
                GarnetSafeEpochGuard.BeginUnsafe(ref dualContext.KernelSession);

                var arrDstKey = destinationKey.ToArray();
                var arrSrcKey = sourceKey.ToArray();

                GarnetObjectStoreOutput srcObject = new();
                var srcGetStatus = GET<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(arrSrcKey, ref srcObject);

                if (srcGetStatus == GarnetStatus.NOTFOUND)
                    return GarnetStatus.NOTFOUND;

                if (srcObject.garnetObject is not SetObject srcSetObject)
                    return GarnetStatus.WRONGTYPE;

                // If the keys are the same, no operation is performed.
                var sameKey = sourceKey.ReadOnlySpan.SequenceEqual(destinationKey.ReadOnlySpan);
                if (sameKey)
                    return GarnetStatus.OK;

                GarnetObjectStoreOutput dstObject = new();
                var dstGetStatus = GET<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(arrDstKey, ref dstObject);

                SetObject dstSetObject;
                if (dstGetStatus == GarnetStatus.OK)
                {
                    if (dstObject.garnetObject is not SetObject tmpDstSetObject)
                        return GarnetStatus.WRONGTYPE;
                    dstSetObject = tmpDstSetObject;
                }
                else
                    dstSetObject = new SetObject();

                var arrMember = member.ToArray();

                if (!srcSetObject.Set.Remove(arrMember))
                    return GarnetStatus.OK;

                srcSetObject.UpdateSize(arrMember, false);
                if (srcSetObject.Set.Count == 0)
                    _ = EXPIRE<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(sourceKey, TimeSpan.Zero, out _, StoreType.Object, ExpireOption.None);

                _ = dstSetObject.Set.Add(arrMember);
                dstSetObject.UpdateSize(arrMember);

                if (dstGetStatus == GarnetStatus.NOTFOUND)
                {
                    var setStatus = SET<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(arrDstKey, dstSetObject);
                    if (setStatus == GarnetStatus.OK)
                        smoveResult = 1;
                }
                else
                    smoveResult = 1;
            }
            finally
            {
                GarnetSafeEpochGuard.EndUnsafe(ref dualContext.KernelSession);
                if (createTransaction)
                    txnManager.Commit(true);
            }

            return GarnetStatus.OK;
        }


        /// <summary>
        /// Returns the members of the set resulting from the intersection of all the given sets.
        /// Keys that do not exist are considered to be empty sets.
        /// </summary>
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

            try
            {
                // Perform Store operation under unsafe epoch control for pointer safety with speed, and always use TransactionalSessionLocker as we're in a transaction.
                // We have already locked via TransactionManager.Run so we only need to acquire the epoch here; operations within the transaction can use GarnetUnsafeEpochGuard.
                GarnetSafeEpochGuard.BeginUnsafe(ref dualContext.KernelSession);

                return SetIntersect<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(keys, out output);
            }
            finally
            {
                GarnetSafeEpochGuard.EndUnsafe(ref dualContext.KernelSession);
                if (createTransaction)
                    txnManager.Commit(true);
            }
        }

        /// <summary>
        /// This command is equal to SINTER, but instead of returning the resulting set, it is stored in destination.
        /// If destination already exists, it is overwritten.
        /// </summary>
        public GarnetStatus SetIntersectStore(byte[] key, ArgSlice[] keys, out int count)
        {
            count = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var destination = scratchBufferManager.CreateArgSlice(key);

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

            try
            {
                // Perform Store operation under unsafe epoch control for pointer safety with speed, and always use TransactionalSessionLocker as we're in a transaction.
                // We have already locked via TransactionManager.Run so we only need to acquire the epoch here; operations within the transaction can use GarnetUnsafeEpochGuard.
                GarnetSafeEpochGuard.BeginUnsafe(ref dualContext.KernelSession);

                var status = SetIntersect<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(keys, out var members);
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
                        _ = SET<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(key, newSetObject);
                    }
                    else
                        _ = EXPIRE<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(destination, TimeSpan.Zero, out _, StoreType.Object, ExpireOption.None);

                    count = members.Count;
                }

                return status;
            }
            finally
            {
                GarnetSafeEpochGuard.EndUnsafe(ref dualContext.KernelSession);
                if (createTransaction)
                    txnManager.Commit(true);
            }
        }


        private GarnetStatus SetIntersect<TKeyLocker, TEpochGuard>(ArgSlice[] keys, out HashSet<byte[]> output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            output = new HashSet<byte[]>(ByteArrayComparer.Instance);
            if (keys.Length == 0)
                return GarnetStatus.OK;

            GarnetObjectStoreOutput first = new();
            var status = GET<TKeyLocker, TEpochGuard>(keys[0].ToArray(), ref first);
            if (status == GarnetStatus.OK)
            {
                if (first.garnetObject is not SetObject firstObject)
                {
                    output = default;
                    return GarnetStatus.WRONGTYPE;
                }
                output = new HashSet<byte[]>(firstObject.Set, ByteArrayComparer.Instance);
            }
            else
                return GarnetStatus.OK;

            for (var i = 1; i < keys.Length; i++)
            {
                // intersection of anything with empty set is empty set
                if (output.Count == 0)
                {
                    output.Clear();
                    return GarnetStatus.OK;
                }

                GarnetObjectStoreOutput next = new();
                status = GET<TKeyLocker, TEpochGuard>(keys[i].ToArray(), ref next);
                if (status == GarnetStatus.OK)
                {
                    if (next.garnetObject is not SetObject nextObject)
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

            try
            {
                // Perform Store operation under unsafe epoch control for pointer safety with speed, and always use TransactionalSessionLocker as we're in a transaction.
                // We have already locked via TransactionManager.Run so we only need to acquire the epoch here; operations within the transaction can use GarnetUnsafeEpochGuard.
                GarnetSafeEpochGuard.BeginUnsafe(ref dualContext.KernelSession);

                return SetUnion<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(keys, out output);
            }
            finally
            {
                GarnetSafeEpochGuard.EndUnsafe(ref dualContext.KernelSession);
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

            var destination = scratchBufferManager.CreateArgSlice(key);

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

            try
            {
                // Perform Store operation under unsafe epoch control for pointer safety with speed, and always use TransactionalSessionLocker as we're in a transaction.
                // We have already locked via TransactionManager.Run so we only need to acquire the epoch here; operations within the transaction can use GarnetUnsafeEpochGuard.
                GarnetSafeEpochGuard.BeginUnsafe(ref dualContext.KernelSession);

                var status = SetUnion<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(keys, out var members);

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
                        _ = SET<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(key, newSetObject);
                    }
                    else
                        _ = EXPIRE<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(destination, TimeSpan.Zero, out _, StoreType.Object, ExpireOption.None);

                    count = members.Count;
                }

                return status;
            }
            finally
            {
                GarnetSafeEpochGuard.EndUnsafe(ref dualContext.KernelSession);
                if (createTransaction)
                    txnManager.Commit(true);
            }
        }

        private GarnetStatus SetUnion<TKeyLocker, TEpochGuard>(ArgSlice[] keys, out HashSet<byte[]> output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            output = new HashSet<byte[]>(ByteArrayComparer.Instance);
            if (keys.Length == 0)
                return GarnetStatus.OK;

            foreach (var item in keys)
            {
                GarnetObjectStoreOutput currObject = new();
                if (GET<TKeyLocker, TEpochGuard>(item.ToArray(), ref currObject) == GarnetStatus.OK)
                {
                    if (currObject.garnetObject is not SetObject setObject)
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
        public GarnetStatus SetAdd<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
           => RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <summary>
        /// Removes the specified members from the set.
        /// Specified members that are not a member of this set are ignored. 
        /// If key does not exist, this command returns 0.
        /// </summary>
        public GarnetStatus SetRemove<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <summary>
        /// Returns the number of elements of the set.
        /// </summary>
        public GarnetStatus SetLength<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => ReadObjectStoreOperation<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <summary>
        /// Returns all members of the set at key.
        /// </summary>
        public GarnetStatus SetMembers<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => ReadObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <summary>
        /// Returns if member is a member of the set stored at key.
        /// </summary>
        public GarnetStatus SetIsMember<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => ReadObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <summary>
        /// Removes and returns one or more random members from the set at key.
        /// </summary>
        public GarnetStatus SetPop<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => RMWObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <summary>
        /// When called with just the key argument, return a random element from the set value stored at key.
        /// If the provided count argument is positive, return an array of distinct elements. 
        /// The array's length is either count or the set's cardinality (SCARD), whichever is lower.
        /// If called with a negative count, the behavior changes and the command is allowed to return the same element multiple times. 
        /// In this case, the number of returned elements is the absolute value of the specified count.
        /// </summary>
        public GarnetStatus SetRandomMember<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => ReadObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

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

            try
            {
                // Perform Store operation under unsafe epoch control for pointer safety with speed, and always use TransactionalSessionLocker as we're in a transaction.
                // We have already locked via TransactionManager.Run so we only need to acquire the epoch here; operations within the transaction can use GarnetUnsafeEpochGuard.
                GarnetSafeEpochGuard.BeginUnsafe(ref dualContext.KernelSession);

                return InternalSetDiff<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(keys, out members);
            }
            finally
            {
                GarnetSafeEpochGuard.EndUnsafe(ref dualContext.KernelSession);
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

            var destination = scratchBufferManager.CreateArgSlice(key);

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

            try
            {
                // Perform Store operation under unsafe epoch control for pointer safety with speed, and always use TransactionalSessionLocker as we're in a transaction.
                // We have already locked via TransactionManager.Run so we only need to acquire the epoch here; operations within the transaction can use GarnetUnsafeEpochGuard.
                GarnetSafeEpochGuard.BeginUnsafe(ref dualContext.KernelSession);

                var status = InternalSetDiff<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(keys, out var diffSet);

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
                        _ = SET<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(key, newSetObject);
                    }
                    else
                    {
                        _ = EXPIRE<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(destination, TimeSpan.Zero, out _, StoreType.Object, ExpireOption.None);
                    }

                    count = diffSet.Count;
                }

                return status;
            }
            finally
            {
                GarnetSafeEpochGuard.EndUnsafe(ref dualContext.KernelSession);
                if (createTransaction)
                    txnManager.Commit(true);
            }
        }

        private GarnetStatus InternalSetDiff<TKeyLocker, TEpochGuard>(ArgSlice[] keys, out HashSet<byte[]> output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            output = [];
            if (keys.Length == 0)
                return GarnetStatus.OK;

            // first SetObject
            GarnetObjectStoreOutput first = new();
            var status = GET<TKeyLocker, TEpochGuard>(keys[0].ToArray(), ref first);
            if (status == GarnetStatus.OK)
            {
                if (first.garnetObject is not SetObject firstObject)
                {
                    output = default;
                    return GarnetStatus.WRONGTYPE;
                }
                output = new HashSet<byte[]>(firstObject.Set, ByteArrayComparer.Instance);
            }
            else
                return GarnetStatus.OK;

            // after SetObjects
            for (var i = 1; i < keys.Length; i++)
            {
                GarnetObjectStoreOutput next = new();
                status = GET<TKeyLocker, TEpochGuard>(keys[i].ToArray(), ref next);
                if (status == GarnetStatus.OK)
                {
                    if (next.garnetObject is not SetObject nextObject)
                    {
                        output = default;
                        return GarnetStatus.WRONGTYPE;
                    }
                    output.ExceptWith(nextObject.Set);
                }
            }

            return GarnetStatus.OK;
        }
    }
}