// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Garnet.common;
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
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key">ArgSlice with key</param>
        /// <param name="member"></param>
        /// <param name="saddCount"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        internal unsafe GarnetStatus SetAdd<TObjectContext>(ArgSlice key, ArgSlice member, out int saddCount, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            saddCount = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            var input = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, member);

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)input.ptr;
            rmwInput->header.type = GarnetObjectType.Set;
            rmwInput->header.SetOp = SetOperation.SADD;
            rmwInput->count = 1;
            rmwInput->done = 0;

            RMWObjectStoreOperation(key.ToArray(), input, out var output, ref objectStoreContext);

            saddCount = output.opsDone;
            return GarnetStatus.OK;
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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            saddCount = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare header in buffer
            var rmwInput = (ObjectInputHeader*)scratchBufferManager.CreateArgSlice(ObjectInputHeader.Size).ptr;
            rmwInput->header.type = GarnetObjectType.Set;
            rmwInput->header.SetOp = SetOperation.SADD;
            rmwInput->count = members.Length;
            rmwInput->done = 0;

            // Iterate through all inputs and add them to the scratch buffer in RESP format
            int inputLength = sizeof(ObjectInputHeader);
            foreach (var member in members)
            {
                var tmp = scratchBufferManager.FormatScratchAsResp(0, member);
                inputLength += tmp.Length;
            }

            var input = scratchBufferManager.GetSliceFromTail(inputLength);

            var status = RMWObjectStoreOperation(key.ToArray(), input, out var output, ref objectStoreContext);
            saddCount = output.opsDone;

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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            sremCount = 0;

            if (key.Length == 0 || member.Length == 0)
                return GarnetStatus.OK;

            var input = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, member);

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)input.ptr;
            rmwInput->header.type = GarnetObjectType.Set;
            rmwInput->header.SetOp = SetOperation.SREM;
            rmwInput->count = 1;
            rmwInput->done = 0;

            var status = RMWObjectStoreOperation(key.ToArray(), input, out var output, ref objectStoreContext);
            sremCount = output.opsDone;

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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            sremCount = 0;

            if (key.Length == 0 || members.Length == 0)
                return GarnetStatus.OK;

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)scratchBufferManager.CreateArgSlice(ObjectInputHeader.Size).ptr;
            rmwInput->header.type = GarnetObjectType.Set;
            rmwInput->header.SetOp = SetOperation.SREM;
            rmwInput->count = members.Length;
            rmwInput->done = 0;

            var inputLength = sizeof(ObjectInputHeader);
            foreach (var member in members)
            {
                var tmp = scratchBufferManager.FormatScratchAsResp(0, member);
                inputLength += tmp.Length;
            }

            var input = scratchBufferManager.GetSliceFromTail(inputLength);

            RMWObjectStoreOperation(key.ToArray(), input, out var output, ref objectStoreContext);

            sremCount = output.countDone;
            return GarnetStatus.OK;
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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            count = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            var input = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, key);
            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)input.ptr;
            rmwInput->header.type = GarnetObjectType.Set;
            rmwInput->header.SetOp = SetOperation.SCARD;
            rmwInput->count = 1;
            rmwInput->done = 0;

            var status = ReadObjectStoreOperation(key.ToArray(), input, out var output, ref objectStoreContext);

            count = output.countDone;
            return GarnetStatus.OK;
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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            members = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            var input = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, key);
            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)input.ptr;
            rmwInput->header.type = GarnetObjectType.Set;
            rmwInput->header.SetOp = SetOperation.SMEMBERS;
            rmwInput->count = 1;
            rmwInput->done = 0;

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

            var status = RMWObjectStoreOperationWithOutput(key.ToArray(), input, ref objectStoreContext, ref outputFooter);

            if (status == GarnetStatus.OK)
                members = ProcessRespArrayOutput(outputFooter, out _);

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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            elements = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Construct input for operation
            var input = scratchBufferManager.CreateArgSlice(ObjectInputHeader.Size);

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)input.ptr;
            rmwInput->header.type = GarnetObjectType.Set;
            rmwInput->header.SetOp = SetOperation.SPOP;
            rmwInput->count = count;
            rmwInput->done = 0;

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

            var status = RMWObjectStoreOperationWithOutput(key.ToArray(), input, ref objectStoreContext, ref outputFooter);

            if (status != GarnetStatus.OK)
                return status;

            //process output
            elements = ProcessRespArrayOutput(outputFooter, out _);

            return GarnetStatus.OK;
        }

        /// <summary>
        /// Iterates members of a Set key and their associated members using a cursor,
        /// a match pattern and count parameters
        /// </summary>
        /// <param name="key">The key of the set</param>
        /// <param name="cursor">The value of the cursor</param>
        /// <param name="match">The pattern to match the members</param>
        /// <param name="count">Limit number for the response</param>
        /// <param name="items">The list of items for the response</param>
        /// <param name="objectStoreContext"></param>
        public unsafe GarnetStatus SetScan<TObjectContext>(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items, ref TObjectContext objectStoreContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            items = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            if (String.IsNullOrEmpty(match))
                match = "*";

            // Prepare header in input buffer
            var inputSize = ObjectInputHeader.Size + sizeof(int);
            var rmwInput = scratchBufferManager.CreateArgSlice(inputSize).ptr;
            ((ObjectInputHeader*)rmwInput)->header.type = GarnetObjectType.Set;
            ((ObjectInputHeader*)rmwInput)->header.SetOp = SetOperation.SSCAN;

            // Number of tokens in the input after the header (match, value, count, value)
            ((ObjectInputHeader*)rmwInput)->count = 4;
            ((ObjectInputHeader*)rmwInput)->done = (int)cursor;
            rmwInput += ObjectInputHeader.Size;

            // Object Input Limit
            (*(int*)rmwInput) = ObjectScanCountLimit;
            int inputLength = sizeof(ObjectInputHeader) + sizeof(int);

            ArgSlice tmp;

            // Write match
            var matchPatternValue = Encoding.ASCII.GetBytes(match.Trim());
            fixed (byte* matchKeywordPtr = CmdStrings.MATCH, matchPatterPtr = matchPatternValue)
            {
                tmp = scratchBufferManager.FormatScratchAsResp(0, new ArgSlice(matchKeywordPtr, CmdStrings.MATCH.Length),
                            new ArgSlice(matchPatterPtr, matchPatternValue.Length));
            }
            inputLength += tmp.Length;

            // Write count
            int lengthCountNumber = NumUtils.NumDigits(count);
            byte[] countBytes = new byte[lengthCountNumber];

            fixed (byte* countPtr = CmdStrings.COUNT, countValuePtr = countBytes)
            {
                byte* countValuePtr2 = countValuePtr;
                NumUtils.IntToBytes(count, lengthCountNumber, ref countValuePtr2);

                tmp = scratchBufferManager.FormatScratchAsResp(0, new ArgSlice(countPtr, CmdStrings.COUNT.Length),
                          new ArgSlice(countValuePtr, countBytes.Length));
            }
            inputLength += tmp.Length;

            var input = scratchBufferManager.GetSliceFromTail(inputLength);

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };
            var status = ReadObjectStoreOperationWithOutput(key.ToArray(), input, ref objectStoreContext, ref outputFooter);

            items = default;
            if (status == GarnetStatus.OK)
                items = ProcessRespArrayOutput(outputFooter, out _, isScanOutput: true);

            return status;

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
        public GarnetStatus SetAdd<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => RMWObjectStoreOperation(key, input, out output, ref objectContext);

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
        public GarnetStatus SetRemove<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => RMWObjectStoreOperation(key, input, out output, ref objectContext);

        /// <summary>
        /// Returns the number of elements of the set.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SetLength<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => ReadObjectStoreOperation(key, input, out output, ref objectContext);

        /// <summary>
        /// Returns all members of the set at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SetMembers<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => ReadObjectStoreOperationWithOutput(key, input, ref objectContext, ref outputFooter);

        /// <summary>
        /// Removes and returns one or more random members from the set at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SetPop<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => RMWObjectStoreOperationWithOutput(key, input, ref objectContext, ref outputFooter);

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

            var objectStoreLockableContext = txnManager.ObjectStoreLockableContext;

            try
            {
                (members, var status) = _setDiff(keys, ref objectStoreLockableContext);
                if (status == GarnetStatus.WRONGTYPE)
                {
                    return status;
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
        /// Diff result store.
        /// Returns the number of result set.
        /// </summary>
        /// <param name="key">destination</param>
        /// <param name="keys"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public GarnetStatus SetDiffStore(byte[] key, ArgSlice[] keys, out int count)
        {
            count = default;

            if (key.Length == 0 || keys.Length == 0)
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

            var objectStoreLockableContext = txnManager.ObjectStoreLockableContext;

            try
            {
                (var diffSet, var status) = _setDiff(keys, ref objectStoreLockableContext);

                if (status == GarnetStatus.WRONGTYPE)
                {
                    return status;
                }

                var asMembers = new ArgSlice[diffSet.Count];
                for (var i = 0; i < diffSet.Count; i++)
                {
                    asMembers[i] = scratchBufferManager.CreateArgSlice(diffSet.ElementAt(i));
                }

                var newSetObject = new SetObject();
                foreach (var item in diffSet)
                {
                    newSetObject.Set.Add(item);
                    newSetObject.UpdateSize(item);
                }
                _ = SET(key, newSetObject, ref objectStoreLockableContext);
                count = diffSet.Count;
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }

            return GarnetStatus.OK;
        }

        private (HashSet<byte[]>, GarnetStatus) _setDiff<TObjectContext>(ArgSlice[] keys, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            var result = new HashSet<byte[]>();

            var status = GET(keys[0].Bytes, out var first, ref objectContext);
            if (status == GarnetStatus.OK)
            {
                if (first.garnetObject is SetObject firstObject)
                {
                    result.UnionWith(firstObject.Set);
                }
                else
                {
                    return (result, GarnetStatus.WRONGTYPE);
                }
            }

            for (var i = 1; i < keys.Length; i++)
            {
                status = GET(keys[i].Bytes, out var next, ref objectContext);
                if (status == GarnetStatus.OK)
                {
                    if (next.garnetObject is SetObject nextObject)
                    {
                        var nextSet = nextObject.Set;
                        var interItems = result.Intersect(nextSet, nextSet.Comparer);
                        result.ExceptWith(interItems);
                    }
                    else
                    {
                        return (result, GarnetStatus.WRONGTYPE);
                    }
                }
            }

            return (result, GarnetStatus.OK);
        }
    }
}