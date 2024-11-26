// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Text;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    sealed partial class StorageSession : IDisposable
    {
        /// <summary>
        /// Adds all the element arguments to the HyperLogLog data structure stored at the variable name specified as key.
        /// </summary>
        public unsafe GarnetStatus HyperLogLogAdd<TContext>(ArgSlice key, string[] elements, out bool updated, ref TContext context)
             where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            updated = false;

            parseState.Initialize(1);

            var input = new RawStringInput(RespCommand.PFADD, ref parseState);

            var output = stackalloc byte[1];
            byte pfaddUpdated = 0;

            foreach (var element in elements)
            {
                var elementSlice = scratchBufferManager.CreateArgSlice(element);
                parseState.SetArgument(0, elementSlice);

                var o = new SpanByteAndMemory(output, 1);
                var sbKey = key.SpanByte;
                RMW_MainStore(ref sbKey, ref input, ref o, ref context);

                scratchBufferManager.RewindScratchBuffer(ref elementSlice);

                //Invalid HLL Type
                if (*output == (byte)0xFF)
                {
                    pfaddUpdated = 0;
                    break;
                }
                pfaddUpdated |= *output;
            }

            updated = pfaddUpdated > 0;
            return GarnetStatus.OK;
        }

        /// <summary>
        /// Adds one element to the HyperLogLog data structure stored at the variable name specified.
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public GarnetStatus HyperLogLogAdd<TContext>(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, ref TContext context)
          where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            => RMW_MainStore(ref key, ref input, ref output, ref context);

        public unsafe GarnetStatus HyperLogLogLength<TContext>(Span<ArgSlice> keys, out long count, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            parseState.Initialize(keys.Length);
            for (var i = 0; i < keys.Length; i++)
            {
                parseState.SetArgument(i, keys[i]);
            }

            var input = new RawStringInput(RespCommand.PFCOUNT, ref parseState);

            return HyperLogLogLength(ref input, out count, out _, ref context);
        }

        /// <summary>
        /// Returns the approximated cardinality computed by the HyperLogLog data structure stored at the specified key,
        /// or 0 if the key does not exist.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="count"></param>
        /// <param name="error"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HyperLogLogLength<TContext>(ref RawStringInput input, out long count, out bool error, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            error = false;
            count = default;

            if (input.parseState.Count == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                var dstKey = input.parseState.GetArgSliceByRef(0);
                txnManager.SaveKeyEntryToLock(dstKey, false, LockType.Exclusive);
                for (var i = 1; i < input.parseState.Count; i++)
                {
                    var currSrcKey = input.parseState.GetArgSliceByRef(i);
                    txnManager.SaveKeyEntryToLock(currSrcKey, false, LockType.Shared);
                }
                txnManager.Run(true);
            }

            var currTransactionalContext = txnManager.TransactionalContext;

            try
            {
                sectorAlignedMemoryHll1 ??= new SectorAlignedMemory(hllBufferSize + sectorAlignedMemoryPoolAlignment,
                    sectorAlignedMemoryPoolAlignment);
                sectorAlignedMemoryHll2 ??= new SectorAlignedMemory(hllBufferSize + sectorAlignedMemoryPoolAlignment,
                    sectorAlignedMemoryPoolAlignment);
                var srcReadBuffer = sectorAlignedMemoryHll1.GetValidPointer();
                var dstReadBuffer = sectorAlignedMemoryHll2.GetValidPointer();
                var dstMergeBuffer = new SpanByteAndMemory(srcReadBuffer, hllBufferSize);
                var srcMergeBuffer = new SpanByteAndMemory(dstReadBuffer, hllBufferSize);
                var isFirst = false;

                for (var i = 0; i < input.parseState.Count; i++)
                {
                    var currInput = new RawStringInput(RespCommand.PFCOUNT);

                    var srcKey = input.parseState.GetArgSliceByRef(i).SpanByte;

                    var status = GET(ref srcKey, ref currInput, ref srcMergeBuffer, ref currTransactionalContext);
                    // Handle case merging source key does not exist
                    if (status == GarnetStatus.NOTFOUND)
                        continue;
                    // Invalid Type
                    if (*(long*)srcReadBuffer == -1)
                    {
                        error = true;
                        break;
                    }

                    var sbSrcHLL = srcMergeBuffer.SpanByte;
                    var sbDstHLL = dstMergeBuffer.SpanByte;

                    var srcHLL = sbSrcHLL.ToPointer();
                    var dstHLL = sbDstHLL.ToPointer();

                    if (!isFirst)
                    {
                        isFirst = true;
                        if (i == input.parseState.Count - 1)
                            count = HyperLogLog.DefaultHLL.Count(srcMergeBuffer.SpanByte.ToPointer());
                        else
                            Buffer.MemoryCopy(srcHLL, dstHLL, sbSrcHLL.Length, sbSrcHLL.Length);
                        continue;
                    }

                    HyperLogLog.DefaultHLL.TryMerge(srcHLL, dstHLL, sbDstHLL.Length);

                    if (i == input.parseState.Count - 1)
                    {
                        count = HyperLogLog.DefaultHLL.Count(dstHLL);
                    }
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
        /// Merge multiple HyperLogLog values into a unique value that will approximate the cardinality 
        /// of the union of the observed Sets of the source HyperLogLog structures.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="error"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HyperLogLogMerge(ref RawStringInput input, out bool error)
        {
            error = false;

            if (input.parseState.Count == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                var dstKey = input.parseState.GetArgSliceByRef(0);
                txnManager.SaveKeyEntryToLock(dstKey, false, LockType.Exclusive);
                for (var i = 1; i < input.parseState.Count; i++)
                {
                    var currSrcKey = input.parseState.GetArgSliceByRef(i);
                    txnManager.SaveKeyEntryToLock(currSrcKey, false, LockType.Shared);
                }
                txnManager.Run(true);
            }

            var currTransactionalContext = txnManager.TransactionalContext;

            try
            {
                sectorAlignedMemoryHll1 ??= new SectorAlignedMemory(hllBufferSize + sectorAlignedMemoryPoolAlignment, sectorAlignedMemoryPoolAlignment);
                var readBuffer = sectorAlignedMemoryHll1.GetValidPointer();

                var dstKey = input.parseState.GetArgSliceByRef(0).SpanByte;

                for (var i = 1; i < input.parseState.Count; i++)
                {
                    #region readSrcHLL

                    var currInput = new RawStringInput(RespCommand.PFMERGE);

                    var mergeBuffer = new SpanByteAndMemory(readBuffer, hllBufferSize);
                    var srcKey = input.parseState.GetArgSliceByRef(i).SpanByte;

                    var status = GET(ref srcKey, ref currInput, ref mergeBuffer, ref currTransactionalContext);
                    // Handle case merging source key does not exist
                    if (status == GarnetStatus.NOTFOUND)
                        continue;
                    // Invalid Type
                    if (*(long*)readBuffer == -1)
                    {
                        error = true;
                        break;
                    }

                    #endregion

                    #region mergeToDst

                    var mergeSlice = new ArgSlice(ref mergeBuffer.SpanByte);

                    parseState.InitializeWithArgument(mergeSlice);

                    currInput.parseState = parseState;
                    SET_Conditional(ref dstKey, ref currInput, ref mergeBuffer, ref currTransactionalContext);

                    #endregion
                }
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }
            return GarnetStatus.OK;
        }
    }
}