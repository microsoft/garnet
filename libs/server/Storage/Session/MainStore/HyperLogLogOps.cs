// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession : IDisposable
    {
        /// <summary>
        /// Adds all the element arguments to the HyperLogLog data structure stored at the variable name specified as key.
        /// </summary>
        public unsafe GarnetStatus HyperLogLogAdd<TStringContext>(PinnedSpanByte key, string[] elements, out bool updated, ref TStringContext context)
             where TStringContext : ITsavoriteContext<StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            updated = false;

            parseState.Initialize(1);

            var input = new StringInput(RespCommand.PFADD, ref parseState);

            byte output = 0;
            byte pfaddUpdated = 0;

            foreach (var element in elements)
            {
                var elementSlice = scratchBufferBuilder.CreateArgSlice(element);
                parseState.SetArgument(0, elementSlice);

                var o = StringOutput.FromPinnedSpan(new Span<byte>(ref output));

                _ = RMW_MainStore(key.ReadOnlySpan, ref input, ref o, ref context);

                scratchBufferBuilder.RewindScratchBuffer(elementSlice);

                //Invalid HLL Type
                if (output == (byte)0xFF)
                {
                    pfaddUpdated = 0;
                    break;
                }
                pfaddUpdated |= output;
            }

            updated = pfaddUpdated > 0;
            return GarnetStatus.OK;
        }

        /// <summary>
        /// Adds one element to the HyperLogLog data structure stored at the variable name specified.
        /// </summary>
        /// <typeparam name="TStringContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public GarnetStatus HyperLogLogAdd<TStringContext>(PinnedSpanByte key, ref StringInput input, ref StringOutput output, ref TStringContext context)
          where TStringContext : ITsavoriteContext<StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            => RMW_MainStore(key.ReadOnlySpan, ref input, ref output, ref context);

        public unsafe GarnetStatus HyperLogLogLength<TStringContext>(Span<PinnedSpanByte> keys, out long count, ref TStringContext context)
            where TStringContext : ITsavoriteContext<StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            parseState.Initialize(keys.Length);
            for (var i = 0; i < keys.Length; i++)
            {
                parseState.SetArgument(i, keys[i]);
            }

            var input = new StringInput(RespCommand.PFCOUNT, ref parseState);

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
        public unsafe GarnetStatus HyperLogLogLength<TStringContext>(ref StringInput input, out long count, out bool error, ref TStringContext context)
            where TStringContext : ITsavoriteContext<StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
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
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Main);
                txnManager.SaveKeyEntryToLock(dstKey, LockType.Exclusive);
                for (var i = 1; i < input.parseState.Count; i++)
                {
                    var currSrcKey = input.parseState.GetArgSliceByRef(i);
                    txnManager.SaveKeyEntryToLock(currSrcKey, LockType.Shared);
                }
                _ = txnManager.Run(true);
            }

            var currTransactionalContext = txnManager.StringTransactionalContext;

            try
            {
                sectorAlignedMemoryHll1 ??= new SectorAlignedMemory(hllBufferSize + sectorAlignedMemoryPoolAlignment,
                    sectorAlignedMemoryPoolAlignment);
                sectorAlignedMemoryHll2 ??= new SectorAlignedMemory(hllBufferSize + sectorAlignedMemoryPoolAlignment,
                    sectorAlignedMemoryPoolAlignment);
                var srcReadBuffer = sectorAlignedMemoryHll1.GetValidPointer();
                var dstReadBuffer = sectorAlignedMemoryHll2.GetValidPointer();
                StringOutput dstMergeBuffer = default;
                dstMergeBuffer.SpanByteAndMemory = SpanByteAndMemory.FromPinnedPointer(srcReadBuffer, hllBufferSize);
                StringOutput srcMergeBuffer = default;
                srcMergeBuffer.SpanByteAndMemory = SpanByteAndMemory.FromPinnedPointer(dstReadBuffer, hllBufferSize);
                var isFirst = false;

                for (var i = 0; i < input.parseState.Count; i++)
                {
                    var currInput = new StringInput(RespCommand.PFCOUNT);

                    var srcKey = input.parseState.GetArgSliceByRef(i);

                    var status = GET(srcKey, ref currInput, ref srcMergeBuffer, ref currTransactionalContext);
                    // Handle case merging source key does not exist
                    if (status == GarnetStatus.NOTFOUND)
                        continue;
                    // Invalid Type
                    if (*(long*)srcReadBuffer == -1)
                    {
                        error = true;
                        break;
                    }

                    var sbSrcHLL = srcMergeBuffer.SpanByteAndMemory.SpanByte;
                    var sbDstHLL = dstMergeBuffer.SpanByteAndMemory.SpanByte;

                    var srcHLL = sbSrcHLL.ToPointer();
                    var dstHLL = sbDstHLL.ToPointer();

                    if (!isFirst)
                    {
                        isFirst = true;
                        if (i == input.parseState.Count - 1)
                            count = HyperLogLog.DefaultHLL.Count(srcMergeBuffer.SpanByteAndMemory.SpanByte.ToPointer());
                        else
                            Buffer.MemoryCopy(srcHLL, dstHLL, sbSrcHLL.Length, sbSrcHLL.Length);
                        continue;
                    }

                    _ = HyperLogLog.DefaultHLL.TryMerge(srcHLL, dstHLL, sbDstHLL.Length);

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
        public unsafe GarnetStatus HyperLogLogMerge(ref StringInput input, out bool error)
        {
            error = false;

            if (input.parseState.Count == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Main);
                var dstKey = input.parseState.GetArgSliceByRef(0);
                txnManager.SaveKeyEntryToLock(dstKey, LockType.Exclusive);
                for (var i = 1; i < input.parseState.Count; i++)
                {
                    var currSrcKey = input.parseState.GetArgSliceByRef(i);
                    txnManager.SaveKeyEntryToLock(currSrcKey, LockType.Shared);
                }
                _ = txnManager.Run(true);
            }

            var currTransactionalContext = txnManager.StringTransactionalContext;

            try
            {
                sectorAlignedMemoryHll1 ??= new SectorAlignedMemory(hllBufferSize + sectorAlignedMemoryPoolAlignment, sectorAlignedMemoryPoolAlignment);
                var readBuffer = sectorAlignedMemoryHll1.GetValidPointer();

                var dstKey = input.parseState.GetArgSliceByRef(0);

                for (var i = 1; i < input.parseState.Count; i++)
                {
                    #region readSrcHLL

                    var currInput = new StringInput(RespCommand.PFMERGE);

                    StringOutput mergeBuffer = default;
                    mergeBuffer.SpanByteAndMemory = SpanByteAndMemory.FromPinnedPointer(readBuffer, hllBufferSize);
                    var srcKey = input.parseState.GetArgSliceByRef(i);

                    var status = GET(srcKey, ref currInput, ref mergeBuffer, ref currTransactionalContext);
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

                    var mergeSlice = mergeBuffer.SpanByteAndMemory.SpanByte;

                    parseState.InitializeWithArgument(mergeSlice);

                    currInput.parseState = parseState;
                    SET_Conditional(dstKey, ref currInput, ref mergeBuffer, ref currTransactionalContext);

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