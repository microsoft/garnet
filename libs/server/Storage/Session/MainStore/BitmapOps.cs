// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession : IDisposable
    {
        public unsafe GarnetStatus StringSetBit<TStringContext>(PinnedSpanByte key, PinnedSpanByte offset, bool bit, out bool previous, ref TStringContext context)
            where TStringContext : ITsavoriteContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            previous = false;

            if (key.Length == 0)
                return GarnetStatus.OK;

            var setValBytes = stackalloc byte[1];
            setValBytes[0] = (byte)(bit ? '1' : '0');
            var setValSlice = PinnedSpanByte.FromPinnedPointer(setValBytes, 1);

            parseState.InitializeWithArguments(offset, setValSlice);

            var input = new StringInput(RespCommand.SETBIT, RespMetaCommand.None, ref parseState, arg1: ParseUtils.ReadLong(offset));

            SpanByteAndMemory output = new(null);
            RMW_MainStore(key.ReadOnlySpan, ref input, ref output, ref context);

            return GarnetStatus.OK;
        }

        public unsafe GarnetStatus StringGetBit<TStringContext>(PinnedSpanByte key, PinnedSpanByte offset, out bool bValue, ref TStringContext context)
            where TStringContext : ITsavoriteContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            bValue = false;

            if (key.Length == 0)
                return GarnetStatus.OK;

            parseState.InitializeWithArgument(offset);

            var input = new StringInput(RespCommand.GETBIT, RespMetaCommand.None, ref parseState, arg1: ParseUtils.ReadLong(offset));

            SpanByteAndMemory output = new(null);
            var status = Read_MainStore(key.ReadOnlySpan, ref input, ref output, ref context);

            if (status == GarnetStatus.OK && !output.IsSpanByte)
            {
                fixed (byte* outputPtr = output.MemorySpan)
                {
                    var refPtr = outputPtr;
                    if (*refPtr == ':')
                    {
                        refPtr++;
                        bValue = *refPtr == '1';
                    }
                }
                output.Memory.Dispose();
            }

            return status;
        }

        public unsafe GarnetStatus StringBitOperation(ref StringInput input, BitmapOperation bitOp, out long result)
        {
            var maxBitmapLen = int.MinValue;
            var minBitmapLen = int.MaxValue;
            var status = GarnetStatus.NOTFOUND;
            var keys = input.parseState.Parameters;
            var keyCount = keys.Length;

            // 8 byte start pointer
            // 4 byte int length
            Span<byte> output = stackalloc byte[12];
            var srcBitmapPtrs = stackalloc byte*[keyCount - 1];
            var srcBitmapEndPtrs = stackalloc byte*[keyCount - 1];

            var createTransaction = false;
            if (txnManager.state != TxnState.Running)
            {
                createTransaction = true;
                Debug.Assert(txnManager.state == TxnState.None);
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Main);
                txnManager.SaveKeyEntryToLock(keys[0], LockType.Exclusive);
                for (var i = 1; i < keys.Length; i++)
                    txnManager.SaveKeyEntryToLock(keys[i], LockType.Shared);
                _ = txnManager.Run(true);
            }

            // Perform under unsafe epoch control for pointer safety.
            var uc = txnManager.TransactionalUnsafeContext;

            try
            {
                uc.BeginUnsafe();
            readFromScratch:
                var localHeadAddress = HeadAddress;
                var keysFound = 0;

                for (var i = 1; i < keys.Length; i++)
                {
                    var srcKey = keys[i];
                    //Read srcKey
                    var outputBitmap = SpanByteAndMemory.FromPinnedSpan(output);
                    status = ReadWithUnsafeContext(srcKey, ref input, ref outputBitmap, localHeadAddress, out bool epochChanged, ref uc);
                    if (epochChanged)
                    {
                        goto readFromScratch;
                    }

                    //Skip if key does not exist
                    if (status == GarnetStatus.NOTFOUND)
                        continue;

                    var outputBitmapPtr = outputBitmap.SpanByte.ToPointer();
                    var localBitmapPtr = (byte*)(nuint)(*(ulong*)outputBitmapPtr);
                    var localBitmapLength = *(int*)(outputBitmapPtr + 8);

                    // Keep track of pointers returned from ISessionFunctions
                    srcBitmapPtrs[keysFound] = localBitmapPtr;
                    srcBitmapEndPtrs[keysFound] = localBitmapPtr + localBitmapLength;
                    keysFound++;

                    maxBitmapLen = Math.Max(localBitmapLength, maxBitmapLen);
                    minBitmapLen = Math.Min(localBitmapLength, minBitmapLen);
                }

                // Check if at least one key is found and execute bitop
                if (keysFound > 0)
                {
                    // Allocate result buffer
                    if (sectorAlignedMemoryBitmap == null || maxBitmapLen > bitmapBufferSize)
                    {
                        bitmapBufferSize = Math.Max(bitmapBufferSize, maxBitmapLen);

                        sectorAlignedMemoryBitmap?.Dispose();
                        sectorAlignedMemoryBitmap = new SectorAlignedMemory(bitmapBufferSize, sectorAlignedMemoryPoolAlignment);
                    }

                    var dstBitmapPtr = sectorAlignedMemoryBitmap.GetValidPointer();
                    BitmapManager.InvokeBitOperationUnsafe(bitOp, keysFound, srcBitmapPtrs, srcBitmapEndPtrs, dstBitmapPtr, maxBitmapLen, minBitmapLen);

                    if (maxBitmapLen > 0)
                    {
                        var dstKey = keys[0];
                        var dstBitmapSpanByte = PinnedSpanByte.FromPinnedPointer(dstBitmapPtr, maxBitmapLen);
                        status = SET(dstKey, dstBitmapSpanByte, ref uc);
                    }
                }
                else
                {
                    // Return OK even when no source keys were found
                    status = GarnetStatus.OK;
                    maxBitmapLen = 0;
                }
            }
            finally
            {
                // Suspend Thread
                uc.EndUnsafe();
                if (createTransaction)
                    txnManager.Commit(true);
            }
            result = maxBitmapLen;
            return status;
        }

        public GarnetStatus StringBitOperation(BitmapOperation bitOp, PinnedSpanByte destinationKey, PinnedSpanByte[] keys, out long result)
        {
            result = 0;
            if (destinationKey.Length == 0)
                return GarnetStatus.OK;

            var args = new PinnedSpanByte[keys.Length + 1];
            args[0] = destinationKey;
            keys.CopyTo(args, 1);

            parseState.InitializeWithArguments(args);

            var input = new StringInput(RespCommand.BITOP, RespMetaCommand.None, ref parseState);

            return StringBitOperation(ref input, bitOp, out result);
        }

        public unsafe GarnetStatus StringBitCount<TStringContext>(PinnedSpanByte key, long start, long end, bool useBitInterval, out long result, ref TStringContext context)
             where TStringContext : ITsavoriteContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            result = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Get parameter lengths
            var startLength = NumUtils.CountDigits(start);
            var endLength = NumUtils.CountDigits(end);

            // Calculate # of bytes to store parameters
            var sliceBytes = 1 + startLength + endLength;

            // Get buffer from scratch buffer manager
            var paramsSlice = scratchBufferBuilder.CreateArgSlice(sliceBytes);
            var paramsSpan = paramsSlice.Span;
            var paramsSpanOffset = 0;

            // Store parameters in buffer

            // Use bit interval
            var useBitIntervalSpan = paramsSpan.Slice(paramsSpanOffset, 1);
            (useBitInterval ? "1"u8 : "0"u8).CopyTo(useBitIntervalSpan);
            var useBitIntervalSlice = PinnedSpanByte.FromPinnedSpan(useBitIntervalSpan);
            paramsSpanOffset += 1;

            // Start
            var startSpan = paramsSpan.Slice(paramsSpanOffset, startLength);
            _ = NumUtils.WriteInt64(start, startSpan);
            var startSlice = PinnedSpanByte.FromPinnedSpan(startSpan);
            paramsSpanOffset += startLength;

            // End
            var endSpan = paramsSpan.Slice(paramsSpanOffset, endLength);
            _ = NumUtils.WriteInt64(end, endSpan);
            var endSlice = PinnedSpanByte.FromPinnedSpan(endSpan);

            SpanByteAndMemory output = new(null);

            parseState.InitializeWithArguments(startSlice, endSlice, useBitIntervalSlice);

            var input = new StringInput(RespCommand.BITCOUNT, RespMetaCommand.None, ref parseState);

            scratchBufferBuilder.RewindScratchBuffer(paramsSlice);

            var status = Read_MainStore(key.ReadOnlySpan, ref input, ref output, ref context);

            if (status == GarnetStatus.OK)
            {
                if (!output.IsSpanByte)
                {
                    fixed (byte* outputPtr = output.MemorySpan)
                    {
                        var refPtr = outputPtr;
                        _ = RespReadUtils.TryReadInt64(out result, ref refPtr, refPtr + sizeof(long));
                    }
                    output.Memory.Dispose();
                }
            }

            return status;
        }

        public unsafe GarnetStatus StringBitField<TStringContext>(PinnedSpanByte key, List<BitFieldCmdArgs> commandArguments, out List<long?> result, ref TStringContext context)
             where TStringContext : ITsavoriteContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var input = new StringInput(RespCommand.BITFIELD);

            result = [];

            for (var i = 0; i < commandArguments.Count; i++)
            {
                var isGet = commandArguments[i].secondaryCommand == RespCommand.GET;

                // Get parameter lengths
                var op = commandArguments[i].secondaryCommand.ToString();
                var encodingPrefix = (commandArguments[i].typeInfo & (byte)BitFieldSign.SIGNED) > 0 ? "i"u8 : "u"u8;
                var encodingSuffix = commandArguments[i].typeInfo & 0x7F;
                var encodingSuffixLength = NumUtils.CountDigits(encodingSuffix);
                var offsetLength = NumUtils.CountDigits(commandArguments[i].offset);
                var valueLength = isGet ? 0 : NumUtils.CountDigits(commandArguments[i].value);
                var overflowType = ((BitFieldOverflow)commandArguments[i].overflowType).ToString();

                // Calculate # of bytes to store parameters
                var sliceBytes = op.Length +
                                 1 + encodingSuffixLength +
                                 offsetLength +
                                 valueLength +
                                 overflowType.Length;

                // Get buffer from scratch buffer manager
                var paramsSlice = scratchBufferBuilder.CreateArgSlice(sliceBytes);
                var paramsSpan = paramsSlice.Span;
                var paramsSpanOffset = 0;

                // Store parameters in buffer

                // Secondary op code
                var opSpan = paramsSpan.Slice(paramsSpanOffset, op.Length);
                _ = Encoding.UTF8.GetBytes(op, opSpan);
                var opSlice = PinnedSpanByte.FromPinnedSpan(opSpan);
                paramsSpanOffset += opSpan.Length;

                // Encoding
                var encodingSpan = paramsSpan.Slice(paramsSpanOffset, 1 + encodingSuffixLength);
                encodingSpan[0] = encodingPrefix[0];
                var encodingSuffixSpan = encodingSpan.Slice(1);
                _ = NumUtils.WriteInt64(encodingSuffix, encodingSuffixSpan);
                var encodingSlice = PinnedSpanByte.FromPinnedSpan(encodingSpan);
                paramsSpanOffset += 1 + encodingSuffixLength;

                // Offset
                var offsetSpan = paramsSpan.Slice(paramsSpanOffset, offsetLength);
                _ = NumUtils.WriteInt64(commandArguments[i].offset, offsetSpan);
                var offsetSlice = PinnedSpanByte.FromPinnedSpan(offsetSpan);
                paramsSpanOffset += offsetLength;

                // Value
                PinnedSpanByte valueSlice = default;
                if (!isGet)
                {
                    var valueSpan = paramsSpan.Slice(paramsSpanOffset, valueLength);
                    _ = NumUtils.WriteInt64(commandArguments[i].value, valueSpan);
                    valueSlice = PinnedSpanByte.FromPinnedSpan(valueSpan);
                    paramsSpanOffset += valueLength;
                }

                // Overflow Type
                var overflowTypeSpan = paramsSpan.Slice(paramsSpanOffset, overflowType.Length);
                _ = Encoding.UTF8.GetBytes(overflowType, overflowTypeSpan);
                var overflowTypeSlice = PinnedSpanByte.FromPinnedSpan(overflowTypeSpan);

                var output = new SpanByteAndMemory(null);

                if (isGet)
                {
                    parseState.InitializeWithArguments(opSlice, encodingSlice, offsetSlice, overflowTypeSlice);
                }
                else
                {
                    parseState.InitializeWithArguments(opSlice, encodingSlice, offsetSlice,
                        valueSlice, overflowTypeSlice);
                }

                input.parseState = parseState;
                var status = commandArguments[i].secondaryCommand == RespCommand.GET ?
                    Read_MainStore(key.ReadOnlySpan, ref input, ref output, ref context) :
                    RMW_MainStore(key.ReadOnlySpan, ref input, ref output, ref context);

                scratchBufferBuilder.RewindScratchBuffer(paramsSlice);

                if (status == GarnetStatus.NOTFOUND && commandArguments[i].secondaryCommand == RespCommand.GET)
                {
                    result.Add(0);
                }
                else
                {
                    if (status == GarnetStatus.OK)
                    {
                        long resultCmd;
                        var error = false;
                        if (!output.IsSpanByte)
                        {
                            fixed (byte* outputPtr = output.MemorySpan)
                            {
                                var refPtr = outputPtr;
                                if (!RespReadUtils.TryReadInt64(out resultCmd, ref refPtr, refPtr + output.Length))
                                    error = true;
                            }
                            output.Memory.Dispose();
                        }
                        else
                        {
                            var refPtr = output.SpanByte.ToPointer();
                            if (!RespReadUtils.TryReadInt64(out resultCmd, ref refPtr, refPtr + output.SpanByte.Length))
                                error = true;
                        }
                        result.Add(error ? null : resultCmd);
                    }
                }
            }
            return GarnetStatus.OK;
        }

        public GarnetStatus StringSetBit<TStringContext>(PinnedSpanByte key, ref StringInput input, ref SpanByteAndMemory output, ref TStringContext context)
          where TStringContext : ITsavoriteContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            => RMW_MainStore(key.ReadOnlySpan, ref input, ref output, ref context);

        public GarnetStatus StringGetBit<TStringContext>(PinnedSpanByte key, ref StringInput input, ref SpanByteAndMemory output, ref TStringContext context)
            where TStringContext : ITsavoriteContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            => Read_MainStore(key.ReadOnlySpan, ref input, ref output, ref context);

        public unsafe GarnetStatus StringBitCount<TStringContext>(PinnedSpanByte key, ref StringInput input, ref SpanByteAndMemory output, ref TStringContext context)
         where TStringContext : ITsavoriteContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
             => Read_MainStore(key.ReadOnlySpan, ref input, ref output, ref context);

        public unsafe GarnetStatus StringBitPosition<TStringContext>(PinnedSpanByte key, ref StringInput input, ref SpanByteAndMemory output, ref TStringContext context)
            where TStringContext : ITsavoriteContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
             => Read_MainStore(key.ReadOnlySpan, ref input, ref output, ref context);

        public unsafe GarnetStatus StringBitField<TStringContext>(PinnedSpanByte key, ref StringInput input, RespCommand secondaryCommand, ref SpanByteAndMemory output, ref TStringContext context)
            where TStringContext : ITsavoriteContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            GarnetStatus status;
            if (secondaryCommand == RespCommand.GET)
                status = Read_MainStore(key.ReadOnlySpan, ref input, ref output, ref context);
            else
            {
                Debug.Assert(input.header.cmd != RespCommand.BITFIELD_RO);
                status = RMW_MainStore(key.ReadOnlySpan, ref input, ref output, ref context);
            }
            return status;
        }

        public unsafe GarnetStatus StringBitFieldReadOnly<TStringContext>(PinnedSpanByte key, ref StringInput input, RespCommand secondaryCommand, ref SpanByteAndMemory output, ref TStringContext context)
              where TStringContext : ITsavoriteContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = GarnetStatus.NOTFOUND;

            if (secondaryCommand == RespCommand.GET)
                status = Read_MainStore(key.ReadOnlySpan, ref input, ref output, ref context);
            return status;
        }

    }
}