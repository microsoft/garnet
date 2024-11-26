// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    sealed partial class StorageSession : IDisposable
    {
        public unsafe GarnetStatus StringSetBit<TContext>(ArgSlice key, ArgSlice offset, bool bit, out bool previous, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            previous = false;

            if (key.Length == 0)
                return GarnetStatus.OK;

            var setValBytes = stackalloc byte[1];
            setValBytes[0] = (byte)(bit ? '1' : '0');
            var setValSlice = new ArgSlice(setValBytes, 1);

            parseState.InitializeWithArguments(offset, setValSlice);

            var input = new RawStringInput(RespCommand.SETBIT, ref parseState);

            SpanByteAndMemory output = new(null);
            var keySp = key.SpanByte;
            RMW_MainStore(ref keySp, ref input, ref output, ref context);

            return GarnetStatus.OK;
        }

        public unsafe GarnetStatus StringGetBit<TContext>(ArgSlice key, ArgSlice offset, out bool bValue, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            bValue = false;

            if (key.Length == 0)
                return GarnetStatus.OK;

            parseState.InitializeWithArgument(offset);

            var input = new RawStringInput(RespCommand.GETBIT, ref parseState);

            SpanByteAndMemory output = new(null);
            var keySp = key.SpanByte;
            var status = Read_MainStore(ref keySp, ref input, ref output, ref context);

            if (status == GarnetStatus.OK && !output.IsSpanByte)
            {
                fixed (byte* outputPtr = output.Memory.Memory.Span)
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

        public unsafe GarnetStatus StringBitOperation(ref RawStringInput input, BitmapOperation bitOp, out long result)
        {
            var maxBitmapLen = int.MinValue;
            var minBitmapLen = int.MaxValue;
            var status = GarnetStatus.NOTFOUND;
            var keys = input.parseState.Parameters;
            var keyCount = keys.Length;

            // 8 byte start pointer
            // 4 byte int length
            var output = stackalloc byte[12];
            var srcBitmapStartPtrs = stackalloc byte*[keyCount - 1];
            var srcBitmapEndPtrs = stackalloc byte*[keyCount - 1];

            var createTransaction = false;
            if (txnManager.state != TxnState.Running)
            {
                createTransaction = true;
                Debug.Assert(txnManager.state == TxnState.None);
                txnManager.SaveKeyEntryToLock(keys[0], false, LockType.Exclusive);
                for (var i = 1; i < keys.Length; i++)
                    txnManager.SaveKeyEntryToLock(keys[i], false, LockType.Shared);
                txnManager.Run(true);
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
                    var outputBitmap = new SpanByteAndMemory(output, 12);
                    status = ReadWithUnsafeContext(srcKey, ref input, ref outputBitmap, localHeadAddress, out bool epochChanged, ref uc);
                    if (epochChanged)
                    {
                        goto readFromScratch;
                    }

                    //Skip if key does not exist
                    if (status == GarnetStatus.NOTFOUND)
                        continue;

                    var outputBitmapPtr = outputBitmap.SpanByte.ToPointer();
                    var localSrcBitmapPtr = (byte*)((IntPtr)(*(long*)outputBitmapPtr));
                    var len = *(int*)(outputBitmapPtr + 8);

                    // Keep track of pointers returned from ISessionFunctions
                    srcBitmapStartPtrs[keysFound] = localSrcBitmapPtr;
                    srcBitmapEndPtrs[keysFound] = localSrcBitmapPtr + len;
                    keysFound++;
                    maxBitmapLen = Math.Max(len, maxBitmapLen);
                    minBitmapLen = Math.Min(len, minBitmapLen);
                }

                #region performBitop
                // Allocate result buffers
                sectorAlignedMemoryBitmap ??= new SectorAlignedMemory(bitmapBufferSize + sectorAlignedMemoryPoolAlignment, sectorAlignedMemoryPoolAlignment);
                var dstBitmapPtr = sectorAlignedMemoryBitmap.GetValidPointer() + sectorAlignedMemoryPoolAlignment;
                if (maxBitmapLen + sectorAlignedMemoryPoolAlignment > bitmapBufferSize)
                {
                    do
                    {
                        bitmapBufferSize <<= 1;
                    } while (maxBitmapLen + sectorAlignedMemoryPoolAlignment > bitmapBufferSize);

                    sectorAlignedMemoryBitmap.Dispose();
                    sectorAlignedMemoryBitmap = new SectorAlignedMemory(bitmapBufferSize + sectorAlignedMemoryPoolAlignment, sectorAlignedMemoryPoolAlignment);
                    dstBitmapPtr = sectorAlignedMemoryBitmap.GetValidPointer() + sectorAlignedMemoryPoolAlignment;
                }


                // Check if at least one key is found and execute bitop
                if (keysFound > 0)
                {
                    //1. Multi-way bitmap merge
                    _ = BitmapManager.BitOpMainUnsafeMultiKey(dstBitmapPtr, maxBitmapLen, srcBitmapStartPtrs, srcBitmapEndPtrs, keysFound, minBitmapLen, (byte)bitOp);
                    #endregion

                    if (maxBitmapLen > 0)
                    {
                        var dstKey = keys[0].SpanByte;
                        var valPtr = dstBitmapPtr;
                        valPtr -= sizeof(int);
                        *(int*)valPtr = maxBitmapLen;
                        status = SET(ref dstKey, ref Unsafe.AsRef<SpanByte>(valPtr), ref uc);
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

        public GarnetStatus StringBitOperation(BitmapOperation bitOp, ArgSlice destinationKey, ArgSlice[] keys, out long result)
        {
            result = 0;
            if (destinationKey.Length == 0)
                return GarnetStatus.OK;

            var args = new ArgSlice[keys.Length + 1];
            args[0] = destinationKey;
            keys.CopyTo(args, 1);

            parseState.InitializeWithArguments(args);

            var input = new RawStringInput(RespCommand.BITOP, ref parseState);

            return StringBitOperation(ref input, bitOp, out result);
        }

        public unsafe GarnetStatus StringBitCount<TContext>(ArgSlice key, long start, long end, bool useBitInterval, out long result, ref TContext context)
             where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            result = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Get parameter lengths
            var startLength = NumUtils.NumDigitsInLong(start);
            var endLength = NumUtils.NumDigitsInLong(end);

            // Calculate # of bytes to store parameters
            var sliceBytes = 1 + startLength + endLength;

            // Get buffer from scratch buffer manager
            var paramsSlice = scratchBufferManager.CreateArgSlice(sliceBytes);
            var paramsSpan = paramsSlice.Span;
            var paramsSpanOffset = 0;

            // Store parameters in buffer

            // Use bit interval
            var useBitIntervalSpan = paramsSpan.Slice(paramsSpanOffset, 1);
            (useBitInterval ? "1"u8 : "0"u8).CopyTo(useBitIntervalSpan);
            var useBitIntervalSlice = ArgSlice.FromPinnedSpan(useBitIntervalSpan);
            paramsSpanOffset += 1;

            // Start
            var startSpan = paramsSpan.Slice(paramsSpanOffset, startLength);
            NumUtils.LongToSpanByte(start, startSpan);
            var startSlice = ArgSlice.FromPinnedSpan(startSpan);
            paramsSpanOffset += startLength;

            // End
            var endSpan = paramsSpan.Slice(paramsSpanOffset, endLength);
            NumUtils.LongToSpanByte(end, endSpan);
            var endSlice = ArgSlice.FromPinnedSpan(endSpan);

            SpanByteAndMemory output = new(null);

            parseState.InitializeWithArguments(startSlice, endSlice, useBitIntervalSlice);

            var input = new RawStringInput(RespCommand.BITCOUNT, ref parseState);

            scratchBufferManager.RewindScratchBuffer(ref paramsSlice);

            var keySp = key.SpanByte;

            var status = Read_MainStore(ref keySp, ref input, ref output, ref context);

            if (status == GarnetStatus.OK)
            {
                if (!output.IsSpanByte)
                {
                    fixed (byte* outputPtr = output.Memory.Memory.Span)
                    {
                        var refPtr = outputPtr;
                        RespReadUtils.Read64Int(out result, ref refPtr, refPtr + sizeof(long));
                    }
                    output.Memory.Dispose();
                }
            }

            return status;
        }

        public unsafe GarnetStatus StringBitField<TContext>(ArgSlice key, List<BitFieldCmdArgs> commandArguments, out List<long?> result, ref TContext context)
             where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            var input = new RawStringInput(RespCommand.BITFIELD);

            result = new();
            var keySp = key.SpanByte;

            for (var i = 0; i < commandArguments.Count; i++)
            {
                var isGet = commandArguments[i].secondaryCommand == RespCommand.GET;

                // Get parameter lengths
                var op = commandArguments[i].secondaryCommand.ToString();
                var encodingPrefix = (commandArguments[i].typeInfo & (byte)BitFieldSign.SIGNED) > 0 ? "i"u8 : "u"u8;
                var encodingSuffix = commandArguments[i].typeInfo & 0x7F;
                var encodingSuffixLength = NumUtils.NumDigits(encodingSuffix);
                var offsetLength = NumUtils.NumDigitsInLong(commandArguments[i].offset);
                var valueLength = isGet ? 0 : NumUtils.NumDigitsInLong(commandArguments[i].value);
                var overflowType = ((BitFieldOverflow)commandArguments[i].overflowType).ToString();

                // Calculate # of bytes to store parameters
                var sliceBytes = op.Length +
                                 1 + encodingSuffixLength +
                                 offsetLength +
                                 valueLength +
                                 overflowType.Length;

                // Get buffer from scratch buffer manager
                var paramsSlice = scratchBufferManager.CreateArgSlice(sliceBytes);
                var paramsSpan = paramsSlice.Span;
                var paramsSpanOffset = 0;

                // Store parameters in buffer

                // Secondary op code
                var opSpan = paramsSpan.Slice(paramsSpanOffset, op.Length);
                Encoding.UTF8.GetBytes(op, opSpan);
                var opSlice = ArgSlice.FromPinnedSpan(opSpan);
                paramsSpanOffset += opSpan.Length;

                // Encoding
                var encodingSpan = paramsSpan.Slice(paramsSpanOffset, 1 + encodingSuffixLength);
                encodingSpan[0] = encodingPrefix[0];
                var encodingSuffixSpan = encodingSpan.Slice(1);
                NumUtils.LongToSpanByte(encodingSuffix, encodingSuffixSpan);
                var encodingSlice = ArgSlice.FromPinnedSpan(encodingSpan);
                paramsSpanOffset += 1 + encodingSuffixLength;

                // Offset
                var offsetSpan = paramsSpan.Slice(paramsSpanOffset, offsetLength);
                NumUtils.LongToSpanByte(commandArguments[i].offset, offsetSpan);
                var offsetSlice = ArgSlice.FromPinnedSpan(offsetSpan);
                paramsSpanOffset += offsetLength;

                // Value
                ArgSlice valueSlice = default;
                if (!isGet)
                {
                    var valueSpan = paramsSpan.Slice(paramsSpanOffset, valueLength);
                    NumUtils.LongToSpanByte(commandArguments[i].value, valueSpan);
                    valueSlice = ArgSlice.FromPinnedSpan(valueSpan);
                    paramsSpanOffset += valueLength;
                }

                // Overflow Type
                var overflowTypeSpan = paramsSpan.Slice(paramsSpanOffset, overflowType.Length);
                Encoding.UTF8.GetBytes(overflowType, overflowTypeSpan);
                var overflowTypeSlice = ArgSlice.FromPinnedSpan(overflowTypeSpan);

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
                    Read_MainStore(ref keySp, ref input, ref output, ref context) :
                    RMW_MainStore(ref keySp, ref input, ref output, ref context);

                scratchBufferManager.RewindScratchBuffer(ref paramsSlice);

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
                            fixed (byte* outputPtr = output.Memory.Memory.Span)
                            {
                                var refPtr = outputPtr;
                                if (!RespReadUtils.Read64Int(out resultCmd, ref refPtr, refPtr + output.Length))
                                    error = true;
                            }
                            output.Memory.Dispose();
                        }
                        else
                        {
                            var refPtr = output.SpanByte.ToPointer();
                            if (!RespReadUtils.Read64Int(out resultCmd, ref refPtr, refPtr + output.SpanByte.Length))
                                error = true;
                        }
                        result.Add(error ? null : resultCmd);
                    }
                }
            }
            return GarnetStatus.OK;
        }

        public GarnetStatus StringSetBit<TContext>(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, ref TContext context)
          where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            => RMW_MainStore(ref key, ref input, ref output, ref context);

        public GarnetStatus StringGetBit<TContext>(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            => Read_MainStore(ref key, ref input, ref output, ref context);

        public unsafe GarnetStatus StringBitCount<TContext>(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, ref TContext context)
         where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
             => Read_MainStore(ref key, ref input, ref output, ref context);

        public unsafe GarnetStatus StringBitPosition<TContext>(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
             => Read_MainStore(ref key, ref input, ref output, ref context);

        public unsafe GarnetStatus StringBitField<TContext>(ref SpanByte key, ref RawStringInput input, RespCommand secondaryCommand, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            GarnetStatus status;
            if (secondaryCommand == RespCommand.GET)
                status = Read_MainStore(ref key, ref input, ref output, ref context);
            else
                status = RMW_MainStore(ref key, ref input, ref output, ref context);
            return status;
        }

        public unsafe GarnetStatus StringBitFieldReadOnly<TContext>(ref SpanByte key, ref RawStringInput input, RespCommand secondaryCommand, ref SpanByteAndMemory output, ref TContext context)
              where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            GarnetStatus status = GarnetStatus.NOTFOUND;

            if (secondaryCommand == RespCommand.GET)
                status = Read_MainStore(ref key, ref input, ref output, ref context);
            return status;
        }

    }
}