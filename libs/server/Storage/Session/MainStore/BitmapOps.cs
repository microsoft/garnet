// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession : IDisposable
    {
        public unsafe GarnetStatus StringSetBit<TContext>(ArgSlice key, ArgSlice offset, bool bit, out bool previous, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
        {
            previous = false;

            if (key.Bytes.Length == 0)
                return GarnetStatus.OK;

            int inputSize = sizeof(int) + RespInputHeader.Size + sizeof(long) + sizeof(byte);
            byte* input = scratchBufferManager.CreateArgSlice(inputSize).ptr;

            //initialize the input variable
            byte* pcurr = input;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);

            (*(RespInputHeader*)pcurr).cmd = RespCommand.SETBIT;
            (*(RespInputHeader*)pcurr).flags = 0;
            pcurr += RespInputHeader.Size;

            //offset
            *(long*)pcurr = NumUtils.BytesToLong(offset.Bytes);
            pcurr += sizeof(long);

            //bit value
            *(byte*)(pcurr) = bit ? (byte)0x1 : (byte)0x0;

            SpanByteAndMemory output = new(null);
            var keySp = key.SpanByte;
            RMW_MainStore(ref keySp, ref Unsafe.AsRef<SpanByte>(input), ref output, ref context);

            return GarnetStatus.OK;
        }

        public unsafe GarnetStatus StringGetBit<TContext>(ArgSlice key, ArgSlice offset, out bool bValue, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
        {
            bValue = false;

            if (key.Bytes.Length == 0)
                return GarnetStatus.OK;

            int inputSize = sizeof(int) + RespInputHeader.Size + sizeof(long);
            byte* input = scratchBufferManager.CreateArgSlice(inputSize).ptr;

            //initialize the input variable
            byte* pcurr = input;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);

            (*(RespInputHeader*)pcurr).cmd = RespCommand.GETBIT;
            (*(RespInputHeader*)pcurr).flags = 0;
            pcurr += RespInputHeader.Size;

            //offset
            *(long*)pcurr = NumUtils.BytesToLong(offset.Bytes);
            pcurr += sizeof(long);

            SpanByteAndMemory output = new(null);
            var keySp = key.SpanByte;
            var status = Read_MainStore(ref keySp, ref Unsafe.AsRef<SpanByte>(input), ref output, ref context);

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

        public unsafe GarnetStatus StringBitOperation(ArgSlice[] keys, BitmapOperation bitop, out long result)
        {
            int maxBitmapLen = int.MinValue;
            int minBitmapLen = int.MaxValue;
            var status = GarnetStatus.NOTFOUND;

            int keyCount = keys.Length;

            // prepare input
            int inputSize = sizeof(int) + RespInputHeader.Size;
            byte* pbCmdInput = stackalloc byte[inputSize];

            byte* pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            (*(RespInputHeader*)pcurr).cmd = RespCommand.BITOP;
            (*(RespInputHeader*)pcurr).flags = 0;

            // 8 byte start pointer
            // 4 byte int length
            byte* output = stackalloc byte[12];
            byte** srcBitmapStartPtrs = stackalloc byte*[keyCount - 1];
            byte** srcBitmapEndPtrs = stackalloc byte*[keyCount - 1];

            if (sizeof(byte*) * (keyCount - 1) <= 512)
            {
                byte** tmpSrcBitmapStartPtrs = stackalloc byte*[(keyCount - 1)];
                byte** tmpSrcBitmapEndPtrs = stackalloc byte*[(keyCount - 1)];
                srcBitmapStartPtrs = tmpSrcBitmapStartPtrs;
                srcBitmapEndPtrs = tmpSrcBitmapEndPtrs;
            }

            byte* dstBitmapPtr;

            //TODO: Reenable transaction
            bool createTransaction = false;
            if (txnManager.state != TxnState.Running)
            {
                createTransaction = true;
                Debug.Assert(txnManager.state == TxnState.None);
                txnManager.SaveKeyEntryToLock(keys[0], false, LockType.Exclusive);
                for (int i = 1; i < keys.Length; i++)
                    txnManager.SaveKeyEntryToLock(keys[i], false, LockType.Shared);
                txnManager.Run(true);
            }

            // Perform under unsafe epoch control for pointer safety.
            var uc = txnManager.LockableUnsafeContext;

            try
            {
                uc.BeginUnsafe();
            readFromScratch:
                var localHeadAddress = HeadAddress;
                int srcKeyOffset = 0;

                for (int i = 1; i < keys.Length; i++)
                {
                    ArgSlice srcKey = keys[i];
                    //Read srcKey
                    var outputBitmap = new SpanByteAndMemory(output, 12);
                    status = ReadWithUnsafeContext(srcKey, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref outputBitmap, localHeadAddress, out bool epochChanged, ref uc);
                    if (epochChanged)
                    {
                        goto readFromScratch;
                    }
                    //Skip if key does not exist
                    if (status == GarnetStatus.NOTFOUND)
                        continue;

                    byte* outputBitmapPtr = outputBitmap.SpanByte.ToPointer();
                    byte* localSrcBitmapPtr = (byte*)((IntPtr)(*(long*)outputBitmapPtr));
                    int len = *(int*)(outputBitmapPtr + 8);

                    srcBitmapStartPtrs[srcKeyOffset] = localSrcBitmapPtr;
                    srcBitmapEndPtrs[srcKeyOffset] = localSrcBitmapPtr + len;
                    srcKeyOffset++;
                    maxBitmapLen = Math.Max(len, maxBitmapLen);
                    minBitmapLen = Math.Min(len, minBitmapLen);
                }

                #region performBitop
                sectorAlignedMemoryBitmap ??= new SectorAlignedMemory(bitmapBufferSize + sectorAlignedMemoryPoolAlignment, sectorAlignedMemoryPoolAlignment);
                dstBitmapPtr = sectorAlignedMemoryBitmap.GetValidPointer() + sectorAlignedMemoryPoolAlignment;
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

                //1. Multi-way bitmap merge
                BitmapManager.BitOpMainUnsafeMultiKey(dstBitmapPtr, maxBitmapLen, srcBitmapStartPtrs, srcBitmapEndPtrs, srcKeyOffset, minBitmapLen, (byte)bitop);
                #endregion

                if (maxBitmapLen > 0)
                {
                    SpanByte dstKey = keys[0].SpanByte;
                    byte* valPtr = dstBitmapPtr;
                    valPtr -= sizeof(int);
                    *(int*)valPtr = (int)maxBitmapLen;
                    status = SET(ref dstKey, ref Unsafe.AsRef<SpanByte>(valPtr), ref uc);
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

        public GarnetStatus StringBitOperation(BitmapOperation bitop, ArgSlice destinationKey, ArgSlice[] keys, out long result)
        {
            result = 0;
            if (destinationKey.Bytes.Length == 0)
                return GarnetStatus.OK;
            ArgSlice[] keysBitOp = new ArgSlice[keys.Length + 1];
            keysBitOp[0] = destinationKey;
            keys.CopyTo(keysBitOp, 1);
            return StringBitOperation(keysBitOp, bitop, out result);
        }

        public unsafe GarnetStatus StringBitCount<TContext>(ArgSlice key, long start, long end, bool useBitInterval, out long result, ref TContext context)
             where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
        {
            result = 0;

            if (key.Bytes.Length == 0)
                return GarnetStatus.OK;

            int inputSize = sizeof(int) + RespInputHeader.Size + sizeof(long) + sizeof(long) + sizeof(byte);
            byte* input = scratchBufferManager.CreateArgSlice(inputSize).ptr;

            //initialize the input variable
            byte* pcurr = input;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);

            (*(RespInputHeader*)pcurr).cmd = RespCommand.BITCOUNT;
            (*(RespInputHeader*)pcurr).flags = 0;
            pcurr += RespInputHeader.Size;
            *(long*)(pcurr) = start;
            pcurr += sizeof(long);
            *(long*)(pcurr) = end;
            pcurr += sizeof(long);
            *pcurr = (byte)(useBitInterval ? 1 : 0);

            SpanByteAndMemory output = new(null);
            var keySp = key.SpanByte;

            var status = Read_MainStore(ref keySp, ref Unsafe.AsRef<SpanByte>(input), ref output, ref context);

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
             where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
        {
            int inputSize = sizeof(int) + RespInputHeader.Size + sizeof(byte) + sizeof(byte) + sizeof(long) + sizeof(long) + sizeof(byte);
            byte* input = scratchBufferManager.CreateArgSlice(inputSize).ptr;
            result = new();
            var keySp = key.SpanByte;

            byte* pcurr = input;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);

            (*(RespInputHeader*)(pcurr)).cmd = RespCommand.BITFIELD;
            (*(RespInputHeader*)(pcurr)).flags = 0;
            pcurr += RespInputHeader.Size;

            for (int i = 0; i < commandArguments.Count; i++)
            {
                logger?.LogInformation($"BITFIELD > " +
                    $"[" + $"SECONDARY-OP: {(RespCommand)commandArguments[i].secondaryOpCode}, " +
                    $"SIGN: {((commandArguments[i].typeInfo & (byte)BitFieldSign.SIGNED) > 0 ? BitFieldSign.SIGNED : BitFieldSign.UNSIGNED)}, " +
                    $"BITCOUNT: {(commandArguments[i].typeInfo & 0x7F)}, " +
                    $"OFFSET: {commandArguments[i].offset}, " +
                    $"VALUE: {commandArguments[i].value}, " +
                    $"OVERFLOW: {(BitFieldOverflow)commandArguments[i].overflowType}]");

                pcurr = input + sizeof(int) + RespInputHeader.Size;
                *pcurr = commandArguments[i].secondaryOpCode; pcurr++;
                *pcurr = commandArguments[i].typeInfo; pcurr++;
                *(long*)pcurr = commandArguments[i].offset; pcurr += 8;
                *(long*)pcurr = commandArguments[i].value; pcurr += 8;
                *pcurr = commandArguments[i].overflowType;

                var output = new SpanByteAndMemory(null);
                var status = commandArguments[i].secondaryOpCode == (byte)RespCommand.GET ?
                    Read_MainStore(ref keySp, ref Unsafe.AsRef<SpanByte>(input), ref output, ref context) :
                    RMW_MainStore(ref keySp, ref Unsafe.AsRef<SpanByte>(input), ref output, ref context);

                if (status == GarnetStatus.NOTFOUND && commandArguments[i].secondaryOpCode == (byte)RespCommand.GET)
                {
                    result.Add(0);
                }
                else
                {
                    if (status == GarnetStatus.OK)
                    {
                        long resultCmd = 0;
                        bool error = false;
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

        public GarnetStatus StringSetBit<TContext>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, ref TContext context)
          where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
            => RMW_MainStore(ref key, ref input, ref output, ref context);

        public GarnetStatus StringGetBit<TContext>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
            => Read_MainStore(ref key, ref input, ref output, ref context);

        public unsafe GarnetStatus StringBitCount<TContext>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, ref TContext context)
         where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
             => Read_MainStore(ref key, ref input, ref output, ref context);

        public unsafe GarnetStatus StringBitPosition<TContext>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
             => Read_MainStore(ref key, ref input, ref output, ref context);

        public unsafe GarnetStatus StringBitField<TContext>(ref SpanByte key, ref SpanByte input, byte secondaryCommand, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
        {
            GarnetStatus status;
            if (secondaryCommand == (byte)RespCommand.GET)
                status = Read_MainStore(ref key, ref input, ref output, ref context);
            else
                status = RMW_MainStore(ref key, ref input, ref output, ref context);
            return status;
        }

        public unsafe GarnetStatus StringBitFieldReadOnly<TContext>(ref SpanByte key, ref SpanByte input, byte secondaryCommand, ref SpanByteAndMemory output, ref TContext context)
              where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
        {
            GarnetStatus status = GarnetStatus.NOTFOUND;

            if (secondaryCommand == (byte)RespCommand.GET)
                status = Read_MainStore(ref key, ref input, ref output, ref context);
            return status;
        }

    }
}