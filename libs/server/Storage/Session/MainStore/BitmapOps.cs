// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession : IDisposable
    {
        public unsafe GarnetStatus StringSetBit<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice offset, bool bit, out bool previous)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            previous = false;

            if (key.Length == 0)
                return GarnetStatus.OK;

            var inputSize = sizeof(int) + RespInputHeader.Size + sizeof(long) + sizeof(byte);
            var input = scratchBufferManager.CreateArgSlice(inputSize).ptr;

            //initialize the input variable
            var pcurr = input;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);

            (*(RespInputHeader*)pcurr).cmd = RespCommand.SETBIT;
            (*(RespInputHeader*)pcurr).flags = 0;
            pcurr += RespInputHeader.Size;

            //offset
            *(long*)pcurr = NumUtils.BytesToLong(offset.ReadOnlySpan);
            pcurr += sizeof(long);

            //bit value
            *pcurr = bit ? (byte)0x1 : (byte)0x0;

            SpanByteAndMemory output = new(null);
            var keySp = key.SpanByte;
            _ = RMW_MainStore<TKeyLocker, TEpochGuard>(ref keySp, ref Unsafe.AsRef<SpanByte>(input), ref output);

            return GarnetStatus.OK;
        }

        public unsafe GarnetStatus StringGetBit<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice offset, out bool bValue)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            bValue = false;

            if (key.Length == 0)
                return GarnetStatus.OK;

            var inputSize = sizeof(int) + RespInputHeader.Size + sizeof(long);
            var input = scratchBufferManager.CreateArgSlice(inputSize).ptr;

            //initialize the input variable
            var pcurr = input;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);

            (*(RespInputHeader*)pcurr).cmd = RespCommand.GETBIT;
            (*(RespInputHeader*)pcurr).flags = 0;
            pcurr += RespInputHeader.Size;

            //offset
            *(long*)pcurr = NumUtils.BytesToLong(offset.ReadOnlySpan);
            pcurr += sizeof(long);

            SpanByteAndMemory output = new(null);
            var keySp = key.SpanByte;
            var status = Read_MainStore<TKeyLocker, TEpochGuard>(ref keySp, ref Unsafe.AsRef<SpanByte>(input), ref output);

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

        public unsafe GarnetStatus StringBitOperation(Span<ArgSlice> keys, BitmapOperation bitop, out long result)
        {
            var maxBitmapLen = int.MinValue;
            var minBitmapLen = int.MaxValue;
            var status = GarnetStatus.NOTFOUND;
            var keyCount = keys.Length;

            // prepare input
            var inputSize = sizeof(int) + RespInputHeader.Size;
            var pbCmdInput = stackalloc byte[inputSize];

            var pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            (*(RespInputHeader*)pcurr).cmd = RespCommand.BITOP;
            (*(RespInputHeader*)pcurr).flags = 0;

            // 8 byte start pointer
            // 4 byte int length
            var output = stackalloc byte[12];
            var srcBitmapStartPtrs = stackalloc byte*[keyCount - 1];
            var srcBitmapEndPtrs = stackalloc byte*[keyCount - 1];

            byte* dstBitmapPtr;
            var createTransaction = false;
            if (txnManager.state != TxnState.Running)
            {
                createTransaction = true;
                Debug.Assert(txnManager.state == TxnState.None);
                txnManager.SaveKeyEntryToLock(keys[0], false, LockType.Exclusive);
                for (var i = 1; i < keys.Length; i++)
                    txnManager.SaveKeyEntryToLock(keys[i], false, LockType.Shared);
                _ = txnManager.Run(true);
            }

            try
            {
                // Perform Store operation under unsafe epoch control for pointer safety with speed, and always use TransactionalSessionLocker as we're in a transaction.
                // We have already locked via TransactionManager.Run so we only need to acquire the epoch here; operations within the transaction can use GarnetUnsafeEpochGuard.
                GarnetSafeEpochGuard.BeginUnsafe(ref dualContext.KernelSession);

            readFromScratch:
                var localHeadAddress = HeadAddress;
                var keysFound = 0;

                for (var i = 1; i < keys.Length; i++)
                {
                    var srcKey = keys[i];
                    //Read srcKey
                    var outputBitmap = new SpanByteAndMemory(output, 12);
                    status = ReadWithUnsafeContext<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(srcKey, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref outputBitmap, localHeadAddress, out var epochChanged);
                    if (epochChanged)
                        goto readFromScratch;

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


                // Check if at least one key is found and execute bitop
                if (keysFound > 0)
                {
                    //1. Multi-way bitmap merge
                    _ = BitmapManager.BitOpMainUnsafeMultiKey(dstBitmapPtr, maxBitmapLen, srcBitmapStartPtrs, srcBitmapEndPtrs, keysFound, minBitmapLen, (byte)bitop);
                    #endregion

                    if (maxBitmapLen > 0)
                    {
                        var dstKey = keys[0].SpanByte;
                        var valPtr = dstBitmapPtr;
                        valPtr -= sizeof(int);
                        *(int*)valPtr = maxBitmapLen;
                        status = SET<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(ref dstKey, ref Unsafe.AsRef<SpanByte>(valPtr));
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
                GarnetSafeEpochGuard.EndUnsafe(ref dualContext.KernelSession);
                if (createTransaction)
                    txnManager.Commit(true);
            }
            result = maxBitmapLen;
            return status;
        }

        public GarnetStatus StringBitOperation(BitmapOperation bitop, ArgSlice destinationKey, ArgSlice[] keys, out long result)
        {
            result = 0;
            if (destinationKey.Length == 0)
                return GarnetStatus.OK;
            var keysBitOp = new ArgSlice[keys.Length + 1];
            keysBitOp[0] = destinationKey;
            keys.CopyTo(keysBitOp, 1);
            return StringBitOperation(keysBitOp, bitop, out result);
        }

        public unsafe GarnetStatus StringBitCount<TKeyLocker, TEpochGuard>(ArgSlice key, long start, long end, bool useBitInterval, out long result)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            result = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            var inputSize = sizeof(int) + RespInputHeader.Size + sizeof(long) + sizeof(long) + sizeof(byte);
            var input = scratchBufferManager.CreateArgSlice(inputSize).ptr;

            //initialize the input variable
            var pcurr = input;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);

            (*(RespInputHeader*)pcurr).cmd = RespCommand.BITCOUNT;
            (*(RespInputHeader*)pcurr).flags = 0;
            pcurr += RespInputHeader.Size;
            *(long*)pcurr = start;
            pcurr += sizeof(long);
            *(long*)pcurr = end;
            pcurr += sizeof(long);
            *pcurr = (byte)(useBitInterval ? 1 : 0);

            SpanByteAndMemory output = new(null);
            var keySp = key.SpanByte;

            var status = Read_MainStore<TKeyLocker, TEpochGuard>(ref keySp, ref Unsafe.AsRef<SpanByte>(input), ref output);

            if (status == GarnetStatus.OK)
            {
                if (!output.IsSpanByte)
                {
                    fixed (byte* outputPtr = output.Memory.Memory.Span)
                    {
                        var refPtr = outputPtr;
                        _ = RespReadUtils.Read64Int(out result, ref refPtr, refPtr + sizeof(long));
                    }
                    output.Memory.Dispose();
                }
            }

            return status;
        }

        public unsafe GarnetStatus StringBitField<TKeyLocker, TEpochGuard>(ArgSlice key, List<BitFieldCmdArgs> commandArguments, out List<long?> result)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var inputSize = sizeof(int) + RespInputHeader.Size + sizeof(byte) + sizeof(byte) + sizeof(long) + sizeof(long) + sizeof(byte);
            var input = scratchBufferManager.CreateArgSlice(inputSize).ptr;
            result = [];
            var keySp = key.SpanByte;

            var pcurr = input;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);

            (*(RespInputHeader*)pcurr).cmd = RespCommand.BITFIELD;
            (*(RespInputHeader*)pcurr).flags = 0;
            pcurr += RespInputHeader.Size;

            for (var i = 0; i < commandArguments.Count; i++)
            {
                /* Commenting due to excessive verbosity
                logger?.LogInformation($"BITFIELD > " +
                    $"[" + $"SECONDARY-OP: {(RespCommand)commandArguments[i].secondaryOpCode}, " +
                    $"SIGN: {((commandArguments[i].typeInfo & (byte)BitFieldSign.SIGNED) > 0 ? BitFieldSign.SIGNED : BitFieldSign.UNSIGNED)}, " +
                    $"BITCOUNT: {(commandArguments[i].typeInfo & 0x7F)}, " +
                    $"OFFSET: {commandArguments[i].offset}, " +
                    $"VALUE: {commandArguments[i].value}, " +
                    $"OVERFLOW: {(BitFieldOverflow)commandArguments[i].overflowType}]");
                */
                pcurr = input + sizeof(int) + RespInputHeader.Size;
                *pcurr = commandArguments[i].secondaryOpCode; pcurr++;
                *pcurr = commandArguments[i].typeInfo; pcurr++;
                *(long*)pcurr = commandArguments[i].offset; pcurr += 8;
                *(long*)pcurr = commandArguments[i].value; pcurr += 8;
                *pcurr = commandArguments[i].overflowType;

                var output = new SpanByteAndMemory(null);
                var status = commandArguments[i].secondaryOpCode == (byte)RespCommand.GET
                    ? Read_MainStore<TKeyLocker, TEpochGuard>(ref keySp, ref Unsafe.AsRef<SpanByte>(input), ref output)
                    : RMW_MainStore<TKeyLocker, TEpochGuard>(ref keySp, ref Unsafe.AsRef<SpanByte>(input), ref output);

                if (status == GarnetStatus.NOTFOUND && commandArguments[i].secondaryOpCode == (byte)RespCommand.GET)
                {
                    result.Add(0);
                }
                else
                {
                    if (status == GarnetStatus.OK)
                    {
                        long resultCmd = 0;
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

        public GarnetStatus StringSetBit<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => RMW_MainStore<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        public GarnetStatus StringGetBit<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => Read_MainStore<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        public unsafe GarnetStatus StringBitCount<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
             => Read_MainStore<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        public unsafe GarnetStatus StringBitPosition<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
             => Read_MainStore<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        public GarnetStatus StringBitField<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, byte secondaryCommand, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => secondaryCommand == (byte)RespCommand.GET
                ? Read_MainStore<TKeyLocker, TEpochGuard>(ref key, ref input, ref output)
                : RMW_MainStore<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        public GarnetStatus StringBitFieldReadOnly<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, byte secondaryCommand, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard 
            => (secondaryCommand == (byte)RespCommand.GET)
                ? Read_MainStore<TKeyLocker, TEpochGuard>(ref key, ref input, ref output)
                : GarnetStatus.NOTFOUND;
    }
}