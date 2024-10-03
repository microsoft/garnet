// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
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

            parseState.InitializeWithArguments(ref parseStateBuffer, offset, setValSlice);

            var input = new RawStringInput
            {
                header = new RespInputHeader { cmd = RespCommand.SETBIT },
                parseState = parseState,
                parseStateStartIdx = 0,
            };

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

            parseState.InitializeWithArguments(ref parseStateBuffer, offset);

            var input = new RawStringInput
            {
                header = new RespInputHeader { cmd = RespCommand.GETBIT },
                parseState = parseState,
                parseStateStartIdx = 0,
            };

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
            var uc = txnManager.LockableUnsafeContext;

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

            parseState.InitializeWithArguments(ref parseStateBuffer, args);

            var input = new RawStringInput
            {
                header = new RespInputHeader { cmd = RespCommand.BITOP },
                parseState = parseState,
                parseStateStartIdx = 0
            };

            return StringBitOperation(ref input, bitOp, out result);
        }

        public unsafe GarnetStatus StringBitCount<TContext>(ArgSlice key, long start, long end, bool useBitInterval, out long result, ref TContext context)
             where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            result = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            var useBitIntervalBytes = stackalloc byte[1];
            useBitIntervalBytes[0] = (byte)(useBitInterval ? '1' : '0');
            var useBitIntervalSlice = new ArgSlice(useBitIntervalBytes, 1);

            var startBytes = Encoding.ASCII.GetBytes(start.ToString(CultureInfo.InvariantCulture));
            var endBytes = Encoding.ASCII.GetBytes(end.ToString(CultureInfo.InvariantCulture));

            SpanByteAndMemory output = new(null);
            GarnetStatus status;

            fixed (byte* startPtr = startBytes)
            {
                fixed (byte* endPtr = endBytes)
                {
                    var startSlice = new ArgSlice(startPtr, startBytes.Length);
                    var endSlice = new ArgSlice(endPtr, endBytes.Length);

                    parseState.InitializeWithArguments(ref parseStateBuffer, startSlice, endSlice, useBitIntervalSlice);

                    var input = new RawStringInput
                    {
                        header = new RespInputHeader { cmd = RespCommand.BITCOUNT },
                        parseState = parseState,
                        parseStateStartIdx = 0
                    };

                    var keySp = key.SpanByte;

                    status = Read_MainStore(ref keySp, ref input, ref output, ref context);
                }
            }

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
            var input = new RawStringInput
            {
                header = new RespInputHeader { cmd = RespCommand.BITFIELD },
                parseStateStartIdx = 0
            };

            result = new();
            var keySp = key.SpanByte;

            for (var i = 0; i < commandArguments.Count; i++)
            {
                var op = (RespCommand)commandArguments[i].secondaryOpCode;
                var opBytes = Encoding.ASCII.GetBytes(op.ToString());
                var encodingPrefix = (commandArguments[i].typeInfo & (byte)BitFieldSign.SIGNED) > 0 ? "i" : "u";
                var encodingBytes = Encoding.ASCII.GetBytes($"{encodingPrefix}{(byte)(commandArguments[i].typeInfo & 0x7F)}");
                var offsetBytes = Encoding.ASCII.GetBytes(commandArguments[i].offset.ToString());
                var valueBytes = Encoding.ASCII.GetBytes(commandArguments[i].value.ToString());
                var overflowTypeBytes = Encoding.ASCII.GetBytes(((BitFieldOverflow)commandArguments[i].overflowType).ToString());

                var output = new SpanByteAndMemory(null);
                GarnetStatus status;
                fixed (byte* opPtr = opBytes)
                fixed (byte* encodingPtr = encodingBytes)
                fixed (byte* offsetPtr = offsetBytes)
                fixed (byte* valuePtr = valueBytes)
                fixed (byte* overflowTypePtr = overflowTypeBytes)
                {
                    var opSlice = new ArgSlice(opPtr, opBytes.Length);
                    var encodingSlice = new ArgSlice(encodingPtr, encodingBytes.Length);
                    var offsetSlice = new ArgSlice(offsetPtr, offsetBytes.Length);
                    var valueSlice = new ArgSlice(valuePtr, valueBytes.Length);
                    var overflowTypeSlice = new ArgSlice(overflowTypePtr, overflowTypeBytes.Length);

                    parseState.InitializeWithArguments(ref parseStateBuffer, opSlice, encodingSlice, offsetSlice,
                        valueSlice, overflowTypeSlice);

                    input.parseState = parseState;
                    status = commandArguments[i].secondaryOpCode == (byte)RespCommand.GET ?
                        Read_MainStore(ref keySp, ref input, ref output, ref context) :
                        RMW_MainStore(ref keySp, ref input, ref output, ref context);
                }

                if (status == GarnetStatus.NOTFOUND && commandArguments[i].secondaryOpCode == (byte)RespCommand.GET)
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

        public unsafe GarnetStatus StringBitField<TContext>(ref SpanByte key, ref RawStringInput input, byte secondaryCommand, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            GarnetStatus status;
            if (secondaryCommand == (byte)RespCommand.GET)
                status = Read_MainStore(ref key, ref input, ref output, ref context);
            else
                status = RMW_MainStore(ref key, ref input, ref output, ref context);
            return status;
        }

        public unsafe GarnetStatus StringBitFieldReadOnly<TContext>(ref SpanByte key, ref RawStringInput input, byte secondaryCommand, ref SpanByteAndMemory output, ref TContext context)
              where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            GarnetStatus status = GarnetStatus.NOTFOUND;

            if (secondaryCommand == (byte)RespCommand.GET)
                status = Read_MainStore(ref key, ref input, ref output, ref context);
            return status;
        }

    }
}