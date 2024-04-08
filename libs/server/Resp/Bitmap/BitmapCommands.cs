// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    ///  (1) , (2) , (3) 
    /// overflow check, ptr protection, and status not found implemented for below
    /// GETBIT, SETBIT, BITCOUNT, BITPOS (1),(2)

    public enum BitmapOperation : byte
    {
        /// <summary>
        /// AND
        /// </summary>
        AND,
        /// <summary>
        /// OR
        /// </summary>
        OR,
        /// <summary>
        /// XOR
        /// </summary>
        XOR,
        /// <summary>
        /// NOT
        /// </summary>
        NOT
    }

    internal enum BitFieldOverflow : byte
    {
        WRAP,
        SAT,
        FAIL
    }
    internal enum BitFieldSign : byte
    {
        UNSIGNED = 0x0,
        SIGNED = 0x80
    }

    /// <summary>
    /// struct with parameters for BITFIELD command
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    public struct BitFieldCmdArgs
    {
        /// <summary>
        /// BITFIELD command
        /// </summary>
        [FieldOffset(0)]
        public byte secondaryOpCode;
        /// <summary>
        /// encoding info
        /// </summary>
        [FieldOffset(1)]
        public byte typeInfo;
        /// <summary>
        /// offset
        /// </summary>
        [FieldOffset(2)]
        public long offset;
        /// <summary>
        /// value
        /// </summary>
        [FieldOffset(10)]
        public long value;
        /// <summary>
        /// BitFieldOverflow enum 
        /// </summary>
        [FieldOffset(18)]
        public byte overflowType;

        /// <summary>
        /// add a command to execute in bitfield
        /// </summary>
        /// <param name="secondaryOpCode"></param>
        /// <param name="typeInfo"></param>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        /// <param name="overflowType"></param>
        public BitFieldCmdArgs(byte secondaryOpCode, byte typeInfo, long offset, long value, byte overflowType)
        {
            this.secondaryOpCode = secondaryOpCode;
            this.typeInfo = typeInfo;
            this.offset = offset;
            this.value = value;
            this.overflowType = overflowType;
        }
    }

    /// <summary>
    /// Server session for RESP protocol - sorted set
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Sets or clears the bit at offset in the given key.
        /// The bit is either set or cleared depending on value, which can be either 0 or 1.
        /// When key does not exist, a new key is created.The key is grown to make sure it can hold a bit at offset.
        /// </summary>
        private bool StringSetBit<TGarnetApi>(byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            byte* keyPtr = null;
            int ksize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            byte* bOffsetPtr = null;
            int bOffsetSize = 0;
            if (!RespReadUtils.ReadPtrWithLengthHeader(ref bOffsetPtr, ref bOffsetSize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            ptr += 1 + 1 + 2 + 1 + 2; // $ 1 \r\n [1|0] \r\n
            if (ptr > recvBufferPtr + bytesRead)
                return false;

            Debug.Assert(*(ptr - 7) == '$');
            Debug.Assert(*(ptr - 6) == '1');
            Debug.Assert(*(ptr - 5) == '\r');
            Debug.Assert(*(ptr - 4) == '\n');
            byte bSetVal = (byte)(*(ptr - 3) - '0');
            Debug.Assert(*(ptr - 3) >= '0' && *(ptr - 3) <= '1');
            Debug.Assert(*(ptr - 2) == '\r');
            Debug.Assert(*(ptr - 1) == '\n');

            readHead = (int)(ptr - recvBufferPtr);
            if (NetworkSingleKeySlotVerify(keyPtr, ksize, false))
                return true;

            keyPtr -= sizeof(int);
            *(int*)keyPtr = ksize; //b[ksize <key>]

            #region SetBitCmdInput
            //4 byte length of input
            //1 byte RespCommand
            //1 byte RespInputFlags
            //8 byte bit offset
            //1 byte set/clear bit
            int inputSize = sizeof(int) + RespInputHeader.Size + sizeof(long) + sizeof(byte);
            byte* pbCmdInput = stackalloc byte[inputSize];

            ///////////////
            //Build Input//
            ///////////////
            byte* pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            //1. header
            (*(RespInputHeader*)(pcurr)).cmd = RespCommand.SETBIT;
            (*(RespInputHeader*)(pcurr)).flags = 0;
            pcurr += RespInputHeader.Size;
            //2. cmd args
            *(long*)(pcurr) = NumUtils.BytesToLong(bOffsetSize, bOffsetPtr); pcurr += sizeof(long);
            *(byte*)(pcurr) = bSetVal;
            #endregion

            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            var status = storageApi.StringSetBit(
                ref Unsafe.AsRef<SpanByte>(keyPtr),
                ref Unsafe.AsRef<SpanByte>(pbCmdInput),
                ref o);

            if (status == GarnetStatus.OK)
                dcurr += o.Length;

            return true;
        }

        /// <summary>
        /// Returns the bit value at offset in the key stored.
        /// </summary>
        private bool StringGetBit<TGarnetApi>(byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {

            byte* keyPtr = null;
            int ksize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            byte* bOffsetPtr = null;
            int bOffsetSize = 0;
            if (!RespReadUtils.ReadPtrWithLengthHeader(ref bOffsetPtr, ref bOffsetSize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            readHead = (int)(ptr - recvBufferPtr);
            if (NetworkSingleKeySlotVerify(keyPtr, ksize, true))
                return true;

            keyPtr -= sizeof(int);
            *(int*)keyPtr = ksize; //b[ksize <key>]

            #region GetBitCmdInput
            //4 byte length of input
            //1 byte RespCommand
            //1 byte RespInputFlags
            //8 byte bit offset
            int inputSize = sizeof(int) + RespInputHeader.Size + sizeof(long);
            byte* pbCmdInput = stackalloc byte[inputSize];

            ///////////////
            //Build Input//
            ///////////////
            byte* pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            //1. header
            (*(RespInputHeader*)(pcurr)).cmd = RespCommand.GETBIT;
            (*(RespInputHeader*)(pcurr)).flags = 0;
            pcurr += RespInputHeader.Size;
            //2. cmd args
            *(long*)(pcurr) = NumUtils.BytesToLong(bOffsetSize, bOffsetPtr);
            #endregion

            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            var status = storageApi.StringGetBit(ref Unsafe.AsRef<SpanByte>(keyPtr), ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref o);

            if (status == GarnetStatus.NOTFOUND)
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                    SendAndReset();
            else
                dcurr += o.Length;

            return true;
        }

        /// <summary>
        /// Count the number of set bits in a key. 
        /// It can be specified an interval for counting, passing the start and end arguments.
        /// </summary>
        private bool StringBitCount<TGarnetApi>(byte* ptr, int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            //<[Get Key]>
            byte* keyPtr = null;
            int ksize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            //Process offsets here if they exist
            int startOffset = 0; // default is at the start of bitmap array
            int endOffset = -1; // default is at the end of the bitmap array (negative values indicate offset starting from end)
            byte bitOffsetType = 0x0; // treat offsets as byte or bit offsets
            if (count > 1)//Start offset exists
            {
                if (!RespReadUtils.ReadIntWithLengthHeader(out startOffset, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (count > 2)
                {
                    if (!RespReadUtils.ReadIntWithLengthHeader(out endOffset, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                }
            }

            if (count > 3)
            {
                if (!RespReadUtils.ReadStringWithLengthHeader(out var offsetType, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                bitOffsetType = offsetType.Equals("BIT", StringComparison.OrdinalIgnoreCase) ? (byte)0x1 : (byte)0x0;
            }

            readHead = (int)(ptr - recvBufferPtr);
            if (NetworkSingleKeySlotVerify(keyPtr, ksize, true))
                return true;

            keyPtr -= sizeof(int);
            *(int*)keyPtr = ksize; //b[ksize <key>]
            #region BitCountCmdInput
            //4 byte length of input
            //1 byte RespCommand
            //1 byte RespInputFlags
            //8 byte bit startOffset
            //8 byte bit endOffset
            int inputSize = sizeof(int) + RespInputHeader.Size + sizeof(long) + sizeof(long) + sizeof(byte);
            byte* pbCmdInput = stackalloc byte[inputSize];

            ///////////////
            //Build Input//
            ///////////////
            byte* pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            //1. header
            (*(RespInputHeader*)(pcurr)).cmd = RespCommand.BITCOUNT;
            (*(RespInputHeader*)(pcurr)).flags = 0;
            pcurr += RespInputHeader.Size;
            //2. cmd args
            *(long*)(pcurr) = startOffset; pcurr += 8;
            *(long*)(pcurr) = endOffset; pcurr += 8;
            *pcurr = bitOffsetType;
            #endregion

            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));

            var status = storageApi.StringBitCount(ref Unsafe.AsRef<SpanByte>(keyPtr), ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref o);

            if (status == GarnetStatus.OK)
            {
                if (!o.IsSpanByte)
                    SendAndReset(o.Memory, o.Length);
                else
                    dcurr += o.Length;
            }
            else if (status == GarnetStatus.NOTFOUND)
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Returns the position of the first bit set to 1 or 0 in a key.
        /// </summary>
        private bool StringBitPosition<TGarnetApi>(byte* ptr, int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            //<[Get Key]>
            byte* keyPtr = null;
            int ksize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            ptr += 1 + 1 + 2 + 1 + 2; // $ 1 \r\n [1|0] \r\n
            if (ptr > recvBufferPtr + bytesRead)
                return false;

            Debug.Assert(*(ptr - 7) == '$');
            Debug.Assert(*(ptr - 6) == '1');
            Debug.Assert(*(ptr - 5) == '\r');
            Debug.Assert(*(ptr - 4) == '\n');
            byte bSetVal = (byte)(*(ptr - 3) - '0');
            Debug.Assert(*(ptr - 3) >= '0' && *(ptr - 3) <= '1');
            Debug.Assert(*(ptr - 2) == '\r');
            Debug.Assert(*(ptr - 1) == '\n');

            //Process offsets here if they exist
            int startOffset = 0; // default is at the start of bitmap array
            int endOffset = -1; // default is at the end of the bitmap array (negative values indicate offset starting from end)
            byte bitOffsetType = 0x0; // treat offsets as byte or bit offsets

            if (count > 2)//Start offset exists
            {
                if (!RespReadUtils.ReadIntWithLengthHeader(out startOffset, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (count > 3)
                {
                    if (!RespReadUtils.ReadIntWithLengthHeader(out endOffset, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                }
            }

            if (count > 4)
            {
                if (!RespReadUtils.ReadStringWithLengthHeader(out var offsetType, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                bitOffsetType = offsetType.Equals("BIT", StringComparison.OrdinalIgnoreCase) ? (byte)0x1 : (byte)0x0;
            }

            readHead = (int)(ptr - recvBufferPtr);
            if (NetworkSingleKeySlotVerify(keyPtr, ksize, true))
                return true;
            keyPtr -= sizeof(int);
            *(int*)keyPtr = ksize; //b[ksize <key>]

            #region BitPosCmdIO
            //4 byte length of input
            //1 byte RespCommand
            //1 byte RespInputFlags
            //1 byte setVal
            //8 byte bit startOffset
            //8 byte bit endOffset
            int inputSize = sizeof(int) + RespInputHeader.Size + sizeof(byte) + sizeof(long) + sizeof(long) + sizeof(byte);
            byte* pbCmdInput = stackalloc byte[inputSize];

            ///////////////
            //Build Input//
            ///////////////
            byte* pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            //1. header
            (*(RespInputHeader*)(pcurr)).cmd = RespCommand.BITPOS;
            (*(RespInputHeader*)(pcurr)).flags = 0;
            pcurr += RespInputHeader.Size;
            //2. cmd args
            *(byte*)(pcurr) = bSetVal; pcurr++;
            *(long*)(pcurr) = startOffset; pcurr += 8;
            *(long*)(pcurr) = endOffset; pcurr += 8;
            *pcurr = bitOffsetType;
            #endregion

            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));

            var status = storageApi.StringBitPosition(ref Unsafe.AsRef<SpanByte>(keyPtr), ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref o);

            if (status == GarnetStatus.OK)
            {
                if (!o.IsSpanByte)
                    SendAndReset(o.Memory, o.Length);
                else
                    dcurr += o.Length;
            }
            else if (status == GarnetStatus.NOTFOUND)
            {
                var resp = bSetVal == 0 ? CmdStrings.RESP_RETURN_VAL_0 : CmdStrings.RESP_RETURN_VAL_N1;
                while (!RespWriteUtils.WriteDirect(resp, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Performs bitwise operations on multiple strings and store the result.
        /// </summary>
        private bool StringBitOperation<TGarnetApi>(int count, byte* ptr, BitmapOperation bitop, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var keyCount = count;
            ArgSlice[] keys = new ArgSlice[keyCount];

            //Read keys
            for (int i = 0; i < keys.Length; i++)
            {
                keys[i] = new();
                if (!RespReadUtils.ReadPtrWithLengthHeader(ref keys[i].ptr, ref keys[i].length, ref ptr, recvBufferPtr + bytesRead))
                    return false;
            }

            readHead = (int)(ptr - recvBufferPtr);
            if (NetworkKeyArraySlotVerify(ref keys, false))
                return true;

            if (sizeof(byte*) * (keyCount - 1) > 512)
            {
                throw new Exception("Bitop source key limit (64) exceeded");
            }

            var status = storageApi.StringBitOperation(keys, bitop, out long result);

            if (status != GarnetStatus.NOTFOUND)
            {
                while (!RespWriteUtils.WriteInteger(result, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Performs arbitrary bitfield integer operations on strings.
        /// </summary>
        private bool StringBitField<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            //BITFIELD key [GET encoding offset] [SET encoding offset value] [INCRBY encoding offset increment] [OVERFLOW WRAP| SAT | FAIL]
            //Extract Key//
            byte* keyPtr = null;
            int ksize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            int currCount = 0;
            int endCount = count - 1;
            int secondaryCmdCount = 0;
            byte overFlowType = (byte)BitFieldOverflow.WRAP;

            List<BitFieldCmdArgs> bitfieldArgs = new();
            byte secondaryOPcode = default;
            byte encodingInfo = default;
            long offset = default;
            long value = default;
            while (currCount < endCount)
            {
                //Get subcommand
                if (!RespReadUtils.ReadStringWithLengthHeader(out var command, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                //process overflow command
                if (command.Equals("OVERFLOW", StringComparison.OrdinalIgnoreCase))
                {
                    //Get overflow parameter
                    if (!RespReadUtils.ReadStringWithLengthHeader(out var overflowArg, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    if (overflowArg.Equals("WRAP", StringComparison.OrdinalIgnoreCase))
                        overFlowType = (byte)BitFieldOverflow.WRAP;
                    else if (overflowArg.Equals("SAT", StringComparison.OrdinalIgnoreCase))
                        overFlowType = (byte)BitFieldOverflow.SAT;
                    else if (overflowArg.Equals("FAIL", StringComparison.OrdinalIgnoreCase))
                        overFlowType = (byte)BitFieldOverflow.FAIL;
                    //At this point processed two arguments
                    else
                    {
                        while (!RespWriteUtils.WriteAsciiDirect($"-ERR Overflow type {overflowArg} not supported\r\n", ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                    currCount += 2;
                    continue;
                }
                else
                {
                    //[GET <encoding> <offset>] [SET <encoding> <offset> <value>] [INCRBY <encoding> <offset> <increment>]
                    //Process encoding argument
                    if (!RespReadUtils.ReadStringWithLengthHeader(out var encodingArg, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    if (!RespReadUtils.ReadStringWithLengthHeader(out var offsetArg, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    //Subcommand takes 2 args, encoding and offset
                    if (command.Equals("GET", StringComparison.OrdinalIgnoreCase))
                    {
                        secondaryOPcode = (byte)RespCommand.GET;
                        currCount += 3;// Skip 3 args including subcommand
                    }
                    else
                    {
                        //SET and INCRBY take 3 args, encoding, offset, and valueArg
                        if (command.Equals("SET", StringComparison.OrdinalIgnoreCase))
                            secondaryOPcode = (byte)RespCommand.SET;
                        else if (command.Equals("INCRBY", StringComparison.OrdinalIgnoreCase))
                            secondaryOPcode = (byte)RespCommand.INCRBY;
                        else
                        {
                            while (!RespWriteUtils.WriteAsciiDirect($"-ERR Bitfield command {command} not supported\r\n", ref dcurr, dend))
                                SendAndReset();
                            return true;
                        }

                        if (!RespReadUtils.ReadStringWithLengthHeader(out var valueArg, ref ptr, recvBufferPtr + bytesRead))
                            return false;

                        value = long.Parse(valueArg);
                        currCount += 4;// Skip 4 args including subcommand
                    }

                    //Identify sign for number
                    byte sign = encodingArg.StartsWith("i", StringComparison.OrdinalIgnoreCase) ? (byte)BitFieldSign.SIGNED : (byte)BitFieldSign.UNSIGNED;
                    //Number of bits in signed number
                    byte bitCount = (byte)int.Parse(encodingArg.AsSpan(1));
                    //At most 64 bits can fit into encoding info
                    encodingInfo = (byte)(sign | bitCount);

                    //Calculate number offset from bitCount if offsetArg starts with #
                    bool offsetType = offsetArg.StartsWith("#", StringComparison.OrdinalIgnoreCase);
                    offset = offsetType ? long.Parse(offsetArg.AsSpan(1)) : long.Parse(offsetArg);
                    offset = offsetType ? (offset * bitCount) : offset;
                }

                bitfieldArgs.Add(new(secondaryOPcode, encodingInfo, offset, value, overFlowType));
                secondaryCmdCount++;
            }

            if (NetworkSingleKeySlotVerify(keyPtr, ksize, false))
            {
                readHead = (int)(ptr - recvBufferPtr);
                return true;
            }

            keyPtr -= sizeof(int);
            *(int*)keyPtr = ksize;

            while (!RespWriteUtils.WriteArrayLength(secondaryCmdCount, ref dcurr, dend))
                SendAndReset();

            //4 byte length of input
            //1 byte RespCommand
            //1 byte RespInputFlags
            //1 byte secondary op-code
            //1 type info
            //8 offset
            //8 increment by quantity or value set
            //1 byte increment behaviour info
            int inputSize = sizeof(int) + RespInputHeader.Size + sizeof(byte) + sizeof(byte) + sizeof(long) + sizeof(long) + sizeof(byte);
            byte* pbCmdInput = stackalloc byte[inputSize];

            ///////////////
            //Build Input//
            ///////////////
            byte* pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            //1. header
            (*(RespInputHeader*)(pcurr)).cmd = RespCommand.BITFIELD;
            (*(RespInputHeader*)(pcurr)).flags = 0;
            pcurr += RespInputHeader.Size;

            for (int i = 0; i < secondaryCmdCount; i++)
            {
                logger?.LogInformation($"BITFIELD > " +
                    $"[" + $"SECONDARY-OP: {(RespCommand)bitfieldArgs[i].secondaryOpCode}, " +
                    $"SIGN: {((bitfieldArgs[i].typeInfo & (byte)BitFieldSign.SIGNED) > 0 ? BitFieldSign.SIGNED : BitFieldSign.UNSIGNED)}, " +
                    $"BITCOUNT: {(bitfieldArgs[i].typeInfo & 0x7F)}, " +
                    $"OFFSET: {bitfieldArgs[i].offset}, " +
                    $"VALUE: {bitfieldArgs[i].value}, " +
                    $"OVERFLOW: {(BitFieldOverflow)bitfieldArgs[i].overflowType}]");

                pcurr = pbCmdInput + sizeof(int) + RespInputHeader.Size;
                *pcurr = bitfieldArgs[i].secondaryOpCode; pcurr++;
                *pcurr = bitfieldArgs[i].typeInfo; pcurr++;
                *(long*)pcurr = bitfieldArgs[i].offset; pcurr += 8;
                *(long*)pcurr = bitfieldArgs[i].value; pcurr += 8;
                *pcurr = bitfieldArgs[i].overflowType;

                var output = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));

                var status = storageApi.StringBitField(ref Unsafe.AsRef<SpanByte>(keyPtr), ref Unsafe.AsRef<SpanByte>(pbCmdInput), bitfieldArgs[i].secondaryOpCode, ref output);

                if (status == GarnetStatus.NOTFOUND && bitfieldArgs[i].secondaryOpCode == (byte)RespCommand.GET)
                {
                    while (!RespWriteUtils.WriteArrayItem(0, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    if (!output.IsSpanByte)
                        SendAndReset(output.Memory, output.Length);
                    else
                        dcurr += output.Length;
                }
            }

            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Performs arbitrary read-only bitfield integer operations
        /// </summary>
        private bool StringBitFieldReadOnly<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            //BITFIELD key [GET encoding offset] [SET encoding offset value] [INCRBY encoding offset increment] [OVERFLOW WRAP| SAT | FAIL]
            //Extract Key//
            byte* keyPtr = null;
            int ksize = 0;

            //Extract key to process for bitfield
            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            int currCount = 0;
            int endCount = count - 1;
            int secondaryCmdCount = 0;
            byte overFlowType = (byte)BitFieldOverflow.WRAP;

            List<BitFieldCmdArgs> bitfieldArgs = new();
            byte secondaryOPcode = default;
            byte encodingInfo = default;
            long offset = default;
            long value = default;
            bool writeError = false;
            while (currCount < endCount)
            {
                if (writeError)
                {
                    //Drain command arguments in case of error in parsing subcommand args
                    while (currCount < endCount)
                    {
                        //Extract bitfield subcommand
                        if (!RespReadUtils.ReadStringWithLengthHeader(out var errorCommand, ref ptr, recvBufferPtr + bytesRead))
                            return false;
                        currCount++;
                    }
                    if (currCount == endCount) break;
                }

                //process overflow command
                if (!RespReadUtils.ReadStringWithLengthHeader(out var command, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                //Process overflow subcommand
                if (command.Equals("OVERFLOW", StringComparison.OrdinalIgnoreCase))
                {
                    //Get overflow parameter
                    if (!RespReadUtils.ReadStringWithLengthHeader(out var overflowArg, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    if (overflowArg.Equals("WRAP", StringComparison.OrdinalIgnoreCase))
                        overFlowType = (byte)BitFieldOverflow.WRAP;
                    else if (overflowArg.Equals("SAT", StringComparison.OrdinalIgnoreCase))
                        overFlowType = (byte)BitFieldOverflow.SAT;
                    else if (overflowArg.Equals("FAIL", StringComparison.OrdinalIgnoreCase))
                        overFlowType = (byte)BitFieldOverflow.FAIL;
                    else
                    {
                        while (!RespWriteUtils.WriteAsciiDirect($"-ERR Overflow type {overflowArg} not supported\r\n", ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                    currCount += 2;
                    continue;
                }
                else
                {
                    //[GET <encoding> <offset>] [SET <encoding> <offset> <value>] [INCRBY <encoding> <offset> <increment>]
                    //Process encoding argument
                    if (!RespReadUtils.ReadStringWithLengthHeader(out var encoding, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    //Process offset argument
                    if (!RespReadUtils.ReadStringWithLengthHeader(out var offsetArg, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    //Subcommand takes 2 args, encoding and offset
                    if (command.Equals("GET", StringComparison.OrdinalIgnoreCase))
                    {
                        secondaryOPcode = (byte)RespCommand.GET;
                        currCount += 3;// Skip 3 args including subcommand
                    }
                    else
                    {
                        //SET and INCRBY take 3 args, encoding, offset, and valueArg
                        writeError = true;
                        if (!RespReadUtils.ReadStringWithLengthHeader(out var valueArg, ref ptr, recvBufferPtr + bytesRead))
                            return false;

                        value = Int64.Parse(valueArg);
                        currCount += 4;// Skip 4 args including subcommand
                    }

                    //Identify sign for number
                    byte sign = encoding.StartsWith("i", StringComparison.OrdinalIgnoreCase) ? (byte)BitFieldSign.SIGNED : (byte)BitFieldSign.UNSIGNED;
                    //Number of bits in signed number
                    byte bitCount = (byte)int.Parse(encoding.AsSpan(1));
                    encodingInfo = (byte)(sign | bitCount);

                    //Calculate number offset from bitCount if offsetArg starts with #
                    bool offsetType = offsetArg.StartsWith("#", StringComparison.OrdinalIgnoreCase);
                    offset = offsetType ? long.Parse(offsetArg.AsSpan(1)) : long.Parse(offsetArg);
                    offset = offsetType ? (offset * bitCount) : offset;
                }

                bitfieldArgs.Add(new(secondaryOPcode, encodingInfo, offset, value, overFlowType));
                secondaryCmdCount++;
            }

            //Process only bitfield GET and skip any other subcommand.
            if (writeError)
            {
                while (!RespWriteUtils.WriteDirect("-ERR BITFIELD_RO only supports the GET subcommand.\r\n"u8, ref dcurr, dend))
                    SendAndReset();
                readHead = (int)(ptr - recvBufferPtr);
                return true;
            }

            //Verify cluster slot readonly for Bitfield_RO variant
            if (NetworkSingleKeySlotVerify(keyPtr, ksize, true))
            {
                readHead = (int)(ptr - recvBufferPtr);
                return true;
            }

            keyPtr -= sizeof(int);
            *(int*)keyPtr = ksize;

            while (!RespWriteUtils.WriteArrayLength(secondaryCmdCount, ref dcurr, dend))
                SendAndReset();

            //4 byte length of input
            //1 byte RespCommand
            //1 byte RespInputFlags                        
            //1 byte secondary op-code
            //1 type info            
            //8 offset
            //8 increment by quantity or value set            
            //1 byte increment behaviour info          
            int inputSize = sizeof(int) + RespInputHeader.Size + sizeof(byte) + sizeof(byte) + sizeof(long) + sizeof(long) + sizeof(byte);
            byte* pbCmdInput = stackalloc byte[inputSize];

            ///////////////
            //Build Input//
            ///////////////
            byte* pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            //1. header
            (*(RespInputHeader*)(pcurr)).cmd = RespCommand.BITFIELD;
            (*(RespInputHeader*)(pcurr)).flags = 0;
            pcurr += RespInputHeader.Size;

            for (int i = 0; i < secondaryCmdCount; i++)
            {
                logger?.LogInformation($"BITFIELD > " +
                    $"[" + $"SECONDARY-OP: {(RespCommand)bitfieldArgs[i].secondaryOpCode}, " +
                    $"SIGN: {((bitfieldArgs[i].typeInfo & (byte)BitFieldSign.SIGNED) > 0 ? BitFieldSign.SIGNED : BitFieldSign.UNSIGNED)}, " +
                    $"BITCOUNT: {(bitfieldArgs[i].typeInfo & 0x7F)}, " +
                    $"OFFSET: {bitfieldArgs[i].offset}, " +
                    $"VALUE: {bitfieldArgs[i].value}, " +
                    $"OVERFLOW: {(BitFieldOverflow)bitfieldArgs[i].overflowType}]");

                pcurr = pbCmdInput + sizeof(int) + RespInputHeader.Size;
                *pcurr = bitfieldArgs[i].secondaryOpCode; pcurr++;
                *pcurr = bitfieldArgs[i].typeInfo; pcurr++;
                *(long*)pcurr = bitfieldArgs[i].offset; pcurr += 8;
                *(long*)pcurr = bitfieldArgs[i].value; pcurr += 8;
                *pcurr = bitfieldArgs[i].overflowType;

                var output = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));

                var status = storageApi.StringBitFieldReadOnly(ref Unsafe.AsRef<SpanByte>(keyPtr), ref Unsafe.AsRef<SpanByte>(pbCmdInput), bitfieldArgs[i].secondaryOpCode, ref output);

                if (status == GarnetStatus.NOTFOUND && bitfieldArgs[i].secondaryOpCode == (byte)RespCommand.GET)
                {
                    while (!RespWriteUtils.WriteArrayItem(0, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    if (!output.IsSpanByte)
                        SendAndReset(output.Memory, output.Length);
                    else
                        dcurr += output.Length;
                }
            }

            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

    }
}