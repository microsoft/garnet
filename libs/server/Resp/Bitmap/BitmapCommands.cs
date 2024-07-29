// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        /// NONE
        /// </summary>
        NONE,
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
        private bool NetworkStringSetBit<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 3)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.SETBIT), parseState.Count);
            }

            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;

            if (!parseState.TryGetLong(1, out var bOffset))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var bSetValSlice = parseState.GetArgSliceByRef(2);
            Debug.Assert(bSetValSlice.length == 1);
            var bSetVal = (byte)(bSetValSlice.ReadOnlySpan[0] - '0');
            Debug.Assert(bSetVal == 0 || bSetVal == 1);

            if (NetworkMultiKeySlotVerify(readOnly: false, firstKey: 0, lastKey: 0))
                return true;

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
            *(long*)(pcurr) = bOffset; pcurr += sizeof(long);
            *pcurr = bSetVal;
            #endregion

            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            var status = storageApi.StringSetBit(
                ref sbKey,
                ref Unsafe.AsRef<SpanByte>(pbCmdInput),
                ref o);

            if (status == GarnetStatus.OK)
                dcurr += o.Length;

            return true;
        }

        /// <summary>
        /// Returns the bit value at offset in the key stored.
        /// </summary>
        private bool NetworkStringGetBit<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 2)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.GETBIT), parseState.Count);
            }

            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;

            if (!parseState.TryGetLong(1, out var bOffset))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (NetworkMultiKeySlotVerify(readOnly: true, firstKey: 0, lastKey: 0))
                return true;

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
            *(long*)(pcurr) = bOffset;
            #endregion

            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            var status = storageApi.StringGetBit(ref sbKey, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref o);

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
        private bool NetworkStringBitCount<TGarnetApi>(int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count < 1 || count > 4)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.BITCOUNT), parseState.Count);
            }

            //<[Get Key]>
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;

            //Process offsets here if they exist
            var startOffset = 0; // default is at the start of bitmap array
            var endOffset = -1; // default is at the end of the bitmap array (negative values indicate offset starting from end)
            byte bitOffsetType = 0x0; // treat offsets as byte or bit offsets
            if (count > 1)//Start offset exists
            {
                if (!parseState.TryGetInt(1, out startOffset) || (count > 2 && !parseState.TryGetInt(2, out endOffset)))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
            }

            if (count > 3)
            {
                var sbOffsetType = parseState.GetArgSliceByRef(3).ReadOnlySpan;
                bitOffsetType = sbOffsetType.EqualsUpperCaseSpanIgnoringCase("BIT"u8) ? (byte)0x1 : (byte)0x0;
            }

            if (NetworkMultiKeySlotVerify(readOnly: false, firstKey: 0, lastKey: 0))
                return true;

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

            var status = storageApi.StringBitCount(ref sbKey, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref o);

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
        private bool NetworkStringBitPosition<TGarnetApi>(int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count < 2 || count > 5)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.BITPOS), parseState.Count);
            }

            //<[Get Key]>
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;

            var bSetValSlice = parseState.GetArgSliceByRef(1);
            Debug.Assert(bSetValSlice.length == 1);
            var bSetVal = (byte)(bSetValSlice.ReadOnlySpan[0] - '0');
            Debug.Assert(bSetVal == 0 || bSetVal == 1);

            //Process offsets here if they exist
            var startOffset = 0; // default is at the start of bitmap array
            var endOffset = -1; // default is at the end of the bitmap array (negative values indicate offset starting from end)
            byte bitOffsetType = 0x0; // treat offsets as byte or bit offsets

            if (count > 2)//Start offset exists
            {
                if (!parseState.TryGetInt(2, out startOffset) || (count > 3 && !parseState.TryGetInt(3, out endOffset)))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
            }

            if (count > 4)
            {
                var sbOffsetType = parseState.GetArgSliceByRef(4).ReadOnlySpan;
                bitOffsetType = sbOffsetType.EqualsUpperCaseSpanIgnoringCase("BIT"u8) ? (byte)0x1 : (byte)0x0;
            }

            if (NetworkMultiKeySlotVerify(readOnly: true, firstKey: 0, lastKey: 0))
                return true;

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

            var status = storageApi.StringBitPosition(ref sbKey, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref o);

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
        private bool NetworkStringBitOperation<TGarnetApi>(BitmapOperation bitop, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // Too few keys
            if (parseState.Count < 2)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS, ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            if (parseState.Count > 64)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_BITOP_KEY_LIMIT, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (NetworkMultiKeySlotVerify(readOnly: false, firstKey: 0, lastKey: -1))
                return true;

            _ = storageApi.StringBitOperation(parseState.Parameters, bitop, out var result);
            while (!RespWriteUtils.WriteInteger(result, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Performs arbitrary bitfield integer operations on strings.
        /// </summary>
        private bool StringBitField<TGarnetApi>(int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count < 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.BITFIELD), count);
            }

            //BITFIELD key [GET encoding offset] [SET encoding offset value] [INCRBY encoding offset increment] [OVERFLOW WRAP| SAT | FAIL]
            //Extract Key//
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;

            int currCount = 1;
            int secondaryCmdCount = 0;
            byte overFlowType = (byte)BitFieldOverflow.WRAP;

            List<BitFieldCmdArgs> bitfieldArgs = new();
            byte secondaryOPcode = default;
            byte encodingInfo = default;
            long offset = default;
            long value = default;
            while (currCount < count)
            {
                //Get subcommand
                var command = parseState.GetArgSliceByRef(currCount++).ReadOnlySpan;

                //process overflow command
                if (command.EqualsUpperCaseSpanIgnoringCase("OVERFLOW"u8))
                {
                    //Get overflow parameter
                    var overflowArg = parseState.GetArgSliceByRef(currCount++).ReadOnlySpan;

                    if (overflowArg.EqualsUpperCaseSpanIgnoringCase("WRAP"u8))
                        overFlowType = (byte)BitFieldOverflow.WRAP;
                    else if (overflowArg.EqualsUpperCaseSpanIgnoringCase("SAT"u8))
                        overFlowType = (byte)BitFieldOverflow.SAT;
                    else if (overflowArg.EqualsUpperCaseSpanIgnoringCase("FAIL"u8))
                        overFlowType = (byte)BitFieldOverflow.FAIL;
                    //At this point processed two arguments
                    else
                    {
                        while (!RespWriteUtils.WriteError($"ERR Overflow type {Encoding.ASCII.GetString(overflowArg)} not supported", ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }

                    continue;
                }

                //[GET <encoding> <offset>] [SET <encoding> <offset> <value>] [INCRBY <encoding> <offset> <increment>]
                //Process encoding argument
                var encodingArg = parseState.GetString(currCount++);
                var offsetArg = parseState.GetString(currCount++);

                //Subcommand takes 2 args, encoding and offset
                if (command.EqualsUpperCaseSpanIgnoringCase("GET"u8))
                {
                    secondaryOPcode = (byte)RespCommand.GET;
                }
                else
                {
                    //SET and INCRBY take 3 args, encoding, offset, and valueArg
                    if (command.EqualsUpperCaseSpanIgnoringCase("SET"u8))
                        secondaryOPcode = (byte)RespCommand.SET;
                    else if (command.EqualsUpperCaseSpanIgnoringCase("INCRBY"u8))
                        secondaryOPcode = (byte)RespCommand.INCRBY;
                    else
                    {
                        while (!RespWriteUtils.WriteError($"ERR Bitfield command {Encoding.ASCII.GetString(command)} not supported", ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }

                    if (!parseState.TryGetLong(currCount++, out value))
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                            SendAndReset();

                        return true;
                    }
                }

                //Identify sign for number
                byte sign = encodingArg.StartsWith("i", StringComparison.OrdinalIgnoreCase) ? (byte)BitFieldSign.SIGNED : (byte)BitFieldSign.UNSIGNED;
                //Number of bits in signed number
                byte bitCount = (byte)int.Parse(encodingArg.AsSpan(1));
                //At most 64 bits can fit into encoding info
                encodingInfo = (byte)(sign | bitCount);

                //Calculate number offset from bitCount if offsetArg starts with #
                bool offsetType = offsetArg.StartsWith("#", StringComparison.Ordinal);
                offset = offsetType ? long.Parse(offsetArg.AsSpan(1)) : long.Parse(offsetArg);
                offset = offsetType ? (offset * bitCount) : offset;

                bitfieldArgs.Add(new(secondaryOPcode, encodingInfo, offset, value, overFlowType));
                secondaryCmdCount++;
            }

            if (NetworkMultiKeySlotVerify(readOnly: false, firstKey: 0, lastKey: 0))
                return true;

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

            for (var i = 0; i < secondaryCmdCount; i++)
            {
                /* Commenting due to excessive verbosity
                logger?.LogInformation($"BITFIELD > " +
                    $"[" + $"SECONDARY-OP: {(RespCommand)bitfieldArgs[i].secondaryOpCode}, " +
                    $"SIGN: {((bitfieldArgs[i].typeInfo & (byte)BitFieldSign.SIGNED) > 0 ? BitFieldSign.SIGNED : BitFieldSign.UNSIGNED)}, " +
                    $"BITCOUNT: {(bitfieldArgs[i].typeInfo & 0x7F)}, " +
                    $"OFFSET: {bitfieldArgs[i].offset}, " +
                    $"VALUE: {bitfieldArgs[i].value}, " +
                    $"OVERFLOW: {(BitFieldOverflow)bitfieldArgs[i].overflowType}]");
                */
                pcurr = pbCmdInput + sizeof(int) + RespInputHeader.Size;
                *pcurr = bitfieldArgs[i].secondaryOpCode; pcurr++;
                *pcurr = bitfieldArgs[i].typeInfo; pcurr++;
                *(long*)pcurr = bitfieldArgs[i].offset; pcurr += 8;
                *(long*)pcurr = bitfieldArgs[i].value; pcurr += 8;
                *pcurr = bitfieldArgs[i].overflowType;

                var output = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));

                var status = storageApi.StringBitField(ref sbKey, ref Unsafe.AsRef<SpanByte>(pbCmdInput), bitfieldArgs[i].secondaryOpCode, ref output);

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

            return true;
        }

        /// <summary>
        /// Performs arbitrary read-only bitfield integer operations
        /// </summary>
        private bool StringBitFieldReadOnly<TGarnetApi>(int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            //BITFIELD key [GET encoding offset] [SET encoding offset value] [INCRBY encoding offset increment] [OVERFLOW WRAP| SAT | FAIL]
            //Extract Key//
            //Extract key to process for bitfield
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;

            int currCount = 1;
            int secondaryCmdCount = 0;
            byte overFlowType = (byte)BitFieldOverflow.WRAP;

            List<BitFieldCmdArgs> bitfieldArgs = new();
            byte secondaryOPcode = default;
            byte encodingInfo = default;
            long offset = default;
            long value = default;
            bool writeError = false;
            while (currCount < count)
            {
                //process overflow command
                var command = parseState.GetArgSliceByRef(currCount++).ReadOnlySpan;

                //Process overflow subcommand
                if (command.EqualsUpperCaseSpanIgnoringCase("OVERFLOW"u8))
                {
                    //Get overflow parameter
                    var overflowArg = parseState.GetArgSliceByRef(currCount++).ReadOnlySpan;

                    if (overflowArg.EqualsUpperCaseSpanIgnoringCase("WRAP"u8))
                        overFlowType = (byte)BitFieldOverflow.WRAP;
                    else if (overflowArg.EqualsUpperCaseSpanIgnoringCase("SAT"u8))
                        overFlowType = (byte)BitFieldOverflow.SAT;
                    else if (overflowArg.EqualsUpperCaseSpanIgnoringCase("FAIL"u8))
                        overFlowType = (byte)BitFieldOverflow.FAIL;
                    else
                    {
                        while (!RespWriteUtils.WriteError($"ERR Overflow type {Encoding.ASCII.GetString(overflowArg)} not supported", ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }

                    continue;
                }

                //[GET <encoding> <offset>] [SET <encoding> <offset> <value>] [INCRBY <encoding> <offset> <increment>]
                //Process encoding argument
                var encoding = parseState.GetString(currCount++);

                //Process offset argument
                var offsetArg = parseState.GetString(currCount++);

                //Subcommand takes 2 args, encoding and offset
                if (command.EqualsUpperCaseSpanIgnoringCase("GET"u8))
                {
                    secondaryOPcode = (byte)RespCommand.GET;
                }
                else
                {
                    //SET and INCRBY take 3 args, encoding, offset, and valueArg
                    writeError = true;
                    if (!parseState.TryGetLong(currCount++, out value))
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                            SendAndReset();

                        return true;
                    }
                }

                //Identify sign for number
                byte sign = encoding.StartsWith("i", StringComparison.OrdinalIgnoreCase) ? (byte)BitFieldSign.SIGNED : (byte)BitFieldSign.UNSIGNED;
                //Number of bits in signed number
                byte bitCount = (byte)int.Parse(encoding.AsSpan(1));
                encodingInfo = (byte)(sign | bitCount);

                //Calculate number offset from bitCount if offsetArg starts with #
                bool offsetType = offsetArg.StartsWith("#", StringComparison.Ordinal);
                offset = offsetType ? long.Parse(offsetArg.AsSpan(1)) : long.Parse(offsetArg);
                offset = offsetType ? (offset * bitCount) : offset;

                bitfieldArgs.Add(new(secondaryOPcode, encodingInfo, offset, value, overFlowType));
                secondaryCmdCount++;
            }

            //Process only bitfield GET and skip any other subcommand.
            if (writeError)
            {
                while (!RespWriteUtils.WriteError("ERR BITFIELD_RO only supports the GET subcommand."u8, ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            //Verify cluster slot readonly for Bitfield_RO variant
            if (NetworkMultiKeySlotVerify(readOnly: true, firstKey: 0, lastKey: 0))
                return true;

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
                /* Commenting due to excessive verbosity
                logger?.LogInformation($"BITFIELD > " +
                    $"[" + $"SECONDARY-OP: {(RespCommand)bitfieldArgs[i].secondaryOpCode}, " +
                    $"SIGN: {((bitfieldArgs[i].typeInfo & (byte)BitFieldSign.SIGNED) > 0 ? BitFieldSign.SIGNED : BitFieldSign.UNSIGNED)}, " +
                    $"BITCOUNT: {(bitfieldArgs[i].typeInfo & 0x7F)}, " +
                    $"OFFSET: {bitfieldArgs[i].offset}, " +
                    $"VALUE: {bitfieldArgs[i].value}, " +
                    $"OVERFLOW: {(BitFieldOverflow)bitfieldArgs[i].overflowType}]");
                */
                pcurr = pbCmdInput + sizeof(int) + RespInputHeader.Size;
                *pcurr = bitfieldArgs[i].secondaryOpCode; pcurr++;
                *pcurr = bitfieldArgs[i].typeInfo; pcurr++;
                *(long*)pcurr = bitfieldArgs[i].offset; pcurr += 8;
                *(long*)pcurr = bitfieldArgs[i].value; pcurr += 8;
                *pcurr = bitfieldArgs[i].overflowType;

                var output = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));

                var status = storageApi.StringBitFieldReadOnly(ref sbKey, ref Unsafe.AsRef<SpanByte>(pbCmdInput), bitfieldArgs[i].secondaryOpCode, ref output);

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

            return true;
        }
    }
}