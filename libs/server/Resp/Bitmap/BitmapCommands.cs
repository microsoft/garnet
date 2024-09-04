// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
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
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.SETBIT));
            }

            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;

            // Validate offset
            if (!parseState.TryGetLong(1, out _))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER, ref dcurr,
                           dend))
                    SendAndReset();
                return true;
            }

            // Validate value
            var bSetValSlice = parseState.GetArgSliceByRef(2).ReadOnlySpan;
            if (bSetValSlice.Length != 1 || (bSetValSlice[0] != '0' && bSetValSlice[0] != '1'))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_BIT_IS_NOT_INTEGER, ref dcurr,
                           dend))
                    SendAndReset();
                return true;
            }

            var inputHeader = new RawStringInput
            {
                header = new RespInputHeader { cmd = RespCommand.SETBIT },
                parseState = parseState,
                parseStateStartIdx = 1
            };

            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            var status = storageApi.StringSetBit(
                ref sbKey,
                ref inputHeader,
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
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.GETBIT));
            }

            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;

            // Validate offset
            if (!parseState.TryGetLong(1, out _))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER, ref dcurr,
                           dend))
                    SendAndReset();
                return true;
            }

            var inputHeader = new RawStringInput
            {
                header = new RespInputHeader { cmd = RespCommand.GETBIT },
                parseState = parseState,
                parseStateStartIdx = 1,
            };

            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            var status = storageApi.StringGetBit(ref sbKey, ref inputHeader, ref o);

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
        private bool NetworkStringBitCount<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var count = parseState.Count;
            if (count < 1 || count > 4)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.BITCOUNT));
            }

            // <[Get Key]>
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;

            // Validate start & end offsets, if exist
            if (parseState.Count > 1)
            {
                if (!parseState.TryGetInt(1, out _) || (parseState.Count > 2 && !parseState.TryGetInt(2, out _)))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr,
                               dend))
                        SendAndReset();
                    return true;
                }
            }

            var inputHeader = new RawStringInput
            {
                header = new RespInputHeader { cmd = RespCommand.BITCOUNT },
                parseState = parseState,
                parseStateStartIdx = 1
            };

            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));

            var status = storageApi.StringBitCount(ref sbKey, ref inputHeader, ref o);

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
        private bool NetworkStringBitPosition<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var count = parseState.Count;
            if (count < 2 || count > 5)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.BITPOS));
            }

            // <[Get Key]>
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;

            // Validate value
            var bSetValSlice = parseState.GetArgSliceByRef(1).ReadOnlySpan;
            if (bSetValSlice.Length != 1 || (bSetValSlice[0] != '0' && bSetValSlice[0] != '1'))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_BIT_IS_NOT_INTEGER, ref dcurr,
                           dend))
                    SendAndReset();
                return true;
            }

            // Validate start & end offsets, if exist
            if (parseState.Count > 2)
            {
                if (!parseState.TryGetInt(2, out _) ||
                    (parseState.Count > 3 && !parseState.TryGetInt(3, out _)))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr,
                               dend))
                        SendAndReset();
                    return true;
                }
            }

            // Validate offset range type (BIT / BYTE), if exists
            if (parseState.Count > 4)
            {
                var sbOffsetType = parseState.GetArgSliceByRef(4).ReadOnlySpan;
                if (!sbOffsetType.EqualsUpperCaseSpanIgnoringCase("BIT"u8) &&
                    !sbOffsetType.EqualsUpperCaseSpanIgnoringCase("BYTE"u8))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr,
                               dend))
                        SendAndReset();
                    return true;
                }
            }

            var inputHeader = new RawStringInput
            {
                header = new RespInputHeader { cmd = RespCommand.BITPOS },
                parseState = parseState,
                parseStateStartIdx = 1,
            };

            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));

            var status = storageApi.StringBitPosition(ref sbKey, ref inputHeader, ref o);

            if (status == GarnetStatus.OK)
            {
                if (!o.IsSpanByte)
                    SendAndReset(o.Memory, o.Length);
                else
                    dcurr += o.Length;
            }
            else if (status == GarnetStatus.NOTFOUND)
            {
                var resp = bSetValSlice[0] == '0' ? CmdStrings.RESP_RETURN_VAL_0 : CmdStrings.RESP_RETURN_VAL_N1;
                while (!RespWriteUtils.WriteDirect(resp, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Performs bitwise operations on multiple strings and store the result.
        /// </summary>
        private bool NetworkStringBitOperation<TGarnetApi>(BitmapOperation bitOp, ref TGarnetApi storageApi)
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

            var input = new RawStringInput
            {
                header = new RespInputHeader { cmd = RespCommand.BITOP },
                parseState = parseState,
                parseStateStartIdx = 0,
            };

            _ = storageApi.StringBitOperation(ref input, bitOp, out var result);
            while (!RespWriteUtils.WriteInteger(result, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Performs arbitrary bitfield integer operations on strings.
        /// </summary>
        private bool StringBitField<TGarnetApi>(ref TGarnetApi storageApi, bool readOnly = false)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.BITFIELD));
            }

            // BITFIELD key [GET encoding offset] [SET encoding offset value] [INCRBY encoding offset increment] [OVERFLOW WRAP| SAT | FAIL]
            // Extract Key
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;

            var currTokenIdx = 1;

            var isOverflowTypeSet = false;
            ArgSlice overflowTypeSlice = default;
            var secondaryCommandArgs = new List<(RespCommand, ArgSlice[])>();

            while (currTokenIdx < parseState.Count)
            {
                // Get subcommand
                var commandSlice = parseState.GetArgSliceByRef(currTokenIdx++);
                var command = commandSlice.ReadOnlySpan;

                // Process overflow command
                if (!readOnly && command.EqualsUpperCaseSpanIgnoringCase("OVERFLOW"u8))
                {
                    // Get overflow parameter
                    overflowTypeSlice = parseState.GetArgSliceByRef(currTokenIdx);
                    isOverflowTypeSet = true;

                    if (!parseState.TryGetEnum(currTokenIdx, true, out BitFieldOverflow _))
                    {
                        while (!RespWriteUtils.WriteError(
                                   $"ERR Overflow type {parseState.GetString(currTokenIdx)} not supported",
                                   ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }

                    currTokenIdx++;

                    continue;
                }

                // [GET <encoding> <offset>] [SET <encoding> <offset> <value>] [INCRBY <encoding> <offset> <increment>]
                // Process encoding argument
                var encodingSlice = parseState.GetArgSliceByRef(currTokenIdx);
                var offsetSlice = parseState.GetArgSliceByRef(currTokenIdx + 1);
                var encodingArg = parseState.GetString(currTokenIdx);
                var offsetArg = parseState.GetString(currTokenIdx + 1);
                currTokenIdx += 2;

                // Validate encoding
                if (encodingArg.Length < 2 ||
                    (encodingArg[0] != 'i' && encodingArg[0] != 'u') ||
                    !int.TryParse(encodingArg.AsSpan(1), out var bitCount) ||
                    bitCount > 64 ||
                    (bitCount == 64 && encodingArg[0] == 'u'))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_INVALID_BITFIELD_TYPE, ref dcurr,
                               dend))
                        SendAndReset();
                    return true;
                }

                // Validate offset
                var isOffsetValid = offsetArg[0] == '#'
                    ? long.TryParse(offsetArg.AsSpan(1), out _)
                    : long.TryParse(offsetArg, out _);

                if (!isOffsetValid)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER, ref dcurr,
                               dend))
                        SendAndReset();
                    return true;
                }

                // Subcommand takes 2 args, encoding and offset
                if (command.EqualsUpperCaseSpanIgnoringCase("GET"u8))
                {
                    secondaryCommandArgs.Add((RespCommand.GET, [commandSlice, encodingSlice, offsetSlice]));
                }
                else
                {
                    if (readOnly)
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }

                    RespCommand op;
                    // SET and INCRBY take 3 args, encoding, offset, and valueArg
                    if (command.EqualsUpperCaseSpanIgnoringCase("SET"u8))
                        op = RespCommand.SET;
                    else if (command.EqualsUpperCaseSpanIgnoringCase("INCRBY"u8))
                        op = RespCommand.INCRBY;
                    else
                    {
                        while (!RespWriteUtils.WriteError(
                                   $"ERR Bitfield command {Encoding.ASCII.GetString(command)} not supported", ref dcurr,
                                   dend))
                            SendAndReset();
                        return true;
                    }

                    // Validate value
                    var valueSlice = parseState.GetArgSliceByRef(currTokenIdx);
                    if (!parseState.TryGetLong(currTokenIdx, out _))
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr,
                                   dend))
                            SendAndReset();
                        return true;
                    }
                    currTokenIdx++;

                    secondaryCommandArgs.Add((op, [commandSlice, encodingSlice, offsetSlice, valueSlice]));
                }
            }

            while (!RespWriteUtils.WriteArrayLength(secondaryCommandArgs.Count, ref dcurr, dend))
                SendAndReset();

            var inputHeader = new RawStringInput
            {
                header = new RespInputHeader { cmd = RespCommand.BITFIELD },
                parseStateStartIdx = 0,
            };

            for (var i = 0; i < secondaryCommandArgs.Count; i++)
            {
                var opCode = (byte)secondaryCommandArgs[i].Item1;
                var opArgs = secondaryCommandArgs[i].Item2;
                ArgSlice[] secParseStateBuffer = default;
                var secParseState = new SessionParseState();
                secParseState.Initialize(ref secParseStateBuffer,
                    opArgs.Length + (isOverflowTypeSet ? 1 : 0));

                for (var j = 0; j < opArgs.Length; j++)
                {
                    secParseStateBuffer[j] = opArgs[j];
                }

                if (isOverflowTypeSet)
                {
                    secParseStateBuffer[^1] = overflowTypeSlice;
                }

                inputHeader.parseState = secParseState;

                var output = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
                var status = storageApi.StringBitField(ref sbKey, ref inputHeader, opCode,
                    ref output);

                if (status == GarnetStatus.NOTFOUND && opCode == (byte)RespCommand.GET)
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
        private bool StringBitFieldReadOnly<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // BITFIELD_RO key [GET encoding offset [GET encoding offset] ... ]
            return StringBitField(ref storageApi, true);
        }
    }
}