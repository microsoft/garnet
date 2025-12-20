// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    using SecondaryCommandList = List<(RespCommand, PinnedSpanByte[])>;

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
        NOT,

        /// <summary>
        /// DIFF
        /// </summary>
        DIFF
    }

    internal enum BitFieldOverflow : byte
    {
        // IMPORTANT: Any changes to the values of this enum should be reflected in its parser (SessionParseStateExtensions.TryGetBitFieldOverflow)

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
        public RespCommand secondaryCommand;

        /// <summary>
        /// encoding info
        /// </summary>
        [FieldOffset(sizeof(RespCommand))]
        public byte typeInfo;

        /// <summary>
        /// offset
        /// </summary>
        [FieldOffset(sizeof(RespCommand) + sizeof(byte))]
        public long offset;

        /// <summary>
        /// value
        /// </summary>
        [FieldOffset(sizeof(RespCommand) + sizeof(byte) + sizeof(long))]
        public long value;

        /// <summary>
        /// BitFieldOverflow enum 
        /// </summary>
        [FieldOffset(sizeof(RespCommand) + sizeof(byte) + (2 * sizeof(long)))]
        public byte overflowType;

        /// <summary>
        /// add a command to execute in bitfield
        /// </summary>
        /// <param name="secondaryCommand"></param>
        /// <param name="typeInfo"></param>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        /// <param name="overflowType"></param>
        public BitFieldCmdArgs(RespCommand secondaryCommand, byte typeInfo, long offset, long value, byte overflowType)
        {
            this.secondaryCommand = secondaryCommand;
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

            var key = parseState.GetArgSliceByRef(0);

            // Validate offset
            if (!parseState.TryGetLong(1, out var offset) || (offset < 0))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
            }

            // Validate value
            var bSetValSlice = parseState.GetArgSliceByRef(2).ReadOnlySpan;
            if (bSetValSlice.Length != 1 || (bSetValSlice[0] != '0' && bSetValSlice[0] != '1'))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_BIT_IS_NOT_INTEGER);
            }

            var input = new StringInput(RespCommand.SETBIT, metaCommand, ref parseState, startIdx: 1, arg1: offset);

            var o = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr));
            var status = storageApi.StringSetBit(key, ref input, ref o);

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

            var key = parseState.GetArgSliceByRef(0);

            // Validate offset
            if (!parseState.TryGetLong(1, out var offset) || (offset < 0))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
            }

            var input = new StringInput(RespCommand.GETBIT, metaCommand, ref parseState, startIdx: 1, arg1: offset);

            var o = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr));
            var status = storageApi.StringGetBit(key, ref input, ref o);

            if (status == GarnetStatus.NOTFOUND)
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
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
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.BITCOUNT));

            // <[Get Key]>
            var key = parseState.GetArgSliceByRef(0);

            // Validate start & end offsets, if exist
            if (parseState.Count > 1)
            {
                if (!parseState.TryGetInt(1, out _) || (parseState.Count > 2 && !parseState.TryGetInt(2, out _)))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
                }
            }

            var input = new StringInput(RespCommand.BITCOUNT, metaCommand, ref parseState, startIdx: 1);

            var o = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.StringBitCount(key, ref input, ref o);

            if (status == GarnetStatus.OK)
            {
                if (!o.IsSpanByte)
                    SendAndReset(o.Memory, o.Length);
                else
                    dcurr += o.Length;
            }
            else if (status == GarnetStatus.NOTFOUND)
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
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
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.BITPOS));

            // <[Get Key]>
            var key = parseState.GetArgSliceByRef(0);

            // Validate value
            var bSetValSlice = parseState.GetArgSliceByRef(1).ReadOnlySpan;
            if (bSetValSlice.Length != 1 || (bSetValSlice[0] != '0' && bSetValSlice[0] != '1'))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_BIT_IS_NOT_INTEGER);
            }

            // Validate start & end offsets, if exist
            if (parseState.Count > 2)
            {
                if (!parseState.TryGetInt(2, out _) ||
                    (parseState.Count > 3 && !parseState.TryGetInt(3, out _)))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
                }
            }

            // Validate offset range type (BIT / BYTE), if exists
            if (parseState.Count > 4)
            {
                var sbOffsetType = parseState.GetArgSliceByRef(4).ReadOnlySpan;
                if (!sbOffsetType.EqualsUpperCaseSpanIgnoringCase("BIT"u8) &&
                    !sbOffsetType.EqualsUpperCaseSpanIgnoringCase("BYTE"u8))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                }
            }

            var input = new StringInput(RespCommand.BITPOS, metaCommand, ref parseState, startIdx: 1);

            var o = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.StringBitPosition(key, ref input, ref o);

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
                while (!RespWriteUtils.TryWriteDirect(resp, ref dcurr, dend))
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
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS);
            }

            if (bitOp == BitmapOperation.DIFF && parseState.Count < 3)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_BITOP_DIFF_TWO_SOURCE_KEYS_REQUIRED);
            }

            if (parseState.Count > 64)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_BITOP_KEY_LIMIT);
            }

            var input = new StringInput(RespCommand.BITOP, metaCommand, ref parseState);

            _ = storageApi.StringBitOperation(ref input, bitOp, out var result);
            while (!RespWriteUtils.TryWriteInt64(result, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Performs arbitrary bitfield integer operations on strings.
        /// </summary>
        private bool StringBitField<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.BITFIELD));
            }

            // BITFIELD key [GET encoding offset] [SET encoding offset value] [INCRBY encoding offset increment] [OVERFLOW WRAP| SAT | FAIL]
            // Extract Key
            var key = parseState.GetArgSliceByRef(0);

            var isOverflowTypeSet = false;
            PinnedSpanByte overflowTypeSlice = default;
            var secondaryCommandArgs = new SecondaryCommandList();

            var currTokenIdx = 1;
            while (currTokenIdx < parseState.Count)
            {
                // Get subcommand
                var commandSlice = parseState.GetArgSliceByRef(currTokenIdx++);
                var command = commandSlice.ReadOnlySpan;

                // Process overflow command
                if (command.EqualsUpperCaseSpanIgnoringCase("OVERFLOW"u8))
                {
                    // Validate overflow type
                    if (currTokenIdx >= parseState.Count || !parseState.TryGetBitFieldOverflow(currTokenIdx, out _))
                    {
                        return AbortWithErrorMessage(CmdStrings.RESP_ERR_INVALID_OVERFLOW_TYPE);
                    }

                    // Get overflow parameter
                    overflowTypeSlice = parseState.GetArgSliceByRef(currTokenIdx++);
                    isOverflowTypeSet = true;

                    continue;
                }

                // [GET <encoding> <offset>] [SET <encoding> <offset> <value>] [INCRBY <encoding> <offset> <increment>]
                // Process encoding argument
                if ((currTokenIdx >= parseState.Count) || !parseState.TryGetBitfieldEncoding(currTokenIdx, out _, out _))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_INVALID_BITFIELD_TYPE);
                }
                var encodingSlice = parseState.GetArgSliceByRef(currTokenIdx++);

                // Process offset argument
                if ((currTokenIdx >= parseState.Count) || !parseState.TryGetBitfieldOffset(currTokenIdx, out _, out _))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
                }
                var offsetSlice = parseState.GetArgSliceByRef(currTokenIdx++);

                // GET Subcommand takes 2 args, encoding and offset
                if (command.EqualsUpperCaseSpanIgnoringCase(CmdStrings.GET))
                {
                    secondaryCommandArgs.Add((RespCommand.GET, [commandSlice, encodingSlice, offsetSlice]));
                }
                else
                {
                    RespCommand op;
                    // SET and INCRBY take 3 args, encoding, offset, and valueArg
                    if (command.EqualsUpperCaseSpanIgnoringCase(CmdStrings.SET))
                        op = RespCommand.SET;
                    else if (command.EqualsUpperCaseSpanIgnoringCase(CmdStrings.INCRBY))
                        op = RespCommand.INCRBY;
                    else
                    {
                        return AbortWithErrorMessage($"ERR Bitfield command {Encoding.ASCII.GetString(command)} not supported"
                                                    );
                    }

                    // Validate value
                    if (currTokenIdx >= parseState.Count || !parseState.TryGetLong(currTokenIdx, out _))
                    {
                        return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
                    }

                    var valueSlice = parseState.GetArgSliceByRef(currTokenIdx);
                    currTokenIdx++;

                    secondaryCommandArgs.Add((op, [commandSlice, encodingSlice, offsetSlice, valueSlice]));
                }
            }

            return StringBitFieldAction(ref storageApi, key, RespCommand.BITFIELD,
                                     secondaryCommandArgs, isOverflowTypeSet, overflowTypeSlice);
        }

        /// <summary>
        /// Performs arbitrary read-only bitfield integer operations
        /// </summary>
        private bool StringBitFieldReadOnly<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.BITFIELD_RO));
            }

            // BITFIELD_RO key [GET encoding offset [GET encoding offset] ... ]
            // Extract Key
            var key = parseState.GetArgSliceByRef(0);

            var secondaryCommandArgs = new SecondaryCommandList();

            var currTokenIdx = 1;
            while (currTokenIdx < parseState.Count)
            {
                // Get subcommand
                var commandSlice = parseState.GetArgSliceByRef(currTokenIdx++);
                var command = commandSlice.ReadOnlySpan;

                // Read-only variant supports only GET subcommand
                if (!command.EqualsUpperCaseSpanIgnoringCase(CmdStrings.GET))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                }

                // GET Subcommand takes 2 args, encoding and offset

                // Process encoding argument
                if ((currTokenIdx >= parseState.Count) || !parseState.TryGetBitfieldEncoding(currTokenIdx, out _, out _))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_INVALID_BITFIELD_TYPE);
                }
                var encodingSlice = parseState.GetArgSliceByRef(currTokenIdx++);

                // Process offset argument
                if ((currTokenIdx >= parseState.Count) || !parseState.TryGetBitfieldOffset(currTokenIdx, out _, out _))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
                }
                var offsetSlice = parseState.GetArgSliceByRef(currTokenIdx++);

                secondaryCommandArgs.Add((RespCommand.GET, [commandSlice, encodingSlice, offsetSlice]));
            }

            return StringBitFieldAction(ref storageApi, key, RespCommand.BITFIELD_RO, secondaryCommandArgs);
        }

        private bool StringBitFieldAction<TGarnetApi>(ref TGarnetApi storageApi,
                                                      PinnedSpanByte sbKey,
                                                      RespCommand cmd,
                                                      SecondaryCommandList secondaryCommandArgs,
                                                      bool isOverflowTypeSet = false,
                                                      PinnedSpanByte overflowTypeSlice = default)
            where TGarnetApi : IGarnetApi
        {
            while (!RespWriteUtils.TryWriteArrayLength(secondaryCommandArgs.Count, ref dcurr, dend))
                SendAndReset();

            var input = new StringInput(cmd, metaCommand, ref parseState);

            for (var i = 0; i < secondaryCommandArgs.Count; i++)
            {
                var opCode = secondaryCommandArgs[i].Item1;
                var opArgs = secondaryCommandArgs[i].Item2;
                parseState.Initialize(opArgs.Length + (isOverflowTypeSet ? 1 : 0));

                for (var j = 0; j < opArgs.Length; j++)
                {
                    parseState.SetArgument(j, opArgs[j]);
                }

                if (isOverflowTypeSet)
                {
                    parseState.SetArgument(opArgs.Length, overflowTypeSlice);
                }

                input.parseState = parseState;

                var output = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr));
                var status = storageApi.StringBitField(sbKey, ref input, opCode,
                    ref output);

                if (status == GarnetStatus.NOTFOUND && opCode == RespCommand.GET)
                {
                    while (!RespWriteUtils.TryWriteInt32(0, ref dcurr, dend))
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