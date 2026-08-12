// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
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
            if (!parseState.TryGetLong(1, out var offset) || (offset < 0) || !BitmapManager.IsValidBitOffset(offset))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
            }

            // Validate value
            var bSetValSlice = parseState.GetArgSliceByRef(2).ReadOnlySpan;
            if (bSetValSlice.Length != 1 || (bSetValSlice[0] != '0' && bSetValSlice[0] != '1'))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_BIT_IS_NOT_INTEGER);
            }

            var input = new StringInput(RespCommand.SETBIT, ref parseState, startIdx: 1, arg1: offset);

            var output = GetStringOutput();
            var status = storageApi.StringSetBit(key, ref input, ref output);

            if (status == GarnetStatus.OK)
                dcurr += output.SpanByteAndMemory.Length;
            else if (status == GarnetStatus.WRONGTYPE)
                WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);

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
            if (!parseState.TryGetLong(1, out var offset) || (offset < 0) || !BitmapManager.IsValidBitOffset(offset))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
            }

            var input = new StringInput(RespCommand.GETBIT, ref parseState, startIdx: 1, arg1: offset);

            var output = GetStringOutput();
            var status = storageApi.StringGetBit(key, ref input, ref output);

            if (status == GarnetStatus.NOTFOUND)
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                    SendAndReset();
            else if (status == GarnetStatus.WRONGTYPE)
                WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
            else
                dcurr += output.SpanByteAndMemory.Length;

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
            if (count is not 1 and not 3 and not 4)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.BITCOUNT));
            }

            // <[Get Key]>
            var key = parseState.GetArgSliceByRef(0);

            // Extract parameters in command order:
            // start, end, [BIT|BYTE]
            var useBitIndex = true;
            if (count > 1)
            {
                if (!parseState.TryGetLong(1, out _) || !parseState.TryGetLong(2, out _))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
                }

                if (count > 3)
                {
                    var spanOffsetType = parseState.GetArgSliceByRef(3).ReadOnlySpan;
                    if (spanOffsetType.EqualsUpperCaseSpanIgnoringCase("BIT"u8))
                        useBitIndex = true;
                    else if (spanOffsetType.EqualsUpperCaseSpanIgnoringCase("BYTE"u8))
                        useBitIndex = false;
                    else
                    {
                        return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                    }
                }
            }

            var input = new StringInput(RespCommand.BITCOUNT, ref parseState, startIdx: 1, arg1: useBitIndex ? 1 : 0);

            var output = GetStringOutput();

            var status = storageApi.StringBitCount(key, ref input, ref output);
            if (status == GarnetStatus.OK)
            {
                ProcessOutput(output.SpanByteAndMemory);
            }
            else if (status == GarnetStatus.NOTFOUND)
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                    SendAndReset();
            }
            else if (status == GarnetStatus.WRONGTYPE)
            {
                WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
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
            if (count is < 2 or > 5)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.BITPOS));
            }

            // <[Get Key]>
            var key = parseState.GetArgSliceByRef(0);

            // Validate value
            var bSetValSlice = parseState.GetArgSliceByRef(1).ReadOnlySpan;
            if (bSetValSlice.Length != 1 || (bSetValSlice[0] != '0' && bSetValSlice[0] != '1'))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_BIT_IS_NOT_INTEGER);
            }

            long startOffset = 0;
            long endOffset = -1;
            byte offsetType = 0x0;
            var hasStartOffset = false;
            var hasEndOffset = false;

            // Extract parameters in command order, consistent with PrivateMethods BITPOS decoding:
            // bit, start, end, [BIT|BYTE]
            if (count > 2)
            {
                // start
                if (!parseState.TryGetLong(2, out startOffset))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
                }
                hasStartOffset = true;

                if (count > 3)
                {
                    // end
                    if (!parseState.TryGetLong(3, out endOffset))
                    {
                        return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
                    }
                    hasEndOffset = true;

                    if (count > 4)
                    {
                        // Optional index type (default BYTE)
                        var sbOffsetType = parseState.GetArgSliceByRef(4).ReadOnlySpan;
                        if (sbOffsetType.EqualsUpperCaseSpanIgnoringCase("BIT"u8))
                        {
                            offsetType = 0x1;
                        }
                        else if (!sbOffsetType.EqualsUpperCaseSpanIgnoringCase("BYTE"u8))
                        {
                            return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                        }
                    }
                }
            }

            if (BitmapManager.TryValidateBitPosOffsets(startOffset, endOffset, offsetType, hasStartOffset, hasEndOffset))
            {
                while (!RespWriteUtils.TryWriteInt64(-1, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var input = new StringInput(RespCommand.BITPOS, ref parseState, startIdx: 1);

            var output = GetStringOutput();

            var status = storageApi.StringBitPosition(key, ref input, ref output);

            if (status == GarnetStatus.OK)
            {
                ProcessOutput(output.SpanByteAndMemory);
            }
            else if (status == GarnetStatus.WRONGTYPE)
            {
                WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
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

            // NOT is unary: exactly one source key (parseState is [destkey, srckey]).
            // Extra source keys otherwise produce an undefined result instead of an error.
            if (bitOp == BitmapOperation.NOT && parseState.Count > 2)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_BITOP_NOT_SINGLE_SOURCE_KEY);
            }

            if (parseState.Count > 64)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_BITOP_KEY_LIMIT);
            }

            var input = new StringInput(RespCommand.BITOP, ref parseState);

            var res = storageApi.StringBitOperation(ref input, bitOp, out var result);

            if (res == GarnetStatus.WRONGTYPE)
            {
                WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
            }
            else
            {
                while (!RespWriteUtils.TryWriteInt64(result, ref dcurr, dend))
                    SendAndReset();
            }

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

            var hasWriteSubCommands = false;
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
                if ((currTokenIdx >= parseState.Count) || !parseState.TryGetBitfieldEncoding(currTokenIdx, out var bitCount, out _))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_INVALID_BITFIELD_TYPE);
                }
                var encodingSlice = parseState.GetArgSliceByRef(currTokenIdx++);

                // Process offset argument
                if ((currTokenIdx >= parseState.Count) || !parseState.TryGetBitfieldOffset(currTokenIdx, out var offset, out var multiplyOffset))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
                }

                if (!BitmapManager.TryValidateBitfieldOffset(offset, (byte)bitCount, multiplyOffset, out _, out _))
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

                    // SET and INCRBY modify the value stored
                    hasWriteSubCommands = true;

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
                                     secondaryCommandArgs, hasWriteSubCommands, isOverflowTypeSet, overflowTypeSlice);
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
                if ((currTokenIdx >= parseState.Count) || !parseState.TryGetBitfieldEncoding(currTokenIdx, out var bitCount, out _))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_INVALID_BITFIELD_TYPE);
                }
                var encodingSlice = parseState.GetArgSliceByRef(currTokenIdx++);

                // Process offset argument
                if ((currTokenIdx >= parseState.Count) || !parseState.TryGetBitfieldOffset(currTokenIdx, out var offset, out var multiplyOffset))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
                }

                if (!BitmapManager.TryValidateBitfieldOffset(offset, (byte)bitCount, multiplyOffset, out _, out _))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
                }

                var offsetSlice = parseState.GetArgSliceByRef(currTokenIdx++);

                secondaryCommandArgs.Add((RespCommand.GET, [commandSlice, encodingSlice, offsetSlice]));
            }

            return StringBitFieldAction(ref storageApi, key, RespCommand.BITFIELD_RO, secondaryCommandArgs, hasWriteCommands: false);
        }

        private bool StringBitFieldAction<TGarnetApi>(ref TGarnetApi storageApi,
                                                      PinnedSpanByte sbKey,
                                                      RespCommand cmd,
                                                      SecondaryCommandList secondaryCommandArgs,
                                                      bool hasWriteCommands,
                                                      bool isOverflowTypeSet = false,
                                                      PinnedSpanByte overflowTypeSlice = default)
            where TGarnetApi : IGarnetApi
        {
            var input = new StringInput(cmd);

            var startDCurr = dcurr;

            var arrayLengthWritten = RespWriteUtils.TryWriteArrayLength(secondaryCommandArgs.Count, ref dcurr, dend);

            if (secondaryCommandArgs.Count == 1)
            {
                // Special case, this is a single command so we never need to use a transaction
                _ = HandleFirstSubCommand(this, secondaryCommandArgs, sbKey, isOverflowTypeSet, overflowTypeSlice, ref storageApi, startDCurr, arrayLengthWritten, ref input);
                return true;
            }
            else
            {
                // For multiple commands, we need to be in a transaction so the type doesn't change under us
                using (txnManager.PromoteToTransaction(TransactionStoreTypes.Main, sbKey, hasWriteCommands ? LockType.Exclusive : LockType.Shared))
                {
                    // First sub command has to deal with WRONGTYPE and array length
                    if (HandleFirstSubCommand(this, secondaryCommandArgs, sbKey, isOverflowTypeSet, overflowTypeSlice, ref transactionalGarnetApi, startDCurr, arrayLengthWritten, ref input))
                    {
                        // WRONGTYPE, nothing left to do
                        return true;
                    }

                    // Remainder don't have to worry about array length not bein present, nor WRONGTYPE responses
                    for (var i = 1; i < secondaryCommandArgs.Count; i++)
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

                        var output = GetStringOutput();
                        var status = transactionalGarnetApi.StringBitField(sbKey, ref input, opCode,
                            ref output);

                        if (status == GarnetStatus.NOTFOUND && opCode == RespCommand.GET)
                        {
                            while (!RespWriteUtils.TryWriteInt32(0, ref dcurr, dend))
                                SendAndReset();
                        }
                        else
                        {
                            ProcessOutput(output.SpanByteAndMemory);
                        }
                    }
                }
            }

            return true;

            // Handle first subcommand in a BITFIELD*; this is special because we _might_ not have written array length yet, and we have to check for WRONGTYPE
            //
            // Returns true if we error'd out and the whole command response was written
            // Returns false if more work is left to be done
            static bool HandleFirstSubCommand<TSubGarnetApi>(RespServerSession self, SecondaryCommandList secondaryCommandArgs, PinnedSpanByte sbKey, bool isOverflowTypeSet, PinnedSpanByte overflowTypeSlice, ref TSubGarnetApi storageApi, byte* startDCurr, bool arrayLengthWritten, ref StringInput input)
                where TSubGarnetApi : IGarnetApi
            {
                var opCode = secondaryCommandArgs[0].Item1;
                var opArgs = secondaryCommandArgs[0].Item2;
                self.parseState.Initialize(opArgs.Length + (isOverflowTypeSet ? 1 : 0));

                for (var j = 0; j < opArgs.Length; j++)
                {
                    self.parseState.SetArgument(j, opArgs[j]);
                }

                if (isOverflowTypeSet)
                {
                    self.parseState.SetArgument(opArgs.Length, overflowTypeSlice);
                }

                input.parseState = self.parseState;

                var output = self.GetStringOutput();
                var status = storageApi.StringBitField(sbKey, ref input, opCode, ref output);

                // WRONGTYPE needs to overwrite the array length, if we wrote it
                if (status == GarnetStatus.WRONGTYPE)
                {
                    self.dcurr = startDCurr;
                    self.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
                    return true;
                }
                else
                {
                    // Most of the time the array length will have fit, but we have to handle the case where it wasn't
                    if (!arrayLengthWritten)
                    {
                        // If we have any result written inline, copy it out before we write the array length
                        if (output.SpanByteAndMemory.IsSpanByte && output.SpanByteAndMemory.Length > 0)
                        {
                            var mem = MemoryPool<byte>.Shared.Rent(output.SpanByteAndMemory.Length);
                            output.SpanByteAndMemory.ReadOnlySpan.CopyTo(mem.Memory.Span);

                            output.SpanByteAndMemory = new(mem, output.SpanByteAndMemory.Length);
                        }

                        self.WriteArrayLength(secondaryCommandArgs.Count);
                    }

                    // Handle result
                    if (status == GarnetStatus.NOTFOUND && opCode == RespCommand.GET)
                    {
                        self.WriteInt32(0);
                    }
                    else
                    {
                        self.ProcessOutput(output.SpanByteAndMemory);
                    }
                }

                return false;
            }
        }
    }
}