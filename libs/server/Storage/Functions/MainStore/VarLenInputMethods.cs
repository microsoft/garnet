// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, RawStringInput, SpanByteAndMemory, long>
    {
        /// <summary>
        /// Parse ASCII byte array into long and validate that only contains ASCII decimal characters
        /// </summary>
        /// <param name="length">Length of byte array</param>
        /// <param name="source">Pointer to byte array</param>
        /// <param name="val">Parsed long value</param>
        /// <returns>True if input contained only ASCII decimal characters, otherwise false</returns>
        static bool IsValidNumber(int length, byte* source, out long val)
        {
            try
            {
                // Check for valid number
                return NumUtils.TryReadInt64(length, source, out val);
            }
            catch
            {
                // Signal value is not a valid number
                val = 0;
                return false;
            }
        }

        /// <summary>
        /// Parse ASCII byte array into double and validate that only contains ASCII decimal characters
        /// </summary>
        /// <param name="length">Length of byte array</param>
        /// <param name="source">Pointer to byte array</param>
        /// <param name="val">Parsed long value</param>
        /// <returns>True if input contained only ASCII decimal characters, otherwise false</returns>
        static bool IsValidDouble(int length, byte* source, out double val)
        {
            try
            {
                // Check for valid number
                return NumUtils.TryReadDouble(length, source, out val) || !double.IsFinite(val);
            }
            catch
            {
                // Signal value is not a valid number
                val = 0;
                return false;
            }
        }

        /// <inheritdoc/>
        public RecordFieldInfo GetRMWInitialFieldInfo(SpanByte key, ref RawStringInput input)
        {
            var cmd = input.header.cmd;
            var fieldInfo = new RecordFieldInfo()
            {
                KeyDataSize = key.Length,
                ValueDataSize = 0,
                HasETag = input.header.CheckWithETagFlag()
            };

            switch (cmd)
            {
                case RespCommand.SETBIT:
                    var bOffset = input.arg1;
                    fieldInfo.ValueDataSize = BitmapManager.Length(bOffset);
                    return fieldInfo;
                case RespCommand.BITFIELD:
                case RespCommand.BITFIELD_RO:
                    var bitFieldArgs = GetBitFieldArguments(ref input);
                    fieldInfo.ValueDataSize = BitmapManager.LengthFromType(bitFieldArgs);
                    return fieldInfo;
                case RespCommand.PFADD:
                    fieldInfo.ValueDataSize = HyperLogLog.DefaultHLL.SparseInitialLength(ref input);
                    return fieldInfo;
                case RespCommand.PFMERGE:
                    fieldInfo.ValueDataSize = input.parseState.GetArgSliceByRef(0).SpanByte.Length;
                    return fieldInfo;

                case RespCommand.SET:
                case RespCommand.SETEXNX:
                    fieldInfo.ValueDataSize = input.parseState.GetArgSliceByRef(0).SpanByte.Length;
                    fieldInfo.HasExpiration = input.arg1 != 0;
                    return fieldInfo;

                case RespCommand.SETKEEPTTL:
                    // Copy input to value; do not change expiration
                    fieldInfo.ValueDataSize = input.parseState.GetArgSliceByRef(0).SpanByte.Length;
                    return fieldInfo;
                case RespCommand.SETRANGE:
                    var offset = input.parseState.GetInt(0);
                    var newValue = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;
                    fieldInfo.ValueDataSize = newValue.Length + offset;
                    return fieldInfo;

                case RespCommand.APPEND:
                    var valueLength = input.parseState.GetArgSliceByRef(0).Length;
                    fieldInfo.ValueDataSize = valueLength;
                    return fieldInfo;

                case RespCommand.INCR:
                    fieldInfo.ValueDataSize = 1; // # of digits in "1"
                    return fieldInfo;

                case RespCommand.DECR:
                    fieldInfo.ValueDataSize = 2; // # of digits in "-1"
                    return fieldInfo;

                case RespCommand.INCRBY:
                    var ndigits = NumUtils.CountDigits(input.arg1, out var isNegative);

                    fieldInfo.ValueDataSize = ndigits + (isNegative ? 1 : 0);
                    return fieldInfo;

                case RespCommand.DECRBY:
                    ndigits = NumUtils.CountDigits(-input.arg1, out isNegative);

                    fieldInfo.ValueDataSize = ndigits + (isNegative ? 1 : 0);
                    return fieldInfo;

                case RespCommand.INCRBYFLOAT:
                    fieldInfo.ValueDataSize = input.parseState.TryGetDouble(0, out var incrByFloat)
                        ? NumUtils.CountCharsInDouble(incrByFloat, out var _, out var _, out var _)
                        : sizeof(int);
                    return fieldInfo;

                default:
                    if (cmd > RespCommandExtensions.LastValidCommand)
                    {
                        var functions = functionsState.GetCustomCommandFunctions((ushort)cmd);
                        fieldInfo.ValueDataSize = functions.GetInitialLength(ref input);
                    }
                    else
                        fieldInfo.ValueDataSize = input.parseState.GetArgSliceByRef(0).ReadOnlySpan.Length;
                    fieldInfo.HasETag = input.header.CheckWithETagFlag();
                    fieldInfo.HasExpiration = input.arg1 != 0;
                    return fieldInfo;
            }
        }

        /// <inheritdoc/>
        public RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref RawStringInput input)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
        {
            var fieldInfo = new RecordFieldInfo()
            {
                KeyDataSize = srcLogRecord.Key.Length,
                ValueDataSize = 0,
                HasETag = input.header.CheckWithETagFlag(),
                HasExpiration = srcLogRecord.Info.HasExpiration
            };

            if (input.header.cmd != RespCommand.NONE)
            {
                var cmd = input.header.cmd;

                switch (cmd)
                {
                    case RespCommand.INCR:
                    case RespCommand.INCRBY:
                        var incrByValue = input.header.cmd == RespCommand.INCRBY ? input.arg1 : 1;

                        var curr = NumUtils.ReadInt64(srcLogRecord.ValueSpan.AsSpan());
                        var next = curr + incrByValue;

                        fieldInfo.ValueDataSize = NumUtils.CountDigits(next, out var isNegative) + (isNegative ? 1 : 0);
                        return fieldInfo;

                    case RespCommand.DECR:
                    case RespCommand.DECRBY:
                        var decrByValue = input.header.cmd == RespCommand.DECRBY ? input.arg1 : 1;

                        curr = NumUtils.ReadInt64(srcLogRecord.ValueSpan.AsSpan());
                        next = curr - decrByValue;

                        fieldInfo.ValueDataSize = NumUtils.CountDigits(next, out isNegative) + (isNegative ? 1 : 0);
                        return fieldInfo;
                    case RespCommand.INCRBYFLOAT:
                        // We don't need to TryGetDouble here because InPlaceUpdater will raise an error before we reach this point
                        var incrByFloat = input.parseState.GetDouble(0);

                        _ = NumUtils.TryReadDouble(srcLogRecord.ValueSpan.AsSpan(), out var currVal);
                        var nextVal = currVal + incrByFloat;

                        fieldInfo.ValueDataSize = NumUtils.CountCharsInDouble(nextVal, out _, out _, out _);
                        return fieldInfo;

                    case RespCommand.SETBIT:
                        var bOffset = input.arg1;
                        fieldInfo.ValueDataSize = BitmapManager.NewBlockAllocLength(srcLogRecord.ValueSpan.Length, bOffset);
                        return fieldInfo;

                    case RespCommand.BITFIELD:
                    case RespCommand.BITFIELD_RO:
                        var bitFieldArgs = GetBitFieldArguments(ref input);
                        fieldInfo.ValueDataSize = BitmapManager.NewBlockAllocLengthFromType(bitFieldArgs, srcLogRecord.ValueSpan.Length);
                        return fieldInfo;

                    case RespCommand.PFADD:
                        fieldInfo.ValueDataSize = HyperLogLog.DefaultHLL.UpdateGrow(ref input, srcLogRecord.ValueSpan.ToPointer());
                        return fieldInfo;

                    case RespCommand.PFMERGE:
                        var srcHLL = input.parseState.GetArgSliceByRef(0).SpanByte.ToPointer();
                        var dstHLL = srcLogRecord.ValueSpan.ToPointer();
                        fieldInfo.ValueDataSize = HyperLogLog.DefaultHLL.MergeGrow(srcHLL, dstHLL);
                        return fieldInfo;

                    case RespCommand.SETKEEPTTLXX:
                    case RespCommand.SETKEEPTTL:
                        fieldInfo.ValueDataSize = input.parseState.GetArgSliceByRef(0).Length;
                        return fieldInfo;

                    case RespCommand.SET:
                    case RespCommand.SETEXXX:
                    case RespCommand.SETEXNX:
                        fieldInfo.ValueDataSize = input.parseState.GetArgSliceByRef(0).Length;
                        return fieldInfo;

                    case RespCommand.PERSIST:
                        fieldInfo.HasExpiration = false;
                        fieldInfo.ValueDataSize = srcLogRecord.ValueSpan.Length;
                        return fieldInfo;

                    case RespCommand.SETIFGREATER:
                    case RespCommand.SETIFMATCH:
                        fieldInfo.ValueDataSize = input.parseState.GetArgSliceByRef(0).ReadOnlySpan.Length;
                        fieldInfo.HasETag = true;
                        return fieldInfo;

                    case RespCommand.EXPIRE:
                    case RespCommand.PEXPIRE:
                    case RespCommand.EXPIREAT:
                    case RespCommand.PEXPIREAT:
                        fieldInfo.HasExpiration = true;
                        fieldInfo.ValueDataSize = srcLogRecord.ValueSpan.Length;
                        return fieldInfo;

                    case RespCommand.SETRANGE:
                        var offset = input.parseState.GetInt(0);
                        var newValue = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

                        var newValueSize = newValue.Length + offset;
                        fieldInfo.ValueDataSize = (newValueSize > srcLogRecord.ValueSpan.Length)
                            ? newValueSize + offset
                            : srcLogRecord.ValueSpan.Length;
                        return fieldInfo;

                    case RespCommand.GETDEL:
                        // No additional allocation needed.
                        break;

                    case RespCommand.GETEX:
                        fieldInfo.ValueDataSize = srcLogRecord.ValueSpan.Length;

                        // If both EX and PERSIST were specified, EX wins
                        if (input.arg1 > 0)
                            fieldInfo.HasExpiration = true;
                        else if (input.parseState.Count > 0)
                        {
                            if (input.parseState.GetArgSliceByRef(0).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.PERSIST))
                                fieldInfo.HasExpiration = false;
                        }

                        return fieldInfo;

                    case RespCommand.APPEND:
                        fieldInfo.ValueDataSize = srcLogRecord.ValueSpan.Length + input.parseState.GetArgSliceByRef(0).Length;
                        return fieldInfo;

                    default:
                        if (cmd > RespCommandExtensions.LastValidCommand)
                        {
                            var functions = functionsState.GetCustomCommandFunctions((ushort)cmd);
                            fieldInfo.ValueDataSize = functions.GetLength(srcLogRecord.ValueSpan.AsReadOnlySpan(), ref input);
                            fieldInfo.HasExpiration = input.arg1 != 0;
                            return fieldInfo;
                        }
                        throw new GarnetException("Unsupported operation on input");
                }
            }

            fieldInfo.ValueDataSize = input.parseState.GetArgSliceByRef(0).Length;
            fieldInfo.HasExpiration = input.arg1 != 0;
            return fieldInfo;
        }

        public RecordFieldInfo GetUpsertFieldInfo(SpanByte key, SpanByte value, ref RawStringInput input)
        {
            var fieldInfo = new RecordFieldInfo()
            {
                KeyDataSize = key.Length,
                ValueDataSize = value.Length,
                HasETag = input.header.CheckWithETagFlag()
            };

            switch (input.header.cmd)
            {
                case RespCommand.SET:
                case RespCommand.SETEX:
                case RespCommand.APPEND:
                    fieldInfo.HasExpiration = input.arg1 != 0;
                    break;
            }
            return fieldInfo;
        }
    }
}