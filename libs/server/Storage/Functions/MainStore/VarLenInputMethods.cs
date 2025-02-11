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
                return NumUtils.TryBytesToLong(length, source, out val);
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
                return NumUtils.TryBytesToDouble(length, source, out val) || !double.IsFinite(val);
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
                KeySize = key.TotalSize,
                ValueSize = SpanField.FieldLengthPrefixSize
            };

            switch (cmd)
            {
                case RespCommand.SETBIT:
                    var bOffset = input.parseState.GetLong(0);
                    fieldInfo.ValueSize += BitmapManager.Length(bOffset);
                    return fieldInfo;
                case RespCommand.BITFIELD:
                    var bitFieldArgs = GetBitFieldArguments(ref input);
                    fieldInfo.ValueSize += BitmapManager.LengthFromType(bitFieldArgs);
                    return fieldInfo;
                case RespCommand.PFADD:
                    fieldInfo.ValueSize += HyperLogLog.DefaultHLL.SparseInitialLength(ref input);
                    return fieldInfo;
                case RespCommand.PFMERGE:
                    fieldInfo.ValueSize += input.parseState.GetArgSliceByRef(0).SpanByte.Length;
                    return fieldInfo;

                case RespCommand.SET:
                case RespCommand.SETEXNX:
                    fieldInfo.ValueSize += input.parseState.GetArgSliceByRef(0).SpanByte.Length;
                    fieldInfo.HasExpiration = input.arg1 != 0;
                    return fieldInfo;

                case RespCommand.SETKEEPTTL:
                    // Copy input to value; do not change expiration
                    fieldInfo.ValueSize += input.parseState.GetArgSliceByRef(0).SpanByte.Length;
                    return fieldInfo;
                case RespCommand.SETRANGE:
                    var offset = input.parseState.GetInt(0);
                    var newValue = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;
                    fieldInfo.ValueSize += newValue.Length + offset;
                    return fieldInfo;

                case RespCommand.APPEND:
                    var valueLength = input.parseState.GetArgSliceByRef(0).Length;
                    fieldInfo.ValueSize += valueLength;
                    return fieldInfo;

                case RespCommand.INCR:
                    fieldInfo.ValueSize += 1; // # of digits in "1"
                    return fieldInfo;

                case RespCommand.DECR:
                    fieldInfo.ValueSize += 2; // # of digits in "-1"
                    return fieldInfo;

                case RespCommand.INCRBY:
                    var fNeg = false;
                    var ndigits = NumUtils.NumDigitsInLong(input.arg1, ref fNeg);

                    fieldInfo.ValueSize += ndigits + (fNeg ? 1 : 0);
                    return fieldInfo;

                case RespCommand.DECRBY:
                    fNeg = false;
                    ndigits = NumUtils.NumDigitsInLong(-input.arg1, ref fNeg);

                    fieldInfo.ValueSize += ndigits + (fNeg ? 1 : 0);
                    return fieldInfo;

                case RespCommand.INCRBYFLOAT:
                    if (input.parseState.TryGetDouble(0, out var incrByFloat))
                        fieldInfo.ValueSize += NumUtils.NumOfCharInDouble(incrByFloat, out var _, out var _, out var _);
                    return fieldInfo;

                default:
                    if ((ushort)cmd >= CustomCommandManager.StartOffset)
                    {
                        var functions = functionsState.customCommands[(ushort)cmd - CustomCommandManager.StartOffset].functions;
                        fieldInfo.ValueSize += functions.GetInitialLength(ref input);
                    }
                    else
                        fieldInfo.ValueSize += input.parseState.GetArgSliceByRef(0).ReadOnlySpan.Length;
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
                KeySize = srcLogRecord.Key.TotalSize,
                ValueSize = SpanField.FieldLengthPrefixSize,
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

                        var curr = NumUtils.BytesToLong(srcLogRecord.ValueSpan.AsSpan());
                        var next = curr + incrByValue;

                        var fNeg = false;
                        fieldInfo.ValueSize += NumUtils.NumDigitsInLong(next, ref fNeg) + (fNeg ? 1 : 0);
                        return fieldInfo;

                    case RespCommand.DECR:
                    case RespCommand.DECRBY:
                        var decrByValue = input.header.cmd == RespCommand.DECRBY ? input.arg1 : 1;

                        curr = NumUtils.BytesToLong(srcLogRecord.ValueSpan.AsSpan());
                        next = curr - decrByValue;

                        fNeg = false;

                        fieldInfo.ValueSize += NumUtils.NumDigitsInLong(next, ref fNeg) + (fNeg ? 1 : 0);
                        return fieldInfo;

                    case RespCommand.INCRBYFLOAT:
                        // We don't need to TryGetDouble here because InPlaceUpdater will raise an error before we reach this point
                        var incrByFloat = input.parseState.GetDouble(0);

                        _ = NumUtils.TryBytesToDouble(srcLogRecord.ValueSpan.AsSpan(), out var currVal);
                        var nextVal = currVal + incrByFloat;

                        fieldInfo.ValueSize += NumUtils.NumOfCharInDouble(nextVal, out _, out _, out _);
                        return fieldInfo;

                    case RespCommand.SETBIT:
                        var bOffset = input.parseState.GetLong(0);
                        fieldInfo.ValueSize += BitmapManager.NewBlockAllocLength(srcLogRecord.ValueSpan.Length, bOffset);
                        return fieldInfo;

                    case RespCommand.BITFIELD:
                        var bitFieldArgs = GetBitFieldArguments(ref input);
                        fieldInfo.ValueSize += BitmapManager.NewBlockAllocLengthFromType(bitFieldArgs, srcLogRecord.ValueSpan.Length);
                        return fieldInfo;

                    case RespCommand.PFADD:
                        fieldInfo.ValueSize += HyperLogLog.DefaultHLL.UpdateGrow(ref input, srcLogRecord.ValueSpan.ToPointer());
                        return fieldInfo;

                    case RespCommand.PFMERGE:
                        var srcHLL = input.parseState.GetArgSliceByRef(0).SpanByte.ToPointer();
                        var dstHLL = srcLogRecord.ValueSpan.ToPointer();
                        fieldInfo.ValueSize += HyperLogLog.DefaultHLL.MergeGrow(srcHLL, dstHLL);
                        return fieldInfo;

                    case RespCommand.SETKEEPTTLXX:
                    case RespCommand.SETKEEPTTL:
                        fieldInfo.ValueSize += input.parseState.GetArgSliceByRef(0).Length;
                        return fieldInfo;

                    case RespCommand.SET:
                    case RespCommand.SETEXXX:
                    case RespCommand.SETEXNX:
                        break;

                    case RespCommand.PERSIST:
                        fieldInfo.HasExpiration = false;
                        fieldInfo.ValueSize += srcLogRecord.ValueSpan.Length;
                        return fieldInfo;

                    case RespCommand.EXPIRE:
                    case RespCommand.PEXPIRE:
                    case RespCommand.EXPIREAT:
                    case RespCommand.PEXPIREAT:
                        fieldInfo.HasExpiration = true;
                        fieldInfo.ValueSize += srcLogRecord.ValueSpan.Length;
                        return fieldInfo;

                    case RespCommand.SETRANGE:
                        var offset = input.parseState.GetInt(0);
                        var newValue = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

                        fieldInfo.ValueSize += (newValue.Length + offset > srcLogRecord.ValueSpan.Length)
                            ? newValue.Length + offset
                            : srcLogRecord.ValueSpan.Length;
                        return fieldInfo;

                    case RespCommand.GETDEL:
                        // No additional allocation needed.
                        break;

                    case RespCommand.GETEX:
                        fieldInfo.ValueSize += srcLogRecord.ValueSpan.Length;

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
                        fieldInfo.ValueSize += srcLogRecord.ValueSpan.Length + input.parseState.GetArgSliceByRef(0).Length;
                        return fieldInfo;

                    default:
                        if ((ushort)cmd >= CustomCommandManager.StartOffset)
                        {
                            var functions = functionsState.customCommands[(ushort)cmd - CustomCommandManager.StartOffset].functions;
                            fieldInfo.ValueSize = functions.GetLength(srcLogRecord.ValueSpan.AsReadOnlySpan(), ref input);
                            fieldInfo.HasExpiration = input.arg1 != 0;
                            return fieldInfo;
                        }
                        throw new GarnetException("Unsupported operation on input");
                }
            }

            fieldInfo.ValueSize += input.parseState.GetArgSliceByRef(0).Length;
            fieldInfo.HasExpiration = input.arg1 != 0;
            return fieldInfo;
        }

        public RecordFieldInfo GetUpsertFieldInfo(SpanByte key, SpanByte value, ref RawStringInput input)
        {
            var fieldInfo = new RecordFieldInfo()
            {
                KeySize = key.TotalSize,
                ValueSize = value.TotalSize
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