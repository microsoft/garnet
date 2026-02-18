// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<StringInput, StringOutput, long>
    {
        /// <summary>
        /// Parse ASCII byte array into long and validate that only contains ASCII decimal characters
        /// </summary>
        /// <param name="source">Source string to evaluate</param>
        /// <param name="val">Parsed long value</param>
        /// <returns>True if input contained only ASCII decimal characters, otherwise false</returns>
        static bool IsValidNumber(ReadOnlySpan<byte> source, out long val)
        {
            try
            {
                // Check for valid number
                return NumUtils.TryReadInt64(source, out val);
            }
            catch
            {
                // Signal value is not a valid number
                val = 0;
                return false;
            }
        }

        /// <summary>
        /// Parse ASCII byte array into long and validate that only contains ASCII decimal characters
        /// </summary>
        /// <param name="source">Source string to evaluate</param>
        /// <param name="val">Parsed long value</param>
        /// <returns>True if input contained only ASCII decimal characters, otherwise false</returns>
        static bool IsValidNumber(byte* source, int sourceLen, out long val)
        {
            try
            {
                // Check for valid number
                return NumUtils.TryReadInt64(sourceLen, source, out val);
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
        /// <param name="source">Source string to evaluate</param>
        /// <param name="val">Parsed long value</param>
        /// <returns>True if input contained only ASCII decimal characters, otherwise false</returns>
        static bool IsValidDouble(ReadOnlySpan<byte> source, out double val)
        {
            try
            {
                // Check for valid number
                return NumUtils.TryReadDouble(source, out val) || !double.IsFinite(val);
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
        /// <param name="source">Source string to evaluate</param>
        /// <param name="val">Parsed long value</param>
        /// <returns>True if input contained only ASCII decimal characters, otherwise false</returns>
        static bool IsValidDouble(byte* source, int sourceLen, out double val)
        {
            try
            {
                // Check for valid number
                return NumUtils.TryReadDouble(sourceLen, source, out val) || !double.IsFinite(val);
            }
            catch
            {
                // Signal value is not a valid number
                val = 0;
                return false;
            }
        }

        /// <inheritdoc/>
        public RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref StringInput input)
        {
            var cmd = input.header.cmd;
            var fieldInfo = new RecordFieldInfo()
            {
                KeySize = key.Length,
                ValueSize = 0,
                HasETag = input.header.CheckWithETagFlag()
            };

            switch (cmd)
            {
                case RespCommand.SETBIT:
                    var bOffset = input.arg1;
                    fieldInfo.ValueSize = BitmapManager.Length(bOffset);
                    return fieldInfo;
                case RespCommand.BITFIELD:
                    var bitFieldArgs = GetBitFieldArguments(ref input);
                    fieldInfo.ValueSize = BitmapManager.LengthFromType(bitFieldArgs);
                    return fieldInfo;
                case RespCommand.PFADD:
                    fieldInfo.ValueSize = HyperLogLog.DefaultHLL.SparseInitialLength(ref input);
                    return fieldInfo;
                case RespCommand.PFMERGE:
                    fieldInfo.ValueSize = input.parseState.GetArgSliceByRef(0).Length;
                    return fieldInfo;

                case RespCommand.SETIFGREATER:
                case RespCommand.SETIFMATCH:
                    fieldInfo.ValueSize = input.parseState.GetArgSliceByRef(0).Length;
                    fieldInfo.HasETag = true;
                    fieldInfo.HasExpiration = input.arg1 != 0;
                    return fieldInfo;

                case RespCommand.SET:
                case RespCommand.SETEXNX:
                    fieldInfo.ValueSize = input.parseState.GetArgSliceByRef(0).Length;
                    fieldInfo.HasExpiration = input.arg1 != 0;
                    return fieldInfo;

                case RespCommand.SETKEEPTTL:
                    // Copy input to value; do not change expiration
                    fieldInfo.ValueSize = input.parseState.GetArgSliceByRef(0).Length;
                    return fieldInfo;
                case RespCommand.SETRANGE:
                    var offset = input.parseState.GetInt(0);
                    var newValue = input.parseState.GetArgSliceByRef(1);
                    fieldInfo.ValueSize = newValue.Length + offset;
                    return fieldInfo;

                case RespCommand.APPEND:
                    var valueLength = input.parseState.GetArgSliceByRef(0).Length;
                    fieldInfo.ValueSize = valueLength;
                    return fieldInfo;

                case RespCommand.INCR:
                    fieldInfo.ValueSize = 1; // # of digits in "1"
                    return fieldInfo;

                case RespCommand.DECR:
                    fieldInfo.ValueSize = 2; // # of digits in "-1"
                    return fieldInfo;

                case RespCommand.INCRBY:
                    var ndigits = NumUtils.CountDigits(input.arg1, out var isNegative);

                    fieldInfo.ValueSize = ndigits + (isNegative ? 1 : 0);
                    return fieldInfo;

                case RespCommand.DECRBY:
                    ndigits = NumUtils.CountDigits(-input.arg1, out isNegative);

                    fieldInfo.ValueSize = ndigits + (isNegative ? 1 : 0);
                    return fieldInfo;

                case RespCommand.INCRBYFLOAT:
                    var incrByFloat = BitConverter.Int64BitsToDouble(input.arg1);
                    fieldInfo.ValueSize = NumUtils.CountCharsInDouble(incrByFloat, out var _, out var _, out var _);
                    return fieldInfo;

                default:
                    if (cmd > RespCommandExtensions.LastValidCommand)
                    {
                        var functions = functionsState.GetCustomCommandFunctions((ushort)cmd);
                        fieldInfo.ValueSize = functions.GetInitialLength(ref input);
                    }
                    else
                        fieldInfo.ValueSize = input.parseState.GetArgSliceByRef(0).Length;
                    fieldInfo.HasETag = input.header.CheckWithETagFlag();
                    fieldInfo.HasExpiration = input.arg1 != 0;
                    return fieldInfo;
            }
        }

        /// <inheritdoc/>
        public RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref StringInput input)
            where TSourceLogRecord : ISourceLogRecord
        {
            var fieldInfo = new RecordFieldInfo()
            {
                KeySize = srcLogRecord.Key.Length,
                ValueSize = 0,
                HasETag = input.header.CheckWithETagFlag() || srcLogRecord.Info.HasETag,
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

                        var value = srcLogRecord.ValueSpan;
                        fieldInfo.ValueSize = 2; // # of digits in "-1", in case of invalid number (which may throw instead)
                                                 // TODO set error as in PrivateMethods.IsValidNumber and test in caller, to avoid the log record allocation. This would require 'output'
                        if (srcLogRecord.IsPinnedValue ? IsValidNumber(srcLogRecord.PinnedValuePointer, value.Length, out _) : IsValidNumber(value, out _))
                        {
                            // TODO Consider adding a way to cache curr for the IPU call
                            var curr = NumUtils.ReadInt64(value);
                            var next = curr + incrByValue;

                            fieldInfo.ValueSize = NumUtils.CountDigits(next, out var isNegative) + (isNegative ? 1 : 0);
                        }
                        return fieldInfo;

                    case RespCommand.DECR:
                    case RespCommand.DECRBY:
                        var decrByValue = input.header.cmd == RespCommand.DECRBY ? input.arg1 : 1;

                        value = srcLogRecord.ValueSpan;
                        fieldInfo.ValueSize = 2; // # of digits in "-1", in case of invalid number (which may throw instead).
                        if (srcLogRecord.IsPinnedValue ? IsValidNumber(srcLogRecord.PinnedValuePointer, value.Length, out _) : IsValidNumber(value, out _))
                        {
                            var curr = NumUtils.ReadInt64(value);
                            var next = curr - decrByValue;

                            fieldInfo.ValueSize = NumUtils.CountDigits(next, out var isNegative) + (isNegative ? 1 : 0);
                        }
                        return fieldInfo;
                    case RespCommand.INCRBYFLOAT:
                        var incrByFloat = BitConverter.Int64BitsToDouble(input.arg1);

                        value = srcLogRecord.ValueSpan;
                        fieldInfo.ValueSize = 2; // # of digits in "-1", in case of invalid number (which may throw instead)
                        if (srcLogRecord.IsPinnedValue ? IsValidDouble(srcLogRecord.PinnedValuePointer, value.Length, out _) : IsValidDouble(value, out _))
                        {
                            _ = NumUtils.TryReadDouble(srcLogRecord.ValueSpan, out var currVal);
                            var nextVal = currVal + incrByFloat;

                            fieldInfo.ValueSize = NumUtils.CountCharsInDouble(nextVal, out _, out _, out _);
                        }
                        return fieldInfo;

                    case RespCommand.SETBIT:
                        var bOffset = input.arg1;
                        fieldInfo.ValueSize = BitmapManager.NewBlockAllocLength(srcLogRecord.ValueSpan.Length, bOffset);
                        return fieldInfo;

                    case RespCommand.BITFIELD:
                        var bitFieldArgs = GetBitFieldArguments(ref input);
                        fieldInfo.ValueSize = BitmapManager.NewBlockAllocLengthFromType(bitFieldArgs, srcLogRecord.ValueSpan.Length);
                        return fieldInfo;

                    case RespCommand.PFADD:
                        // TODO: call HyperLogLog.DefaultHLL.IsValidHYLL and check error return per RMWMethods. This would require 'output'. Also carry this result through to RMWMethods.
                        if (srcLogRecord.IsPinnedValue)
                            fieldInfo.ValueSize = HyperLogLog.DefaultHLL.UpdateGrow(ref input, srcLogRecord.PinnedValuePointer);
                        else
                            fixed (byte* valuePtr = srcLogRecord.ValueSpan)
                                fieldInfo.ValueSize = HyperLogLog.DefaultHLL.UpdateGrow(ref input, valuePtr);
                        return fieldInfo;

                    case RespCommand.PFMERGE:
                        // TODO: call HyperLogLog.DefaultHLL.IsValidHYLL and check error return per RMWMethods. This would require 'output'. Also carry this result through to RMWMethods.
                        var srcHLL = input.parseState.GetArgSliceByRef(0).ToPointer();
                        if (srcLogRecord.IsPinnedValue)
                            fieldInfo.ValueSize = HyperLogLog.DefaultHLL.MergeGrow(srcHLL, srcLogRecord.PinnedValuePointer);
                        else
                            fixed (byte* dstHLL = srcLogRecord.ValueSpan)
                                fieldInfo.ValueSize = HyperLogLog.DefaultHLL.MergeGrow(srcHLL, dstHLL);
                        return fieldInfo;

                    case RespCommand.SETKEEPTTLXX:
                    case RespCommand.SETKEEPTTL:
                        fieldInfo.ValueSize = input.parseState.GetArgSliceByRef(0).Length;
                        return fieldInfo;

                    case RespCommand.SET:
                    case RespCommand.SETEXXX:
                    case RespCommand.SETEXNX:
                        fieldInfo.ValueSize = input.parseState.GetArgSliceByRef(0).Length;
                        fieldInfo.HasExpiration = input.arg1 != 0;
                        return fieldInfo;

                    case RespCommand.SETIFGREATER:
                    case RespCommand.SETIFMATCH:
                        fieldInfo.ValueSize = input.parseState.GetArgSliceByRef(0).Length;
                        fieldInfo.HasETag = true;
                        fieldInfo.HasExpiration = input.arg1 != 0 || srcLogRecord.Info.HasExpiration;
                        return fieldInfo;

                    case RespCommand.SETRANGE:
                        var offset = input.parseState.GetInt(0);
                        var newValue = input.parseState.GetArgSliceByRef(1);

                        fieldInfo.ValueSize = newValue.Length + offset;
                        if (fieldInfo.ValueSize < srcLogRecord.ValueSpan.Length)
                            fieldInfo.ValueSize = srcLogRecord.ValueSpan.Length;
                        return fieldInfo;

                    case RespCommand.GETEX:
                        fieldInfo.ValueSize = srcLogRecord.ValueSpan.Length;

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
                        fieldInfo.ValueSize = srcLogRecord.ValueSpan.Length + input.parseState.GetArgSliceByRef(0).Length;
                        return fieldInfo;

                    case RespCommand.GETDEL:
                    case RespCommand.DELIFGREATER:
                        // Min allocation (only metadata) needed since this is going to be used for tombstoning anyway.
                        return fieldInfo;

                    default:
                        if (cmd > RespCommandExtensions.LastValidCommand)
                        {
                            var functions = functionsState.GetCustomCommandFunctions((ushort)cmd);
                            fieldInfo.ValueSize = functions.GetLength(srcLogRecord.ValueSpan, ref input);
                            fieldInfo.HasExpiration = input.arg1 != 0;
                            return fieldInfo;
                        }
                        throw new GarnetException("Unsupported operation on input");
                }
            }

            fieldInfo.ValueSize = input.parseState.GetArgSliceByRef(0).Length;
            fieldInfo.HasExpiration = input.arg1 != 0;
            return fieldInfo;
        }

        public RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref StringInput input)
        {
            var fieldInfo = new RecordFieldInfo()
            {
                KeySize = key.Length,
                ValueSize = value.Length,
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

        public RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref StringInput input)
            => throw new GarnetException("String store should not be called with IHeapObject");

        public RecordFieldInfo GetUpsertFieldInfo<TSourceLogRecord>(ReadOnlySpan<byte> key, in TSourceLogRecord inputLogRecord, ref StringInput input)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (inputLogRecord.Info.ValueIsObject)
                throw new GarnetException("String store should not be called with IHeapObject");
            return inputLogRecord.GetRecordFieldInfo();
        }
    }
}