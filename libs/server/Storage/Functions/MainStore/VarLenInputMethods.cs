﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long>
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
            val = 0;
            try
            {
                // Check for valid number
                if (!NumUtils.TryBytesToLong(length, source, out val))
                {
                    // Signal value is not a valid number
                    return false;
                }
            }
            catch
            {
                // Signal value is not a valid number
                return false;
            }
            return true;
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
            val = 0;
            try
            {
                // Check for valid number
                if (!NumUtils.TryBytesToDouble(length, source, out val) || !double.IsFinite(val))
                {
                    // Signal value is not a valid number
                    return false;
                }
            }
            catch
            {
                // Signal value is not a valid number
                return false;
            }
            return true;
        }

        /// <inheritdoc/>
        public int GetRMWInitialValueLength(ref RawStringInput input)
        {
            var metadataSize = input.arg1 == 0 ? 0 : sizeof(long);
            var cmd = input.header.cmd;

            switch (cmd)
            {
                case RespCommand.SETBIT:
                    var bOffset = input.parseState.GetLong(0);
                    return sizeof(int) + BitmapManager.Length(bOffset);
                case RespCommand.BITFIELD:
                    var bitFieldArgs = GetBitFieldArguments(ref input);
                    return sizeof(int) + BitmapManager.LengthFromType(bitFieldArgs);
                case RespCommand.PFADD:
                    return sizeof(int) + HyperLogLog.DefaultHLL.SparseInitialLength(ref input);
                case RespCommand.PFMERGE:
                    var length = input.parseState.GetArgSliceByRef(0).SpanByte.Length;
                    return sizeof(int) + length;
                case RespCommand.SETRANGE:
                    var offset = input.parseState.GetInt(0);
                    var newValue = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;
                    return sizeof(int) + newValue.Length + offset;

                case RespCommand.APPEND:
                    var valueLength = input.parseState.GetArgSliceByRef(0).Length;
                    return sizeof(int) + valueLength;

                case RespCommand.INCR:
                    return sizeof(int) + 1; // # of digits in "1"

                case RespCommand.DECR:
                    return sizeof(int) + 2; // # of digits in "-1"

                case RespCommand.INCRBY:
                    var fNeg = false;
                    var ndigits = NumUtils.NumDigitsInLong(input.arg1, ref fNeg);

                    return sizeof(int) + ndigits + (fNeg ? 1 : 0);

                case RespCommand.DECRBY:
                    fNeg = false;
                    ndigits = NumUtils.NumDigitsInLong(-input.arg1, ref fNeg);

                    return sizeof(int) + ndigits + (fNeg ? 1 : 0);
                case RespCommand.INCRBYFLOAT:
                    if (!input.parseState.TryGetDouble(0, out var incrByFloat))
                        return sizeof(int);

                    ndigits = NumUtils.NumOfCharInDouble(incrByFloat, out var _, out var _, out var _);

                    return sizeof(int) + ndigits;

                default:
                    if (cmd > RespCommandExtensions.LastValidCommand)
                    {
                        var functions = functionsState.GetCustomCommandFunctions((ushort)cmd);
                        // Compute metadata size for result
                        metadataSize = input.arg1 switch
                        {
                            -1 => 0,
                            0 => 0,
                            _ => 8,
                        };
                        return sizeof(int) + metadataSize + functions.GetInitialLength(ref input);
                    }

                    int allocationForEtag = input.header.CheckWithEtagFlag() ? Constants.EtagSize : 0;
                    return sizeof(int) + input.parseState.GetArgSliceByRef(0).ReadOnlySpan.Length + metadataSize + allocationForEtag;
            }
        }

        /// <inheritdoc/>
        public int GetRMWModifiedValueLength(ref SpanByte t, ref RawStringInput input, bool hasEtag)
        {
            if (input.header.cmd != RespCommand.NONE)
            {
                var cmd = input.header.cmd;
                bool withEtag = input.header.CheckWithEtagFlag();
                int etagOffset = hasEtag || withEtag ? Constants.EtagSize : 0;

                switch (cmd)
                {
                    case RespCommand.INCR:
                    case RespCommand.INCRBY:
                        var incrByValue = input.header.cmd == RespCommand.INCRBY ? input.arg1 : 1;

                        var curr = NumUtils.BytesToLong(t.AsSpan(etagOffset));
                        var next = curr + incrByValue;

                        var fNeg = false;
                        var ndigits = NumUtils.NumDigitsInLong(next, ref fNeg);
                        ndigits += fNeg ? 1 : 0;

                        return sizeof(int) + ndigits + t.MetadataSize + etagOffset;

                    case RespCommand.DECR:
                    case RespCommand.DECRBY:
                        var decrByValue = input.header.cmd == RespCommand.DECRBY ? input.arg1 : 1;

                        curr = NumUtils.BytesToLong(t.AsSpan(etagOffset));
                        next = curr - decrByValue;

                        fNeg = false;
                        ndigits = NumUtils.NumDigitsInLong(next, ref fNeg);
                        ndigits += fNeg ? 1 : 0;

                        return sizeof(int) + ndigits + t.MetadataSize + etagOffset;
                    case RespCommand.INCRBYFLOAT:
                        // We don't need to TryGetDouble here because InPlaceUpdater will raise an error before we reach this point
                        var incrByFloat = input.parseState.GetDouble(0);

                        NumUtils.TryBytesToDouble(t.AsSpan(etagOffset), out var currVal);
                        var nextVal = currVal + incrByFloat;

                        ndigits = NumUtils.NumOfCharInDouble(nextVal, out _, out _, out _);

                        return sizeof(int) + ndigits + t.MetadataSize + etagOffset;
                    case RespCommand.SETBIT:
                        var bOffset = input.parseState.GetLong(0);
                        return sizeof(int) + BitmapManager.NewBlockAllocLength(t.Length, bOffset);
                    case RespCommand.BITFIELD:
                        var bitFieldArgs = GetBitFieldArguments(ref input);
                        return sizeof(int) + BitmapManager.NewBlockAllocLengthFromType(bitFieldArgs, t.Length);
                    case RespCommand.PFADD:
                        var length = sizeof(int);
                        var v = t.ToPointer();
                        length += HyperLogLog.DefaultHLL.UpdateGrow(ref input, v);
                        return length + t.MetadataSize;

                    case RespCommand.PFMERGE:
                        length = sizeof(int);
                        var srcHLL = input.parseState.GetArgSliceByRef(0).SpanByte.ToPointer();
                        var dstHLL = t.ToPointer();
                        length += HyperLogLog.DefaultHLL.MergeGrow(srcHLL, dstHLL);
                        return length + t.MetadataSize;

                    case RespCommand.SETKEEPTTLXX:
                    case RespCommand.SETKEEPTTL:
                        var setValue = input.parseState.GetArgSliceByRef(0);
                        if (!withEtag)
                            etagOffset = 0;
                        return sizeof(int) + t.MetadataSize + setValue.Length + etagOffset;

                    case RespCommand.SET:
                    case RespCommand.SETEXXX:
                        if (!withEtag)
                            etagOffset = 0;
                        return sizeof(int) + input.parseState.GetArgSliceByRef(0).Length + (input.arg1 == 0 ? 0 : sizeof(long)) + etagOffset;
                    case RespCommand.PERSIST:
                        return sizeof(int) + t.LengthWithoutMetadata;
                    case RespCommand.SETIFMATCH:
                        var newValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                        int metadataSize = input.arg1 == 0 ? t.MetadataSize : sizeof(long);
                        return sizeof(int) + newValue.Length + Constants.EtagSize + metadataSize;
                    case RespCommand.EXPIRE:
                    case RespCommand.PEXPIRE:
                    case RespCommand.EXPIREAT:
                    case RespCommand.PEXPIREAT:
                        return sizeof(int) + t.Length + sizeof(long);

                    case RespCommand.SETRANGE:
                        var offset = input.parseState.GetInt(0);
                        newValue = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

                        if (newValue.Length + offset > t.LengthWithoutMetadata - etagOffset)
                            return sizeof(int) + newValue.Length + offset + t.MetadataSize + etagOffset;
                        return sizeof(int) + t.Length;

                    case RespCommand.GETDEL:
                        // No additional allocation needed.
                        break;

                    case RespCommand.GETEX:
                        return sizeof(int) + t.LengthWithoutMetadata + (input.arg1 > 0 ? sizeof(long) : 0);

                    case RespCommand.APPEND:
                        var valueLength = input.parseState.GetArgSliceByRef(0).Length;
                        return sizeof(int) + t.Length + valueLength;

                    default:
                        if (cmd > RespCommandExtensions.LastValidCommand)
                        {
                            var functions = functionsState.GetCustomCommandFunctions((ushort)cmd);
                            // compute metadata for result
                            metadataSize = input.arg1 switch
                            {
                                -1 => 0,
                                0 => t.MetadataSize,
                                _ => 8,
                            };
                            return sizeof(int) + metadataSize + functions.GetLength(t.AsReadOnlySpan(), ref input);
                        }
                        throw new GarnetException("Unsupported operation on input");
                }
            }

            return sizeof(int) + input.parseState.GetArgSliceByRef(0).Length +
                (input.arg1 == 0 ? 0 : sizeof(long));
        }

        public int GetUpsertValueLength(ref SpanByte t, ref RawStringInput input)
        {
            switch (input.header.cmd)
            {
                case RespCommand.SET:
                case RespCommand.SETEX:
                    return input.arg1 == 0 ? t.TotalSize : sizeof(int) + t.LengthWithoutMetadata + sizeof(long);
            }

            return t.TotalSize;
        }
    }
}