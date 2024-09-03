// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Reflection.Metadata.Ecma335;
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

        /// <inheritdoc/>
        public int GetRMWInitialValueLength(ref RawStringInput input)
        {
            var cmd = input.header.cmd;
            switch (cmd)
            {
                case RespCommand.SETBIT:
                    var bOffset = input.parseState.GetLong(input.parseStateStartIdx);
                    return sizeof(int) + BitmapManager.Length(bOffset);
                case RespCommand.BITFIELD:
                    var bitFieldArgs = GetBitFieldArguments(ref input);
                    return sizeof(int) + BitmapManager.LengthFromType(bitFieldArgs);
                case RespCommand.PFADD:
                    return sizeof(int) + HyperLogLog.DefaultHLL.SparseInitialLength(ref input);
                case RespCommand.PFMERGE:
                    var length = input.parseState.GetArgSliceByRef(input.parseStateStartIdx).SpanByte.Length;
                    return sizeof(int) + length;
                case RespCommand.SETRANGE:
                    var offset = input.parseState.GetInt(input.parseStateStartIdx);
                    var newValue = input.parseState.GetArgSliceByRef(input.parseStateStartIdx + 1).ReadOnlySpan;
                    return sizeof(int) + newValue.Length + offset;

                case RespCommand.APPEND:
                    var valueLength = input.parseState.GetArgSliceByRef(input.parseStateStartIdx).Length;
                    return sizeof(int) + valueLength;

                case RespCommand.INCRBY:
                    if (!input.parseState.TryGetLong(input.parseStateStartIdx, out var next))
                        return sizeof(int);
                    
                    var fNeg = false;
                    var ndigits = NumUtils.NumDigitsInLong(next, ref fNeg);

                    return sizeof(int) + ndigits + (fNeg ? 1 : 0);

                case RespCommand.DECRBY:
                    if (!input.parseState.TryGetLong(input.parseStateStartIdx, out next))
                        return sizeof(int);

                    next = -next;

                    fNeg = false;
                    ndigits = NumUtils.NumDigitsInLong(next, ref fNeg);

                    return sizeof(int) + ndigits + (fNeg ? 1 : 0);

                default:
                    if ((byte)cmd >= 200)
                    {
                        var functions = functionsState.customCommands[(byte)cmd - 200].functions;
                        // Compute metadata size for result
                        int metadataSize = input.arg1 switch
                        {
                            -1 => 0,
                            0 => 0,
                            _ => 8,
                        };
                        return sizeof(int) + metadataSize + functions.GetInitialLength(ref input);
                    }

                    return sizeof(int) + input.parseState.GetArgSliceByRef(input.parseStateStartIdx).ReadOnlySpan.Length +
                        (input.arg1 == 0 ? 0 : sizeof(long));
            }
        }

        /// <inheritdoc/>
        public int GetRMWModifiedValueLength(ref SpanByte t, ref RawStringInput input)
        {
            if (input.header.cmd != RespCommand.NONE)
            {
                var cmd = input.header.cmd;
                switch (cmd)
                {
                    case RespCommand.INCR:
                    case RespCommand.INCRBY:
                        // We don't need to TryGetLong here because InPlaceUpdater will raise an error before we reach this point
                        var incrByValue = input.parseState.GetLong(input.parseStateStartIdx);

                        var curr = NumUtils.BytesToLong(t.AsSpan());
                        var next = curr + incrByValue;

                        var fNeg = false;
                        var ndigits = NumUtils.NumDigitsInLong(next, ref fNeg);
                        ndigits += fNeg ? 1 : 0;

                        return sizeof(int) + ndigits + t.MetadataSize;

                    case RespCommand.DECR:
                    case RespCommand.DECRBY:
                        // We don't need to TryGetLong here because InPlaceUpdater will raise an error before we reach this point
                        var decrByValue = input.parseState.GetLong(input.parseStateStartIdx);

                        curr = NumUtils.BytesToLong(t.AsSpan());
                        next = curr + (cmd == RespCommand.DECR ? decrByValue : -decrByValue);

                        fNeg = false;
                        ndigits = NumUtils.NumDigitsInLong(next, ref fNeg);
                        ndigits += fNeg ? 1 : 0;

                        return sizeof(int) + ndigits + t.MetadataSize;
                    case RespCommand.SETBIT:
                        var bOffset = input.parseState.GetLong(input.parseStateStartIdx);
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
                        var srcHLL = input.parseState.GetArgSliceByRef(input.parseStateStartIdx).SpanByte.ToPointer();
                        var dstHLL = t.ToPointer();
                        length += HyperLogLog.DefaultHLL.MergeGrow(srcHLL, dstHLL);
                        return length + t.MetadataSize;

                    case RespCommand.SETKEEPTTLXX:
                    case RespCommand.SETKEEPTTL:
                        var setValue = input.parseState.GetArgSliceByRef(input.parseStateStartIdx);
                        return sizeof(int) + t.MetadataSize + setValue.Length;

                    case RespCommand.SET:
                    case RespCommand.SETEXXX:
                    case RespCommand.PERSIST:
                        break;

                    case RespCommand.EXPIRE:
                    case RespCommand.PEXPIRE:
                        return sizeof(int) + t.Length + sizeof(long);

                    case RespCommand.SETRANGE:
                        var offset = input.parseState.GetInt(input.parseStateStartIdx);
                        var newValue = input.parseState.GetArgSliceByRef(input.parseStateStartIdx + 1).ReadOnlySpan;

                        if (newValue.Length + offset > t.LengthWithoutMetadata)
                            return sizeof(int) + newValue.Length + offset + t.MetadataSize;
                        return sizeof(int) + t.Length;

                    case RespCommand.GETDEL:
                        // No additional allocation needed.
                        break;

                    case RespCommand.APPEND:
                        var valueLength = input.parseState.GetArgSliceByRef(input.parseStateStartIdx).Length;
                        return sizeof(int) + t.Length + valueLength;

                    default:
                        if ((byte)cmd >= 200)
                        {
                            var functions = functionsState.customCommands[(byte)cmd - 200].functions;
                            // compute metadata for result
                            var metadataSize = input.arg1 switch
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

            return sizeof(int) + input.parseState.GetArgSliceByRef(input.parseStateStartIdx).ReadOnlySpan.Length +
                (input.arg1 == 0 ? 0 : sizeof(long));
        }
    }
}