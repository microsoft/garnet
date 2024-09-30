// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
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
        public int GetRMWInitialValueLength(ref SpanByte input)
        {
            var inputspan = input.AsSpan();
            var inputPtr = input.ToPointer();
            var cmd = inputspan[0];
            switch ((RespCommand)cmd)
            {
                case RespCommand.SETBIT:
                    return sizeof(int) + BitmapManager.Length(inputPtr + RespInputHeader.Size);
                case RespCommand.BITFIELD:
                    return sizeof(int) + BitmapManager.LengthFromType(inputPtr + RespInputHeader.Size);
                case RespCommand.PFADD:
                    byte* i = inputPtr + RespInputHeader.Size;
                    return sizeof(int) + HyperLogLog.DefaultHLL.SparseInitialLength(i);
                case RespCommand.PFMERGE:
                    i = inputPtr + RespInputHeader.Size;
                    int length = *(int*)i;//[hll allocated size = 4 byte] + [hll data structure]
                    return sizeof(int) + length;
                case RespCommand.SETRANGE:
                    var offset = *((int*)(inputPtr + RespInputHeader.Size));
                    var newValueSize = *((int*)(inputPtr + RespInputHeader.Size + sizeof(int)));
                    return sizeof(int) + newValueSize + offset + input.MetadataSize;

                case RespCommand.APPEND:
                    var valueLength = *(int*)(inputPtr + RespInputHeader.Size);
                    return sizeof(int) + valueLength;

                case RespCommand.INCRBY:
                    if (!IsValidNumber(input.LengthWithoutMetadata - RespInputHeader.Size, inputPtr + RespInputHeader.Size, out var next))
                        return sizeof(int);

                    var fNeg = false;
                    var ndigits = NumUtils.NumDigitsInLong(next, ref fNeg);

                    return sizeof(int) + ndigits + (fNeg ? 1 : 0);

                case RespCommand.DECRBY:
                    if (!IsValidNumber(input.LengthWithoutMetadata - RespInputHeader.Size, inputPtr + RespInputHeader.Size, out next))
                        return sizeof(int);
                    next = -next;

                    fNeg = false;
                    ndigits = NumUtils.NumDigitsInLong(next, ref fNeg);

                    return sizeof(int) + ndigits + (fNeg ? 1 : 0);
                case RespCommand.SETWITHETAG:
                    // same space as SET but with 8 additional bytes for etag at the front of the payload
                    return sizeof(int) + input.Length - RespInputHeader.Size + sizeof(long);
                default:
                    if (cmd >= 200)
                    {
                        var functions = functionsState.customCommands[cmd - 200].functions;
                        // Compute metadata size for result
                        int metadataSize = input.ExtraMetadata switch
                        {
                            -1 => 0,
                            0 => 0,
                            _ => 8,
                        };
                        return sizeof(int) + metadataSize + functions.GetInitialLength(input.AsReadOnlySpan().Slice(RespInputHeader.Size));
                    }
                    return sizeof(int) + input.Length - RespInputHeader.Size;
            }
        }

        /// <inheritdoc/>
        public int GetRMWModifiedValueLength(ref SpanByte t, ref SpanByte input, bool hasEtag)
        {
            if (input.Length > 0)
            {
                var inputspan = input.AsSpan();
                var inputPtr = input.ToPointer();
                var cmd = inputspan[0];
                int etagOffset = hasEtag ? 8 : 0;
                bool retainEtag = ((RespInputHeader*)inputPtr)->CheckRetainEtagFlag();

                switch ((RespCommand)cmd)
                {
                    case RespCommand.INCR:
                    case RespCommand.INCRBY:
                        var datalen = inputspan.Length - RespInputHeader.Size;
                        var slicedInputData = inputspan.Slice(RespInputHeader.Size, datalen);

                        // We don't need to TryParse here because InPlaceUpdater will raise an error before we reach this point
                        var curr = NumUtils.BytesToLong(t.AsSpan(etagOffset));
                        var next = curr + NumUtils.BytesToLong(slicedInputData);

                        var fNeg = false;
                        var ndigits = NumUtils.NumDigitsInLong(next, ref fNeg);
                        ndigits += fNeg ? 1 : 0;

                        return sizeof(int) + ndigits + t.MetadataSize + etagOffset;

                    case RespCommand.DECR:
                    case RespCommand.DECRBY:
                        datalen = inputspan.Length - RespInputHeader.Size;
                        slicedInputData = inputspan.Slice(RespInputHeader.Size, datalen);

                        // We don't need to TryParse here because InPlaceUpdater will raise an error before we reach this point
                        curr = NumUtils.BytesToLong(t.AsSpan(etagOffset));
                        var decrBy = NumUtils.BytesToLong(slicedInputData);
                        next = curr + (cmd == (byte)RespCommand.DECR ? decrBy : -decrBy);

                        fNeg = false;
                        ndigits = NumUtils.NumDigitsInLong(next, ref fNeg);
                        ndigits += fNeg ? 1 : 0;

                        return sizeof(int) + ndigits + t.MetadataSize + etagOffset;
                    case RespCommand.SETBIT:
                        return sizeof(int) + BitmapManager.NewBlockAllocLength(inputPtr + RespInputHeader.Size, t.Length);
                    case RespCommand.BITFIELD:
                        return sizeof(int) + BitmapManager.NewBlockAllocLengthFromType(inputPtr + RespInputHeader.Size, t.Length);
                    case RespCommand.PFADD:
                        int length = sizeof(int);
                        byte* i = inputPtr + RespInputHeader.Size;
                        byte* v = t.ToPointer();
                        length += HyperLogLog.DefaultHLL.UpdateGrow(i, v);
                        return length + t.MetadataSize;

                    case RespCommand.PFMERGE:
                        length = sizeof(int);
                        byte* dstHLL = t.ToPointer();
                        byte* srcHLL = inputPtr + RespInputHeader.Size;// srcHLL: <4byte HLL len> <HLL data>
                        length += HyperLogLog.DefaultHLL.MergeGrow(srcHLL + sizeof(int), dstHLL);
                        return length + t.MetadataSize;

                    case RespCommand.SETKEEPTTLXX:
                    case RespCommand.SETKEEPTTL:
                        if (!retainEtag)
                            etagOffset = 0;
                        return sizeof(int) + t.MetadataSize + input.Length - RespInputHeader.Size + etagOffset;

                    case RespCommand.SET:
                    case RespCommand.SETEXXX:
                        if (!retainEtag)
                            etagOffset = 0;
                        return sizeof(int) + input.Length - RespInputHeader.Size + etagOffset;
                    case RespCommand.SETIFMATCH:
                    case RespCommand.PERSIST:
                        break;
                    case RespCommand.SETWITHETAG:
                        // same space as SET but with 8 additional bytes for etag at the front of the payload
                        return sizeof(int) + input.Length - RespInputHeader.Size + sizeof(long);
                    case RespCommand.EXPIRE:
                    case RespCommand.PEXPIRE:
                        return sizeof(int) + t.Length + input.MetadataSize;

                    case RespCommand.SETRANGE:
                        var offset = *((int*)(inputPtr + RespInputHeader.Size));
                        var newValueSize = *((int*)(inputPtr + RespInputHeader.Size + sizeof(int)));

                        if (newValueSize + offset > t.LengthWithoutMetadata - etagOffset)
                            return sizeof(int) + newValueSize + offset + t.MetadataSize + etagOffset;
                        return sizeof(int) + t.Length;

                    case RespCommand.GETDEL:
                        // No additional allocation needed.
                        break;

                    case RespCommand.APPEND:
                        var valueLength = *((int*)(inputPtr + RespInputHeader.Size));
                        return sizeof(int) + t.Length + valueLength;

                    default:
                        if (cmd >= 200)
                        {
                            var functions = functionsState.customCommands[cmd - 200].functions;
                            // compute metadata for result
                            int metadataSize = input.ExtraMetadata switch
                            {
                                -1 => 0,
                                0 => t.MetadataSize,
                                _ => 8,
                            };
                            return sizeof(int) + metadataSize + functions.GetLength(t.AsReadOnlySpan(), input.AsReadOnlySpan().Slice(RespInputHeader.Size));
                        }
                        throw new GarnetException("Unsupported operation on input");
                }
            }

            return sizeof(int) + input.Length - RespInputHeader.Size;
        }
    }
}