// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using Garnet.common;

namespace Garnet.server
{
    // [Implementation of getbitfield operation]
    // The algorithm assumes bits are stored in bitmap in a sequence of bytes
    // for which the most significant bit is the most significant bit of the
    // byte appearing at offset position and the least significant bit is the bit
    // appearing at position offset + bitCount.

    // e.g.example of 13-bit value(bits represented with V) stored within two adjacent bytes
    //       [offset / 8][offset / 8 + 1]
    // ..... HHVV VVVV VVVV VVVT....

    // The get algorithm works by constructing a 64-bit value from the individual bits between offset
    // and offset + bitCount, and shifting those bits the right amount on the right to generate the
    // given bitCount-bit value.
    // To extract the given number of bits from the sequence of bytes representing the given bitmap,
    // we need first to read all bytes between the offset and offset + bitCount position.
    // e.g. for the previous example we would read 2 bytes
    // HHVV VVVV VVVV VVVT
    // After reading we need to discard the leading bits (i.e.HH) and the tail bits
    // We achieve this by shifting left the same amount of HH bits and then right 64 - bitCount.

    // A special case exists when the offset appears at a position that does not give us enough bits
    // from the most significant byte portion.
    // In that case we need to read an additional byte and append it to the of the 64 bit value.
    // After that we again shift right byt 64-bitCount position to retain the exact number of bits we need
    // according to the bitCount parameter.

    public unsafe partial class BitmapManager
    {
        /// <summary>
        /// Check if bitmap is large enough to apply bitfield op.
        /// </summary>
        /// <param name="args">Command input parameters.</param>
        /// <param name="vlen">Length of bitfield value.</param>
        /// <returns>True if need to grow value otherwise false.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsLargeEnoughForType(BitFieldCmdArgs args, int vlen)
        {
            return LengthFromType(args) <= vlen;
        }

        /// <summary>
        /// Length in bytes based on offset calculated as raw bit offset or from typeInfo bitCount.
        /// </summary>
        /// <param name="args">Command input parameters.</param>
        /// <returns>Integer number of bytes required to perform bitfield op.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int LengthFromType(BitFieldCmdArgs args)
        {
            var offset = args.offset;
            var bitCount = (byte)(args.typeInfo & 0x7F);
            return LengthInBytes(offset + bitCount - 1);
        }

        /// <summary>
        /// Get allocation size for bitfield command.
        /// </summary>
        /// <param name="args">Command input parameters.</param>
        /// <param name="valueLen">Current length of bitfield value.</param>
        /// <returns>Integer number of bytes required to perform bitfield operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int NewBlockAllocLengthFromType(BitFieldCmdArgs args, int valueLen)
        {
            var lengthInBytes = LengthFromType(args);
            return valueLen > lengthInBytes ? valueLen : lengthInBytes;
        }

        /// <summary>
        /// Check if bitfield operation will overflow.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="incrBy"></param>
        /// <param name="result"></param>
        /// <param name="bitCount"></param>
        /// <param name="overflowType"></param>
        /// <param name="signed"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        private static bool CheckBitfieldOverflow(long value, long incrBy, out long result, byte bitCount, byte overflowType, bool signed)
        {
            long newValue;
            bool overflow;
            if (signed)
            {
                (newValue, overflow) = CheckSignedBitfieldOverflow(value, incrBy, bitCount, overflowType);
            }
            else
            {
                ulong nv;
                (nv, overflow) = CheckUnsignedBitfieldOverflow((ulong)value, incrBy, bitCount, overflowType);
                newValue = (long)nv;
            }

            // Ignore overflow flag if warp or sat, do not need to return nil or skip set in that case
            overflow = overflowType == (byte)BitFieldOverflow.FAIL && overflow;
            result = newValue;

            return overflow;

            static (ulong, bool) CheckUnsignedBitfieldOverflow(ulong value, long incrBy, byte bitCount, byte overflowType)
            {
                var maxVal = bitCount == 64 ? ulong.MaxValue : (1UL << bitCount) - 1;
                var maxAdd = maxVal - value;

                var neg = incrBy < 0;
                //get absolute value of given increment
                var absIncrBy = incrBy < 0 ? (ulong)(~incrBy) + 1UL : (ulong)incrBy;
                //overflow if absolute increment is larger than diff of maxVal and current value
                var overflow = (absIncrBy > maxAdd);
                //underflow if absolute increment bigger than increment and increment is negative
                var underflow = (absIncrBy > value) && neg;

                var result = neg ? value - absIncrBy : value + absIncrBy;
                var mask = maxVal;
                result &= mask;
                switch (overflowType)
                {
                    case (byte)BitFieldOverflow.WRAP:
                        if (overflow || underflow)
                            return (result, true);
                        return (result, false);
                    case (byte)BitFieldOverflow.SAT:
                        if (overflow) return (maxVal, true);
                        else if (underflow) return (0, true);
                        return (result, false);
                    case (byte)BitFieldOverflow.FAIL:
                        if (overflow || underflow)
                            return (0, true);
                        return (result, false);
                    default:
                        throw new GarnetException("Invalid overflow type");
                }
            }

            static (long, bool) CheckSignedBitfieldOverflow(long value, long incrBy, byte bitCount, byte overflowType)
            {
                var signbit = 1L << (bitCount - 1);
                var mask = bitCount == 64 ? -1 : (signbit - 1);

                var result = (value + incrBy);
                //if operands are both negative possibility for underflow
                //underflow if sign bit is zero
                var underflow = (result & signbit) == 0 && value < 0 && incrBy < 0;
                //if operands are both positive possibility of overflow
                //overflow if any of the 64-bitcount most significant bits are set.
                var overflow = (ulong)(result & ~mask) > 0 && value >= 0 && incrBy > 0;

                switch (overflowType)
                {
                    case (byte)BitFieldOverflow.WRAP:
                        if (underflow || overflow)
                        {
                            var res = (ulong)result;
                            if (bitCount < 64)
                            {
                                ulong msb = (ulong)signbit;
                                ulong smask = (ulong)mask;
                                res = (res & msb) > 0 ? (res | ~smask) : (res & smask);
                            }
                            return ((long)res, true);
                        }
                        return (result, false);
                    case (byte)BitFieldOverflow.SAT:
                        var maxVal = bitCount == 64 ? long.MaxValue : (signbit - 1);
                        if (overflow) //overflow
                        {
                            return (maxVal, true);
                        }
                        else if (underflow) //underflow
                        {
                            var minVal = -maxVal - 1;
                            return (minVal, true);
                        }
                        return (result, false);
                    case (byte)BitFieldOverflow.FAIL:
                        if (underflow || overflow)
                            return (0, true);
                        return (result, false);
                    default:
                        throw new GarnetException("Invalid overflow type");
                }
            }
        }

        /// <summary>
        /// Get bitfield value from bitmap
        /// </summary>
        /// <param name="buf">Buffer being used to write value into</param>
        /// <param name="curr">First byte of bitfield value</param>
        /// <param name="cend">Last byte of bitfield value </param>
        /// <param name="vend">Last byte of bitfield value bounded by bitmap length</param>
        /// <param name="bitmapLength">Bitmap length</param>
        /// <param name="offset">Bit offset for value</param>
        /// <param name="encoding">Encoding of value</param>
        /// <param name="signed">Whether the bitfield is signed</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long GetValue(ref byte* buf, ref byte* curr, byte* cend, byte* vend, long bitmapLength, long offset, byte encoding, bool signed)
        {
            var byteIndexStart = Index(offset);
            var byteIndexEnd = Index(offset + encoding) + 1;

            if (curr < cend) buf[7] = *curr++;
            if (curr < cend) buf[6] = *curr++;
            if (curr < cend) buf[5] = *curr++;
            if (curr < cend) buf[4] = *curr++;
            if (curr < cend) buf[3] = *curr++;
            if (curr < cend) buf[2] = *curr++;
            if (curr < cend) buf[1] = *curr++;
            if (curr < cend) buf[0] = *curr++;
            var returnValue = *(long*)buf;

            // Prune leading bits
            var _left = (int)(offset - (byteIndexStart << 3));
            returnValue <<= _left;

            // Extract an additional byte if 64 bit buffer needs more bytes
            // Append the byte to the end of long value
            // After that need to shift by _right because total size will be 64 bit and we need bitCount amount
            if ((64 - _left) < encoding)
            {
                // Extract number of bits skipped because of offset and position them at the tail of the partially constructed 64-bit value
                var _lsb = (byte)(curr < vend ? (*curr) >> (8 - _left) : 0);
                returnValue |= _lsb;
            }

            // Shift 64 bit value to construct the given number based of bitCount
            var _right = 64 - encoding;
            returnValue = signed ?
                returnValue >> _right :
                (long)(((ulong)returnValue) >> _right);

            return returnValue;
        }

        /// <summary>
        /// Set bitfield value in bitmap
        /// </summary>
        /// <param name="buf">Buffer being used to write value into</param>
        /// <param name="curr">First byte of bitfield value</param>
        /// <param name="cend">Last byte of bitfield value </param>
        /// <param name="vend">Last byte of bitfield value bounded by bitmap length</param>
        /// <param name="bitmap">Bitmap start</param>
        /// <param name="offset">Bit offset for value</param>
        /// <param name="encoding">Encoding of value</param>
        /// <param name="unalignedBits"></param>
        /// <param name="newValue">New bitfield value</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void SetValue(byte* buf, byte* curr, byte* cend, byte* vend, byte* bitmap, long offset, int encoding, int unalignedBits, long newValue)
        {
            var byteIndexStart = Index(offset);
            var tmp = (ulong)newValue & ((encoding == 64) ? ulong.MaxValue : ((1UL << encoding) - 1));
            // Assume value fits at offset + 64 bit
            var pbits = encoding;// Prefix bits;
            var sbits = 0;// Suffix bits

            if (encoding > unalignedBits)
            {
                sbits = encoding - unalignedBits;// How many bits need to go to 9-th byte
                var smask = (1UL << sbits) - 1;// Mask to extract bits for 9-th byte
                var _sbits = 8 - sbits;// How many bits to keep from 9-th byte
                var _msb = (byte)((tmp & smask) << _sbits);// Extract suffix bits and position left at 9-th byte
                var _b9 = (byte)(*curr & ((1 << _sbits) - 1));// Extract least significant bits from 9-th byte
                *curr = (byte)(_msb | _b9);// Combine bits from newValue and least significant bits of 9-th byte and store at 9-th byte

                pbits = encoding - sbits;// Remaining bits to store between byteIndexStart and byteIndexEnd
                tmp &= ~smask;// Clear bits stored at 9-th byte
            }

            var _shf = unalignedBits - encoding;// Position of least significant bit for the remaining portion of the new value 
            var mask = encoding == 64 ? ulong.MaxValue : ((1UL << pbits) - 1);// Mask for the remaining portion of the new value
            if (_shf < 0)// Remaining bits are position too far on the left
            {
                // Shift remaining bits right
                tmp >>= -_shf;
                // Shift mask left based on number of bits stored at 9-th byte and then shift right to position mask
                mask = ~((mask << sbits) >> (-_shf));
            }
            else
            {
                tmp <<= _shf;
                mask = ~(mask << _shf);
            }

            var oldV = *(ulong*)buf;
            tmp = (oldV & mask) | tmp;
            curr = bitmap + byteIndexStart;
            if (curr < cend) *curr++ = (byte)((tmp >> 56) & 0xFF);
            if (curr < cend) *curr++ = (byte)((tmp >> 48) & 0xFF);
            if (curr < cend) *curr++ = (byte)((tmp >> 40) & 0xFF);
            if (curr < cend) *curr++ = (byte)((tmp >> 32) & 0xFF);
            if (curr < cend) *curr++ = (byte)((tmp >> 24) & 0xFF);
            if (curr < cend) *curr++ = (byte)((tmp >> 16) & 0xFF);
            if (curr < cend) *curr++ = (byte)((tmp >> 8) & 0xFF);
            if (curr < cend) *curr++ = (byte)((tmp >> 0) & 0xFF);
        }

        /// <summary>
        /// Implementation of bitfield GET operation.
        /// </summary>
        /// <param name="bitmap"></param>
        /// <param name="bitmapLength"></param>
        /// <param name="offset"></param>
        /// <param name="encoding"></param>
        /// <param name="signed"></param>
        /// <returns></returns>
        private static long GetBitfield(byte* bitmap, long bitmapLength, long offset, byte encoding, bool signed)
        {
            var byteIndexStart = Index(offset);
            var byteIndexEnd = Index(offset + encoding) + 1;
            var buf = stackalloc byte[8];
            *(ulong*)buf = 0;

            // Simple case value is beyond current length
            if (byteIndexStart >= bitmapLength) return 0;

            var vend = bitmap + bitmapLength;
            var curr = bitmap + byteIndexStart;
            var cend = bitmap + byteIndexEnd < vend ? (bitmap + byteIndexEnd) : vend;
            var returnValue = GetValue(ref buf, ref curr, cend, vend, bitmapLength, offset, encoding, signed);

            return returnValue;
        }

        /// <summary>
        /// Implementation of bitfield SET operation.
        /// </summary>
        /// <param name="bitmap"></param>
        /// <param name="bitmapLength"></param>
        /// <param name="offset"></param>
        /// <param name="encoding"></param>
        /// <param name="signed"></param>
        /// <param name="newValue"></param>
        /// <param name="overflowType"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        private static (long, bool) SetBitfield(byte* bitmap, long bitmapLength, long offset, byte encoding, bool signed, long newValue, byte overflowType)
        {
            var byteIndexStart = Index(offset);
            var byteIndexEnd = Index(offset + encoding) + 1;
            var buf = stackalloc byte[8];
            *(ulong*)buf = 0;

            // Simple case value is beyond current length
            if (byteIndexStart >= bitmapLength)
                throw new GarnetException("Setting bitfield failed: Size of bitmap smaller than offset provided.");

            #region getValue
            // Shift with typeInfo in mind            
            var vend = bitmap + bitmapLength;
            var curr = bitmap + byteIndexStart;
            var cend = bitmap + byteIndexEnd < vend ? (bitmap + byteIndexEnd) : vend;
            var oldValue = GetValue(ref buf, ref curr, cend, vend, bitmapLength, offset, encoding, signed);

            #endregion

            #region checkOverflow
            if (overflowType == (byte)BitFieldOverflow.FAIL && CheckBitfieldOverflow(oldValue, 0, out _, encoding, overflowType, signed))
                return (0, true);
            #endregion

            #region setValue
            // Prune leading bits
            var _left = (int)(offset - (byteIndexStart << 3));
            var unalignedBits = 64 - _left;
            SetValue(buf, curr, cend, vend, bitmap, offset, encoding, unalignedBits, newValue);
            #endregion

            return (oldValue, false);
        }

        /// <summary>
        /// Implementation of bitfield INCR operation.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="valLen"></param>
        /// <param name="offset"></param>
        /// <param name="encoding"></param>
        /// <param name="signed"></param>
        /// <param name="incrementByValue"></param>
        /// <param name="overflowType"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        private static (long, bool) IncrementBitfield(byte* value, long valLen, long offset, byte encoding, bool signed, long incrementByValue, byte overflowType)
        {
            var byteIndexStart = Index(offset);
            var byteIndexEnd = Index(offset + encoding) + 1;
            var buf = stackalloc byte[8];
            *(ulong*)buf = 0;

            // Simple case value is beyond current length
            if (byteIndexStart >= valLen)
                throw new GarnetException("Setting bitfield failed: Size of bitmap smaller than offset provided.");

            #region getValue
            // Prune leading bits
            var vend = value + valLen;
            var curr = value + byteIndexStart;
            var cend = value + byteIndexEnd < vend ? (value + byteIndexEnd) : vend;
            var oldValue = GetValue(ref buf, ref curr, cend, vend, valLen, offset, encoding, signed);
            #endregion

            #region incrByValue
            var overflow = CheckBitfieldOverflow(oldValue, incrementByValue, out var newValue, encoding, overflowType, signed);
            #endregion

            #region setValue
            // Calculate bits 
            var _left = (int)(offset - (byteIndexStart << 3));
            var unalignedBits = 64 - _left;
            SetValue(buf, curr, cend, vend, value, offset, encoding, unalignedBits, newValue);
            #endregion

            return (newValue, overflow);
        }

        /// <summary>
        /// Execute bitfield operation described at input on bitmap stored within value.
        /// </summary>
        /// <param name="args"></param>
        /// <param name="value"></param>
        /// <param name="valLen"></param>
        /// <returns></returns>
        public static (long, bool) BitFieldExecute(BitFieldCmdArgs args, byte* value, int valLen)
        {
            var bitCount = (byte)(args.typeInfo & 0x7F);
            var signed = (args.typeInfo & (byte)BitFieldSign.SIGNED) > 0;

            return args.secondaryCommand switch
            {
                RespCommand.SET => SetBitfield(value, valLen, args.offset, bitCount, signed, args.value, args.overflowType),
                RespCommand.INCRBY => IncrementBitfield(value, valLen, args.offset, bitCount, signed, args.value, args.overflowType),
                RespCommand.GET => (GetBitfield(value, valLen, args.offset, bitCount, signed), false),
                _ => throw new GarnetException("BITFIELD secondary op not supported"),
            };
        }

        /// <summary>
        /// Execute readonly bitfield operation described at input on bitmap stored within value.
        /// </summary>
        /// <param name="args"></param>
        /// <param name="value"></param>
        /// <param name="valLen"></param>
        /// <returns></returns>
        public static long BitFieldExecute_RO(BitFieldCmdArgs args, byte* value, int valLen)
        {
            var bitCount = (byte)(args.typeInfo & 0x7F);
            var signed = (args.typeInfo & (byte)BitFieldSign.SIGNED) > 0;

            return args.secondaryCommand switch
            {
                RespCommand.GET => GetBitfield(value, valLen, args.offset, bitCount, signed),
                _ => throw new GarnetException("BITFIELD secondary op not supported"),
            };
        }
    }
}