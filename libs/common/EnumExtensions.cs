// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace Garnet.common
{
    /// <summary>
    /// Conversions between an enum and the 64-bit representation of its underlying value.
    /// </summary>
    public static class EnumExtensions
    {
        /// <summary>
        /// Converts a 64-bit representation of an underlying value to <typeparamref name="TEnum"/>,
        /// provided it fits the enum's underlying type and names a declared member.
        /// </summary>
        /// <typeparam name="TEnum">Enum type to convert to.</typeparam>
        /// <param name="value">Underlying value widened to 64 bits.</param>
        /// <param name="result">Resulting enum member, or <see langword="default"/> on failure.</param>
        /// <returns><see langword="true"/> if the value names a declared member.</returns>
        public static bool TryParseToEnum<TEnum>(this long value, out TEnum result) where TEnum : unmanaged, Enum
        {
            // Get the real runtime container type of the enum
            var underlyingType = Enum.GetUnderlyingType(typeof(TEnum));

            // Prevent overflow errors by validating the long fits inside smaller containers
            switch (Type.GetTypeCode(underlyingType))
            {
                case TypeCode.SByte:
                    if (value is < sbyte.MinValue or > sbyte.MaxValue) goto Fail;
                    break;
                case TypeCode.Byte:
                    if (value is < byte.MinValue or > byte.MaxValue) goto Fail;
                    break;
                case TypeCode.Int16:
                    if (value is < short.MinValue or > short.MaxValue) goto Fail;
                    break;
                case TypeCode.UInt16:
                    if (value is < ushort.MinValue or > ushort.MaxValue) goto Fail;
                    break;
                case TypeCode.Int32:
                    if (value is < int.MinValue or > int.MaxValue) goto Fail;
                    break;
                case TypeCode.UInt32:
                    if (value is < uint.MinValue or > uint.MaxValue) goto Fail;
                    break;
                    // Int64 and UInt64 span the whole 64-bit range and need no boundary check. A ulong
                    // member above long.MaxValue is held bit-cast as a negative long, which round-trips.
            }

            // Narrow to the width of TEnum before reinterpreting, so the result is correct on any endianness
            result = FromInt64<TEnum>(value);

            // Perform a strict structural definition check. The generic overload takes TEnum directly, so it
            // neither boxes nor throws for a mismatched container type (unlike Enum.IsDefined(Type, object)).
            if (Enum.IsDefined(result))
                return true;

        Fail:
            result = default;
            return false;
        }

        /// <summary>
        /// Reinterprets the low <c>sizeof(TEnum)</c> bytes of a 64-bit value as <typeparamref name="TEnum"/>.
        /// </summary>
        /// <typeparam name="TEnum">Enum type to convert to.</typeparam>
        /// <param name="value">Underlying value widened to 64 bits.</param>
        /// <returns>The reinterpreted enum member, which need not be a declared one.</returns>
        static TEnum FromInt64<TEnum>(long value) where TEnum : unmanaged, Enum
        {
            // Unsafe bit-cast conversion bypasses boxing allocations
            switch (Unsafe.SizeOf<TEnum>())
            {
                case sizeof(byte):
                    {
                        var narrowed = unchecked((byte)value);
                        return Unsafe.As<byte, TEnum>(ref narrowed);
                    }
                case sizeof(ushort):
                    {
                        var narrowed = unchecked((ushort)value);
                        return Unsafe.As<ushort, TEnum>(ref narrowed);
                    }
                case sizeof(uint):
                    {
                        var narrowed = unchecked((uint)value);
                        return Unsafe.As<uint, TEnum>(ref narrowed);
                    }
                default:
                    return Unsafe.As<long, TEnum>(ref value);
            }
        }

        /// <summary>
        /// Boxes a 64-bit representation of an underlying value as an instance of
        /// <paramref name="underlyingType"/>, the form <see cref="Enum.IsDefined(Type, object)"/> and
        /// <see cref="Enum.GetName(Type, object)"/> require.
        /// </summary>
        /// <param name="raw">Underlying value widened to 64 bits.</param>
        /// <param name="underlyingType">Underlying type of the enum, from <see cref="Enum.GetUnderlyingType"/>.</param>
        /// <returns>The value boxed as <paramref name="underlyingType"/>.</returns>
        public static object ToEnumLiteral(this long raw, Type underlyingType) => Type.GetTypeCode(underlyingType) switch
        {
            TypeCode.SByte => unchecked((sbyte)raw),
            TypeCode.Byte => unchecked((byte)raw),
            TypeCode.Int16 => unchecked((short)raw),
            TypeCode.UInt16 => unchecked((ushort)raw),
            TypeCode.Int32 => unchecked((int)raw),
            TypeCode.UInt32 => unchecked((uint)raw),
            TypeCode.UInt64 => unchecked((ulong)raw),
            _ => raw,
        };

        /// <summary>
        /// Parses a member of <paramref name="enumType"/> from either its underlying numeric value or its
        /// name, and returns the underlying value widened to 64 bits. A numeric value is parsed as the
        /// enum's declared underlying type — so it neither overflows nor silently wraps — and is accepted
        /// only if it names a declared member.
        /// </summary>
        /// <param name="value">Value to parse.</param>
        /// <param name="enumType">Enum type to parse against.</param>
        /// <param name="parsed">Underlying value of the resolved member, widened to 64 bits.</param>
        /// <returns><see langword="true"/> if the value names a declared member.</returns>
        public static bool TryParseEnumToLong(this string value, Type enumType, out long parsed)
        {
            parsed = 0;
            if (value == null || enumType == null || !enumType.IsEnum)
                return false;

            var underlyingType = Enum.GetUnderlyingType(enumType);
            if (TryParseUnderlying(underlyingType, value, out parsed))
                return Enum.IsDefined(enumType, parsed.ToEnumLiteral(underlyingType));

            if (!Enum.TryParse(enumType, value, ignoreCase: true, out var boxed) ||
                !Enum.IsDefined(enumType, boxed))
            {
                parsed = 0;
                return false;
            }

            parsed = ToInt64(underlyingType, boxed);
            return true;

            // Widen a boxed enum member to the 64-bit slot representation.
            static long ToInt64(Type underlyingType, object boxed) => Type.GetTypeCode(underlyingType) switch
            {
                TypeCode.UInt64 => unchecked((long)Convert.ToUInt64(boxed, CultureInfo.InvariantCulture)),
                _ => Convert.ToInt64(boxed, CultureInfo.InvariantCulture),
            };

            // Parse a numeric literal as the given underlying type, widening the result into the 64-bit slot.
            // Signed types sign-extend and unsigned types zero-extend, so ToUnderlyingValue recovers the
            // original value exactly.
            static bool TryParseUnderlying(Type underlyingType, string value, out long parsed)
            {
                const NumberStyles Styles = NumberStyles.Integer;
                var culture = CultureInfo.InvariantCulture;

                switch (Type.GetTypeCode(underlyingType))
                {
                    case TypeCode.SByte:
                        {
                            var ok = sbyte.TryParse(value, Styles, culture, out var v);
                            parsed = v;
                            return ok;
                        }
                    case TypeCode.Byte:
                        {
                            var ok = byte.TryParse(value, Styles, culture, out var v);
                            parsed = v;
                            return ok;
                        }
                    case TypeCode.Int16:
                        {
                            var ok = short.TryParse(value, Styles, culture, out var v);
                            parsed = v;
                            return ok;
                        }
                    case TypeCode.UInt16:
                        {
                            var ok = ushort.TryParse(value, Styles, culture, out var v);
                            parsed = v;
                            return ok;
                        }
                    case TypeCode.Int32:
                        {
                            var ok = int.TryParse(value, Styles, culture, out var v);
                            parsed = v;
                            return ok;
                        }
                    case TypeCode.UInt32:
                        {
                            var ok = uint.TryParse(value, Styles, culture, out var v);
                            parsed = v;
                            return ok;
                        }
                    case TypeCode.UInt64:
                        {
                            var ok = ulong.TryParse(value, Styles, culture, out var v);
                            parsed = unchecked((long)v);
                            return ok;
                        }
                    default:
                        return long.TryParse(value, Styles, culture, out parsed);
                }
            }
        }
    }
}