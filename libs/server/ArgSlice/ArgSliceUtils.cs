// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// ArgSlice utils
    /// </summary>
    public static class ArgSliceUtils
    {
        private static ReadOnlySpan<byte> CharToHexLookup =>
        [
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 15
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 31
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 47
            0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0x6,  0x7,  0x8,  0x9,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 63
            0xFF, 0xA,  0xB,  0xC,  0xD,  0xE,  0xF,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 79
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 95
            0xFF, 0xa,  0xb,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 111
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 127
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 143
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 159
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 175
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 191
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 207
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 223
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 239
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF  // 255
        ];

        /// <summary>
        /// Compute hash slot of given ArgSlice
        /// </summary>
        public static unsafe ushort HashSlot(ref ArgSlice argSlice)
            => HashSlotUtils.HashSlot(argSlice.ptr, argSlice.Length);

        /// <summary>
        /// Takes a quoted string from given ArgSlice and unescapes it. Destructive: Writes over the input memory.
        /// </summary>
        /// <param name="slice"></param>
        /// <param name="acceptUppercaseEscapes"></param>
        /// <remarks>See TryParseInlineCommandArguments() for the quoting/escaping rules</remarks>
        /// <returns></returns>
        public static unsafe ArgSlice Unescape(ArgSlice slice, bool acceptUppercaseEscapes = false)
        {
            // Too short for quoting.
            if (slice.Length <= 1)
            {
                return slice;
            }

            // Get the last character to know if this is a quoted context.
            var type = slice.Span[slice.Length - 1];

            // Nothing to do if it's not.
            if (!AsciiUtils.IsQuoteChar(type))
            {
                return slice;
            }

            // It's a quoted context, so we need to check for escapes.

            // Too short for escaping
            if (slice.Length <= 3)
            {
                return new ArgSlice(slice.ptr + 1, slice.Length - 2);
            }

            // How many bytes do we need to shift thanks to quoting and escaping.
            var shift = 0;
            // start offset
            var start = 0;
            // Are we in a quoting context?
            var qStart = false;

            // Optimize command case by changing our start point instead of shifting the entire string.
            if (slice.Span[0] == type)
            {
                qStart = true;
                start = 1;
            }

            for (var j = start; j < slice.Span.Length - shift - 1; ++j)
            {
                if (!qStart)
                {
                    if (slice.Span[j + shift] == type)
                    {
                        qStart = true;
                        shift++;
                        slice.Span[j] = slice.Span[j + 1];
                    }
                    continue;
                }

                if (slice.Span[j + shift] != '\\')
                {
                    // If we shifted earlier, we need to shift the rest too.
                    if (shift > 0)
                        slice.Span[j] = slice.Span[j + shift];
                    continue;
                }

                // ' context recognize only this particular sequence
                if (type == '\'')
                {
                    if (slice.Span[j + shift + 1] == '\'')
                        shift++;
                    if (shift > 0)
                        slice.Span[j] = slice.Span[j + shift];
                    continue;
                }

                // Process escapes
                shift++;
                var c = acceptUppercaseEscapes ?
                            (char)AsciiUtils.ToLower(slice.Span[j + shift]) :
                            (char)slice.Span[j + shift];

                switch (c)
                {
                    case 'a':
                        c = '\a';
                        break;
                    case 'b':
                        c = '\b';
                        break;
                    case 'n':
                        c = '\n';
                        break;
                    case 'r':
                        c = '\r';
                        break;
                    case 't':
                        c = '\t';
                        break;
                    case 'x':
                        if (j + shift + 2 < slice.Span.Length)
                        {
                            var val =
                                16 * CharToHexLookup[slice.ReadOnlySpan[j + shift + 1]] +
                                     CharToHexLookup[slice.ReadOnlySpan[j + shift + 2]];

                            if (val < 0xFF)
                            {
                                c = (char)val;
                                shift += 2;
                            }
                            break;
                        }
                        break;
                    default:
                        break;
                }

                slice.Span[j] = (byte)c;
            }

            // Zero unnecessary chars
            slice.Span[(slice.Span.Length - shift - start)..(slice.Span.Length - 1)].Clear();

            // The final cut must remove 1 from length to trim the end quote character.
            // (The starting quote character was already shifted out)
            return new ArgSlice(slice.ptr + start, slice.Span.Length - 1 - shift - start);
        }
    }
}