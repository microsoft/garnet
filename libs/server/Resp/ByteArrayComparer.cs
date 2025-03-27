﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Numerics;

namespace Garnet.server
{
    /// <summary>
    /// Byte array equality comparer
    /// </summary>
    public sealed class ByteArrayComparer : IEqualityComparer<byte[]>
#if NET9_0_OR_GREATER
        , IAlternateEqualityComparer<ReadOnlySpan<byte>, byte[]>
#endif
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly ByteArrayComparer Instance = new();

        /// <inheritdoc />
        public bool Equals(byte[] left, byte[] right)
            => new ReadOnlySpan<byte>(left).SequenceEqual(new ReadOnlySpan<byte>(right));

        private ByteArrayComparer() { }

        /// <inheritdoc />
        public unsafe int GetHashCode(byte[] key)
        {
            fixed (byte* k = key)
            {
                return (int)HashBytes(k, key.Length);
            }
        }

        static unsafe long HashBytes(byte* pbString, int len)
        {
            const long magicno = 40343;
            char* pwString = (char*)pbString;
            int cbBuf = len / 2;
            ulong hashState = (ulong)len;

            for (int i = 0; i < cbBuf; i++, pwString++)
                hashState = magicno * hashState + *pwString;

            if ((len & 1) > 0)
            {
                byte* pC = (byte*)pwString;
                hashState = magicno * hashState + *pC;
            }

            return (long)BitOperations.RotateRight(magicno * hashState, 4);
        }

        /// <inheritdoc/>
        public bool Equals(ReadOnlySpan<byte> alternate, byte[] other) => alternate.SequenceEqual(other);

        /// <inheritdozc/>
        public unsafe int GetHashCode(ReadOnlySpan<byte> alternate)
        {
            fixed (byte* alternatePtr = alternate)
            {
                return (int)HashBytes(alternatePtr, alternate.Length);
            }
        }

        /// <inheritdozc/>
        public byte[] Create(ReadOnlySpan<byte> alternate) => alternate.ToArray();
    }
}