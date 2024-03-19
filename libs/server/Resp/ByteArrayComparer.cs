// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Numerics;

namespace Garnet.server
{
    /// <summary>
    /// Byte array equality comparer
    /// </summary>
    public class ByteArrayComparer : IEqualityComparer<byte[]>
    {
        /// <summary>
        /// Equals
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <returns></returns>
        public bool Equals(byte[] left, byte[] right)
            => new ReadOnlySpan<byte>(left).SequenceEqual(new ReadOnlySpan<byte>(right));

        /// <summary>
        /// Get hash code
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
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
    }
}