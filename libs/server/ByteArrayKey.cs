// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Numerics;

namespace Garnet.server
{
    /// <summary>
    /// Specialized key type for storing byte arrays, in one of two styles:
    /// (1) byte[]: a heap-allocated byte array that is not pinned
    /// (2) ArgSlice: a pinned byte array
    /// </summary>
    public struct ByteArrayKey : IEquatable<ByteArrayKey>
    {
        byte[] arrBytes;
        ArgSlice arrSlice;

        internal ByteArrayKey(byte[] arrBytes)
        {
            this.arrBytes = arrBytes;
        }

        internal ByteArrayKey(ArgSlice arrSlice)
        {
            this.arrSlice = arrSlice;
        }

        public static ByteArrayKey CopyFrom(ReadOnlySpan<byte> bytes, bool usePinned)
        {
            var arrBytes = GC.AllocateUninitializedArray<byte>(bytes.Length, usePinned);
            bytes.CopyTo(arrBytes);
            if (usePinned)
            {
                return new ByteArrayKey(ArgSlice.FromPinnedSpan(bytes));
            }
            else
            {
                return new ByteArrayKey(arrBytes);
            }
        }

        public ReadOnlySpan<byte> ReadOnlySpan
            => arrBytes != null ? new ReadOnlySpan<byte>(arrBytes) : arrSlice.ReadOnlySpan;

        /// <inheritdoc/>
        public unsafe bool Equals(ByteArrayKey other)
            => ReadOnlySpan.SequenceEqual(other.ReadOnlySpan);

        /// <inheritdoc />
        public override bool Equals(object obj)
            => obj is ScriptHashKey other && Equals(other);

        /// <inheritdoc />
        public override unsafe int GetHashCode()
        {
            if (arrBytes != null)
            {
                fixed (byte* k = arrBytes)
                {
                    return (int)HashBytes(k, arrBytes.Length);
                }
            }
            else
            {
                return (int)HashBytes(arrSlice.ptr, arrSlice.length);
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