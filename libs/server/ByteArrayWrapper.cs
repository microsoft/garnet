// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Numerics;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Specialized key type for storing byte arrays (pinned and unpinned).
    /// </summary>
    public readonly struct ByteArrayWrapper
    {
        readonly byte[] arrBytes;
        readonly PinnedSpanByte arrSlice;

        internal ByteArrayWrapper(byte[] arrBytes, bool isPinned = false)
        {
            this.arrBytes = arrBytes;
            if (isPinned)
                this.arrSlice = PinnedSpanByte.FromPinnedSpan(arrBytes);
        }

        internal ByteArrayWrapper(PinnedSpanByte arrSlice)
        {
            this.arrSlice = arrSlice;
        }

        public static ByteArrayWrapper CopyFrom(ReadOnlySpan<byte> bytes, bool usePinned)
        {
            var arrBytes = GC.AllocateUninitializedArray<byte>(bytes.Length, usePinned);
            bytes.CopyTo(arrBytes);
            return new ByteArrayWrapper(arrBytes, usePinned);
        }

        public unsafe ReadOnlySpan<byte> ReadOnlySpan
            => arrSlice.IsValid ? arrSlice.ReadOnlySpan : new ReadOnlySpan<byte>(arrBytes);

        /// <inheritdoc />
        public override unsafe int GetHashCode()
        {
            if (arrSlice.IsValid)
                return (int)HashBytes(arrSlice.ToPointer(), arrSlice.Length);

            fixed (byte* k = arrBytes)
                return (int)HashBytes(k, arrBytes.Length);
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