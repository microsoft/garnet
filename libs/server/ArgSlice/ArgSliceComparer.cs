﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;

namespace Garnet.server
{
    /// <summary>
    /// ArgSlice Comparer
    /// </summary>
    public sealed class ArgSliceComparer : IEqualityComparer<ArgSlice>
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly ArgSliceComparer Instance = new();

        /// <inheritdoc />
        public bool Equals(ArgSlice x, ArgSlice y) => x.Equals(y);

        /// <inheritdoc />
        public unsafe int GetHashCode([DisallowNull] ArgSlice obj)
        {
            fixed (byte* ptr = obj.Span)
            {
                return (int)HashBytes(ptr, obj.Length);
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