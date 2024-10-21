// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace BDN.benchmark.Cluster
{
    /// <summary>
    /// Request struct
    /// </summary>
    unsafe struct Request
    {
        public byte[] buffer;
        public byte* ptr;

        public Request(int size)
        {
            buffer = GC.AllocateArray<byte>(size, pinned: true);
            ptr = (byte*)Unsafe.AsPointer(ref buffer[0]);
        }
    }

}