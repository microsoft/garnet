// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Tsavorite.kvbench
{
    /// <summary>
    /// 128-byte padded long. Two cache-line stride per slot fully isolates each
    /// counter from its neighbours, including against L2 spatial prefetch.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 128)]
    public struct PaddedLong
    {
        [FieldOffset(0)] public long Value;
    }

    /// <summary>
    /// 128-byte padded bool. Lives on its own field to prevent cross-line
    /// prefetch aliasing with worker scoreboard counters.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 128)]
    public struct PaddedBool
    {
        [FieldOffset(0)] public bool Value;
    }
}