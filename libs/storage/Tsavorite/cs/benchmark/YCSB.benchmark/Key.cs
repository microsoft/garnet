// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Tsavorite.benchmark
{
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct Key
    {
        [FieldOffset(0)]
        public long value;

        public override string ToString() => "{ " + value + " }";

        public struct Comparer : IKeyComparer<Key>
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public readonly long GetHashCode64(ref Key key) => Utility.GetHashCode(key.value);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Equals(ref Key key1, ref Key key2) => key1.value == key2.value;
        }
    }
}