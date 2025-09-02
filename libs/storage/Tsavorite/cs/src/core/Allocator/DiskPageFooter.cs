// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    internal struct DiskPageFooter
    {
        /// <summary>The size of the struct. Must be a power of 2.</summary>
        internal const int Size = sizeof(long) * 4;

        const int CurrentVersion = 1;

        /// <summary>Version of this page header.</summary>
        [FieldOffset(0)]
        internal int version;

        /// <summary>The length of the next chunk of serialized object data or long overflow data, in one of two formats each supporting a very rare edge case:
        /// <list type="bullet">
        ///     <item>If we have an overflow byte[] or an object that supports <see cref="IHeapObject.SerializedSizeIsExact"/>, then that object's length may be larger than
        ///         can fit into the available value-length bytes (e.g. if there are 3 key-length bytes, there will be space for 3 value-length bytes, or 16MB.
        ///         If that is the case *and* the indicator word's position is at the end of the page, then only the first bytes of the value length that are in the indicator
        ///         word can fit on the page, so we ignore them and this is the full value length.</item>
        ///     <item>If we have an object that does not support <see cref="IHeapObject.SerializedSizeIsExact"/> *and the key ends less than sizeof(int) bytes from the end
        ///         of the page (before footer), then those bytes are ignored and this contains the full length of the next chunk.</item>
        /// </list>
        /// </summary>
        [FieldOffset(sizeof(int))]
        internal long valueLength;
        [FieldOffset(sizeof(long) + sizeof(int))]
        internal int unusedInt1;

        // Unused; as they become used, start with higher #
        [FieldOffset(sizeof(long) * 2)]
        internal long unusedLong2;
        [FieldOffset(sizeof(long) * 3)]
        internal long unusedLong1;

        /// <summary>
        /// Initializes to defaults.
        /// </summary>
        /// <returns></returns>
        internal int Initialize()
        {
            this = default;
            version = CurrentVersion;
            return Size;
        }

        /// <summary>
        /// Set the offset to the first record's start.
        /// </summary>
        /// <param name="length">The length of the continuation chunk on the next page.</param>
        internal void SetValueLength(int length)
        {
            Debug.Assert(valueLength == 0, $"valueLength {valueLength} should only be set once");
            valueLength = length;
        }


        public override string ToString()
            => $"ver {version}, valueLen {valueLength}, ui1 {unusedInt1}, ul1 {unusedLong1}, ul2 {unusedLong2}";
    }
}
