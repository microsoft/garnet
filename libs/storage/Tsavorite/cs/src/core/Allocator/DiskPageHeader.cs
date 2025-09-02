// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    internal struct DiskPageHeader
    {
        const int OffsetNotSet = -1;
        const ushort CurrentVersion = 1;

        /// <summary>The size of the struct. Must be a power of 2.</summary>
        internal const int Size = sizeof(long) * 8;

        /// <summary>Version of this page header.</summary>
        [FieldOffset(0)]
        internal ushort version;

        /// <summary>The size of a sector when this page was created</summary>
        [FieldOffset(sizeof(ushort))]
        internal ushort sectorSize;

        /// <summary>Offset from start of page to the first record that starts on this page. If it is not
        ///     sizeof(DiskPageHeader), then there is a record extending from the prior page. If this is -1
        ///     then no record starts on this page; it is an interior page of a multi-page record.</summary>
        [FieldOffset(sizeof(int))]
        internal int offsetToFirstRecordStart;

        // Unused; as they become used, start with higher #
        [FieldOffset(sizeof(long))]
        internal long unusedLong7;
        [FieldOffset(sizeof(long) * 2)]
        internal long unusedLong6;
        [FieldOffset(sizeof(long) * 3)]
        internal long unusedLong5;
        [FieldOffset(sizeof(long) * 4)]
        internal long unusedLong4;
        [FieldOffset(sizeof(long) * 5)]
        internal long unusedLong3;
        [FieldOffset(sizeof(long) * 6)]
        internal long unusedLong2;
        [FieldOffset(sizeof(long) * 7)]
        internal long unusedLong1;

        /// <summary>
        /// Initializes the first record offset to "unset" and returns the size of the struct.
        /// </summary>
        /// <returns></returns>
        internal int Initialize(int sectorSize)
        {
            this = default;
            version = CurrentVersion;
            sectorSize = (ushort)sectorSize;
            offsetToFirstRecordStart = OffsetNotSet;
            return Size;
        }

        /// <summary>
        /// Set the offset to the first record's start.
        /// </summary>
        /// <param name="offset">The record start offset; must include <see cref="Size"/>.</param>
        internal void SetFirstRecordOffset(int offset)
        {
            if (offsetToFirstRecordStart == OffsetNotSet)
                offsetToFirstRecordStart = offset;
        }

        public override string ToString()
            => $"ver {version}, offsetToFirstRec {offsetToFirstRecordStart}, ul1 {unusedLong1}, ul2 {unusedLong2}, ul3 {unusedLong3}, ul4 {unusedLong4}, ul5 {unusedLong5}, ul6 {unusedLong6}, ul7 {unusedLong7}";
    }
}
