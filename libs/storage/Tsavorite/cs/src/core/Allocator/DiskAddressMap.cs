// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;

namespace Tsavorite.core
{
    /// <summary>
    /// This is a simple hash table implemented as a vector with hash-and-probe.
    /// It maps from a logicalAddress' offset on the allocator page to its on-disk address.
    /// </summary>
    internal struct DiskAddressMap
    {
        /// <summary>.Net object overhead for byte arrays. TODO consolidate Commons</summary>
        internal const int ByteArrayOverhead = 24;

        /// <summary>An element of the map array.</summary>
        struct Pair
        {
            /// <summary>Offset of the record in the in-memory log page</summary>
            internal readonly int logOffset;
            /// <summary>Address of the record on the disk</summary>
            internal readonly long diskAddress;

            /// <summary><see cref="diskAddress"/> will always be at least <see cref="DiskPageHeader.Size"/> for any record.</summary>
            internal readonly bool IsSet => diskAddress != 0;

            internal Pair(int logOffset, long diskAddress)
            {
                this.logOffset = logOffset;
                this.diskAddress = diskAddress;
            }
        }

        /// <summary>The map array.</summary>
        Pair[] array;

        /// <summary>The number of items in the map array.</summary>
        int count;

        /// <summary>When we start to flush a page we call this to initialize the array</summary>
        /// <param name="estimatedNumberOfRecords"></param>
        internal void Initialize(int estimatedNumberOfRecords)
        {
            // Allocate the array to be the lowest power of 2 above twice the expected number of records.
            // We target it to be half-full so we won't have to probe far.
            array = new Pair[Utility.NextPowerOf2(estimatedNumberOfRecords * 2)];
        }

        internal readonly long this[int logOffset]
        {
            get
            {
                var code = (uint)Utility.Murmur3(logOffset);
                var index = code & (array.Length - 1);
                var wrapped = false;

                // We should always find this logOffset so don't need another "normal" exit criteria.
                for (; ; index++)
                {
                    if (index == array.Length)
                    {
                        if (wrapped)
                            throw new TsavoriteException("Failed to find logOffset");
                        wrapped = true;
                        index = 0;
                    }
                    if (array[index].logOffset == logOffset)
                        return array[index].diskAddress;
                }
            }
        }

        /// <summary>
        /// Add an offset and diskAddress pair to the map.
        /// </summary>
        /// <param name="logOffset"></param>
        /// <param name="diskAddress"></param>
        internal void Add(int logOffset, long diskAddress)
        {
            Debug.Assert(LogAddress.IsOnDisk(diskAddress), "Forgot to SetIsOnDisk(diskAddress)");
            if (array.Length - count < array.Length / 4)
                Grow();

            var code = (uint)Utility.Murmur3(logOffset);
            var index = code & (array.Length - 1);
            var wrapped = false;

            // We've ensured we have open spaces so don't need another "normal" exit criteria.
            for (; ; ++index)
            {
                if (index == array.Length)
                {
                    if (wrapped)
                        throw new TsavoriteException("Failed to find logOffset");
                    wrapped = true;
                    index = 0;
                }
                if (!array[index].IsSet)
                {
                    array[index] = new(logOffset, diskAddress);
                    ++count;
                    break;
                }
            }
        }

        private void Grow()
        {
            // Double the array size then re-hash
            var newArray = new Pair[array.Length * 2];
            for (var ii = 0; ii < array.Length; ii++)
            {
                ref var pair = ref array[ii];
                if (pair.IsSet)
                    Add(pair.logOffset, pair.diskAddress);
            }
            array = newArray;
        }

        internal void Clear()
        {
            Array.Clear(array, 0, array.Length);
            count = 0;
        }
    }
}
