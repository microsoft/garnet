// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>Represents a concurrent counter that is partitioned for performance.</summary>
    internal unsafe struct ConcurrentCounter
    {
        [StructLayout(LayoutKind.Explicit, Size = Constants.kCacheLineBytes)]
        private struct Counter
        {
            [FieldOffset(0)]
            internal long value;

            // Padding to ensure each counter is on its own cache line
            [FieldOffset(8)]
            private fixed long padding[Constants.kCacheLineBytes / sizeof(long) - 1];
        }

        private Counter[] partitions;
        private Counter* partitionsPtr;
        const int partitionCount = 1; // Using a single partition for now

        /// <summary>Initializes a new instance of the <see cref="ConcurrentCounter"/> struct.</summary>
        public ConcurrentCounter()
        {
            var size = partitionCount * Constants.kCacheLineBytes; // sizeof(Counter);

            // Allocate partitions array with an extra partition rounded up to cache line size.
            // This allows using the partition array starting from an address aligned to cache line size.
            var alignedSize = Utility.RoundUp(size + Constants.kCacheLineBytes, Constants.kCacheLineBytes);
            partitions = GC.AllocateArray<Counter>(alignedSize / Constants.kCacheLineBytes, true); // sizeof(Counter));
            long partitionsAlignedPtr = Utility.RoundUp((long)Unsafe.AsPointer(ref partitions[0]), Constants.kCacheLineBytes);
            partitionsPtr = (Counter*)partitionsAlignedPtr;
        }

        /// <summary>Increments the counter by the specified value.</summary>
        /// <param name="incrValue">The value to increment the counter by.</param>
        internal void Increment(long incrValue)
        {
            if (incrValue != 0)
            {
                var partition = Environment.CurrentManagedThreadId % partitionCount;
                _ = Interlocked.Add(ref partitionsPtr[partition].value, incrValue);
            }
        }

        /// <summary>Gets the total value of the counter.</summary>
        internal long Total
        {
            get
            {
                // return sum of all partitioned counter values
                var total = 0L;
                for (var i = 0; i < partitionCount; i++)
                    total += partitionsPtr[i].value;
                return total;
            }
        }
    }
}