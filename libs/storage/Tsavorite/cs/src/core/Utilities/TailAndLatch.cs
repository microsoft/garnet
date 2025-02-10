// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// The tail index and latch bit and operations on them, for simple growable vectors
    /// </summary>
    internal struct TailAndLatch(long word = TailAndLatch.DefaultInitialWord)
    {
        internal const long DefaultInitialWord = kTailBitMask;  // Tail is -1

        internal long word = word;

        const int kTailBits = sizeof(int) * 8;
        const long kTailBitMask = (1L << kTailBits) - 1;

        const long kLatchBitMask = 1L << kTailBits;

        internal int Tail
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get { return GetTail(word); }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { word = (word & ~kTailBitMask) | (value & kTailBitMask); }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetTail(long word) => (int)(word & kTailBitMask);

        internal readonly bool IsLatched => (word & kLatchBitMask) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void AcquireForPush<TElement>(ref TElement[] array)
        {
            for (; ; _ = Thread.Yield())
            {
                var local = new TailAndLatch(word);
                if (!local.IsLatched)
                {
                    var original = local.word;
                    ++local.Tail;
                    if (original == Interlocked.CompareExchange(ref word, local.SetLatch(), original))
                    {
                        // We got the latch and the increment. We incremented tail so we may now be indexing past end of array (this includes the initial allocation).
                        if (local.Tail >= array.Length)
                        {
                            try
                            {
                                var newArray = new TElement[array.Length * 2];
                                Array.Copy(array, newArray, array.Length);
                                array = newArray;
                            }
                            catch
                            {
                                // Restore on OOM
                                word = original;
                                throw;
                            }
                        }
                        return;
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryAcquireForPop()
        {
            for (; ; _ = Thread.Yield())
            {
                var local = new TailAndLatch(word);

                // If we have no elements, return false
                if (local.Tail <= 0)
                    return false;

                if (!local.IsLatched)
                {
                    var original = local.word;
                    --local.Tail;

                    if (original == Interlocked.CompareExchange(ref word, local.SetLatch(), original))
                        return true;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long SetLatch() => word |= kLatchBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long ClearLatch() => word &= ~kLatchBitMask;

        /// <inheritdoc/>
        public override readonly string ToString() => $"tail {Tail}, latch {IsLatched}";
    }
}
