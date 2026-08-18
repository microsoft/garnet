// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
#define CHECK_FREE      // disabled by default in Release due to overhead; must match BufferPool.cs
#endif

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;

    /// <summary>
    /// The per-level shared-queue buffer pool: an array of <c>ConcurrentQueue</c>s where <c>queue[i]</c> holds
    /// buffers of size <c>2^i * sectorSize</c>. It has no per-thread parking — every <see cref="Get(int, bool)"/>
    /// and <see cref="Return"/> for a size class serializes on that class's single queue head, so it does not
    /// scale with thread count. <see cref="SectorAlignedBufferPool"/> constructs and routes to one of these only when
    /// <see cref="SectorAlignedBufferPool.UseOriginReturn"/> is false; buffers keep <c>pool</c> pointing at the
    /// owning <see cref="SectorAlignedBufferPool"/>, so <see cref="SectorAlignedMemory.Return()"/> routes back
    /// here through it.
    /// </summary>
    internal sealed unsafe class LegacyBufferPool
    {
        private const int levels = 32;
        private readonly int recordSize;
        private readonly int sectorSize;
        private readonly bool unpinOnReturn;
        private readonly SectorAlignedBufferPool owner;
        private readonly ConcurrentQueue<SectorAlignedMemory>[] queue;

        internal LegacyBufferPool(int recordSize, int sectorSize, SectorAlignedBufferPool owner, bool unpinOnReturn)
        {
            this.recordSize = recordSize;
            this.sectorSize = sectorSize;
            this.owner = owner;
            this.unpinOnReturn = unpinOnReturn;
            queue = new ConcurrentQueue<SectorAlignedMemory>[levels];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int Position(int v)
        {
            if (v == 1) return 0;
            return BitOperations.Log2((uint)v - 1) + 1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal SectorAlignedMemory Get(int numRecords) => Get(numRecords, clearOnReturn: true);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal SectorAlignedMemory Get(int numRecords, bool clearOnReturn)
        {
            int required_bytes = numRecords * recordSize;
            int requiredSize = RoundUp(required_bytes, sectorSize);
            int index = Position(requiredSize / sectorSize);
            if (queue[index] == null)
            {
                var localPool = new ConcurrentQueue<SectorAlignedMemory>();
                Interlocked.CompareExchange(ref queue[index], localPool, null);
            }

            if (!SectorAlignedBufferPool.Disabled && queue[index].TryDequeue(out SectorAlignedMemory page))
            {
#if CHECK_FREE
                page.Free = false;
#endif // CHECK_FREE
                if (unpinOnReturn)
                {
                    page.handle = GCHandle.Alloc(page.buffer, GCHandleType.Pinned);
                    page.aligned_pointer = (byte*)RoundUp(page.handle.AddrOfPinnedObject(), sectorSize);
                    page.aligned_offset = (int)((long)page.aligned_pointer - page.handle.AddrOfPinnedObject());
                }
                // If the renter wants the historical zero-init contract and the slot is
                // dirty from a prior opt-out rental, clear here. Renters that themselves
                // opt out of the clear (clearOnReturn=false) will overwrite the buffer's
                // read region and don't need it cleared, regardless of incoming dirty state.
                if (clearOnReturn && page.isDirty)
                {
                    Array.Clear(page.buffer, 0, page.buffer.Length);
                    page.isDirty = false;
                }
                page.required_bytes = required_bytes;
                page.clearOnReturn = clearOnReturn;
                return page;
            }

            page = new SectorAlignedMemory(level: index)
            {
                // Add an additional sector for the leading RoundUp of pageAddr to sectorSize.
                buffer = GC.AllocateArray<byte>(sectorSize * ((1 << index) + 1), !unpinOnReturn)
            };
            if (unpinOnReturn)
                page.handle = GCHandle.Alloc(page.buffer, GCHandleType.Pinned);
            long pageAddr = (long)Unsafe.AsPointer(ref page.buffer[0]);
            page.aligned_pointer = (byte*)RoundUp(pageAddr, sectorSize);
            page.aligned_offset = (int)((long)page.aligned_pointer - pageAddr);
            page.required_bytes = required_bytes;
            // Freshly-allocated buffer from GC.AllocateArray is zero-init; isDirty stays false.
            page.clearOnReturn = clearOnReturn;
            page.pool = owner;
            return page;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Return(SectorAlignedMemory page)
        {
#if CHECK_FREE
            page.Free = true;
#endif // CHECK_FREE

            Debug.Assert(queue[page.Level] != null);
            page.available_bytes = 0;
            page.required_bytes = 0;
            page.valid_offset = 0;
            if (page.clearOnReturn)
            {
                Array.Clear(page.buffer, 0, page.buffer.Length);
                page.isDirty = false;
            }
            else
            {
                // Renter opted out of clear; the buffer may carry non-zero tail bytes from
                // the previous IO. A future default Get will lazy-clear before handing it
                // to a write-staging caller that depends on zero tail padding.
                page.isDirty = true;
            }
            // Reset the rental policy so a buffer that's been opted-out once doesn't
            // surprise the next renter (which gets the default safe behaviour unless
            // it also opts out via the Get overload).
            page.clearOnReturn = true;
            if (!SectorAlignedBufferPool.Disabled)
            {
                if (unpinOnReturn)
                {
                    page.handle.Free();
                    page.handle = default;
                }
                queue[page.Level].Enqueue(page);
            }
            else
            {
                if (unpinOnReturn)
                    page.handle.Free();
                page.buffer = null;
            }
        }

        internal void Free()
        {
            for (int i = 0; i < levels; i++)
            {
                if (queue[i] == null) continue;
                while (queue[i].TryDequeue(out SectorAlignedMemory result))
                    result.buffer = null;
            }
        }

        internal void Print()
        {
            for (int i = 0; i < levels; i++)
            {
                if (queue[i] == null) continue;
                foreach (var item in queue[i])
                {
                    Console.WriteLine("  " + item.ToString());
                }
            }
        }
    }
}