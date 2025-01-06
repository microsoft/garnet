// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Garnet.server
{
    /// <summary>
    /// Provides a mapping of Lua allocations on to the POH.
    /// 
    /// Unlike <see cref="LuaLimitedManagedAllocator"/>, allocations are not limited.
    /// </summary>
    /// <remarks>
    /// This is implemented in terms of <see cref="LuaLimitedManagedAllocator"/> because
    /// it is expected to be ununusual to want allocations on the POH but unlimitted.
    /// </remarks>
    internal sealed class LuaManagedAllocator : ILuaAllocator
    {
        private const int DefaultAllocationBytes = 2 * 1_024 * 1_024;

        private readonly List<LuaLimitedManagedAllocator> subAllocators = [];
        public LuaManagedAllocator() { }

        /// <inheritdoc/>
        public ref byte AllocateNew(int sizeBytes, out bool failed)
        {
            for (var i = 0; i < subAllocators.Count; i++)
            {
                var alloc = subAllocators[i];

                ref var ret = ref alloc.AllocateNew(sizeBytes, out failed);
                if (!failed)
                {
                    return ref ret;
                }
            }

            var newAllocSize = DefaultAllocationBytes;

            // Need to account for overhead in LuaLimitedManagedAllocator blocks
            var minAllocSize = sizeBytes + sizeof(int);
            if (newAllocSize < minAllocSize)
            {
                newAllocSize = (int)BitOperations.RoundUpToPowerOf2((nuint)minAllocSize);
                if (newAllocSize < 0)
                {
                    // Handle overflow
                    failed = true;
                    return ref Unsafe.NullRef<byte>();
                }
            }

            var newAlloc = new LuaLimitedManagedAllocator(newAllocSize);
            subAllocators.Add(newAlloc);

            ref var newRet = ref newAlloc.AllocateNew(sizeBytes, out failed);
            Debug.Assert(!failed, "Failure shouldn't be possible with new alloc");

            return ref newRet;
        }

        /// <inheritdoc/>
        public void Free(ref byte start, int sizeBytes)
        {
            for (var i = 0; i < subAllocators.Count; i++)
            {
                var alloc = subAllocators[i];

                if (alloc.ContainsRef(ref start))
                {
                    alloc.Free(ref start, sizeBytes);

                    return;
                }
            }

            throw new InvalidOperationException("Allocation could not be found");
        }

        /// <inheritdoc/>
        public ref byte ResizeAllocation(ref byte start, int oldSizeBytes, int newSizeBytes, out bool failed)
        {
            for (var i = 0; i < subAllocators.Count; i++)
            {
                var alloc = subAllocators[i];

                if (alloc.ContainsRef(ref start))
                {
                    ref var ret = ref alloc.ResizeAllocation(ref start, oldSizeBytes, newSizeBytes, out failed);
                    if (!failed)
                    {
                        return ref ret;
                    }

                    // Have to make a copy into a new allocation
                    ref var newAlloc = ref AllocateNew(newSizeBytes, out failed);
                    if (failed)
                    {
                        return ref Unsafe.NullRef<byte>();
                    }

                    var copyLen = newSizeBytes < oldSizeBytes ? newSizeBytes : oldSizeBytes;
                    var from = MemoryMarshal.CreateReadOnlySpan(ref start, copyLen);
                    var to = MemoryMarshal.CreateSpan(ref newAlloc, copyLen);
                    from.CopyTo(to);

                    // Release the old allocation
                    alloc.Free(ref start, oldSizeBytes);

                    failed = false;
                    return ref newAlloc;
                }
            }

            throw new InvalidOperationException("Allocation could not be found");
        }
    }
}