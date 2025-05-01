// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Garnet.server
{
    /// <summary>
    /// Lua allocator which still uses native memory, but tracks it and can impose limits.
    /// 
    /// <see cref="GC.AddMemoryPressure(long)"/> is also used to inform .NET host of native allocations.
    /// </summary>
    internal unsafe class LuaTrackedAllocator : ILuaAllocator
    {
        private readonly int? limitBytes;

        private int allocatedBytes;

        private bool isInfallible;
        private object infallibleAllocations;

        internal LuaTrackedAllocator(int? limitBytes)
        {
            this.limitBytes = limitBytes;
        }

        /// <inheritdoc/>
        public void EnterInfallibleAllocationRegion()
        {
            Debug.Assert(!isInfallible, "Cannot recursively enter an infallible region");

            isInfallible = true;
        }

        /// <inheritdoc/>
        public bool TryExitInfallibleAllocationRegion()
        {
            Debug.Assert(isInfallible, "Cannot exit an infallible region that hasn't been entered");

            isInfallible = false;

            return infallibleAllocations == null;
        }

        /// <inheritdoc/>
        public ref byte AllocateNew(int sizeBytes, out bool failed)
        {
            if (limitBytes != null)
            {
                if (allocatedBytes + sizeBytes > limitBytes)
                {
                    if (isInfallible)
                    {
                        failed = false;
                        return ref InfallibleAllocate(sizeBytes);
                    }

                    failed = true;
                    return ref Unsafe.NullRef<byte>();
                }

                allocatedBytes += sizeBytes;
            }

            if (sizeBytes > 0)
            {
                GC.AddMemoryPressure(sizeBytes);
            }

            failed = false;

            var ptr = NativeMemory.Alloc((nuint)sizeBytes);
            return ref Unsafe.AsRef<byte>(ptr);
        }

        /// <inheritdoc/>
        public void Free(ref byte start, int sizeBytes)
        {
            if (infallibleAllocations != null && IsInfallibleAllocation(ref start))
            {
                // Just ignore these
                return;
            }

            NativeMemory.Free(Unsafe.AsPointer(ref start));

            if (sizeBytes > 0)
            {
                allocatedBytes -= sizeBytes;

                GC.RemoveMemoryPressure(sizeBytes);
            }
        }

        /// <inheritdoc/>
        public ref byte ResizeAllocation(ref byte start, int oldSizeBytes, int newSizeBytes, out bool failed)
        {
            var delta = newSizeBytes - oldSizeBytes;

            if (delta == 0)
            {
                failed = false;
                return ref start;
            }

            if (infallibleAllocations != null && IsInfallibleAllocation(ref start))
            {
                failed = false;
                ref var ret = ref InfallibleAllocate(newSizeBytes);
                MemoryMarshal.CreateReadOnlySpan(ref start, Math.Min(newSizeBytes, oldSizeBytes)).CopyTo(MemoryMarshal.CreateSpan(ref ret, newSizeBytes));

                return ref ret;
            }

            if (limitBytes != null)
            {
                if (allocatedBytes + delta > limitBytes)
                {
                    if (isInfallible)
                    {
                        failed = false;
                        ref var ret = ref InfallibleAllocate(newSizeBytes);
                        MemoryMarshal.CreateReadOnlySpan(ref start, Math.Min(newSizeBytes, oldSizeBytes)).CopyTo(MemoryMarshal.CreateSpan(ref ret, newSizeBytes));

                        return ref ret;
                    }

                    failed = true;
                    return ref Unsafe.NullRef<byte>();
                }

                allocatedBytes += delta;
            }

            if (delta < 0)
            {
                GC.RemoveMemoryPressure(-delta);
            }
            else
            {
                GC.AddMemoryPressure(delta);
            }

            failed = false;

            var ptr = NativeMemory.Realloc(Unsafe.AsPointer(ref start), (nuint)newSizeBytes);
            return ref Unsafe.AsRef<byte>(ptr);
        }

        /// <summary>
        /// Allocate a new pinned byte[] and root it in <see cref="infallibleAllocations"/>.
        /// </summary>
        private ref byte InfallibleAllocate(int sizeBytes)
        {
            var ret = GC.AllocateUninitializedArray<byte>(sizeBytes, pinned: true);
            if (infallibleAllocations == null)
            {
                infallibleAllocations = ret;
            }
            else if (infallibleAllocations is byte[] firstAlloc)
            {
                var container = new List<byte[]>() { firstAlloc, ret };
                infallibleAllocations = container;
            }
            else
            {
                var container = (List<byte[]>)infallibleAllocations;
                container.Add(ret);
            }

            return ref MemoryMarshal.GetArrayDataReference(ret);
        }

        /// <summary>
        /// Returns true if <paramref name="byteStart"/> points into an infallible allocations.
        /// </summary>
        private bool IsInfallibleAllocation(ref byte byteStart)
        {
            if (infallibleAllocations is byte[] arr)
            {
                ref var arrStart = ref MemoryMarshal.GetArrayDataReference(arr);
                return Unsafe.AreSame(ref arrStart, ref byteStart);
            }
            else
            {
                var container = (List<byte[]>)infallibleAllocations;
                foreach (var containedArr in container)
                {
                    ref var arrStart = ref MemoryMarshal.GetArrayDataReference(containedArr);
                    if (Unsafe.AreSame(ref arrStart, ref byteStart))
                    {
                        return true;
                    }
                }
            }

            return false;
        }
    }
}