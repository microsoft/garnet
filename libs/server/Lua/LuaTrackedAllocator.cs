// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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

        internal LuaTrackedAllocator(int? limitBytes)
        {
            this.limitBytes = limitBytes;
        }

        /// <inheritdoc/>
        public ref byte AllocateNew(int sizeBytes, out bool failed)
        {
            if (limitBytes != null)
            {
                if (allocatedBytes + sizeBytes > limitBytes)
                {
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

            if (limitBytes != null)
            {
                if (allocatedBytes + delta > limitBytes)
                {
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
    }
}