// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Common interface for Lua allocators.
    /// 
    /// Lua itself has a somewhat esoteric alloc interface,
    /// this maps to something akin to malloc/free/realloc though
    /// with some C# niceties.
    /// 
    /// Note that all returned references must be pinned, as Lua is not aware
    /// of the .NET GC.
    /// </summary>
    internal interface ILuaAllocator
    {
        /// <summary>
        /// Allocate a new chunk of memory of at least <paramref name="sizeBytes"/> size.
        /// 
        /// Note that 0-sized allocations MUST succeed and MUST return a non-null reference.
        /// A 0-sized allocation will NOT be dereferenced.
        /// 
        /// If it cannot be satisfied, <paramref name="failed"/> must be set to false.
        /// </summary>
        ref byte AllocateNew(int sizeBytes, out bool failed);

        /// <summary>
        /// Free an allocation previously obtained from <see cref="AllocateNew"/>
        /// or <see cref="ResizeAllocation(ref byte, int, int, out bool)"/>.
        /// 
        /// <paramref name="sizeBytes"/> will match sizeBytes or newSizeBytes, respectively, that
        /// was passed when the allocation was obtained.
        /// </summary>
        void Free(ref byte start, int sizeBytes);

        /// <summary>
        /// Resize an existing allocation.
        /// 
        /// Data up to <paramref name="newSizeBytes"/> must be preserved.
        /// 
        /// <paramref name="start"/> will be a previously obtained, non-null, allocation.
        /// <paramref name="oldSizeBytes"/> will be the sizeBytes or newSizeBytes that was passed when the allocaiton was obtained.
        /// 
        /// This should resize in place if possible.
        /// 
        /// If in place resizeing is not possible, the previously allocation will be freed if this method succeeds.
        /// 
        /// If this allocation cannot be satisifed, <paramref name="failed"/> must be set to false.
        /// </summary>
        ref byte ResizeAllocation(ref byte start, int oldSizeBytes, int newSizeBytes, out bool failed);
    }
}