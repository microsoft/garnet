// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Concurrent;

namespace Tsavorite.core
{
    /// <summary>
    /// Owns the lifetime of direct-VM blocks backing circular-buffer log pages and recovery/scan frames, freeing
    /// them from its <b>finalizer</b> rather than deterministically on Dispose.
    /// <para>
    /// This matches the managed backend's timing and is required for correctness: a log page may have an
    /// in-flight device flush/read (the device holds a raw pointer, not a managed reference) that outlives the
    /// allocator's Dispose. Managed <c>byte[]</c> pages stay mapped until the GC reclaims them — which happens
    /// only after the owning store (and therefore the device) is unreachable — so the in-flight IO never touches
    /// unmapped memory. <c>munmap</c>/<c>VirtualFree</c> is immediate, so freeing at Dispose would unmap a page
    /// while the device is still copying it (observed as an AccessViolation). Registering the blocks here and
    /// freeing them only when this registry is finalized reproduces the managed lifetime exactly: the registry is
    /// reachable only through the allocator, so it is finalized after the store and its device are gone.
    /// </para>
    /// </summary>
    internal sealed class NativePageBlockRegistry
    {
        readonly ConcurrentBag<DirectVmBlock> blocks = new();

        /// <summary>Record a block so it is freed when this registry is finalized.</summary>
        internal void Register(in DirectVmBlock block)
        {
            if (!block.IsEmpty)
                blocks.Add(block);
        }

        ~NativePageBlockRegistry()
        {
            foreach (var block in blocks)
                DirectVirtualMemory.Free(block);
        }
    }
}
