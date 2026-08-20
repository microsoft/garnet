// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace Tsavorite.core
{
    /// <summary>
    /// Owns the lifetime of direct-VM blocks backing circular-buffer log pages and recovery/scan frames, freeing
    /// them from its <b>finalizer</b> rather than deterministically on Dispose.
    /// <para>
    /// A log page may have an in-flight device flush/read (the device holds a raw pointer, not a managed reference)
    /// that outlives the allocator's Dispose. <c>munmap</c>/<c>VirtualFree</c> is immediate, so freeing at Dispose
    /// could unmap a page while the device is still copying it. This registry is reachable only through the
    /// allocator, so it is finalized after the store and its device are gone, at which point no IO can reference
    /// the blocks. It holds only a bounded set at teardown: the live tail log pages and the two live hash-index
    /// tables (superseded index tables are freed deterministically on grow).
    /// </para>
    /// </summary>
    internal sealed class NativePageBlockRegistry
    {
        // Registrations are infrequent and lightly contended (page allocation). A plain lock-guarded list keeps
        // both Register and the finalizer sweep allocation-free — the finalizer runs on the GC finalizer thread,
        // where allocating is best avoided. When the finalizer runs the registry is unreachable, so no concurrent
        // Register can race.
        readonly List<DirectVmBlock> blocks = new();
        readonly object gate = new();

        /// <summary>Record a block so it is freed when this registry is finalized.</summary>
        internal void Register(in DirectVmBlock block)
        {
            if (block.IsEmpty)
                return;
            lock (gate)
                blocks.Add(block);
        }

        ~NativePageBlockRegistry()
        {
            for (var i = 0; i < blocks.Count; i++)
                DirectVirtualMemory.Free(blocks[i]);
        }
    }
}