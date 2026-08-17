// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Owns a set of direct-VM (mmap/VirtualAlloc) blocks whose raw pointers are handed to a device for async IO,
    /// and arbitrates when such a block may be unmapped. Shared by the two direct-VM surfaces, which differ only in
    /// what a block is and how a quiesced block is finally disposed:
    /// <list type="bullet">
    /// <item>the hybrid log's circular-buffer pages (<see cref="AllocatorBase{TStoreFunctions, TAllocator}"/>),
    /// indexed by page index, whose IO is a snapshot-checkpoint flush and whose quiesced blocks are recycled into a
    /// bounded free pool;</item>
    /// <item>the main hash index's two table versions (<see cref="TsavoriteBase"/>), indexed by version, whose IO is
    /// an index checkpoint and whose superseded blocks are unmapped outright.</item>
    /// </list>
    /// The shared problem is the same in both: a block can become dead (page evicted / table superseded by a grow)
    /// while a device write issued from its raw pointer is still in flight, and that IO is not gated by any
    /// address watermark, so unmapping on the spot would tear the write. The owner tracks outstanding IO, parks
    /// blocks retired during that window, and releases them when the IO drains; blocks still live or still parked at
    /// teardown are handed to a finalization-owned <see cref="NativePageBlockRegistry"/>, because the device outlives
    /// the store and a canceled or hung write must not be able to wedge teardown.
    /// <para>A null owner denotes the managed backend, for which none of this applies.</para>
    /// </summary>
    internal sealed class DirectVmBlockOwner
    {
        /// <summary>The currently installed blocks, indexed by circular-buffer page index (hybrid log) or by hash
        /// index version (main index). An empty entry means the slot holds no direct-VM block.</summary>
        internal readonly DirectVmBlock[] Blocks;

        /// <summary>Serializes retire-vs-drain and, for the caller that needs it, its own terminal-disposition
        /// teardown (the log allocator disposes its free-page pool under this gate so a block can never be enqueued
        /// into an already-drained pool).</summary>
        internal readonly object Gate = new();

        /// <summary>Count of in-flight device IO reading these blocks' raw pointers: a producer sentinel held for
        /// the duration of a flush's issuance, plus one unit per issued write, released by its completion callback
        /// on success AND error. While &gt; 0, a retired block is parked rather than unmapped.</summary>
        long ioOutstanding;

        /// <summary>Blocks retired while IO was outstanding, awaiting the drain. Guarded by <see cref="Gate"/>.</summary>
        List<DirectVmBlock> deferredFrees;

        /// <summary>Lazily created at teardown; owns any block that cannot be unmapped synchronously.</summary>
        NativePageBlockRegistry registry;

        internal DirectVmBlockOwner(int blockCount) => Blocks = new DirectVmBlock[blockCount];

        /// <summary>Count one unit of in-flight device IO issued from these blocks' raw pointers.</summary>
        internal void BeginIo() => _ = Interlocked.Increment(ref ioOutstanding);

        /// <summary>Release one unit of in-flight device IO. Returns true if this was the last outstanding unit, in
        /// which case the caller should drain via <see cref="TryDrainDeferred"/>.</summary>
        internal bool EndIo() => Interlocked.Decrement(ref ioOutstanding) == 0;

        /// <summary>Park <paramref name="block"/> for later reclamation if device IO is outstanding. Returns true if
        /// the block was parked (the caller must not touch it again); false if no IO references it, in which case the
        /// caller performs its own terminal disposition. Double-checked so the common no-IO case takes no lock.</summary>
        internal bool TryDeferFree(in DirectVmBlock block)
        {
            if (Volatile.Read(ref ioOutstanding) == 0)
                return false;
            lock (Gate)
            {
                if (Volatile.Read(ref ioOutstanding) == 0)
                    return false;
                (deferredFrees ??= new()).Add(block);
                return true;
            }
        }

        /// <summary>Claim the parked blocks if IO has quiesced, or null if there is nothing to reclaim. Re-checks
        /// under <see cref="Gate"/> because another flush may have started between the caller's <see cref="EndIo"/>
        /// and taking the lock. The caller disposes the returned blocks outside the lock.</summary>
        internal DirectVmBlock[] TryDrainDeferred()
        {
            lock (Gate)
            {
                if (Volatile.Read(ref ioOutstanding) != 0 || deferredFrees is not { Count: > 0 })
                    return null;
                var toFree = deferredFrees.ToArray();
                deferredFrees.Clear();
                return toFree;
            }
        }

        /// <summary>Hand every still-installed block to the finalization-owned registry and clear the slots. Used at
        /// teardown, where an in-flight flush or read may still reference a block and the device is disposed by the
        /// owner afterwards.</summary>
        internal void HandOffInstalledBlocks()
        {
            for (var ii = 0; ii < Blocks.Length; ++ii)
            {
                if (Blocks[ii].IsEmpty)
                    continue;
                (registry ??= new NativePageBlockRegistry()).Register(Blocks[ii]);
                Blocks[ii] = default;
            }
        }

        /// <summary>Hand every parked block to the finalization-owned registry. Deliberately does NOT wait for
        /// <see cref="ioOutstanding"/> to drain: the device is disposed after the store, so a canceled or hung write
        /// would otherwise wedge teardown forever.</summary>
        internal void HandOffDeferredBlocks()
        {
            lock (Gate)
            {
                if (deferredFrees is not { Count: > 0 })
                    return;
                var reg = registry ??= new NativePageBlockRegistry();
                foreach (var block in deferredFrees)
                    reg.Register(block);
                deferredFrees.Clear();
            }
        }
    }
}