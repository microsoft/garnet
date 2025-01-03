// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Garnet.server
{
    /// <summary>
    /// Provides a mapping of Lua allocations onto the POH.
    /// 
    /// Pre-allocates the full maximum allocation.
    /// </summary>
    /// <remarks>
    /// This is a really naive allocator, just has a free list tracked with pointers in block headers.
    /// 
    /// The hope is that Lua's GC keeps the actual use of this down substantially.
    /// </remarks>
    internal sealed class LuaLimitedManagedAllocator : ILuaAllocator
    {
        /// <summary>
        /// Minimum size we'll round all Lua allocs up to.
        /// 
        /// Based on largest "normal" type Lua will allocate.
        /// </summary>
        private const int LuaAllocMinSizeBytes = 8;

        /// <summary>
        /// Represents a block of memory in this mapper.
        /// 
        /// Blocks always have a size of at least <see cref="LuaAllocMinSizeBytes"/>.
        /// 
        /// Blocks are either free or in use:
        ///  - if free, <see cref="SizeBytesRaw"/> will be positive.
        ///  - if in use, <see cref="SizeBytesRaw"/> will be negative.
        ///  
        /// If free
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Size = StructSizeBytes)]
        private struct BlockHeader
        {
            public const int StructSizeBytes = sizeof(int) + LuaAllocMinSizeBytes;

            /// <summary>
            /// Size of the block - always valid.
            /// 
            /// If negative, block is in use.
            /// </summary>
            [FieldOffset(0 * sizeof(int))]
            public int SizeBytesRaw;

            /// <summary>
            /// Index of previous block if this block is in the free list.
            /// 
            /// Only valid if the block is free.
            /// </summary>
            [FieldOffset(1 * sizeof(int))]
            public int PrevFreeBlockIndexRaw;

            /// <summary>
            /// Index of next block if this block is in the free list.
            /// 
            /// Only valid if block is free.
            /// </summary>
            [FieldOffset(2 * sizeof(int))]
            public int NextFreeBlockIndexRaw;

            /// <summary>
            /// True if block is free.
            /// </summary>
            public readonly bool IsFree
            => SizeBytesRaw > 0;

            /// <summary>
            /// True if block is in use.
            /// </summary>
            public readonly bool IsInUse
            => SizeBytesRaw < 0;

            /// <summary>
            /// Size of the block in bytes.
            /// 
            /// Prefer to checking <see cref="SizeBytesRaw"/> directly, as it accounts for state bits.
            /// </summary>
            public readonly int SizeBytes
            {
                get
                {
                    if (IsFree)
                    {
                        return SizeBytesRaw;
                    }
                    else
                    {
                        Debug.Assert(IsInUse, "In illegal state");

                        return -SizeBytesRaw;
                    }
                }
            }

            /// <summary>
            /// Index of the next block in the free list, if any.
            /// </summary>
            public readonly int NextFreeBlockIndex
            {
                get
                {
                    Debug.Assert(IsFree, "Can't be in free list if allocated");

                    return NextFreeBlockIndexRaw;
                }
            }

            /// <summary>
            /// Index of the previous block in the free list, if any.
            /// </summary>
            public readonly int PrevFreeBlockIndex
            {
                get
                {
                    Debug.Assert(IsFree, "Can't be in free list if allocated");

                    return PrevFreeBlockIndexRaw;
                }
            }

            /// <summary>
            /// Grab a reference to return to users.
            /// </summary>
            [UnscopedRef]
            public ref byte DataReference
            => ref Unsafe.AddByteOffset(ref Unsafe.As<BlockHeader, byte>(ref this), sizeof(int));

            /// <summary>
            /// For debugging purposes, all the data covered by this block.
            /// </summary>
            public ReadOnlySpan<byte> Data
            => MemoryMarshal.CreateReadOnlySpan(ref DataReference, SizeBytes);

            /// <summary>
            /// Mark block free.
            /// 
            /// After this, the block can be placed in the free list.
            /// </summary>
            public void MarkFree()
            {
                Debug.Assert(IsInUse, "Double free");

                SizeBytesRaw = -SizeBytesRaw;
            }

            /// <summary>
            /// Mark block in use.
            /// 
            /// After this, the <see cref="NextFreeBlockIndex"/> and <see cref="PrevFreeBlockIndex"/> properties cannot be accessed.
            /// </summary>
            public void MarkInUse()
            {
                Debug.Assert(IsFree, "Already allocated");

                SizeBytesRaw = -SizeBytesRaw;
            }

            /// <summary>
            /// Get a reference to the next block in the free list, or a null ref.
            /// </summary>
            public readonly ref BlockHeader GetNextFreeBlockRef(ref byte dataStartRef)
            {
                var ix = NextFreeBlockIndex;
                if (ix == -1)
                {
                    return ref Unsafe.NullRef<BlockHeader>();
                }

                ref var asByte = ref Unsafe.Add(ref dataStartRef, ix);
                return ref Unsafe.As<byte, BlockHeader>(ref asByte);
            }

            /// <summary>
            /// Get a reference to the prev block in the free list, or a null ref.
            /// </summary>
            public readonly ref BlockHeader GetPrevFreeBlockRef(ref byte dataStartRef)
            {
                var ix = PrevFreeBlockIndex;
                if (ix == -1)
                {
                    return ref Unsafe.NullRef<BlockHeader>();
                }

                ref var asByte = ref Unsafe.Add(ref dataStartRef, ix);
                return ref Unsafe.As<byte, BlockHeader>(ref asByte);
            }

            /// <summary>
            /// Get the block the comes after this one in data.
            /// 
            /// If we're at the end of data, returns a null ref.
            /// </summary>
            public ref BlockHeader GetNextAdjacentBlockRef(ref byte dataStartRef, int finalOffset)
            {
                var curOffset = GetDataIndex(ref dataStartRef);
                var nextOffset = curOffset + sizeof(int) + SizeBytes;

                ref var asByte = ref Unsafe.AddByteOffset(ref dataStartRef, nextOffset);

                if (nextOffset >= finalOffset)
                {
                    return ref Unsafe.NullRef<BlockHeader>();
                }

                return ref Unsafe.As<byte, BlockHeader>(ref asByte);
            }

            /// <summary>
            /// Get the index of this <see cref="BlockHeader"/> in the containing managed array.
            /// </summary>
            public int GetDataIndex(ref byte dataStartRef)
            => (int)Unsafe.ByteOffset(ref dataStartRef, ref Unsafe.As<BlockHeader, byte>(ref this));
        }

        // Very basic allocation story, we just keep block metadata and 
        // a free list in the array.
        private readonly Memory<byte> data;

        private int freeListStartIndex;

        /// <summary>
        /// For testing purposes, how many bytes are allocated to Lua at the moment.
        /// 
        /// This is how many bytes we've handed out, not the size of the backing array on the POH.
        /// </summary>
        internal int AllocatedBytes { get; private set; }

        /// <summary>
        /// For testing purposes, the number of blocks tracked in the free list.
        /// </summary>
        internal int FreeBlockCount
        {
            get
            {
                ref var dataStartRef = ref GetDataStartRef();

                ref var cur = ref GetFreeList(ref dataStartRef);

                var ret = 0;

                while (!Unsafe.IsNullRef(ref cur))
                {
                    ret++;

                    cur = ref cur.GetNextFreeBlockRef(ref dataStartRef);
                }

                return ret;
            }
        }

        /// <summary>
        /// For testing purposes, the size of the initial block.
        /// 
        /// Does not care if the block is free or allocated.
        /// </summary>
        internal int FirstBlockSizeBytes
        {
            get
            {
                ref var dataStartRef = ref GetDataStartRef();

                ref var firstBlock = ref Unsafe.As<byte, BlockHeader>(ref dataStartRef);

                return firstBlock.SizeBytes;
            }
        }

        internal LuaLimitedManagedAllocator(int backingArraySize)
        {
            Debug.Assert(backingArraySize >= LuaAllocMinSizeBytes + sizeof(int), "Too small to ever allocate");

            // Pinned because Lua is oblivious to .NET's GC
            data = GC.AllocateUninitializedArray<byte>(backingArraySize, pinned: true);

            ref var dataStartRef = ref GetDataStartRef();

            // Initialize the first block as a free block covering the whole allocation
            freeListStartIndex = 0;
            ref var firstBlock = ref GetFreeList(ref dataStartRef);
            firstBlock.SizeBytesRaw = backingArraySize - sizeof(int);
            firstBlock.NextFreeBlockIndexRaw = -1;
            firstBlock.PrevFreeBlockIndexRaw = -1;
        }

        /// <summary>
        /// Allocate a new chunk which can fit at least <paramref name="sizeBytes"/> of data.
        /// 
        /// Sets <paramref name="failed"/> to true if the allocation failed.
        /// 
        /// If the allocation failes, the returned ref will be null.
        /// </summary>
        public ref byte AllocateNew(int sizeBytes, out bool failed)
        {
            ref var dataStartRef = ref GetDataStartRef();

            if (sizeBytes == 0)
            {
                // Special case 0 size allocations
                failed = false;
                return ref dataStartRef;
            }

            var actualSizeBytes = RoundToMinAlloc(sizeBytes);

            var firstAttempt = true;

        tryAgain:
            ref var freeList = ref GetFreeList(ref dataStartRef);
            if (Unsafe.IsNullRef(ref freeList))
            {
                // No free space at all
                failed = true;
                return ref Unsafe.NullRef<byte>();
            }

            ref var cur = ref freeList;
            while (!Unsafe.IsNullRef(ref cur))
            {
                Debug.Assert(cur.IsFree, "Free list corrupted");

                if (cur.SizeBytes < actualSizeBytes)
                {
                    // Couldn't fit in the block, move on
                    cur = ref cur.GetNextFreeBlockRef(ref dataStartRef);
                    continue;
                }

                if (ShouldSplit(ref cur, actualSizeBytes))
                {
                    SplitFreeBlock(ref dataStartRef, ref cur, actualSizeBytes);
                    Debug.Assert(cur.IsFree && cur.SizeBytes == actualSizeBytes, "Split produced unexpected block");
                }

                // Cur will work, so remove it from the free list, mark it, and return it
                RemoveFromFreeList(ref dataStartRef, ref cur);
                cur.MarkInUse();

                AllocatedBytes += cur.SizeBytes;

                failed = false;
                return ref cur.DataReference;
            }

            // Expensively compact if we're going to fail anyway
            if (firstAttempt)
            {
                firstAttempt = false;
                if (TryCoalesceAllFreeBlocks())
                {
                    goto tryAgain;
                }
            }

            // Even after compaction we failed to find a large enough block
            failed = true;
            return ref Unsafe.NullRef<byte>();
        }

        /// <summary>
        /// Return a chunk of memory previously acquired by <see cref="AllocateNew"/> or
        /// <see cref="ResizeAllocation"/>.
        /// </summary>
        /// <param name="start">Previously returned (non-null) value.</param>
        /// <param name="sizeBytes">Size passed to last <see cref="AllocateNew"/> or <see cref="ResizeAllocation"/> call.</param>
        public void Free(ref byte start, int sizeBytes)
        {
            ref var dataStartRef = ref GetDataStartRef();
            if (sizeBytes == 0)
            {
                // Special casing 0 size allocations
                Debug.Assert(Unsafe.AreSame(ref dataStartRef, ref start), "Expected all zero size allocs to be the same");

                return;
            }

            ref var blockRef = ref GetBlockRef(ref dataStartRef, ref start);
            Debug.Assert(blockRef.IsInUse, "Should be in use");

            AllocatedBytes -= blockRef.SizeBytes;

            blockRef.MarkFree();
            AddToFreeList(ref dataStartRef, ref blockRef);
        }

        /// <summary>
        /// Akin to <see cref="AllocateNew(int, out bool)"/>, except reuses the original allocation given in <paramref name="start"/> if possible.
        /// </summary>
        public ref byte ResizeAllocation(ref byte start, int oldSizeBytes, int newSizeBytes, out bool failed)
        {
            ref var dataStartRef = ref GetDataStartRef();
            if (oldSizeBytes == 0)
            {
                // Special casing 0 size allocations
                Debug.Assert(Unsafe.AreSame(ref dataStartRef, ref start), "Expected all zero size allocs to be the same");

                return ref AllocateNew(newSizeBytes, out failed);
            }

            ref var curBlock = ref GetBlockRef(ref dataStartRef, ref start);

            // For everything else, move things up to a reasonable multiple
            var actualSizeBytes = RoundToMinAlloc(newSizeBytes);

            if (curBlock.SizeBytes >= newSizeBytes)
            {
                // Existing allocation is large enough

                if (ShouldSplit(ref curBlock, actualSizeBytes))
                {
                    // Move the unused space into a new block

                    SplitInUseBlock(ref dataStartRef, ref curBlock, actualSizeBytes);

                    // And try and coalesce that new block into the largest possible one
                    ref var nextBlock = ref curBlock.GetNextAdjacentBlockRef(ref dataStartRef, data.Length);
                    if (!Unsafe.IsNullRef(ref nextBlock))
                    {
                        while (TryCoalesceSingleBlock(ref dataStartRef, ref nextBlock))
                        {
                        }
                    }
                }

                failed = false;
                return ref start;
            }

            // Attempt to grow the allocation in place
            var keepInPlace = false;

            while (TryCoalesceSingleBlock(ref dataStartRef, ref curBlock))
            {
                if (curBlock.SizeBytes >= actualSizeBytes)
                {
                    keepInPlace = true;
                    break;
                }
            }

            if (keepInPlace)
            {
                // We built a big enough block, so use it
                if (ShouldSplit(ref curBlock, actualSizeBytes))
                {
                    // We coalesced such that there's a lot of empty space at the end of this block, peel it off for later reuse
                    SplitInUseBlock(ref dataStartRef, ref curBlock, actualSizeBytes);
                    Debug.Assert(curBlock.IsInUse && curBlock.SizeBytes == actualSizeBytes, "Split produced unexpected block");
                }

                failed = false;
                return ref start;
            }

            // We couldn't resize in place, so we need to copy into a new alloc
            ref var newAlloc = ref AllocateNew(newSizeBytes, out failed);
            if (failed)
            {
                // Couldn't get a new allocation - per spec this leaves the old allocation alone
                return ref Unsafe.NullRef<byte>();
            }

            // Copy the data over
            var copyLen = newSizeBytes < oldSizeBytes ? newSizeBytes : oldSizeBytes;

            var copyFrom = MemoryMarshal.CreateReadOnlySpan(ref curBlock.DataReference, copyLen);
            var copyInto = MemoryMarshal.CreateSpan(ref newAlloc, copyLen);

            copyFrom.CopyTo(copyInto);

            // Free the old alloc now that data is copied out
            Free(ref start, oldSizeBytes);

            return ref newAlloc;
        }

        /// <summary>
        /// Returns true if this reference might have been handed out by this allocator.
        /// </summary>
        internal bool ContainsRef(ref byte startRef)
        {
            ref var dataStartRef = ref GetDataStartRef();

            var delta = Unsafe.ByteOffset(ref dataStartRef, ref startRef);

            return delta >= 0 && delta < data.Length;
        }

        /// <summary>
        /// Validate the allocator.
        /// 
        /// For testing purposes only.
        /// </summary>
        internal void CheckCorrectness()
        {
            ref var dataStartRef = ref GetDataStartRef();

            // Check for cycles in free lists
            {
                // Basic tortoise and hare:
                //   - freeSlow moves forward 1 link at a time
                //   - freeFast moves forward 2 links at a time
                //   - if freeSlow == freeFast then we have a cycle

                ref var freeSlow = ref GetFreeList(ref dataStartRef);
                ref var freeFast = ref freeSlow;
                if (!Unsafe.IsNullRef(ref freeFast))
                {
                    freeFast = ref freeFast.GetNextFreeBlockRef(ref dataStartRef);
                }

                while (!Unsafe.IsNullRef(ref freeSlow) && !Unsafe.IsNullRef(ref freeFast))
                {
                    freeSlow = ref freeSlow.GetNextFreeBlockRef(ref dataStartRef);
                    freeFast = ref freeFast.GetNextFreeBlockRef(ref dataStartRef);
                    if (!Unsafe.IsNullRef(ref freeFast))
                    {
                        freeFast = ref freeFast.GetNextFreeBlockRef(ref dataStartRef);
                    }

                    Check(!Unsafe.AreSame(ref freeSlow, ref freeFast), "Cycle exists in free list");
                }
            }

            var walkFreeBlocks = 0;
            var walkFreeBytes = 0L;

            // Walk the free list, counting and check that pointer make sense
            {
                ref var prevFree = ref Unsafe.NullRef<BlockHeader>();
                ref var curFree = ref GetFreeList(ref dataStartRef);
                while (!Unsafe.IsNullRef(ref curFree))
                {
                    Check(curFree.IsFree, "Allocated block in free list");

                    var dataIndex = curFree.GetDataIndex(ref dataStartRef);

                    Check(dataIndex < data.Length, "Free block not in managed bounds");
                    Check(dataIndex + sizeof(int) + curFree.SizeBytes <= data.Length, "Free block end not in managed bounds");

                    walkFreeBlocks++;

                    Check(curFree.SizeBytes > 0, "Illegal size for a free block");

                    walkFreeBytes += curFree.SizeBytes;

                    if (!Unsafe.IsNullRef(ref prevFree))
                    {
                        ref var prevFromCur = ref curFree.GetPrevFreeBlockRef(ref dataStartRef);
                        Check(Unsafe.AreSame(ref prevFree, ref prevFromCur), "Prev link invalid");
                    }

                    prevFree = ref curFree;
                    curFree = ref curFree.GetNextFreeBlockRef(ref dataStartRef);
                }
            }

            var scanFreeBlocks = 0;
            var scanFreeBytes = 0L;
            var scanAllocatedBlocks = 0;
            var scanAllocatedBytes = 0L;

            // Scan the whole array, counting free and allocated blocks
            {
                ref var cur = ref Unsafe.As<byte, BlockHeader>(ref dataStartRef);
                while (!Unsafe.IsNullRef(ref cur))
                {
                    var dataIndex = cur.GetDataIndex(ref dataStartRef);

                    Check(dataIndex < data.Length, "Block not in managed bounds");
                    Check(dataIndex + sizeof(int) + cur.SizeBytes <= data.Length, "Block end not in managed bounds");

                    if (cur.IsFree)
                    {
                        scanFreeBlocks++;
                        scanFreeBytes += cur.SizeBytes;
                    }
                    else
                    {
                        Check(cur.IsInUse, "Illegal block state");

                        scanAllocatedBlocks++;
                        scanAllocatedBytes += cur.SizeBytes;
                    }

                    cur = ref cur.GetNextAdjacentBlockRef(ref dataStartRef, data.Length);
                }
            }

            Check(scanFreeBlocks == walkFreeBlocks, "Free block mismatch");
            Check(scanFreeBytes == walkFreeBytes, "Free bytes mismatch");

            Check(scanAllocatedBytes == AllocatedBytes, "Allocated bytes mismatch");

            var totalBlocks = scanAllocatedBlocks + scanFreeBlocks;
            var totalBytes = scanAllocatedBytes + scanFreeBytes;

            var expectedOverhead = totalBlocks * sizeof(int);

            var allBytes = totalBytes + expectedOverhead;
            Check(allBytes == data.Length, "Bytes unaccounted for");

            // Throws if shouldBe is false
            static void Check(bool shouldBe, string errorMsg, [CallerArgumentExpression(nameof(shouldBe))] string shouldBeExpr = null)
            {
                if (shouldBe)
                {
                    return;
                }

                throw new InvalidOperationException($"Check failed: {errorMsg} ({shouldBeExpr})");
            }
        }

        /// <summary>
        /// Do a very expensive pass attempting to coalesce free blocks as much as possible.
        /// </summary>
        internal bool TryCoalesceAllFreeBlocks()
        {
            ref var dataStartRef = ref GetDataStartRef();

            var madeProgress = false;
            ref var cur = ref GetFreeList(ref dataStartRef);
            while (!Unsafe.IsNullRef(ref cur))
            {
                // Coalesce this block repeatedly, so runs of free blocks are collapsed into one
                while (TryCoalesceSingleBlock(ref dataStartRef, ref cur))
                {
                    madeProgress = true;
                }

                cur = ref cur.GetNextFreeBlockRef(ref dataStartRef);
            }

            return madeProgress;
        }

        /// <summary>
        /// Add a the given free block to the free list.
        /// </summary>
        private void AddToFreeList(ref byte dataStartRef, ref BlockHeader block)
        {
            Debug.Assert(block.IsFree, "Can only add free blocks to list");

            var oldFreeListStartIndex = freeListStartIndex;

            var blockIx = block.GetDataIndex(ref dataStartRef);

            block.NextFreeBlockIndexRaw = freeListStartIndex;
            block.PrevFreeBlockIndexRaw = -1;

            freeListStartIndex = blockIx;

            if (oldFreeListStartIndex != -1)
            {
                // Update back pointer in previous block
                ref var oldFreeListHeader = ref Unsafe.As<byte, BlockHeader>(ref Unsafe.Add(ref dataStartRef, oldFreeListStartIndex));
                Debug.Assert(oldFreeListHeader.PrevFreeBlockIndex == -1, "Free list corrupted");

                oldFreeListHeader.PrevFreeBlockIndexRaw = blockIx;
            }
        }

        /// <summary>
        /// Removes the given block from the free list.
        /// </summary>
        private void RemoveFromFreeList(ref byte dataStartRef, ref BlockHeader block)
        {
            Debug.Assert(freeListStartIndex != -1, "Shouldn't be removing from free list if free list is empty");
            Debug.Assert(block.IsFree, "Only valid for free blocks");

            var blockIndex = block.GetDataIndex(ref dataStartRef);

            ref var prevFreeBlock = ref block.GetPrevFreeBlockRef(ref dataStartRef);
            ref var nextFreeBlock = ref block.GetNextFreeBlockRef(ref dataStartRef);

            // We've got a few different states we could be in here:
            //   1. block is the only thing in the free list
            //      => freeListStartIndex == blockIndex, prevFreeBlock == null, nextFreeBlock == null
            //   2. block is the first thing in the free list, but not the only thing
            //      => freeListStartIndex == blockIndex, prevFreeBlock == null, nextFreeBlock != null, nextFreeBLock.prev == blockIndex
            //   3. block is the last thing in the free list, but not the first
            //      => freeListStartIndex != -1, freeListStartIndex != blockIndex, prevFreeBlock != null, prevFreeBlock.next == blockIndex, nextFreeBlock == null
            //   4. block is in the middle of the list somewhere
            //      => freeListStartIndex != -1, freeListStartIndex != blockIndex, prevFreeBlock != null, prefFreeBlock.next == blockIndex, nextFreeBlock != null, nextFreeBlock.prev = next

            if (freeListStartIndex == blockIndex)
            {
                Debug.Assert(Unsafe.IsNullRef(ref prevFreeBlock), "Should be no prev pointer if block is head of free list");

                if (Unsafe.IsNullRef(ref nextFreeBlock))
                {
                    // We're in state #1 - block is the only thing in the free list

                    // Remove this last block from the free list, leaving it empty
                    freeListStartIndex = -1;
                    return;
                }
                else
                {
                    // We're in state #2 - block is first thing in the free list, but not the last
                    Debug.Assert(nextFreeBlock.PrevFreeBlockIndex == blockIndex, "Broken chain in free list");

                    // NextFreeBlock is new head, it needs to prev point now
                    nextFreeBlock.PrevFreeBlockIndexRaw = -1;
                    freeListStartIndex = block.NextFreeBlockIndex;
                    return;
                }
            }
            else
            {
                Debug.Assert(!Unsafe.IsNullRef(ref prevFreeBlock), "Should always have a prev pointer if not head of free list");

                if (Unsafe.IsNullRef(ref nextFreeBlock))
                {
                    // We're in state #3 - block is last thing in the free list, but not the first
                    Debug.Assert(prevFreeBlock.NextFreeBlockIndex == blockIndex, "Broken chain in free list");

                    // Remove pointer to this block from it's preceeding block
                    prevFreeBlock.NextFreeBlockIndexRaw = -1;
                    return;
                }
                else
                {
                    // We're in state #4 - block is just in the middle of the free list somewhere, but not last or first
                    Debug.Assert(prevFreeBlock.NextFreeBlockIndex == blockIndex, "Broken chain in free list");
                    Debug.Assert(nextFreeBlock.PrevFreeBlockIndex == blockIndex, "Broken chain in free list");

                    // Prev needs to skip this block when going forward
                    prevFreeBlock.NextFreeBlockIndexRaw = block.NextFreeBlockIndex;

                    // Next needs to skip this block when going back
                    nextFreeBlock.PrevFreeBlockIndexRaw = block.PrevFreeBlockIndex;
                    return;
                }
            }
        }

        /// <summary>
        /// Attempt to coalesce a block with its adjacent block.
        /// 
        /// <paramref name="block"/> can be free or allocated, but coalescing will only succeed
        /// if the adjacent block is free.
        /// </summary>
        private bool TryCoalesceSingleBlock(ref byte dataStartRef, ref BlockHeader block)
        {
            ref var nextBlock = ref block.GetNextAdjacentBlockRef(ref dataStartRef, data.Length);
            if (Unsafe.IsNullRef(ref nextBlock) || !nextBlock.IsFree)
            {
                return false;
            }

            RemoveFromFreeList(ref dataStartRef, ref nextBlock);
            var newBlockSizeBytes = nextBlock.SizeBytes + block.SizeBytes + sizeof(int);

            if (block.IsFree)
            {
                block.SizeBytesRaw = newBlockSizeBytes;
            }
            else
            {
                // Because we merged a free block into an allocated one we need to update the allocated byte total
                AllocatedBytes -= block.SizeBytes;

                block.SizeBytesRaw = -newBlockSizeBytes;

                AllocatedBytes += block.SizeBytes;
            }

            return true;
        }

        /// <summary>
        /// Split an in use block, such that the current block ends up with a size equal to <paramref name="curBlockUpdateSizeBytes"/>.
        /// </summary>
        private void SplitInUseBlock(ref byte dataStartRef, ref BlockHeader curBlock, int curBlockUpdateSizeBytes)
        {
            Debug.Assert(curBlock.IsInUse, "Only valid for in use blocks");

            var oldSizeBytes = curBlock.SizeBytes;

            ref var newBlock = ref SplitCommon(ref curBlock, curBlockUpdateSizeBytes);

            // New block needs to be placed in free list
            AddToFreeList(ref dataStartRef, ref newBlock);

            // Because we split some bytes out of an allocated block, that means we need to remove those from allocation tracking
            AllocatedBytes -= oldSizeBytes;
            AllocatedBytes += curBlock.SizeBytes;
        }

        /// <summary>
        /// Split a free block such that the current block ends up with a size equal to <paramref name="curBlockUpdateSizeBytes"/>.
        /// </summary>
        private void SplitFreeBlock(ref byte dataStartRef, ref BlockHeader curBlock, int curBlockUpdateSizeBytes)
        {
            Debug.Assert(curBlock.IsFree, "Only valid for free blocks");

            ref var oldNextBlock = ref curBlock.GetNextFreeBlockRef(ref dataStartRef);

            ref var newBlock = ref SplitCommon(ref curBlock, curBlockUpdateSizeBytes);

            var curBlockIndex = curBlock.GetDataIndex(ref dataStartRef);

            // Update newBlock
            newBlock.PrevFreeBlockIndexRaw = curBlockIndex;
            newBlock.NextFreeBlockIndexRaw = curBlock.NextFreeBlockIndex;
            Debug.Assert(newBlock.IsFree, "New block shoud be free");
            Debug.Assert(newBlock.GetDataIndex(ref dataStartRef) < data.Length, "New block out of managed memory");
            Debug.Assert(newBlock.GetDataIndex(ref dataStartRef) + sizeof(int) + newBlock.SizeBytes <= data.Length, "New block out of managed memory");

            var newBlockIndex = newBlock.GetDataIndex(ref dataStartRef);

            // Update curBlock
            curBlock.NextFreeBlockIndexRaw = newBlockIndex;
            Debug.Assert(curBlock.IsFree, "New block shoud be free");
            Debug.Assert(curBlock.GetDataIndex(ref dataStartRef) < data.Length, "Split block out of managed memory");
            Debug.Assert(curBlock.GetDataIndex(ref dataStartRef) + sizeof(int) + curBlock.SizeBytes <= data.Length, "Split block out of managed memory");

            // Update the old next block if it exists
            if (!Unsafe.IsNullRef(ref oldNextBlock))
            {
                Debug.Assert(oldNextBlock.IsFree, "Should have been in free list");
                oldNextBlock.PrevFreeBlockIndexRaw = newBlockIndex;
            }
        }

        /// <summary>
        /// Grab the start of the managed memory we're allocating out of.
        /// </summary>
        private ref byte GetDataStartRef()
        => ref MemoryMarshal.GetReference(data.Span);

        /// <summary>
        /// Turn a reference obtained from <see cref="BlockHeader.DataReference"/> back into a <see cref="BlockHeader"/> reference.
        /// </summary>
        private ref BlockHeader GetBlockRef(ref byte dataStartRef, ref byte userDataRef)
        {
            Debug.Assert(!Unsafe.AreSame(ref dataStartRef, ref userDataRef), "User data is actually 0 size alloc, that doesn't make sense");

            ref var blockStartByteRef = ref Unsafe.Add(ref userDataRef, -sizeof(int));

            Debug.Assert(!Unsafe.IsAddressLessThan(ref blockStartByteRef, ref dataStartRef), "User data is before managed memory");
            Debug.Assert(!Unsafe.IsAddressGreaterThan(ref blockStartByteRef, ref Unsafe.Add(ref dataStartRef, data.Length - 1)), "User data is after managed memory");

            return ref Unsafe.As<byte, BlockHeader>(ref blockStartByteRef);
        }

        /// <summary>
        /// Get the start of the free list.
        /// 
        /// If the free list is empty, returns a null ref.
        /// </summary>
        private ref BlockHeader GetFreeList(ref byte dataStartRef)
        {
            if (freeListStartIndex == -1)
            {
                return ref Unsafe.NullRef<BlockHeader>();
            }

            ref var freeListStart = ref Unsafe.Add(ref dataStartRef, freeListStartIndex);
            ref var punned = ref Unsafe.As<byte, BlockHeader>(ref freeListStart);

            return ref punned;
        }

        /// <summary>
        /// Common logic for splitting blocks.
        /// 
        /// Block here can either be free or in use, so don't make any assumptionsin here.
        /// </summary>
        private static ref BlockHeader SplitCommon(ref BlockHeader curBlock, int curBlockUpdateSizeBytes)
        {
            Debug.Assert(curBlockUpdateSizeBytes >= LuaAllocMinSizeBytes, "Shouldn't split an existing block to be this small");

            var newBlockSizeBytes = curBlock.SizeBytes - sizeof(int) - curBlockUpdateSizeBytes;

            Debug.Assert(newBlockSizeBytes >= LuaAllocMinSizeBytes, "Shouldn't create a new block this small");

            ref var curBlockData = ref curBlock.DataReference;
            ref var newBlockStartByteRef = ref Unsafe.AddByteOffset(ref curBlockData, curBlockUpdateSizeBytes);
            ref var newBlock = ref Unsafe.As<byte, BlockHeader>(ref newBlockStartByteRef);

            // The new block is always free, so assign directly
            newBlock.SizeBytesRaw = newBlockSizeBytes;

            if (curBlock.IsFree)
            {
                curBlock.SizeBytesRaw = curBlockUpdateSizeBytes;
            }
            else
            {
                Debug.Assert(curBlock.IsInUse, "Invalid block state");
                curBlock.SizeBytesRaw = -curBlockUpdateSizeBytes;
            }

            return ref newBlock;
        }

        /// <summary>
        /// Check if a block should be split if it's used to serve a claim of the given size.
        /// </summary>
        private static bool ShouldSplit(ref BlockHeader block, int claimedBytes)
        {
            var unusedBytes = block.SizeBytes - claimedBytes;

            return unusedBytes >= BlockHeader.StructSizeBytes;
        }

        /// <summary>
        /// Turn requested bytes into the actual number of bytes we're going to reserve.
        /// </summary>
        private static int RoundToMinAlloc(int sizeBytes)
        {
            var steps = sizeBytes / LuaAllocMinSizeBytes;
            if ((sizeBytes % LuaAllocMinSizeBytes) != 0)
            {
                steps++;
            }

            var ret = steps * LuaAllocMinSizeBytes;

            Debug.Assert(ret >= sizeBytes, "Rounding logic invalid");

            return ret;
        }
    }
}