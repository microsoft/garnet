// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
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
    /// 
    /// A small optimization is trying to keep a big free block at the head of the free list.
    /// </remarks>
    internal sealed class LuaLimitedManagedAllocator : ILuaAllocator
    {
        /// <summary>
        /// Minimum size we'll round all Lua allocs up to.
        /// 
        /// Based on largest "normal" type Lua will allocate and the needs of <see cref="BlockHeader"/>.
        /// </summary>
        private const int LuaAllocMinSizeBytes = 16;

        /// <summary>
        /// Represents a block of memory in this mapper.
        /// 
        /// Blocks always have a size of at least <see cref="LuaAllocMinSizeBytes"/>.
        /// 
        /// Blocks are either free or in use:
        ///  - if free, <see cref="SizeBytesRaw"/> will be positive.
        ///  - if in use, <see cref="SizeBytesRaw"/> will be negative.
        ///  
        /// If free, the block is in a doublely linked list of free blocks.
        ///  - <see cref="NextFreeBlockRefRaw"/> is undefined if block is in use, 0 if block is tail of list, and ref (as long) to next block otherwise
        ///  - <see cref="PrevFreeBlockRefRaw"/> is undefined if block is in use, 0 if block is head of list, and ref (as long) to previous block otherwise
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Size = StructSizeBytes)]
        private struct BlockHeader
        {
            public const int DataOffset = sizeof(int);
            public const int StructSizeBytes = DataOffset + LuaAllocMinSizeBytes;

            /// <summary>
            /// Size of the block - always valid.
            /// 
            /// If negative, block is in use.
            /// 
            /// If you don't already know the state of the block, use <see cref="SizeBytes"/>.
            /// If you do, use this as it elides a conditional.
            /// </summary>
            [FieldOffset(0)]
            public int SizeBytesRaw;

            /// <summary>
            /// Ref of previous block if this block is in the free list.
            /// 
            /// Only valid if the block is free.
            /// 
            /// Most reads should go through <see cref="PrevFreeBlockRef"/> or <see cref="GetPrevFreeBlockRef"/>.
            /// </summary>
            [FieldOffset(sizeof(int))]
            public long PrevFreeBlockRefRaw;

            /// <summary>
            /// Ref of next block if this block is in the free list.
            /// 
            /// Only valid if block is free.
            /// 
            /// Most reads should go through <see cref="NextFreeBlockRef"/> or <see cref="GetNextFreeBlockRef"/>.
            /// </summary>
            [FieldOffset(sizeof(int) + sizeof(long))]
            public long NextFreeBlockRefRaw;

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
                    Debug.Assert(IsFree || IsInUse, "Illegal state, neither free nor in use");

                    if (IsFree)
                    {
                        return SizeBytesRaw;
                    }
                    else
                    {
                        return -SizeBytesRaw;
                    }
                }
            }

            /// <summary>
            /// Ref (as a long) of the next block in the free list, if any.
            /// </summary>
            public readonly long NextFreeBlockRef
            {
                get
                {
                    Debug.Assert(IsFree, "Can't be in free list if allocated");

                    return NextFreeBlockRefRaw;
                }
            }

            /// <summary>
            /// Ref of the previous block in the free list, if any.
            /// </summary>
            public readonly long PrevFreeBlockRef
            {
                get
                {
                    Debug.Assert(IsFree, "Can't be in free list if allocated");

                    return PrevFreeBlockRefRaw;
                }
            }

            /// <summary>
            /// Grab a reference to return to users.
            /// </summary>
            [UnscopedRef]
            public ref byte DataReference
            => ref Unsafe.AddByteOffset(ref Unsafe.As<BlockHeader, byte>(ref this), DataOffset);

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
            /// After this, the <see cref="NextFreeBlockRef"/> and <see cref="PrevFreeBlockRef"/> properties cannot be accessed.
            /// </summary>
            public void MarkInUse()
            {
                Debug.Assert(IsFree, "Already allocated");

                SizeBytesRaw = -SizeBytesRaw;
            }

            /// <summary>
            /// Get a reference to the next block in the free list, or a reference BEFORE <see cref="GetDataStartRef"/>.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public readonly unsafe ref BlockHeader GetNextFreeBlockRef()
            => ref Unsafe.AsRef<BlockHeader>((void*)NextFreeBlockRef);

            /// <summary>
            /// Get a reference to the prev block in the free list, or a reference BEFORE <see cref="GetDataStartRef"/>.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public readonly unsafe ref BlockHeader GetPrevFreeBlockRef()
            => ref Unsafe.AsRef<BlockHeader>((void*)PrevFreeBlockRef);

            /// <summary>
            /// Get the block the comes after this one in data.
            /// 
            /// If we're at the end of data, returns a a ref BEFORE <paramref name="dataStartRef"/>.
            /// </summary>
            [UnscopedRef]
            public ref BlockHeader GetNextAdjacentBlockRef(ref byte dataStartRef, int finalOffset)
            {
                ref var selfRef = ref Unsafe.As<BlockHeader, byte>(ref this);
                ref var nextRef = ref Unsafe.Add(ref selfRef, DataOffset + SizeBytes);

                var nextOffset = Unsafe.ByteOffset(ref dataStartRef, ref nextRef);

                if (nextOffset >= finalOffset)
                {
                    // For symmetry with GetXXXFreeBlockRef return of (void*)0
                    unsafe
                    {
                        return ref Unsafe.AsRef<BlockHeader>((void*)0);
                    }
                }

                return ref Unsafe.As<byte, BlockHeader>(ref nextRef);
            }

            /// <summary>
            /// Get a value that can be stashed in <see cref="PrevFreeBlockRefRaw"/> or <see cref="NextFreeBlockRefRaw"/>
            /// that refers to this block.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public unsafe long GetRefVal()
            => (long)Unsafe.AsPointer(ref this);
        }

        // Very basic allocation story, we just keep block metadata and 
        // a free list in the array.
        private readonly Memory<byte> data;

        private unsafe void* dataStartPtr;
        private unsafe void* freeListStartPtr;

        private int debugAllocatedBytes;

        /// <summary>
        /// For testing purposes, how many bytes are allocated to Lua at the moment.
        /// 
        /// This is how many bytes we've handed out, not the size of the backing array on the POH.
        /// 
        /// Only available in DEBUG to avoid updating this in RELEASE builds.
        /// </summary>
        internal int AllocatedBytes
        => debugAllocatedBytes;

        /// <summary>
        /// For testing purposes, the number of blocks tracked in the free list.
        /// </summary>
        internal int FreeBlockCount
        {
            get
            {
                ref var dataStartRef = ref GetDataStartRef();

                ref var cur = ref GetFreeList();

                var ret = 0;

                while (IsValidBlockRef(ref cur))
                {
                    ret++;

                    cur = ref cur.GetNextFreeBlockRef();
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
            Debug.Assert(backingArraySize >= BlockHeader.StructSizeBytes, "Too small to ever allocate");

            // Pinned because Lua is oblivious to .NET's GC
            data = GC.AllocateUninitializedArray<byte>(backingArraySize, pinned: true);
            unsafe
            {
                dataStartPtr = Unsafe.AsPointer(ref MemoryMarshal.GetReference(data.Span));
            }

            ref var dataStartRef = ref GetDataStartRef();

            // Initialize the first block as a free block covering the whole allocation
            unsafe
            {
                freeListStartPtr = Unsafe.AsPointer(ref dataStartRef);
            }
            ref var firstBlock = ref GetFreeList();
            firstBlock.SizeBytesRaw = backingArraySize - BlockHeader.DataOffset;
            firstBlock.NextFreeBlockRefRaw = 0;
            firstBlock.PrevFreeBlockRefRaw = 0;
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

            var actualSizeBytes = RoundToMinAlloc(sizeBytes);

            var firstAttempt = true;

        tryAgain:
            ref var freeList = ref GetFreeList();
            ref var cur = ref freeList;
            while (IsValidBlockRef(ref cur))
            {
                Debug.Assert(cur.IsFree, "Free list corrupted");

                // We know this block is free, so touch size directly
                var freeBlockSize = cur.SizeBytesRaw;

                if (freeBlockSize < actualSizeBytes)
                {
                    // Couldn't fit in the block, move on
                    cur = ref cur.GetNextFreeBlockRef();
                    continue;
                }

                if (ShouldSplit(ref cur, actualSizeBytes))
                {
                    ref var newBlock = ref SplitFreeBlock(ref dataStartRef, ref cur, actualSizeBytes);
                    Debug.Assert(cur.IsFree && cur.SizeBytes == actualSizeBytes, "Split produced unexpected block");

                    freeBlockSize = actualSizeBytes;

                    // We know both these blocks are free, so use size directly
                    if (!Unsafe.AreSame(ref cur, ref freeList) && newBlock.SizeBytesRaw > freeList.SizeBytesRaw)
                    {
                        // Move the split block to the head of the list if it's large enough AND we had to traverse
                        // the free list at all
                        //
                        // Idea is to keep bigger things towards the front of the free list if we're spending any time
                        // chasing pointers
                        MoveToHeadOfFreeList(ref dataStartRef, ref freeList, ref newBlock);
                    }
                }

                // Cur will work, so remove it from the free list, mark it, and return it
                RemoveFromFreeList(ref dataStartRef, ref cur);
                cur.MarkInUse();

                UpdateDebugAllocatedBytes(freeBlockSize);

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

            ref var blockRef = ref GetBlockRef(ref dataStartRef, ref start);

            Debug.Assert(blockRef.IsInUse, "Should be in use");

            blockRef.MarkFree();
            AddToFreeList(ref dataStartRef, ref blockRef);
            Debug.Assert(blockRef.IsFree, "Should be free by this point");

            // We know the block is free, so touch size directly
            UpdateDebugAllocatedBytes(-blockRef.SizeBytesRaw);
        }

        /// <summary>
        /// Akin to <see cref="AllocateNew(int, out bool)"/>, except reuses the original allocation given in <paramref name="start"/> if possible.
        /// </summary>
        public ref byte ResizeAllocation(ref byte start, int oldSizeBytes, int newSizeBytes, out bool failed)
        {
            ref var dataStartRef = ref GetDataStartRef();

            ref var curBlock = ref GetBlockRef(ref dataStartRef, ref start);
            Debug.Assert(curBlock.IsInUse, "Shouldn't be resizing an allocation that isn't in use");

            // For everything else, move things up to a reasonable multiple
            var actualSizeBytes = RoundToMinAlloc(newSizeBytes);

            // We know the block is in use, so use raw size directly
            if (-curBlock.SizeBytesRaw >= newSizeBytes)
            {
                // Existing allocation is large enough

                if (ShouldSplit(ref curBlock, actualSizeBytes))
                {
                    // Move the unused space into a new block

                    SplitInUseBlock(ref dataStartRef, ref curBlock, actualSizeBytes);

                    // And try and coalesce that new block into the largest possible one
                    ref var nextBlock = ref curBlock.GetNextAdjacentBlockRef(ref dataStartRef, data.Length);
                    if (IsValidBlockRef(ref nextBlock))
                    {
                        while (TryCoalesceSingleBlock(ref dataStartRef, ref nextBlock, out _))
                        {
                        }
                    }
                }

                failed = false;
                return ref start;
            }

            // Attempt to grow the allocation in place
            var keepInPlace = false;

            while (TryCoalesceSingleBlock(ref dataStartRef, ref curBlock, out var updatedCurBlockSizeBytes))
            {
                if (updatedCurBlockSizeBytes >= actualSizeBytes)
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
                    ref var newBlock = ref SplitInUseBlock(ref dataStartRef, ref curBlock, actualSizeBytes);
                    Debug.Assert(curBlock.IsInUse && curBlock.SizeBytes == actualSizeBytes, "Split produced unexpected block");

                    ref var freeList = ref GetFreeList();

                    // We know freeList is valid and both blocks are free, so check size directly
                    if (!Unsafe.AreSame(ref newBlock, ref freeList) && newBlock.SizeBytesRaw > freeList.SizeBytesRaw)
                    {
                        MoveToHeadOfFreeList(ref dataStartRef, ref freeList, ref newBlock);
                    }
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

                ref var freeSlow = ref GetFreeList();
                ref var freeFast = ref freeSlow;
                if (IsValidBlockRef(ref freeFast))
                {
                    freeFast = ref freeFast.GetNextFreeBlockRef();
                }

                while (IsValidBlockRef(ref freeSlow) && IsValidBlockRef(ref freeFast))
                {
                    freeSlow = ref freeSlow.GetNextFreeBlockRef();
                    freeFast = ref freeFast.GetNextFreeBlockRef();
                    if (IsValidBlockRef(ref freeFast))
                    {
                        freeFast = ref freeFast.GetNextFreeBlockRef();
                    }

                    Check(!Unsafe.AreSame(ref freeSlow, ref freeFast), "Cycle exists in free list");
                }
            }

            var walkFreeBlocks = 0;
            var walkFreeBytes = 0L;

            // Walk the free list, counting and check that pointer make sense
            {
                ref var prevFree = ref Unsafe.NullRef<BlockHeader>();
                ref var curFree = ref GetFreeList();
                while (IsValidBlockRef(ref curFree))
                {
                    Check(curFree.IsFree, "Allocated block in free list");
                    Check(ContainsRef(ref Unsafe.As<BlockHeader, byte>(ref curFree)), "Free block not in managed bounds");

                    walkFreeBlocks++;

                    Check(curFree.SizeBytes > 0, "Illegal size for a free block");

                    walkFreeBytes += curFree.SizeBytes;

                    if (IsValidBlockRef(ref prevFree))
                    {
                        ref var prevFromCur = ref curFree.GetPrevFreeBlockRef();
                        Check(Unsafe.AreSame(ref prevFree, ref prevFromCur), "Prev link invalid");
                    }

                    prevFree = ref curFree;
                    curFree = ref curFree.GetNextFreeBlockRef();
                }
            }

            var scanFreeBlocks = 0;
            var scanFreeBytes = 0L;
            var scanAllocatedBlocks = 0;
            var scanAllocatedBytes = 0L;

            // Scan the whole array, counting free and allocated blocks
            {
                ref var cur = ref Unsafe.As<byte, BlockHeader>(ref dataStartRef);
                while (IsValidBlockRef(ref cur))
                {
                    Check(ContainsRef(ref Unsafe.As<BlockHeader, byte>(ref cur)), "Free block not in managed bounds");

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

            DebugCheck(scanAllocatedBytes == AllocatedBytes, "Allocated bytes mismatch");

            var totalBlocks = scanAllocatedBlocks + scanFreeBlocks;
            var totalBytes = scanAllocatedBytes + scanFreeBytes;

            var expectedOverhead = totalBlocks * BlockHeader.DataOffset;

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

            // Throws if shouldBe is false, but only in DEBUG builds
            [Conditional("DEBUG")]
            static void DebugCheck(bool shouldBe, string errorMsg, [CallerArgumentExpression(nameof(shouldBe))] string shouldBeExpr = null)
            => Check(shouldBe, errorMsg, shouldBeExpr);
        }

        /// <summary>
        /// Do a very expensive pass attempting to coalesce free blocks as much as possible.
        /// </summary>
        internal bool TryCoalesceAllFreeBlocks()
        {
            ref var dataStartRef = ref GetDataStartRef();

            var madeProgress = false;
            ref var cur = ref GetFreeList();
            while (IsValidBlockRef(ref cur))
            {
                // Coalesce this block repeatedly, so runs of free blocks are collapsed into one
                while (TryCoalesceSingleBlock(ref dataStartRef, ref cur, out _))
                {
                    madeProgress = true;
                }

                cur = ref cur.GetNextFreeBlockRef();
            }

            return madeProgress;
        }

        /// <summary>
        /// Add a the given free block to the free list.
        /// </summary>
        private void AddToFreeList(ref byte dataStartRef, ref BlockHeader block)
        {
            Debug.Assert(block.IsFree, "Can only add free blocks to list");

            ref var freeListStartRef = ref GetFreeList();
            if (!IsValidBlockRef(ref freeListStartRef))
            {
                // Free list is empty

                block.PrevFreeBlockRefRaw = 0;
                block.NextFreeBlockRefRaw = 0;

                unsafe
                {
                    freeListStartPtr = Unsafe.AsPointer(ref block);
                }
            }
            else
            {
                Debug.Assert(freeListStartRef.IsFree, "Free list is corrupted");

                // Free list isn't empty - block can go in either the head position
                // or the one-past-the-head position depending on if block is bigger
                // than the free list head

                var freeListStartRefVal = freeListStartRef.GetRefVal();
                var blockRefVal = block.GetRefVal();

                // We know both blocks are free, so use size directly
                if (block.SizeBytesRaw >= freeListStartRef.SizeBytesRaw)
                {
                    // Block is larger, prefer allocating out of it

                    // Move block to head of list, and old head to immediately after block
                    freeListStartRef.PrevFreeBlockRefRaw = blockRefVal;
                    block.NextFreeBlockRefRaw = freeListStartRefVal;
                    block.PrevFreeBlockRefRaw = 0;

                    // Block is new head of free list
                    unsafe
                    {
                        freeListStartPtr = Unsafe.AsPointer(ref block);
                    }
                }
                else
                {
                    // Block is smaller, it's a second choice for allocations

                    ref var oldFreeListNextBlock = ref freeListStartRef.GetNextFreeBlockRef();

                    // Move block to right after free list head
                    block.PrevFreeBlockRefRaw = freeListStartRefVal;
                    block.NextFreeBlockRefRaw = freeListStartRef.NextFreeBlockRef;
                    freeListStartRef.NextFreeBlockRefRaw = blockRefVal;

                    if (IsValidBlockRef(ref oldFreeListNextBlock))
                    {
                        // Update back-pointer in next block to point to block
                        oldFreeListNextBlock.PrevFreeBlockRefRaw = blockRefVal;
                    }

                    // Head of free list is unchanged
                }
            }
        }

        /// <summary>
        /// Removes the given block from the free list.
        /// </summary>
        private void RemoveFromFreeList(ref byte dataStartRef, ref BlockHeader block)
        {
            Debug.Assert(IsValidBlockRef(ref GetFreeList()), "Shouldn't be removing from free list if free list is empty");
            Debug.Assert(block.IsFree, "Only valid for free blocks");

            var blockRefVal = block.GetRefVal();

            ref var prevFreeBlock = ref block.GetPrevFreeBlockRef();
            ref var nextFreeBlock = ref block.GetNextFreeBlockRef();

            // We've got a few different states we could be in here:
            //   1. block is the only thing in the free list
            //      => freeList == block, prevFreeBlock == null, nextFreeBlock == null
            //   2. block is the first thing in the free list, but not the only thing
            //      => freeList == block, prevFreeBlock == null, nextFreeBlock != null, nextFreeBLock.prev == block
            //   3. block is the last thing in the free list, but not the first
            //      => freeList is valid, freeListStart != block, prevFreeBlock != null, prevFreeBlock.next == block, nextFreeBlock == null
            //   4. block is in the middle of the list somewhere
            //      => freeList is valid, freeListStart != block, prevFreeBlock != null, prefFreeBlock.next == block, nextFreeBlock != null, nextFreeBlock.prev = next

            ref var freeList = ref GetFreeList();

            if (Unsafe.AreSame(ref freeList, ref block))
            {
                Debug.Assert(!IsValidBlockRef(ref prevFreeBlock), "Should be no prev pointer if block is head of free list");

                if (!IsValidBlockRef(ref nextFreeBlock))
                {
                    // We're in state #1 - block is the only thing in the free list

                    // Remove this last block from the free list, leaving it empty
                    unsafe
                    {
                        // For simplicity, treat empty free lists the same way we do end of the free list
                        freeListStartPtr = (void*)0;
                    }
                    return;
                }
                else
                {
                    // We're in state #2 - block is first thing in the free list, but not the last
                    Debug.Assert(nextFreeBlock.PrevFreeBlockRef == blockRefVal, "Broken chain in free list");

                    // NextFreeBlock is new head, it needs to prev point now
                    nextFreeBlock.PrevFreeBlockRefRaw = 0;
                    unsafe
                    {
                        freeListStartPtr = (void*)block.NextFreeBlockRef;
                    }
                    return;
                }
            }
            else
            {
                Debug.Assert(IsValidBlockRef(ref prevFreeBlock), "Should always have a prev pointer if not head of free list");

                if (!IsValidBlockRef(ref nextFreeBlock))
                {
                    // We're in state #3 - block is last thing in the free list, but not the first
                    Debug.Assert(prevFreeBlock.NextFreeBlockRef == blockRefVal, "Broken chain in free list");

                    // Remove pointer to this block from it's preceeding block
                    prevFreeBlock.NextFreeBlockRefRaw = 0;
                    return;
                }
                else
                {
                    // We're in state #4 - block is just in the middle of the free list somewhere, but not last or first
                    Debug.Assert(prevFreeBlock.NextFreeBlockRef == blockRefVal, "Broken chain in free list");
                    Debug.Assert(nextFreeBlock.PrevFreeBlockRef == blockRefVal, "Broken chain in free list");

                    // Prev needs to skip this block when going forward
                    prevFreeBlock.NextFreeBlockRefRaw = block.NextFreeBlockRef;

                    // Next needs to skip this block when going back
                    nextFreeBlock.PrevFreeBlockRefRaw = block.PrevFreeBlockRef;
                    return;
                }
            }
        }

        /// <summary>
        /// Move this given block to the head of the free list.
        /// 
        /// This assumes that <paramref name="block"/> is not already at the head of the free list, and <paramref name="freeList"/> is not
        /// empty.
        /// </summary>
        private void MoveToHeadOfFreeList(ref byte dataStartRef, ref BlockHeader freeList, ref BlockHeader block)
        {
            Debug.Assert(block.IsFree, "Should be free");
            Debug.Assert(freeList.IsFree, "Free list corrupted");
            Debug.Assert(!IsValidBlockRef(ref freeList.GetPrevFreeBlockRef()), "Free list corrupted");
            Debug.Assert(IsValidBlockRef(ref freeList.GetNextFreeBlockRef()), "Free list corrupted");

            // Because block is not head of the free list, we have two cases here
            //   1. Block is in the middle of the list somewhere
            //   2. Block is the tail of the list

            ref var prevBlock = ref block.GetPrevFreeBlockRef();
            Debug.Assert(IsValidBlockRef(ref prevBlock), "Block shouldn't be head of free list");

            ref var nextBlock = ref block.GetNextFreeBlockRef();
            if (IsValidBlockRef(ref nextBlock))
            {
                // Case 1 - block is in the middle of the list

                // Update prev.next so it points to block.next and next.prev so it points to block.prev
                prevBlock.NextFreeBlockRefRaw = block.NextFreeBlockRef;
                nextBlock.PrevFreeBlockRefRaw = block.PrevFreeBlockRef;
            }
            else
            {
                // Case 2 - block is at the end of the list

                // Remove prev's pointer to block
                prevBlock.NextFreeBlockRefRaw = 0;
            }

            // Move block to the head of the list
            freeList.PrevFreeBlockRefRaw = block.GetRefVal();
            block.NextFreeBlockRefRaw = freeList.GetRefVal();
            block.PrevFreeBlockRefRaw = 0;

            // Update freeListStartPtr to refer to block
            unsafe
            {
                freeListStartPtr = Unsafe.AsPointer(ref block);
            }
        }

        /// <summary>
        /// Attempt to coalesce a block with its adjacent block.
        /// 
        /// <paramref name="block"/> can be free or allocated, but coalescing will only succeed
        /// if the adjacent block is free.
        /// 
        /// <paramref name="newBlockSizeBytes"/> is set only if the return is true.
        /// </summary>
        private bool TryCoalesceSingleBlock(ref byte dataStartRef, ref BlockHeader block, out int newBlockSizeBytes)
        {
            ref var nextBlock = ref block.GetNextAdjacentBlockRef(ref dataStartRef, data.Length);
            if (!IsValidBlockRef(ref nextBlock) || !nextBlock.IsFree)
            {
                Unsafe.SkipInit(out newBlockSizeBytes);
                return false;
            }

            // We know nextBlock is free, so touch size directly
            var nextBlockSizeBytes = nextBlock.SizeBytesRaw;

            RemoveFromFreeList(ref dataStartRef, ref nextBlock);

            if (block.IsFree)
            {
                newBlockSizeBytes = nextBlockSizeBytes + block.SizeBytesRaw + BlockHeader.DataOffset;

                block.SizeBytesRaw = newBlockSizeBytes;
            }
            else
            {
                // Because we merged a free block into an allocated one we need to update the allocated byte total

                // We know the block is in use, so block.SizeBytesRaw is -SizeBytes
                newBlockSizeBytes = nextBlockSizeBytes - block.SizeBytesRaw + BlockHeader.DataOffset;
                UpdateDebugAllocatedBytes(block.SizeBytesRaw);

                block.SizeBytesRaw = -newBlockSizeBytes;

                UpdateDebugAllocatedBytes(newBlockSizeBytes);
            }

            return true;
        }

        /// <summary>
        /// Split an in use block, such that the current block ends up with a size equal to <paramref name="curBlockUpdateSizeBytes"/>.
        /// 
        /// Returns a reference to the NEW free block.
        /// </summary>
        private ref BlockHeader SplitInUseBlock(ref byte dataStartRef, ref BlockHeader curBlock, int curBlockUpdateSizeBytes)
        {
            Debug.Assert(curBlock.IsInUse, "Only valid for in use blocks");

            // We know the block is in use, so touch size bytes directly
            var oldSizeBytes = -curBlock.SizeBytesRaw;

            ref var newBlock = ref SplitCommon(ref curBlock, curBlockUpdateSizeBytes);

            // New block needs to be placed in free list
            AddToFreeList(ref dataStartRef, ref newBlock);

            // Because we split some bytes out of an allocated block, that means we need to remove those from allocation tracking
            UpdateDebugAllocatedBytes(-oldSizeBytes);

            // We know the block is in use, so -SizeBytesRaw will be SizeBytes (unconditionally)
            UpdateDebugAllocatedBytes(-curBlock.SizeBytesRaw);

            return ref newBlock;
        }

        /// <summary>
        /// Split a free block such that the current block ends up with a size equal to <paramref name="curBlockUpdateSizeBytes"/>.
        /// 
        /// Returns a reference to the NEW block.
        /// </summary>
        private ref BlockHeader SplitFreeBlock(ref byte dataStartRef, ref BlockHeader curBlock, int curBlockUpdateSizeBytes)
        {
            Debug.Assert(curBlock.IsFree, "Only valid for free blocks");

            ref var oldNextBlock = ref curBlock.GetNextFreeBlockRef();

            ref var newBlock = ref SplitCommon(ref curBlock, curBlockUpdateSizeBytes);

            var curBlockRefVal = curBlock.GetRefVal();

            // Update newBlock
            newBlock.PrevFreeBlockRefRaw = curBlockRefVal;
            newBlock.NextFreeBlockRefRaw = curBlock.NextFreeBlockRef;
            Debug.Assert(newBlock.IsFree, "New block shoud be free");
            Debug.Assert(ContainsRef(ref Unsafe.As<BlockHeader, byte>(ref newBlock)), "New block out of managed memory");

            var newBlockRefVal = newBlock.GetRefVal();

            // Update curBlock
            curBlock.NextFreeBlockRefRaw = newBlockRefVal;
            Debug.Assert(curBlock.IsFree, "New block shoud be free");
            Debug.Assert(ContainsRef(ref Unsafe.As<BlockHeader, byte>(ref curBlock)), "Split block out of managed memory");

            // Update the old next block if it exists
            if (IsValidBlockRef(ref oldNextBlock))
            {
                Debug.Assert(oldNextBlock.IsFree, "Should have been in free list");
                oldNextBlock.PrevFreeBlockRefRaw = newBlockRefVal;
            }

            return ref newBlock;
        }

        /// <summary>
        /// Grab the start of the managed memory we're allocating out of.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe ref byte GetDataStartRef()
        => ref Unsafe.AsRef<byte>(dataStartPtr);

        /// <summary>
        /// Get the start of the free list.
        /// 
        /// If the free list is empty, returns a null ref.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe ref BlockHeader GetFreeList()
        => ref Unsafe.AsRef<BlockHeader>(freeListStartPtr);

        /// <summary>
        /// Turn a reference obtained from <see cref="BlockHeader.DataReference"/> back into a <see cref="BlockHeader"/> reference.
        /// </summary>
        private ref BlockHeader GetBlockRef(ref byte dataStartRef, ref byte userDataRef)
        {
            Debug.Assert(!Unsafe.AreSame(ref dataStartRef, ref userDataRef), "User data is actually 0 size alloc, that doesn't make sense");

            ref var blockStartByteRef = ref Unsafe.Add(ref userDataRef, -BlockHeader.DataOffset);

            Debug.Assert(!Unsafe.IsAddressLessThan(ref blockStartByteRef, ref dataStartRef), "User data is before managed memory");
            Debug.Assert(!Unsafe.IsAddressGreaterThan(ref blockStartByteRef, ref Unsafe.Add(ref dataStartRef, data.Length - 1)), "User data is after managed memory");

            return ref Unsafe.As<byte, BlockHeader>(ref blockStartByteRef);
        }

        /// <summary>
        /// In DEBUG builds, keep track of how many bytes we've allocated for testing purposes.
        /// </summary>
        [Conditional("DEBUG")]
        private void UpdateDebugAllocatedBytes(int by)
        => debugAllocatedBytes += by;

        /// <summary>
        /// Common logic for splitting blocks.
        /// 
        /// Block here can either be free or in use, so don't make any assumptionsin here.
        /// </summary>
        private static ref BlockHeader SplitCommon(ref BlockHeader curBlock, int curBlockUpdateSizeBytes)
        {
            Debug.Assert(curBlockUpdateSizeBytes >= LuaAllocMinSizeBytes, "Shouldn't split an existing block to be this small");

            ref var curBlockData = ref curBlock.DataReference;
            ref var newBlockStartByteRef = ref Unsafe.AddByteOffset(ref curBlockData, curBlockUpdateSizeBytes);
            ref var newBlock = ref Unsafe.As<byte, BlockHeader>(ref newBlockStartByteRef);

            int newBlockSizeBytes;
            if (curBlock.IsFree)
            {
                // We know curBlock is free, so touch SizeBytesRaw directly
                newBlockSizeBytes = curBlock.SizeBytesRaw - BlockHeader.DataOffset - curBlockUpdateSizeBytes;

                curBlock.SizeBytesRaw = curBlockUpdateSizeBytes;
            }
            else
            {
                Debug.Assert(curBlock.IsInUse, "Invalid block state");

                // We know curBlock is is inuse, so touch SizeBytesRaw directly
                newBlockSizeBytes = -curBlock.SizeBytesRaw - BlockHeader.DataOffset - curBlockUpdateSizeBytes;

                curBlock.SizeBytesRaw = -curBlockUpdateSizeBytes;
            }

            Debug.Assert(newBlockSizeBytes >= LuaAllocMinSizeBytes, "Shouldn't create a new block this small");

            // The new block is always free, so positive size
            newBlock.SizeBytesRaw = newBlockSizeBytes;


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
            Debug.Assert(BitOperations.IsPow2(LuaAllocMinSizeBytes), "Assumes min allocation is a power of 2");

            // To avoid special casing 0, we can reserve up to an extra LuaAllocMinSizeBytes
            var ret = (sizeBytes + LuaAllocMinSizeBytes) & ~(LuaAllocMinSizeBytes - 1);

            Debug.Assert(ret > 0, "Rounding logic invalid - not positive");
            Debug.Assert(ret >= sizeBytes, "Rounding logic invalid - did not round up");
            Debug.Assert(ret % LuaAllocMinSizeBytes == 0, "Rounding logic invalid - not a whole multiple of step size");

            return ret;
        }

        /// <summary>
        /// Check if <paramref name="blockRef"/> is valid.
        /// 
        /// Pulled out to indicate intent, it's basically just a null check
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe bool IsValidBlockRef(ref BlockHeader blockRef)
        => Unsafe.AsPointer(ref blockRef) != (void*)0;
    }
}