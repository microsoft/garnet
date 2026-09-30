// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// A container per session to store information of watched keys
    /// </summary>
    internal sealed unsafe class WatchedKeysContainer
    {
        /// <summary>
        /// Array to keep watched keys data
        /// </summary>
        WatchedKeySlice[] keySlices;

        /// <summary>
        /// Version map for watch validation
        /// </summary>
        /// <remarks>Keyed by key hash alone; hash collisions are acceptable by design.</remarks>
        readonly WatchVersionMap versionMap;

        readonly int initialSliceBufferSize;

        /// <summary>
        /// Allocator holding the copied watched-key bytes.
        /// </summary>
        /// <remarks>
        /// Owned by this container rather than shared with the transaction manager's scratch allocator. A watch
        /// outlives the transactions taken while it is held: an internal transaction (SMOVE, LMOVE, RENAME) commits
        /// with <c>internal_txn: true</c>, which resets the transaction allocator without clearing this container, and
        /// the next transaction's keys are then written over the still-watched bytes. What
        /// <see cref="SaveKeysToLock"/> locks and <see cref="SaveKeysToKeyList"/> slot-verifies is read from those
        /// bytes, so the transaction would lock and verify a key the client never watched.
        /// </remarks>
        readonly ScratchBufferAllocator watchScratchBufferAllocator = new();
        int sliceBufferSize;
        int sliceCount;

        public WatchedKeysContainer(int size, WatchVersionMap versionMap)
        {
            this.versionMap = versionMap;
            sliceCount = 0;
            initialSliceBufferSize = size;
        }

        /// <summary>
        /// Reset watched keys
        /// </summary>
        public void Reset()
        {
            sliceCount = 0;
            watchScratchBufferAllocator.Reset();
        }

        /// <summary>
        /// Stop watching a key. Matches on the full key bytes, not the hash, and soft-deletes the entry by clearing
        /// <see cref="WatchedKeySlice.isWatched"/>.
        /// </summary>
        public bool RemoveWatch(PinnedSpanByte key)
        {
            for (var i = 0; i < sliceCount; i++)
            {
                if (key.ReadOnlySpan.SequenceEqual(keySlices[i].slice.ReadOnlySpan))
                {
                    keySlices[i].isWatched = false;
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Start watching a key: copy its bytes into this container's allocator and record the current version of its
        /// hash slot.
        /// </summary>
        /// <remarks>
        /// The hash is the only identity carried into <see cref="versionMap"/>; hash collisions are acceptable by
        /// design, see <see cref="WatchedKeySlice.hash"/>. The copied bytes serve locking and slot verification.
        /// </remarks>
        public void AddWatch(PinnedSpanByte key)
        {
            if (sliceCount >= sliceBufferSize)
            {
                // Double the struct buffer
                sliceBufferSize = sliceBufferSize == 0 ? initialSliceBufferSize : sliceBufferSize * 2;
                var oldBuffer = keySlices;
                keySlices = GC.AllocateUninitializedArray<WatchedKeySlice>(sliceBufferSize, true);
                if (oldBuffer != null) Array.Copy(oldBuffer, keySlices, oldBuffer.Length);
            }

            // Copy key bytes into scratch buffer (independent of receive buffer lifetime)
            var keySlice = watchScratchBufferAllocator.CreateArgSlice(key.ReadOnlySpan);

            keySlices[sliceCount].slice = keySlice;
            keySlices[sliceCount].isWatched = true;
            keySlices[sliceCount].hash = Utility.HashBytes(keySlice.ReadOnlySpan);
            keySlices[sliceCount].version = versionMap.ReadVersion(keySlices[sliceCount].hash);

            sliceCount++;
        }

        /// <summary>
        /// Validate record version to validate that records are unmodified
        /// </summary>
        /// <returns>True if every watched key's version slot still holds the value read at WATCH; false otherwise.</returns>
        /// <remarks>
        /// Compares by hash alone, so a write to a different key sharing a version slot fails this check and aborts the
        /// transaction. Hash collisions are acceptable by design; see <see cref="WatchedKeySlice.hash"/>.
        /// </remarks>
        public bool ValidateWatchVersion()
        {
            for (var i = 0; i < sliceCount; i++)
            {
                var key = keySlices[i];
                if (!key.isWatched) continue;
                if (versionMap.ReadVersion(key.hash) != key.version)
                    return false;
            }
            return true;
        }

        /// <summary>
        /// Add each still-watched key to the transaction's lock set, using the copied key bytes rather than the hash.
        /// </summary>
        public bool SaveKeysToLock(TransactionManager txnManager)
        {
            for (var i = 0; i < sliceCount; i++)
            {
                var watchedKeySlice = keySlices[i];
                if (!watchedKeySlice.isWatched) continue;

                var slice = keySlices[i].slice;
                txnManager.SaveKeyEntryToLock(slice, LockType.Shared);
            }
            return true;
        }

        /// <summary>
        /// Add every watched key to the transaction's key list for cluster slot verification, using the copied key
        /// bytes rather than the hash.
        /// </summary>
        public bool SaveKeysToKeyList(TransactionManager txnManager)
        {
            for (var i = 0; i < sliceCount; i++)
            {
                txnManager.SaveKeyArgSlice(keySlices[i].slice);
            }
            return true;
        }
    }
}