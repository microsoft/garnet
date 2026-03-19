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
        /// Array to keep metadata of watched keys
        /// </summary>
        WatchedKeySlice[] keySlices;

        /// <summary>
        /// Version map for watch validation
        /// </summary>
        readonly WatchVersionMap versionMap;

        readonly int initialSliceBufferSize;
        readonly ScratchBufferAllocator scratchBufferAllocator;
        int sliceBufferSize;
        int sliceCount;

        public WatchedKeysContainer(int size, WatchVersionMap versionMap, ScratchBufferAllocator scratchBufferAllocator)
        {
            this.versionMap = versionMap;
            sliceCount = 0;
            initialSliceBufferSize = size;
            this.scratchBufferAllocator = scratchBufferAllocator;
        }

        /// <summary>
        /// Reset watched keys
        /// </summary>
        public void Reset()
        {
            sliceCount = 0;
            scratchBufferAllocator.Reset();
        }

        public bool RemoveWatch(PinnedSpanByte key)
        {
            for (int i = 0; i < sliceCount; i++)
            {
                if (key.ReadOnlySpan.SequenceEqual(keySlices[i].slice.ReadOnlySpan))
                {
                    keySlices[i].isWatched = false;
                    return true;
                }
            }
            return false;
        }

        public void AddWatch(PinnedSpanByte key)
        {
            if (sliceCount >= sliceBufferSize)
            {
                // Double the struct buffer
                sliceBufferSize = sliceBufferSize == 0 ? initialSliceBufferSize : sliceBufferSize * 2;
                var _oldBuffer = keySlices;
                keySlices = GC.AllocateUninitializedArray<WatchedKeySlice>(sliceBufferSize, true);
                if (_oldBuffer != null) Array.Copy(_oldBuffer, keySlices, _oldBuffer.Length);
            }

            // Copy key bytes into scratch buffer (independent of receive buffer lifetime)
            var keySlice = scratchBufferAllocator.CreateArgSlice(key.ReadOnlySpan);

            keySlices[sliceCount].slice = keySlice;
            keySlices[sliceCount].isWatched = true;
            keySlices[sliceCount].hash = Utility.HashBytes(keySlice.ReadOnlySpan);
            keySlices[sliceCount].version = versionMap.ReadVersion(keySlices[sliceCount].hash);

            sliceCount++;
        }

        /// <summary>
        /// Validate record version to validate that records are unmodified
        /// </summary>
        /// <returns></returns>
        public bool ValidateWatchVersion()
        {
            for (int i = 0; i < sliceCount; i++)
            {
                WatchedKeySlice key = keySlices[i];
                if (!key.isWatched) continue;
                if (versionMap.ReadVersion(key.hash) != key.version)
                    return false;
            }
            return true;
        }

        public bool SaveKeysToLock(TransactionManager txnManager)
        {
            for (int i = 0; i < sliceCount; i++)
            {
                WatchedKeySlice watchedKeySlice = keySlices[i];
                if (!watchedKeySlice.isWatched) continue;

                var slice = keySlices[i].slice;
                txnManager.SaveKeyEntryToLock(slice, LockType.Shared);
            }
            return true;
        }

        public bool SaveKeysToKeyList(TransactionManager txnManager)
        {
            for (int i = 0; i < sliceCount; i++)
            {
                txnManager.SaveKeyArgSlice(keySlices[i].slice);
            }
            return true;
        }
    }
}