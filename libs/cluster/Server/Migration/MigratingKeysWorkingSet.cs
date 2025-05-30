// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal class MigratingKeysWorkingSet
    {
        readonly Dictionary<PinnedSpanByte, KeyMigrationStatus> WorkingSet;
        SingleWriterMultiReaderLock keyDictLock;

        public MigratingKeysWorkingSet()
        {
            WorkingSet = new Dictionary<PinnedSpanByte, KeyMigrationStatus>(PinnedSpanByteComparer.Instance);
        }

        public IEnumerable<KeyValuePair<PinnedSpanByte, KeyMigrationStatus>> GetKeys()
        {
            foreach (var pair in WorkingSet)
                yield return pair;
        }

        /// <summary>
        /// Check if migration working is empty or null
        /// </summary>
        /// <returns></returns>
        public bool IsNullOrEmpty()
            => WorkingSet == null || WorkingSet.Count == 0;

        /// <summary>
        /// Add key to migration working set with corresponding status
        /// </summary>
        /// <param name="key"></param>
        /// <param name="status"></param>
        public bool TryAdd(PinnedSpanByte key, KeyMigrationStatus status)
        {
            try
            {
                keyDictLock.WriteLock();
                return WorkingSet.TryAdd(key, status);
            }
            finally
            {
                keyDictLock.WriteUnlock();
            }
        }

        /// <summary>
        /// Try get status of corresponding key in working set
        /// </summary>
        /// <param name="key"></param>
        /// <param name="status"></param>
        /// <returns></returns>
        public bool TryGetValue(PinnedSpanByte key, out KeyMigrationStatus status)
        {
            try
            {
                keyDictLock.ReadLock();
                // If key is not queued for migration then
                if (!WorkingSet.TryGetValue(key, out status))
                    return false;
            }
            finally
            {
                keyDictLock.ReadUnlock();
            }
            return true;
        }

        /// <summary>
        /// Update status of an existing key
        /// </summary>
        /// <param name="key"></param>
        /// <param name="status"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpdateStatus(PinnedSpanByte key, KeyMigrationStatus status)
            => WorkingSet[key] = status;

        /// <summary>
        /// Clear keys from working set
        /// </summary>
        public void ClearKeys()
        {
            try
            {
                keyDictLock.WriteLock();
                WorkingSet.Clear();
            }
            finally
            {
                keyDictLock.WriteUnlock();
            }
        }
    }
}