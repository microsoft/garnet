// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Garnet.server
{
    internal struct ExtensibleMap<T>
    {
        internal T[] map;
        internal int currIndex = -1;
        readonly bool descIds;
        readonly int minId;
        readonly int maxSize;
        internal readonly ReaderWriterLockSlim mapLock = new();

        private int GetIdFromIndex(int index) => descIds ? minId - index : index;

        private int GetIndexFromId(int id) => descIds ? minId - id : id;

        public ExtensibleMap(int minSize, int minId, int maxId)
        {
            this.map = new T[minSize];
            this.minId = minId;
            this.maxSize = Math.Abs(maxId - minId) + 1;
            this.descIds = minId > maxId;
        }

        public bool TryGetValue(int id, out T value)
        {
            value = default;
            var idx = GetIndexFromId(id);
            return TryGetSafe(idx, out value);
        }

        public bool TrySetValue(int id, ref T value)
        {
            var idx = GetIndexFromId(id);
            return TrySetSafe(idx, ref value);
        }

        public bool TryGetNextId(out int id)
        {
            id = -1;
            var nextIdx = Interlocked.Increment(ref currIndex);
            
            if (nextIdx >= maxSize)
                return false;
            id = GetIdFromIndex(nextIdx);

            return true;
        }

        public int FirstIdSafe(Func<T, bool> predicate)
        {
            mapLock.EnterReadLock();
            try
            {
                for (var i = 0; i <= currIndex; i++)
                {
                    if (predicate(map[i]))
                        return GetIdFromIndex(i);
                }
            }
            finally
            {
                mapLock.ExitReadLock();
            }

            return -1;
        }

        private bool TryGetSafe(int index, out T value)
        {
            value = default;
            mapLock.EnterReadLock();
            try
            {
                if (index < 0 || index > map.Length) return false;

                value = map[index];
                return value != null;
            }
            finally
            {
                mapLock.ExitReadLock();
            }
        }

        private bool TrySetSafe(int index, ref T value)
        {
            if (index < 0 || index >= maxSize) return false;

            mapLock.EnterUpgradeableReadLock();
            try
            {
                if (index < map.Length)
                {
                    map[index] = value;
                    return true;
                }

                mapLock.EnterWriteLock();
                try
                {
                    if (index < map.Length)
                    {
                        map[index] = value;
                        return true;
                    }

                    var newSize = map.Length;
                    while (index >= newSize)
                    {
                        newSize = Math.Min(maxSize, newSize * 2);
                    }

                    var newMap = new T[newSize];
                    Array.Copy(map, newMap, map.Length);
                    map = newMap;
                    map[index] = value;
                    return true;
                }
                finally
                {
                    mapLock.ExitWriteLock();
                }
            }
            finally
            {
                mapLock.ExitUpgradeableReadLock();
            }
        }
    }

    internal static class ExtensibleMapExtensions
    {
        internal static bool MatchCommandSafe<T>(this ExtensibleMap<T> eMap, ReadOnlySpan<byte> cmd, out T value)
            where T : ICustomCommand
        {
            value = default;
            eMap.mapLock.EnterReadLock();
            try
            {
                for (var i = 0; i <= eMap.currIndex; i++)
                {
                    var currCmd = eMap.map[i];
                    if (cmd.SequenceEqual(new ReadOnlySpan<byte>(currCmd.Name)))
                    {
                        value = currCmd;
                        return true;
                    }
                }
            }
            finally
            {
                eMap.mapLock.ExitReadLock();
            }

            return false;
        }

        internal static bool MatchSubCommandSafe<T>(this ExtensibleMap<T> eMap, ReadOnlySpan<byte> cmd, out CustomObjectCommand value)
            where T : CustomObjectCommandWrapper
        {
            value = default;
            eMap.mapLock.EnterReadLock();
            try
            {
                for (var i = 0; i <= eMap.currIndex; i++)
                {
                    if (eMap.map[i].commandMap.MatchCommandSafe(cmd, out value))
                        return true;
                }
            }
            finally
            {
                eMap.mapLock.ExitReadLock();
            }

            return false;
        }
    }
}
