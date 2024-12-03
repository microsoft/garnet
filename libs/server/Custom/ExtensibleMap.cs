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

        public int GetIdFromIndex(int index) => descIds ? minId - index : index;

        public int GetIndexFromId(int cmdId) => descIds ? minId - cmdId : cmdId;

        public ExtensibleMap(int minSize, int minId, int maxId)
        {
            this.map = new T[minSize];
            this.minId = minId;
            this.maxSize = Math.Abs(maxId - minId) + 1;
            this.descIds = minId > maxId;
        }

        public T this[int index]
        {
            get => GetSafe(index);
            set => SetSafe(index, value);
        }

        public bool TryGetNextIndex(out int id)
        {
            id = Interlocked.Increment(ref currIndex);
            return id < maxSize;
        }

        public int FirstIndexSafe(Func<T, bool> predicate)
        {
            mapLock.EnterReadLock();
            try
            {
                for (var i = 0; i <= currIndex; i++)
                {
                    if (predicate(map[i]))
                        return i;
                }
            }
            finally
            {
                mapLock.ExitReadLock();
            }

            return -1;
        }

        private T GetSafe(int index)
        {
            mapLock.EnterReadLock();
            try
            {
                return map[index];
            }
            finally
            {
                mapLock.ExitReadLock();
            }
        }

        private void SetSafe(int index, T value)
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, maxSize, nameof(index));

            mapLock.EnterUpgradeableReadLock();
            try
            {
                if (index < map.Length)
                {
                    map[index] = value;
                    return;
                }

                mapLock.EnterWriteLock();
                try
                {
                    if (index < map.Length)
                    {
                        map[index] = value;
                        return;
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
