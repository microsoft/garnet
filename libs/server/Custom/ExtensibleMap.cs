// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Garnet.server
{
    internal class ExtensibleMap<T>
    {
        protected T[] map;
        protected int currId;
        protected readonly int maxSize;
        protected ReaderWriterLockSlim mapLock = new();

        public ExtensibleMap(int minSize, int maxSize)
        {
            this.map = new T[minSize];
            this.maxSize = maxSize;
        }

        public T this[int index]
        {
            get => GetSafe(index);
            set => SetSafe(index, value);
        }

        public bool TryGetNextId(out int id)
        {
            id = Interlocked.Increment(ref currId);
            return id < maxSize;
        }

        public int FirstIndexSafe(Func<T, bool> predicate)
        {
            mapLock.EnterReadLock();
            try
            {
                for (var i = 0; i <= currId; i++)
                {
                    if (predicate(this[i]))
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

    internal class ExtensibleCustomCommandMap<T>(int minSize, int maxSize) : ExtensibleMap<T>(minSize, maxSize)
        where T : ICustomCommand
    {
        public bool MatchCommandSafe(ReadOnlySpan<byte> cmd, out T value)
        {
            value = default;
            mapLock.EnterReadLock();
            try
            {
                for (var i = 0; i <= currId; i++)
                {
                    if (cmd.SequenceEqual(new ReadOnlySpan<byte>(map[i].Name)))
                    {
                        value = map[i];
                        return true;
                    }
                }
            }
            finally
            {
                mapLock.ExitReadLock();
            }

            return false;
        }
    }

    internal class CustomCommandMap(int minSize, int maxSize) : ExtensibleCustomCommandMap<CustomRawStringCommand>(minSize, maxSize);
    internal class CustomTransactionMap(int minSize, int maxSize) : ExtensibleCustomCommandMap<CustomTransaction>(minSize, maxSize);
    internal class CustomProcedureMap(int minSize, int maxSize) : ExtensibleCustomCommandMap<CustomProcedureWrapper>(minSize, maxSize);

    internal class CustomObjectCommandMap(int minSize, int maxSize) : ExtensibleMap<CustomObjectCommandWrapper>(minSize, maxSize)
    {
        public bool MatchSubCommandSafe(ReadOnlySpan<byte> cmd, out CustomObjectCommand value)
        {
            value = default;
            mapLock.EnterReadLock();
            try
            {
                for (var i = 0; i <= currId; i++)
                {
                    if (map[i].commandMap.MatchCommandSafe(cmd, out value))
                        return true;
                }
            }
            finally
            {
                mapLock.ExitReadLock();
            }

            return false;
        }
    }
}
