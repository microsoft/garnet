// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Garnet.server
{
    internal class ExtensibleMap<T>
    {
        protected T[] map;
        protected int currIndex = -1;
        protected readonly bool descIds;
        protected readonly int maxValue;
        protected readonly int maxSize;
        protected readonly int startOffset;
        protected ReaderWriterLockSlim mapLock = new();

        public int GetIdFromIndex(int index) => descIds ? maxValue - index : index;

        public int GetIndexFromId(int cmdId) => descIds ? maxValue - cmdId : cmdId;

        public ExtensibleMap(int minSize, int maxSize, int startOffset, int maxValue, bool descIds = true)
        {
            this.map = new T[minSize];
            this.maxSize = maxSize;
            this.startOffset = startOffset;
            this.maxValue = maxValue;
            this.descIds = descIds;
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

    internal class ExtensibleCustomCommandMap<T>(int minSize, int maxSize, int startOffset, int maxValue, bool descIds = true) : ExtensibleMap<T>(minSize, maxSize, startOffset, maxValue, descIds)
        where T : ICustomCommand
    {
        public bool MatchCommandSafe(ReadOnlySpan<byte> cmd, out T value)
        {
            value = default;
            mapLock.EnterReadLock();
            try
            {
                for (var i = 0; i <= currIndex; i++)
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

    internal class CustomCommandMap(int minSize, int maxSize, int startOffset) : ExtensibleCustomCommandMap<CustomRawStringCommand>(minSize, maxSize, startOffset, ushort.MaxValue - 1);

    internal class CustomTransactionMap(int minSize, int maxSize, int startOffset) : ExtensibleCustomCommandMap<CustomTransaction>(minSize, maxSize, startOffset, byte.MaxValue);

    internal class CustomProcedureMap(int minSize, int maxSize, int startOffset) : ExtensibleCustomCommandMap<CustomProcedureWrapper>(minSize, maxSize, startOffset, byte.MaxValue);

    internal class CustomObjectCommandMap(int minSize, int maxSize) : ExtensibleMap<CustomObjectCommandWrapper>(minSize, maxSize, CustomCommandManager.TypeIdStartOffset, (byte)GarnetObjectTypeExtensions.FirstSpecialObjectType - 1)
    {
        public bool MatchSubCommandSafe(ReadOnlySpan<byte> cmd, out CustomObjectCommand value)
        {
            value = default;
            mapLock.EnterReadLock();
            try
            {
                for (var i = 0; i <= currIndex; i++)
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
