// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Garnet.server
{
    /// <summary>
    /// This class defines a map of items of type T whose keys are a specified range of IDs (can be descending / ascending)
    /// The size of the underlying array containing the items doubles in size as needed
    /// </summary>
    /// <typeparam name="T">Type of command to store</typeparam>
    internal struct ExpandableMap<T>
    {
        /// <summary>
        /// The underlying array containing the commands
        /// </summary>
        internal T[] map;

        /// <summary>
        /// Reader-writer lock for the underlying command array
        /// </summary>
        internal readonly ReaderWriterLockSlim mapLock = new();

        /// <summary>
        /// Last set index in underlying array
        /// </summary>
        internal int lastSetIndex = -1;

        // The last requested index for assignment
        int currIndex = -1;
        // Initial array size
        readonly int minSize;
        // Value of min item ID
        readonly int minId;
        // Value of max item ID
        readonly int maxSize;
        // True if item IDs are in descending order
        readonly bool descIds;

        /// <summary>
        /// Creates a new instance of ExpandableMap
        /// </summary>
        /// <param name="minSize">Initial size of underlying array</param>
        /// <param name="minId">The minimal item ID value</param>
        /// <param name="maxId">The maximal item ID value (can be smaller than minId for descending order of IDs)</param>
        public ExpandableMap(int minSize, int minId, int maxId)
        {
            this.map = [];
            this.minSize = minSize;
            this.minId = minId;
            this.maxSize = Math.Abs(maxId - minId) + 1;
            this.descIds = minId > maxId;
        }

        /// <summary>
        /// Try to get item by ID
        /// </summary>
        /// <param name="id">Item ID</param>
        /// <param name="value">Item value</param>
        /// <returns>True if item found</returns>
        public bool TryGetValue(int id, out T value)
        {
            var idx = GetIndexFromId(id);
            return TryGetSafe(idx, out value);
        }

        /// <summary>
        /// Checks if ID is mapped to a value in underlying array
        /// </summary>
        /// <param name="id">Item ID</param>
        /// <returns>True if ID exists</returns>
        public bool Exists(int id)
        {
            var idx = GetIndexFromId(id);
            return idx >= 0 && idx <= lastSetIndex;
        }

        /// <summary>
        /// Try to get item by ref by ID
        /// </summary>
        /// <param name="id">Item ID</param>
        /// <returns>Item value</returns>
        public ref T GetValueByRef(int id)
        {
            var idx = GetIndexFromId(id);
            return ref GetSafeByRef(idx);
        }

        /// <summary>
        /// Try to set item by ID
        /// </summary>
        /// <param name="id">Item ID</param>
        /// <param name="value">Item value</param>
        /// <returns>True if assignment succeeded</returns>
        public bool TrySetValue(int id, ref T value)
        {
            var idx = GetIndexFromId(id);
            return TrySetSafe(idx, ref value);
        }

        /// <summary>
        /// Get next item ID for assignment
        /// </summary>
        /// <param name="id">Item ID</param>
        /// <returns>True if item ID available</returns>
        public bool TryGetNextId(out int id)
        {
            id = -1;
            var nextIdx = Interlocked.Increment(ref currIndex);

            if (nextIdx >= maxSize)
                return false;
            id = GetIdFromIndex(nextIdx);

            return true;
        }

        /// <summary>
        /// Find first ID in map of item that fulfills specified predicate
        /// </summary>
        /// <param name="predicate">Predicate</param>
        /// <param name="id">ID if found, otherwise -1</param>
        /// <returns>True if ID found</returns>
        public bool TryFirstIdSafe(Func<T, bool> predicate, out int id)
        {
            id = -1;
            mapLock.EnterReadLock();
            try
            {
                for (var i = 0; i <= currIndex; i++)
                {
                    if (predicate(map[i]))
                    {
                        id = GetIdFromIndex(i);
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

        /// <summary>
        /// Maps map index to item ID
        /// </summary>
        /// <param name="index">Map index</param>
        /// <returns>Item ID</returns>
        private int GetIdFromIndex(int index) => descIds ? minId - index : index;

        /// <summary>
        /// Maps an item ID to a map index
        /// </summary>
        /// <param name="id">Item ID</param>
        /// <returns>Map index</returns>
        private int GetIndexFromId(int id) => descIds ? minId - id : id;

        /// <summary>
        /// Thread-safe method for retrieving an item from the underlying item array
        /// </summary>
        /// <param name="index">Index of item</param>
        /// <param name="value">Value of item</param>
        /// <returns>True if item found</returns>
        private bool TryGetSafe(int index, out T value)
        {
            value = default;
            mapLock.EnterReadLock();
            try
            {
                if (index < 0 || index > lastSetIndex) 
                    return false;

                value = map[index];
                return true;
            }
            finally
            {
                mapLock.ExitReadLock();
            }
        }

        /// <summary>
        /// Thread-safe method for retrieving an item by reference from the underlying item array
        /// </summary>
        /// <param name="index">Index of item</param>
        /// <returns>Value of item</returns>
        private ref T GetSafeByRef(int index)
        {
            mapLock.EnterReadLock();
            try
            {
                if (index < 0 || index > lastSetIndex)
                    throw new ArgumentOutOfRangeException(nameof(index));

                return ref map[index];
            }
            finally
            {
                mapLock.ExitReadLock();
            }
        }

        /// <summary>
        /// Thread-safe method for setting an item in the underlying array at a specified index
        /// </summary>
        /// <param name="index">Index of item</param>
        /// <param name="value">Value of item</param>
        /// <returns>True if set successful</returns>
        private bool TrySetSafe(int index, ref T value)
        {
            if (index < 0 || index >= maxSize) return false;

            mapLock.EnterUpgradeableReadLock();
            try
            {
                // If index within array bounds, set item
                if (index < map.Length)
                {
                    map[index] = value;
                    lastSetIndex = index;
                    return true;
                }

                mapLock.EnterWriteLock();
                try
                {
                    // If index within array bounds, set item
                    if (index < map.Length)
                    {
                        map[index] = value;
                        lastSetIndex = index;
                        return true;
                    }

                    // Double new array size until item can fit
                    var newSize = Math.Max(map.Length, minSize);
                    while (index >= newSize)
                    {
                        newSize = Math.Min(maxSize, newSize * 2);
                    }

                    // Create new array, copy existing items and set new item
                    var newMap = new T[newSize];
                    Array.Copy(map, newMap, map.Length);
                    map = newMap;
                    map[index] = value;
                    lastSetIndex = index;
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

    /// <summary>
    /// Extension methods for ExpandableMap
    /// </summary>
    internal static class ExtensibleMapExtensions
    {
        /// <summary>
        /// Match command name with existing commands in map and return first matching instance
        /// </summary>
        /// <typeparam name="T">Type of command</typeparam>
        /// <param name="eMap">Current instance of ExpandableMap</param>
        /// <param name="cmd">Command name to match</param>
        /// <param name="value">Value of command found</param>
        /// <returns>True if command found</returns>
        internal static bool MatchCommandSafe<T>(this ExpandableMap<T> eMap, ReadOnlySpan<byte> cmd, out T value)
            where T : ICustomCommand
        {
            value = default;
            eMap.mapLock.EnterReadLock();
            try
            {
                for (var i = 0; i <= eMap.lastSetIndex; i++)
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

        /// <summary>
        /// Match sub-command name with existing sub-commands in map and return first matching instance
        /// </summary>
        /// <typeparam name="T">Type of command</typeparam>
        /// <param name="eMap">Current instance of ExpandableMap</param>
        /// <param name="cmd">Sub-command name to match</param>
        /// <param name="value">Value of sub-command found</param>
        /// <returns></returns>
        internal static bool MatchSubCommandSafe<T>(this ExpandableMap<T> eMap, ReadOnlySpan<byte> cmd, out CustomObjectCommand value)
            where T : CustomObjectCommandWrapper
        {
            value = default;
            eMap.mapLock.EnterReadLock();
            try
            {
                for (var i = 0; i <= eMap.lastSetIndex; i++)
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
