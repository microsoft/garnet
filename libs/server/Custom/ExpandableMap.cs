// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Garnet.server
{
    /// <summary>
    /// This interface describes an API for a map of items of type T whose keys are a specified range of IDs (can be descending / ascending)
    /// The size of the underlying array containing the items doubles in size as needed.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal interface IExpandableMap<T>
    {
        /// <summary>
        /// Checks if ID is mapped to a value in underlying array
        /// </summary>
        /// <param name="id">Item ID</param>
        /// <returns>True if ID exists</returns>
        bool Exists(int id);

        /// <summary>
        /// Try to get item by ID
        /// </summary>
        /// <param name="id">Item ID</param>
        /// <param name="value">Item value</param>
        /// <returns>True if item found</returns>
        bool TryGetValue(int id, out T value);

        /// <summary>
        /// Try to get item by ref by ID
        /// </summary>
        /// <param name="id">Item ID</param>
        /// <returns>Item value</returns>
        ref T GetValueByRef(int id);

        /// <summary>
        /// Try to set item by ID
        /// </summary>
        /// <param name="id">Item ID</param>
        /// <param name="value">Item value</param>
        /// <returns>True if assignment succeeded</returns>
        bool TrySetValue(int id, ref T value);

        /// <summary>
        /// Get next item ID for assignment
        /// </summary>
        /// <param name="id">Item ID</param>
        /// <returns>True if item ID available</returns>
        bool TryGetNextId(out int id);

        /// <summary>
        /// Find first ID in map of item that fulfills specified predicate
        /// </summary>
        /// <param name="predicate">Predicate</param>
        /// <param name="id">ID if found, otherwise -1</param>
        /// <returns>True if ID found</returns>
        bool TryGetFirstId(Func<T, bool> predicate, out int id);
    }

    /// <summary>
    /// This struct defines a map of items of type T whose keys are a specified range of IDs (can be descending / ascending)
    /// The size of the underlying array containing the items doubles in size as needed.
    /// This struct is not thread-safe, for a thread-safe option see ConcurrentExpandableMap.
    /// </summary>
    /// <typeparam name="T">Type of item to store</typeparam>
    internal struct ExpandableMap<T> : IExpandableMap<T>
    {
        /// <summary>
        /// The underlying array containing the items
        /// </summary>
        internal T[] map;

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
            this.map = null;
            this.minSize = minSize;
            this.minId = minId;
            this.maxSize = Math.Abs(maxId - minId) + 1;
            this.descIds = minId > maxId;
        }

        /// <inheritdoc />
        public bool TryGetValue(int id, out T value)
        {
            value = default;
            var idx = GetIndexFromId(id);
            if (idx < 0 || idx > lastSetIndex)
                return false;

            value = map[idx];
            return true;
        }

        /// <inheritdoc />
        public bool Exists(int id)
        {
            var idx = GetIndexFromId(id);
            return idx >= 0 && idx <= lastSetIndex;
        }

        /// <inheritdoc />
        public ref T GetValueByRef(int id)
        {
            var idx = GetIndexFromId(id);
            if (idx < 0 || idx > lastSetIndex)
                throw new ArgumentOutOfRangeException(nameof(idx));

            return ref map[idx];
        }

        /// <inheritdoc />
        public bool TrySetValue(int id, ref T value) => TrySetValue(id, ref value, false);

        /// <inheritdoc />
        public bool TryGetNextId(out int id)
        {
            id = -1;
            var nextIdx = ++currIndex;

            if (nextIdx >= maxSize)
                return false;
            id = GetIdFromIndex(nextIdx);

            return true;
        }

        /// <inheritdoc />
        public bool TryGetFirstId(Func<T, bool> predicate, out int id)
        {
            id = -1;
            for (var i = 0; i <= currIndex; i++)
            {
                if (predicate(map[i]))
                {
                    id = GetIdFromIndex(i);
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Try to set item by ID
        /// </summary>
        /// <param name="id">Item ID</param>
        /// <param name="value">Item value</param>
        /// <param name="noExpansion">True if should not attempt to expand the underlying array</param>
        /// <returns>True if assignment succeeded</returns>
        internal bool TrySetValue(int id, ref T value, bool noExpansion)
        {
            var idx = GetIndexFromId(id);
            if (idx < 0 || idx >= maxSize) return false;

            // If index within array bounds, set item
            if (map != null && idx < map.Length)
            {
                map[idx] = value;
                lastSetIndex = idx;
                return true;
            }

            if (noExpansion) return false;

            // Double new array size until item can fit
            var newSize = map != null ? Math.Max(map.Length, minSize) : minSize;
            while (idx >= newSize)
            {
                newSize = Math.Min(maxSize, newSize * 2);
            }

            // Create new array, copy existing items and set new item
            var newMap = new T[newSize];
            if (map != null)
            {
                Array.Copy(map, newMap, map.Length);
            }

            map = newMap;
            map[idx] = value;
            lastSetIndex = idx;
            return true;
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
    }

    /// <summary>
    /// This struct defines a map of items of type T whose keys are a specified range of IDs (can be descending / ascending)
    /// The size of the underlying array containing the items doubles in size as needed
    /// This struct is thread-safe with regard to the underlying array pointer. 
    /// </summary>
    /// <typeparam name="T">Type of item to store</typeparam>
    internal struct ConcurrentExpandableMap<T> : IExpandableMap<T>
    {
        /// <summary>
        /// The underlying array containing the items
        /// </summary>
        internal T[] Map => eMap.map;

        /// <summary>
        /// Reader-writer lock for the underlying item array
        /// </summary>
        internal readonly ReaderWriterLockSlim eMapLock = new();

        /// <summary>
        /// Last set index in underlying array
        /// </summary>
        internal int LastSetIndex => eMap.lastSetIndex;

        ExpandableMap<T> eMap;
        readonly object nextIdLock = new();

        /// <summary>
        /// Creates a new instance of ConcurrentExpandableMap
        /// </summary>
        /// <param name="minSize">Initial size of underlying array</param>
        /// <param name="minId">The minimal item ID value</param>
        /// <param name="maxId">The maximal item ID value (can be smaller than minId for descending order of IDs)</param>
        public ConcurrentExpandableMap(int minSize, int minId, int maxId)
        {
            this.eMap = new ExpandableMap<T>(minSize, minId, maxId);
        }

        /// <inheritdoc />
        public bool TryGetValue(int id, out T value)
        {
            value = default;
            eMapLock.EnterReadLock();
            try
            {
                return eMap.TryGetValue(id, out value);
            }
            finally
            {
                eMapLock.ExitReadLock();
            }
        }

        /// <inheritdoc />
        public bool Exists(int id) => eMap.Exists(id);

        /// <inheritdoc />
        public ref T GetValueByRef(int id)
        {
            try
            {
                return ref eMap.GetValueByRef(id);
            }
            finally
            {
                eMapLock.ExitReadLock();
            }
        }

        /// <inheritdoc />
        public bool TrySetValue(int id, ref T value)
        {
            eMapLock.EnterUpgradeableReadLock();
            try
            {
                // Try to set value without expanding map
                if (eMap.TrySetValue(id, ref value, true))
                    return true;

                eMapLock.EnterWriteLock();
                try
                {
                    return eMap.TrySetValue(id, ref value);
                }
                finally
                {
                    eMapLock.ExitWriteLock();
                }
            }
            finally
            {
                eMapLock.ExitUpgradeableReadLock();
            }
        }

        /// <inheritdoc />
        public bool TryGetNextId(out int id)
        {
            lock (nextIdLock)
            {
                return eMap.TryGetNextId(out id);
            }
        }

        /// <inheritdoc />
        public bool TryGetFirstId(Func<T, bool> predicate, out int id)
        {
            id = -1;
            eMapLock.EnterReadLock();
            try
            {
                return eMap.TryGetFirstId(predicate, out id);
            }
            finally
            {
                eMapLock.ExitReadLock();
            }
        }
    }

    /// <summary>
    /// Extension methods for ConcurrentExpandableMap
    /// </summary>
    internal static class ConcurrentExpandableMapExtensions
    {
        /// <summary>
        /// Match command name with existing commands in map and return first matching instance
        /// </summary>
        /// <typeparam name="T">Type of command</typeparam>
        /// <param name="eMap">Current instance of ConcurrentExpandableMap</param>
        /// <param name="cmd">Command name to match</param>
        /// <param name="value">Value of command found</param>
        /// <returns>True if command found</returns>
        internal static bool MatchCommandSafe<T>(this ConcurrentExpandableMap<T> eMap, ReadOnlySpan<byte> cmd, out T value)
            where T : ICustomCommand
        {
            value = default;
            eMap.eMapLock.EnterReadLock();
            try
            {
                for (var i = 0; i <= eMap.LastSetIndex; i++)
                {
                    var currCmd = eMap.Map[i];
                    if (cmd.SequenceEqual(new ReadOnlySpan<byte>(currCmd.Name)))
                    {
                        value = currCmd;
                        return true;
                    }
                }
            }
            finally
            {
                eMap.eMapLock.ExitReadLock();
            }

            return false;
        }

        /// <summary>
        /// Match sub-command name with existing sub-commands in map and return first matching instance
        /// </summary>
        /// <typeparam name="T">Type of command</typeparam>
        /// <param name="eMap">Current instance of ConcurrentExpandableMap</param>
        /// <param name="cmd">Sub-command name to match</param>
        /// <param name="value">Value of sub-command found</param>
        /// <returns></returns>
        internal static bool MatchSubCommandSafe<T>(this ConcurrentExpandableMap<T> eMap, ReadOnlySpan<byte> cmd, out CustomObjectCommand value)
            where T : CustomObjectCommandWrapper
        {
            value = default;
            eMap.eMapLock.EnterReadLock();
            try
            {
                for (var i = 0; i <= eMap.LastSetIndex; i++)
                {
                    if (eMap.Map[i].commandMap.MatchCommandSafe(cmd, out value))
                        return true;
                }
            }
            finally
            {
                eMap.eMapLock.ExitReadLock();
            }

            return false;
        }
    }
}