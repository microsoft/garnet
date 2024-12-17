// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.common;

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
        /// <param name="updateSize">True if actual size of map should be updated (true by default)</param>
        /// <returns>True if assignment succeeded</returns>
        bool TrySetValue(int id, ref T value, bool updateSize = true);

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
        internal T[] Map { get; private set; }

        /// <summary>
        /// The actual size of the map
        /// i.e. the max index of an inserted item + 1 (not the size of the underlying array)
        /// </summary>
        internal int ActualSize { get; private set; }

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
            this.Map = null;
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
            if (idx < 0 || idx >= ActualSize)
                return false;

            value = Map[idx];
            return true;
        }

        /// <inheritdoc />
        public bool Exists(int id)
        {
            var idx = GetIndexFromId(id);
            return idx >= 0 && idx < ActualSize;
        }

        /// <inheritdoc />
        public ref T GetValueByRef(int id)
        {
            var idx = GetIndexFromId(id);
            if (idx < 0 || idx >= ActualSize)
                throw new ArgumentOutOfRangeException(nameof(idx));

            return ref Map[idx];
        }

        /// <inheritdoc />
        public bool TrySetValue(int id, ref T value, bool updateSize = true) =>
            TrySetValue(id, ref value, false, updateSize);

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
            for (var i = 0; i < ActualSize; i++)
            {
                if (predicate(Map[i]))
                {
                    id = GetIdFromIndex(i);
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Get next item ID for assignment with atomic incrementation of underlying index
        /// </summary>
        /// <param name="id">Item ID</param>
        /// <returns>True if item ID available</returns>
        public bool TryGetNextIdSafe(out int id)
        {
            id = -1;
            var nextIdx = Interlocked.Increment(ref currIndex);

            if (nextIdx >= maxSize)
                return false;
            id = GetIdFromIndex(nextIdx);

            return true;
        }

        /// <summary>
        /// Try to update the actual size of the map based on the inserted item ID
        /// </summary>
        /// <param name="id">The inserted item ID</param>
        /// <param name="noUpdate">True if should not do actual update</param>
        /// <returns>True if actual size should be updated (or was updated if noUpdate is false)</returns>
        internal bool TryUpdateSize(int id, bool noUpdate = false)
        {
            var idx = GetIndexFromId(id);

            // Should not update the size if the index is out of bounds
            // or if index is smaller than the current actual size
            if (idx < 0 || idx < ActualSize || idx >= maxSize) return false;

            if (!noUpdate)
                ActualSize = idx + 1;

            return true;
        }

        /// <summary>
        /// Try to set item by ID
        /// </summary>
        /// <param name="id">Item ID</param>
        /// <param name="value">Item value</param>
        /// <param name="noExpansion">True if should not attempt to expand the underlying array</param>
        /// <param name="updateSize">True if should update actual size of the map</param>
        /// <returns>True if assignment succeeded</returns>
        internal bool TrySetValue(int id, ref T value, bool noExpansion, bool updateSize)
        {
            var idx = GetIndexFromId(id);
            if (idx < 0 || idx >= maxSize) return false;

            // If index within array bounds, set item
            if (Map != null && idx < Map.Length)
            {
                Map[idx] = value;
                if (updateSize) TryUpdateSize(id);
                return true;
            }

            if (noExpansion) return false;

            // Double new array size until item can fit
            var newSize = Map != null ? Math.Max(Map.Length, minSize) : minSize;
            while (idx >= newSize)
            {
                newSize = Math.Min(maxSize, newSize * 2);
            }

            // Create new array, copy existing items and set new item
            var newMap = new T[newSize];
            if (Map != null)
            {
                Array.Copy(Map, newMap, Map.Length);
            }

            Map = newMap;
            Map[idx] = value;
            if (updateSize) TryUpdateSize(id);
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
        /// Reader-writer lock for the underlying item array
        /// </summary>
        internal SingleWriterMultiReaderLock eMapLock = new();

        /// <summary>
        /// The underlying non-concurrent ExpandableMap (should be accessed using the eMapLock)
        /// </summary>
        internal ExpandableMap<T> eMapUnsafe;

        /// <summary>
        /// Creates a new instance of ConcurrentExpandableMap
        /// </summary>
        /// <param name="minSize">Initial size of underlying array</param>
        /// <param name="minId">The minimal item ID value</param>
        /// <param name="maxId">The maximal item ID value (can be smaller than minId for descending order of IDs)</param>
        public ConcurrentExpandableMap(int minSize, int minId, int maxId)
        {
            this.eMapUnsafe = new ExpandableMap<T>(minSize, minId, maxId);
        }

        /// <inheritdoc />
        public bool TryGetValue(int id, out T value)
        {
            value = default;
            eMapLock.ReadLock();
            try
            {
                return eMapUnsafe.TryGetValue(id, out value);
            }
            finally
            {
                eMapLock.ReadUnlock();
            }
        }

        /// <inheritdoc />
        public bool Exists(int id)
        {
            eMapLock.ReadLock();
            try
            {
                return eMapUnsafe.Exists(id);
            }
            finally
            {
                eMapLock.ReadUnlock();
            }
        }

        /// <inheritdoc />
        public ref T GetValueByRef(int id)
        {
            eMapLock.ReadLock();
            try
            {
                return ref eMapUnsafe.GetValueByRef(id);
            }
            finally
            {
                eMapLock.ReadUnlock();
            }
        }

        /// <inheritdoc />
        public bool TrySetValue(int id, ref T value, bool updateSize = true)
        {
            var shouldUpdateSize = false;

            // Try to perform set without taking a write lock first
            eMapLock.ReadLock();
            try
            {
                // Try to set value without expanding map
                if (eMapUnsafe.TrySetValue(id, ref value, true, false))
                {
                    // Check if map size should be updated
                    if (!updateSize || !eMapUnsafe.TryUpdateSize(id, true))
                        return true;
                    shouldUpdateSize = true;
                }
            }
            finally
            {
                eMapLock.ReadUnlock();
            }

            eMapLock.WriteLock();
            try
            {
                // Value already set, just update map size
                if (shouldUpdateSize)
                {
                    eMapUnsafe.TryUpdateSize(id);
                    return true;
                }

                // Try to set value with expanding the map, if needed
                return eMapUnsafe.TrySetValue(id, ref value, false, true);
            }
            finally
            {
                eMapLock.WriteUnlock();
            }
        }

        /// <inheritdoc />
        public bool TryGetNextId(out int id)
        {
            return eMapUnsafe.TryGetNextIdSafe(out id);
        }

        /// <inheritdoc />
        public bool TryGetFirstId(Func<T, bool> predicate, out int id)
        {
            id = -1;
            eMapLock.ReadLock();
            try
            {
                return eMapUnsafe.TryGetFirstId(predicate, out id);
            }
            finally
            {
                eMapLock.ReadUnlock();
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
            eMap.eMapLock.ReadLock();
            try
            {
                for (var i = 0; i < eMap.eMapUnsafe.ActualSize; i++)
                {
                    var currCmd = eMap.eMapUnsafe.Map[i];
                    if (currCmd != null && cmd.SequenceEqual(new ReadOnlySpan<byte>(currCmd.Name)))
                    {
                        value = currCmd;
                        return true;
                    }
                }
            }
            finally
            {
                eMap.eMapLock.ReadUnlock();
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
            eMap.eMapLock.ReadLock();
            try
            {
                for (var i = 0; i < eMap.eMapUnsafe.ActualSize; i++)
                {
                    if (eMap.eMapUnsafe.Map[i] != null && eMap.eMapUnsafe.Map[i].commandMap.MatchCommandSafe(cmd, out value))
                        return true;
                }
            }
            finally
            {
                eMap.eMapLock.ReadUnlock();
            }

            return false;
        }
    }
}