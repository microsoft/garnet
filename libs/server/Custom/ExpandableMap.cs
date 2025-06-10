// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// This struct defines a map of items of type T whose keys are a specified range of IDs (can be descending / ascending)
    /// The size of the underlying array containing the items doubles in size as needed.
    /// This struct is thread-safe, note that it does not support re-setting an already set item.
    /// </summary>
    /// <typeparam name="T">Type of item to store</typeparam>
    internal struct ExpandableMap<T>
    {
        /// <summary>
        /// Reader-writer lock for the underlying item array pointer
        /// </summary>
        internal SingleWriterMultiReaderLock mapLock = new();

        /// <summary>
        /// The underlying array containing the items
        /// </summary>
        internal T[] Map { get; private set; }

        /// <summary>
        /// The actual size of the map
        /// i.e. the max index of an inserted item + 1 (not the size of the underlying array)
        /// </summary>
        internal int ActualSize => actualSize;

        // The actual size of the map
        int actualSize;
        // The last requested index for assignment
        int currIndex = -1;
        // Initial array size
        readonly int minSize;
        // Value of min item ID
        readonly int minId;
        // Value of max item ID
        readonly int maxSize;

        /// <summary>
        /// Creates a new instance of ExpandableMap
        /// </summary>
        /// <param name="minSize">Initial size of underlying array</param>
        /// <param name="minId">The minimal item ID value</param>
        /// <param name="maxId">The maximal item ID value (can be smaller than minId for descending order of IDs)</param>
        public ExpandableMap(int minSize, int minId, int maxId)
        {
            Debug.Assert(maxId > minId);

            this.Map = null;
            this.minSize = minSize;
            this.minId = minId;
            this.maxSize = maxId - minId + 1;
        }

        /// <summary>
        /// Try to get item by ID
        /// </summary>
        /// <param name="id">Item ID</param>
        /// <param name="value">Item value</param>
        /// <returns>True if item found</returns>
        public bool TryGetValue(int id, out T value)
        {
            value = default;
            var idx = id - minId;
            if (idx < 0 || idx >= ActualSize)
                return false;

            value = Map[idx];
            return true;
        }

        /// <summary>
        /// Try to set item by ID
        /// </summary>
        /// <param name="id">Item ID</param>
        /// <param name="value">Item value</param>
        /// <returns>True if assignment succeeded</returns>
        public bool TrySetValueByRef(int id, ref T value)
        {
            // Try to perform set without taking a write lock first
            mapLock.ReadLock();
            try
            {
                // Try to set value without expanding map
                if (this.TrySetValueUnsafe(id, ref value, noExpansion: true))
                    return true;
            }
            finally
            {
                mapLock.ReadUnlock();
            }

            mapLock.WriteLock();
            try
            {
                // Try to set value with expanding the map, if needed
                return this.TrySetValueUnsafe(id, ref value, noExpansion: false);
            }
            finally
            {
                mapLock.WriteUnlock();
            }
        }

        /// <summary>
        /// Try to set item by ID
        /// </summary>
        /// <param name="id">Item ID</param>
        /// <param name="value">Item value</param>
        /// <returns>True if assignment succeeded</returns>
        public bool TrySetValue(int id, T value)
        {
            // Try to perform set without taking a write lock first
            mapLock.ReadLock();
            try
            {
                // Try to set value without expanding map
                if (this.TrySetValueUnsafe(id, ref value, noExpansion: true))
                    return true;
            }
            finally
            {
                mapLock.ReadUnlock();
            }

            mapLock.WriteLock();
            try
            {
                // Try to set value with expanding the map, if needed
                return this.TrySetValueUnsafe(id, ref value, noExpansion: false);
            }
            finally
            {
                mapLock.WriteUnlock();
            }
        }

        /// <summary>
        /// Checks if ID is mapped to a value in underlying array
        /// </summary>
        /// <param name="id">Item ID</param>
        /// <returns>True if ID exists</returns>
        public bool Exists(int id)
        {
            var idx = id - minId;
            return idx >= 0 && idx < ActualSize;
        }

        /// <summary>
        /// Find first ID in map of item that fulfills specified predicate
        /// </summary>
        /// <param name="predicate">Predicate</param>
        /// <param name="id">ID if found, otherwise -1</param>
        /// <returns>True if ID found</returns>
        public bool TryGetFirstId(Func<T, bool> predicate, out int id)
        {
            id = -1;
            var actualSizeSnapshot = ActualSize;
            var mapSnapshot = Map;

            for (var i = 0; i < actualSizeSnapshot; i++)
            {
                if (predicate(mapSnapshot[i]))
                {
                    id = minId + i;
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
        public bool TryGetNextId(out int id)
        {
            id = -1;
            var nextIdx = Interlocked.Increment(ref currIndex);

            if (nextIdx >= maxSize)
                return false;
            id = minId + nextIdx;

            return true;
        }

        /// <summary>
        /// Try to update the actual size of the map based on the inserted item ID
        /// </summary>
        /// <param name="id">The inserted item ID</param>
        /// <returns>True if actual size should be updated (or was updated if noUpdate is false)</returns>
        private bool TryUpdateActualSize(int id)
        {
            var idx = id - minId;

            // Should not update the size if the index is out of bounds
            // or if index is smaller than the current actual size
            if (idx < 0 || idx < ActualSize || idx >= maxSize) return false;

            var oldActualSize = ActualSize;
            var updatedActualSize = idx + 1;
            while (oldActualSize < updatedActualSize)
            {
                var currActualSize = Interlocked.CompareExchange(ref actualSize, updatedActualSize, oldActualSize);
                if (currActualSize == oldActualSize)
                    break;
                oldActualSize = currActualSize;
            }

            return true;
        }

        /// <summary>
        /// Try to set item by ID
        /// This method should only be called from a thread-safe context
        /// </summary>
        /// <param name="id">Item ID</param>
        /// <param name="value">Item value</param>
        /// <param name="noExpansion">True if should not attempt to expand the underlying array</param>
        /// <returns>True if assignment succeeded</returns>
        internal bool TrySetValueUnsafe(int id, ref T value, bool noExpansion)
        {
            var idx = id - minId;
            if (idx < 0 || idx >= maxSize) return false;

            // If index within array bounds, set item
            if (Map != null && idx < Map.Length)
            {
                // This struct does not support setting an already set item
                // This check is not thread-safe, but it is a best-effort attempt at validation.
                Debug.Assert(Equals(Map[idx], default(T)));
                Map[idx] = value;
                TryUpdateActualSize(id);
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
            TryUpdateActualSize(id);
            return true;
        }
    }

    /// <summary>
    /// Extension methods for ExpandableMap
    /// </summary>
    internal static class ExpandableMapExtensions
    {
        /// <summary>
        /// Match command name with existing commands in map and return first matching instance
        /// </summary>
        /// <typeparam name="T">Type of command</typeparam>
        /// <param name="eMap">Current instance of ExpandableMap</param>
        /// <param name="cmd">Command name to match</param>
        /// <param name="value">Value of command found</param>
        /// <returns>True if command found</returns>
        internal static bool MatchCommand<T>(this ExpandableMap<T> eMap, ReadOnlySpan<byte> cmd, out T value)
            where T : ICustomCommand
        {
            value = default;

            // Take the current map instance and its size
            // Note: Map instance could be updated as the map expands, but in this context we only care about its current snapshot
            // Map's actual size could only grow, which is why it's important to take the size snapshot before the map itself.
            var mapSize = eMap.ActualSize;
            var map = eMap.Map;

            // Try to match the specified command with the commands in the current map
            for (var i = 0; i < mapSize; i++)
            {
                var currCmd = map[i];
                if (currCmd != null && cmd.SequenceEqual(new ReadOnlySpan<byte>(currCmd.Name)))
                {
                    value = currCmd;
                    return true;
                }
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
        internal static bool MatchSubCommand<T>(this ExpandableMap<T> eMap, ReadOnlySpan<byte> cmd, out CustomObjectCommand value)
            where T : CustomObjectCommandWrapper
        {
            value = default;

            // Take the current map instance and its size
            // Note: Map instance could be updated as the map expands, but in this context we only care about its current snapshot
            // Map's actual size could only grow, which is why it's important to take the size snapshot before the map itself.
            var mapSize = eMap.ActualSize;
            var map = eMap.Map;

            // Try to match the specified sub-command with each command's sub-command maps
            for (var i = 0; i < mapSize; i++)
            {
                var subCommandEMap = map[i]?.commandMap;
                if (subCommandEMap.HasValue && subCommandEMap.Value.MatchCommand(cmd, out value))
                {
                    return true;
                }
            }

            return false;
        }
    }
}