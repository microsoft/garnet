// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// A dictionary that supports concurrency with similar interface to .NET's ConcurrentDictionary.
    /// However, this dictionary changes the implementation of AddOrUpdate and GetOrAdd functions to
    /// guarantee atomicity per-key for factory lambdas.
    /// </summary>
    /// <typeparam name="TKey">Type of keys in the dictionary</typeparam>
    /// <typeparam name="TValue">Type of values in the dictionary</typeparam>
    internal sealed class SafeConcurrentDictionary<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
    {
        private readonly ConcurrentDictionary<TKey, TValue> dictionary;

        private readonly ConcurrentDictionary<TKey, object> keyLocks = new();

        public SafeConcurrentDictionary()
        {
            dictionary = new();
        }

        public SafeConcurrentDictionary(IEnumerable<KeyValuePair<TKey, TValue>> initialCollection)
        {
            dictionary = new(initialCollection);
        }

        public SafeConcurrentDictionary(IEqualityComparer<TKey> comparer)
        {
            dictionary = new(comparer);
        }

        /// <summary>
        /// Returns the count of the dictionary.
        /// </summary>
        public int Count
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return dictionary.Count;
            }
        }

        /// <summary>
        /// Returns whether or not the dictionary is empty.
        /// </summary>
        public bool IsEmpty
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return dictionary.IsEmpty;
            }
        }

        /// <summary>
        /// Gets or sets the value associated with a key.
        /// </summary>
        public TValue this[TKey key]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return dictionary[key];
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                dictionary[key] = value;
            }
        }

        /// <summary>
        /// Returns a collection of the keys in the dictionary.
        /// </summary>
        public ICollection<TKey> Keys
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return dictionary.Keys;
            }
        }

        /// <summary>
        /// Returns a collection of the values in the dictionary.
        /// </summary>
        public ICollection<TValue> Values
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return dictionary.Values;
            }
        }

        /// <summary>
        /// Adds or updates a key/value pair to the dictionary.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TValue AddOrUpdate(TKey key, Func<TKey, TValue> addValueFactory, Func<TKey, TValue, TValue> updateValueFactory)
        {
            lock (GetLock(key))
            {
                return dictionary.AddOrUpdate(key, addValueFactory, updateValueFactory);
            }
        }

        /// <summary>
        /// Adds or updates a key/value pair to the dictionary.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
        {
            lock (GetLock(key))
            {
                return dictionary.AddOrUpdate(key, addValue, updateValueFactory);
            }
        }

        /// <summary>
        /// Adds a key/value pair to the dictionary if it does not exist.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            if (dictionary.TryGetValue(key, out TValue value))
            {
                return value;
            }
            lock (GetLock(key))
            {
                return dictionary.GetOrAdd(key, valueFactory);
            }
        }

        /// <summary>
        /// Adds a key/value pair to the dictionary if it does not exist.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TValue GetOrAdd(TKey key, TValue value)
        {
            return dictionary.GetOrAdd(key, value);
        }

        /// <summary>
        /// Clears the dictionary.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear()
        {
            dictionary.Clear();
            keyLocks.Clear();
        }

        /// <summary>
        /// Returns whether or not the dictionary contains the specified key.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ContainsKey(TKey key)
        {
            return dictionary.ContainsKey(key);
        }

        /// <summary>
        /// Returns an enumerator of the elements in the dictionary.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return dictionary.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Copies the key/value pairs to a new array.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public KeyValuePair<TKey, TValue>[] ToArray()
        {
            return dictionary.ToArray();
        }

        /// <summary>
        /// Attempts to add the specified key/value to the dictionary if it does not exist.
        /// Returns true or false depending on if the value was added or not, respectively.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryAdd(TKey key, TValue value)
        {
            return dictionary.TryAdd(key, value);
        }

        /// <summary>
        /// Attempts to get the value for the specified key.
        /// Returns true if the key was in the dictionary or false otherwise.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetValue(TKey key, out TValue value)
        {
            return dictionary.TryGetValue(key, out value);
        }

        /// <summary>
        /// Attempts to remove the value for the specified key.
        /// Returns true if the key was in the dictionary or false otherwise.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryRemove(TKey key, out TValue value)
        {
            return dictionary.TryRemove(key, out value);
        }

        /// <summary>
        /// Attempts to remove the value for the specified key based on equality to <paramref name="ifValue"/>.
        /// Returns true if successful, false otherwise (value changed or key not found).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryRemoveConditional(TKey key, in TValue ifValue)
        {
            // From https://devblogs.microsoft.com/pfxteam/little-known-gems-atomic-conditional-removals-from-concurrentdictionary/
            return ((ICollection<KeyValuePair<TKey, TValue>>)dictionary).Remove(new KeyValuePair<TKey, TValue>(key, ifValue));
        }

        /// <summary>
        /// Compares the existing value for the specified key with a specified value,
        /// and updates it if and only if it is a match. Returns true is updated or
        /// false otherwise.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryUpdate(TKey key, TValue newValue, TValue comparisonValue)
        {
            return dictionary.TryUpdate(key, newValue, comparisonValue);
        }

        /// <summary>
        /// Retrieves lock associated with a key (creating it if it does not exist).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private object GetLock(TKey key)
        {
            return keyLocks.GetOrAdd(key, v => new object());
        }
    }
}