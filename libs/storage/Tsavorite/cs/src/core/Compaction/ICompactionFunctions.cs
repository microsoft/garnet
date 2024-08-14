// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Optional functions to be called during compaction.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public interface ICompactionFunctions<TKey, TValue>
    {
        /// <summary>
        /// Checks if record in the Tsavorite log is logically deleted.
        /// If the record was deleted via <see cref="BasicContext{Key, Value, Input, Output, Context, Functions, StoreFunctions, Allocator}.Delete(ref Key, Context)"/>
        /// then this function is not called for such a record.
        /// </summary>
        /// <remarks>
        /// <para>
        /// One possible scenario is if Tsavorite is used to store reference counted records.
        /// Once the record count reaches zero it can be considered to be no longer relevant and 
        /// compaction can skip the record.
        /// </para>
        /// </remarks>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool IsDeleted(ref TKey key, ref TValue value);
    }

    internal struct DefaultCompactionFunctions<TKey, TValue> : ICompactionFunctions<TKey, TValue>
    {
        public bool IsDeleted(ref TKey key, ref TValue value) => false;
    }
}