// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Optional functions to be called during compaction.
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    public interface ICompactionFunctions<Key, Value>
    {
        /// <summary>
        /// Checks if record in the Tsavorite log is logically deleted.
        /// If the record was deleted via <see cref="ClientSession{Key, Value, Input, Output, Context, Functions}.Delete(ref Key, Context, long)"/>
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
        bool IsDeleted(ref Key key, ref Value value);
    }

    internal struct DefaultCompactionFunctions<Key, Value> : ICompactionFunctions<Key, Value>
    {
        public bool IsDeleted(ref Key key, ref Value value) => false;
    }
}