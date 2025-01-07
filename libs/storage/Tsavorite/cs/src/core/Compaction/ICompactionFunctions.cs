// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Optional functions to be called during compaction.
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    public interface ICompactionFunctions<TValue>
    {
        /// <summary>
        /// Checks if record in the Tsavorite log is logically deleted.
        /// If the record was deleted via <see cref="BasicContext{TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator}.Delete(SpanByte, TContext)"/>
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
        bool IsDeleted(SpanByte key, TValue value);
    }

    internal struct DefaultCompactionFunctions<TValue> : ICompactionFunctions<TValue>
    {
        public bool IsDeleted(SpanByte key, TValue value) => false;
    }
}