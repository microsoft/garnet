// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// A list of <see cref="CompletedOutputIterator{TKey, TValue, TInput, TOutput, TContext}"/> for completed outputs from a pending operation.
    /// </summary>
    /// <typeparam name="TKey">The Key type of the <see cref="TsavoriteKV{Key, Value}"/></typeparam>
    /// <typeparam name="TValue">The Value type of the <see cref="TsavoriteKV{Key, Value}"/></typeparam>
    /// <typeparam name="TInput">The session input type</typeparam>
    /// <typeparam name="TOutput">The session output type</typeparam>
    /// <typeparam name="TContext">The session context type</typeparam>
    /// <remarks>The session holds this list and returns an enumeration to the caller of an appropriate CompletePending overload. The session will handle
    /// disposing and clearing this list, but it is best if the caller calls Dispose() after processing the results, so the key, input, and heap containers
    /// are released as soon as possible.</remarks>
    public sealed class CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> : IDisposable
    {
        internal const int kInitialAlloc = 32;
        internal const int kReallocMultuple = 2;
        internal CompletedOutput<TKey, TValue, TInput, TOutput, TContext>[] vector = new CompletedOutput<TKey, TValue, TInput, TOutput, TContext>[kInitialAlloc];
        internal int maxIndex = -1;
        internal int currentIndex = -1;

        internal void TransferFrom(ref TsavoriteKV<TKey, TValue>.PendingContext<TInput, TOutput, TContext> pendingContext, Status status)
        {
            // Note: vector is never null
            if (maxIndex >= vector.Length - 1)
                Array.Resize(ref vector, vector.Length * kReallocMultuple);
            ++maxIndex;
            vector[maxIndex].TransferFrom(ref pendingContext, status);
        }

        /// <summary>
        /// Advance the iterator to the next element.
        /// </summary>
        /// <returns>False if this advances past the last element of the array, else true</returns>
        public bool Next()
        {
            if (currentIndex < maxIndex)
            {
                ++currentIndex;
                return true;
            }
            currentIndex = vector.Length;
            return false;
        }

        /// <summary>
        /// Returns a reference to the current element of the enumeration.
        /// </summary>
        /// <returns>A reference to the current element of the enumeration</returns>
        /// <exception cref="IndexOutOfRangeException"> if there is no current element, either because Next() has not been called or it has advanced
        ///     past the last element of the array
        /// </exception>
        public ref CompletedOutput<TKey, TValue, TInput, TOutput, TContext> Current => ref vector[currentIndex];

        /// <inheritdoc/>
        public void Dispose()
        {
            for (; maxIndex >= 0; --maxIndex)
                vector[maxIndex].Dispose();
            currentIndex = -1;
        }
    }

    /// <summary>
    /// Structure to hold a key and its output for a pending operation.
    /// </summary>
    /// <typeparam name="TKey">The Key type of the <see cref="TsavoriteKV{Key, Value}"/></typeparam>
    /// <typeparam name="TValue">The Value type of the <see cref="TsavoriteKV{Key, Value}"/></typeparam>
    /// <typeparam name="TInput">The session input type</typeparam>
    /// <typeparam name="TOutput">The session output type</typeparam>
    /// <typeparam name="TContext">The session context type</typeparam>
    /// <remarks>The session holds a list of these that it returns to the caller of an appropriate CompletePending overload. The session will handle disposing
    /// and clearing, and will manage Dispose(), but it is best if the caller calls Dispose() after processing the results, so the key, input, and heap containers
    /// are released as soon as possible.</remarks>
    public struct CompletedOutput<TKey, TValue, TInput, TOutput, TContext>
    {
        private IHeapContainer<TKey> keyContainer;
        private IHeapContainer<TInput> inputContainer;

        /// <summary>
        /// The key for this pending operation.
        /// </summary>
        public ref TKey Key => ref keyContainer.Get();

        /// <summary>
        /// The input for this pending operation.
        /// </summary>
        public ref TInput Input => ref inputContainer.Get();

        /// <summary>
        /// The output for this pending operation. It is the caller's responsibility to dispose this if necessary; <see cref="Dispose()"/> will not try to dispose this member.
        /// </summary>
        public TOutput Output;

        /// <summary>
        /// The context for this pending operation.
        /// </summary>
        public TContext Context;

        /// <summary>
        /// The record metadata for this operation
        /// </summary>
        public RecordMetadata RecordMetadata;

        /// <summary>
        /// The status of the operation
        /// </summary>
        public Status Status;

        internal void TransferFrom(ref TsavoriteKV<TKey, TValue>.PendingContext<TInput, TOutput, TContext> pendingContext, Status status)
        {
            // Transfers the containers from the pendingContext, then null them; this is called before pendingContext.Dispose().
            keyContainer = pendingContext.key;
            pendingContext.key = null;
            inputContainer = pendingContext.input;
            pendingContext.input = null;

            Output = pendingContext.output;
            Context = pendingContext.userContext;
            RecordMetadata = new(pendingContext.recordInfo, pendingContext.logicalAddress);
            Status = status;
        }

        internal void Dispose()
        {
            var tempKeyContainer = keyContainer;
            keyContainer = default;
            tempKeyContainer?.Dispose();

            var tempInputContainer = inputContainer;
            inputContainer = default;
            tempInputContainer?.Dispose();

            Output = default;
            Context = default;

            RecordMetadata = default;
            Status = default;
        }
    }
}