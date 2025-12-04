// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// A list of <see cref="CompletedOutputIterator{TInput, TOutput, TContext}"/> for completed outputs from a pending operation.
    /// </summary>
    /// <remarks>The session holds this list and returns an enumeration to the caller of an appropriate CompletePending overload. The session will handle
    /// disposing and clearing this list, but it is best if the caller calls Dispose() after processing the results, so the key, input, and heap containers
    /// are released as soon as possible.</remarks>
    public sealed class CompletedOutputIterator<TInput, TOutput, TContext> : IDisposable
    {
        internal const int kInitialAlloc = 32;
        internal const int kReallocMultuple = 2;
        internal CompletedOutput<TInput, TOutput, TContext>[] vector = new CompletedOutput<TInput, TOutput, TContext>[kInitialAlloc];
        internal int maxIndex = -1;
        internal int currentIndex = -1;

        internal void TransferFrom<TStoreFunctions, TAllocator>(ref TsavoriteKV<TStoreFunctions, TAllocator>.PendingContext<TInput, TOutput, TContext> pendingContext,
                Status status, SectorAlignedBufferPool bufferPool)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            // Note: vector is never null
            if (maxIndex >= vector.Length - 1)
                Array.Resize(ref vector, vector.Length * kReallocMultuple);
            ++maxIndex;
            vector[maxIndex].TransferFrom(ref pendingContext, status, bufferPool);
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
        public ref CompletedOutput<TInput, TOutput, TContext> Current => ref vector[currentIndex];

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
    /// <remarks>The session holds a list of these that it returns to the caller of an appropriate CompletePending overload. The session will handle disposing
    /// and clearing, and will manage Dispose(), but it is best if the caller calls Dispose() after processing the results, so the key, input, and heap containers
    /// are released as soon as possible.</remarks>
    public struct CompletedOutput<TInput, TOutput, TContext>
    {
        private SpanByteHeapContainer keyContainer;
        private IHeapContainer<TInput> inputContainer;

        /// <summary>
        /// The key for this pending operation.
        /// </summary>
        public ReadOnlySpan<byte> Key => keyContainer.Get().ReadOnlySpan;

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

        internal void TransferFrom<TStoreFunctions, TAllocator>(ref TsavoriteKV<TStoreFunctions, TAllocator>.PendingContext<TInput, TOutput, TContext> pendingContext,
                Status status, SectorAlignedBufferPool bufferPool)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            // Transfers the containers from the pendingContext, then null them; this is called before pendingContext.Dispose().
            keyContainer = pendingContext.request_key;
            pendingContext.request_key = null;
            inputContainer = pendingContext.input;
            pendingContext.input = default;

            Output = pendingContext.output;
            Context = pendingContext.userContext;
            RecordMetadata = new(pendingContext.logicalAddress, pendingContext.eTag);
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