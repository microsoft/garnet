// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    public delegate int VectorReadDelegate(ulong context, ReadOnlySpan<byte> key, Span<byte> value);
    public delegate bool VectorWriteDelegate(ulong context, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);
    public delegate bool VectorDeleteDelegate(ulong context, ReadOnlySpan<byte> key);

    /// <summary>
    /// For Mocking/Plugging purposes, represents the actual implementation of a bunch of Vector Set operations.
    /// </summary>
    public unsafe interface IVectorService
    {
        /// <summary>
        /// When creating an index, indicates which method to use.
        /// </summary>
        bool UseUnmanagedCallbacks { get; }

        /// <summary>
        /// Construct a new index to back a Vector Set.
        /// </summary>
        /// <param name="context">Unique value for construction, will be passed for all for operations alongside the returned index.  Always a multiple of 4.</param>
        /// <param name="dimensions">Dimensions of vectors will be passed to future operations. Always > 0</param>
        /// <param name="reduceDims">If non-0, the requested dimension of the random projection to apply before indexing vectors.</param>
        /// <param name="quantType">Type of quantization requested.</param>
        /// <param name="buildExplorationFactor">Exploration factor requested.</param>
        /// <param name="numLinks">Number of links between adjacent vectors requested.</param>
        /// <param name="readCallback">Callback used to read values out of Garnet store.</param>
        /// <param name="writeCallback">Callback used to write values to Garnet store.</param>
        /// <param name="deleteCallback">Callback used to delete values from Garnet store.</param>
        /// <returns>Reference to constructed index.</returns>
        nint CreateIndexUnmanaged(ulong context, uint dimensions, uint reduceDims, VectorQuantType quantType, uint buildExplorationFactor, uint numLinks, delegate* unmanaged[Cdecl]<ulong, nint, nuint, nint, nuint, int> readCallback, delegate* unmanaged[Cdecl]<ulong, nint, nuint, nint, nuint, byte> writeCallback, delegate* unmanaged[Cdecl]<ulong, nint, nuint, byte> deleteCallback);

        /// <summary>
        /// Equivalent of <see cref="CreateIndexUnmanaged"/>, but with managed callbacks.
        /// </summary>
        nint CreateIndexManaged(ulong context, uint dimensions, uint reduceDims, VectorQuantType quantType, uint buildExplorationFactor, uint numLinks, VectorReadDelegate readCallback, VectorWriteDelegate writeCallback, VectorDeleteDelegate deleteCallback);

        /// <summary>
        /// Delete a previously created index.
        /// </summary>
        void DropIndex(ulong context, nint index);

        /// <summary>
        /// Insert a vector into an index.
        /// </summary>
        /// <returns>True if the vector was added, false otherwise.</returns>
        bool Insert(ulong context, nint index, ReadOnlySpan<byte> id, VectorValueType vectorType, ReadOnlySpan<byte> vector, ReadOnlySpan<byte> attributes);

        /// <summary>
        /// Search for similar vectors, given a vector.
        /// 
        /// <paramref name="outputIds"/> are length prefixed with little endian ints.
        /// <paramref name="continuation"/> is non-zero if there are more results to fetch than could be fit in <paramref name="outputIds"/>.
        /// 
        /// Returns number of results placed in outputXXX parameters.
        /// </summary>
        int SearchVector(ulong context, nint index, VectorValueType vectorType, ReadOnlySpan<byte> vector, float delta, int searchExplorationFactor, ReadOnlySpan<byte> filter, int maxFilteringEffort, Span<byte> outputIds, Span<float> outputDistances, out nint continuation);

        /// <summary>
        /// Search for similar vectors, given a vector.
        /// 
        /// <paramref name="outputIds"/> are length prefixed with little endian ints.
        /// <paramref name="continuation"/> is non-zero if there are more results to fetch than could be fit in <paramref name="outputIds"/>.
        /// 
        /// Returns number of results placed in outputXXX parameters.
        /// </summary>
        int SearchElement(ulong context, nint index, ReadOnlySpan<byte> id, float delta, int searchExplorationFactor, ReadOnlySpan<byte> filter, int maxFilteringEffort, Span<byte> outputIds, Span<float> outputDistances, out nint continuation);

        /// <summary>
        /// Continue fetching results when a call to <see cref="SearchVector"/> or <see cref="SearchElement"/> had a non-zero continuation result.
        /// 
        /// Will be called exactly once per continuation provided, and will always be called if a search operation produced a continuation.
        /// </summary>
        int ContinueSearch(ulong context, nint index, nint continuation, Span<byte> outputIds, Span<float> outputDistances, out nint newContinuation);

        /// <summary>
        /// Fetch the embedding of a vector in the vector set, if it exists.
        /// 
        /// This undoes any dimensionality reduction, so values may be approximate.
        /// 
        /// <paramref name="dimensions"/> is always the size of dimensions passed to <see cref="CreateIndexManaged"/> or <see cref="CreateIndexUnmanaged"/>.
        /// </summary>
        bool TryGetEmbedding(ulong context, nint index, ReadOnlySpan<byte> id, Span<float> dimensions);
    }
}