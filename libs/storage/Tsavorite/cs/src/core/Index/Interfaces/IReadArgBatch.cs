// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Batch of arguments to a read operation, including key, input and output
    /// </summary>
    /// <typeparam name="TInput">Type of input</typeparam>
    /// <typeparam name="TOutput">Type of output</typeparam>
    public interface IReadArgBatch<TInput, TOutput>
    {
        /// <summary>
        /// Count of keys/args/outputs.
        /// </summary>
        int Count { get; }

        /// <summary>
        /// Get <paramref name="i"/>th key.
        /// </summary>
        void GetKey(int i, out PinnedSpanByte key);

        /// <summary>
        /// Get <paramref name="i"/>th input.
        /// </summary>
        void GetInput(int i, out TInput input);

        /// <summary>
        /// Get <paramref name="i"/>th output.
        /// </summary>
        void GetOutput(int i, out TOutput output);

        /// <summary>
        /// Set <paramref name="i"/>th output.
        /// </summary>
        void SetOutput(int i, TOutput output);

        /// <summary>
        /// Set <paramref name="i"/>th status.
        /// </summary>
        void SetStatus(int i, Status status);
    }
}