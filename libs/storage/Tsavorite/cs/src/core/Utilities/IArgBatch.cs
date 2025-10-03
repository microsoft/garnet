// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Batch of arguments - interface allows passing in and out an array of arguments without allocations
    /// </summary>
    /// <typeparam name="TArg">Type of argument</typeparam>
    public interface IArgBatch<TArg>
    {
        public int Count { get; }
        public void Get(int i, out TArg arg);
        public void Set(int i, TArg arg);
    }

    /// <summary>
    /// Batch of arguments to a read operation, including key, input and output
    /// </summary>
    /// <typeparam name="TKey">Type of key</typeparam>
    /// <typeparam name="TInput">Type of input</typeparam>
    /// <typeparam name="TOutput">Type of output</typeparam>
    public interface IReadArgBatch<TKey, TInput, TOutput>
    {
        public int Count { get; }
        public void GetKey(int i, out TKey key);
        public void GetInput(int i, out TInput input);
        public void GetOutput(int i, out TOutput output);
        public void SetOutput(int i, TOutput output);
        public void SetStatus(int i, Status status);
    }

    /// <summary>
    /// ArgBatch implementation using an array
    /// </summary>
    /// <typeparam name="TArg">Type of argument</typeparam>
    public readonly struct ArrayArgBatch<TArg> : IArgBatch<TArg>
    {
        readonly TArg[] args;

        public readonly int Count => args.Length;

        public ArrayArgBatch(int count)
        {
            args = new TArg[count];
        }

        public void Set(int i, TArg arg)
        {
            args[i] = arg;
        }

        public void Get(int i, out TArg arg)
        {
            arg = args[i];
        }
    }

    /// <summary>
    /// ArgBatch implementation using a single shared element
    /// </summary>
    /// <typeparam name="TArg">Type of argument</typeparam>
    public struct SingleArgBatch<TArg> : IArgBatch<TArg>
    {
        TArg arg;

        /// <inheritdoc/>
        public readonly int Count { get; }

        public SingleArgBatch(int count, TArg arg)
        {
            Count = count;
            this.arg = arg;
        }

        public void Set(int i, TArg arg)
        {
            this.arg = arg;
        }

        public void Get(int i, out TArg arg)
        {
            arg = this.arg;
        }
    }
}