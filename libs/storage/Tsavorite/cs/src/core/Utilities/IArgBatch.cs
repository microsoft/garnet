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
}