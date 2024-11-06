// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    public interface IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> 
        : IEpochGuard<BasicKernelSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>>
        where TSessionFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>;

    /// <summary>
    /// Perform epoch safety operations when entering/exiting an API 
    /// </summary>
    public struct BasicSafeEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> 
        : IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        where TSessionFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        /// <summary>Acquire (resume) the epoch.</summary>
        public static void BeginUnsafe(ref BasicKernelSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> kernelSession)
            => kernelSession.BeginUnsafe();

        /// <summary>Leave (suspend) the epoch.</summary>
        public static void EndUnsafe(ref BasicKernelSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> kernelSession)
            => kernelSession.EndUnsafe();
    }

    /// <summary>
    /// Do not perform epoch safety operations when entering/exiting an API (assumes a higher code region has done so)
    /// </summary>
    public struct BasicUnsafeEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        : IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        where TSessionFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        /// <summary>Acquire (resume) the epoch.</summary>
        public static void BeginUnsafe(ref BasicKernelSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> kernelSession)
        { }

        /// <summary>Leave (suspend) the epoch.</summary>
        public static void EndUnsafe(ref BasicKernelSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> kernelSession)
        { }
    }
}
