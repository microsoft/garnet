// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Tsavorite.core
{
    /// <summary>
    /// Tsavorite Operations implementation wrapping a pair of operation contexts for dual-store configuration.
    /// </summary>
    public partial class DualContextPair<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1,
                                 TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2, TDualInputConverter> : IDisposable
        where TSessionFunctions1 : ISessionFunctions<TKey1, TValue1, TInput1, TOutput1, TContext>
        where TSessionFunctions2 : ISessionFunctions<TKey2, TValue2, TInput2, TOutput2, TContext>
        where TStoreFunctions1 : IStoreFunctions<TKey1, TValue1>
        where TStoreFunctions2 : IStoreFunctions<TKey2, TValue2>
        where TAllocator1 : IAllocator<TKey1, TValue1, TStoreFunctions1>
        where TAllocator2 : IAllocator<TKey2, TValue2, TStoreFunctions2>
        where TDualInputConverter : IDualInputConverter<TKey1, TInput1, TKey2, TInput2, TOutput2>
    {
        private DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1,
                                 TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2> kernelSession;

        private readonly TDualInputConverter inputConverter;

        const ushort FirstStoreId = 0;
        const ushort SecondStoreId = 1;

        // Stores may be subdivided into partitions
        const ushort FirstPartitionId = 0;
        const ushort SecondPartitionId = 1;

        // Do not create a record if not found on update (used for the first of two stores).
        const long DoNotCreateAddress = -1;

        /// <summary>The Tsavorite kernel</summary>
        public TsavoriteKernel Kernel => kernelSession.clientSession1.Store.Kernel;

        /// <summary>Whether this dual session has a second store</summary>
        public bool IsDual => kernelSession.clientSession2 is not null;

        internal DualContextPair(TsavoriteKV<TKey1, TValue1, TStoreFunctions1, TAllocator1> store1, TSessionFunctions1 sessionFunctions1,
                                 TsavoriteKV<TKey2, TValue2, TStoreFunctions2, TAllocator2> store2, TSessionFunctions2 sessionFunctions2,
                                 TDualInputConverter inputConverter,
                                 ReadCopyOptions readCopyOptions = default)
        {
            kernelSession = new(store1.NewSession<TInput1, TOutput1, TContext, TSessionFunctions1>(sessionFunctions1, readCopyOptions: readCopyOptions),
                                store2?.NewSession<TInput2, TOutput2, TContext, TSessionFunctions2>(sessionFunctions2, readCopyOptions: readCopyOptions));
            this.inputConverter = inputConverter;
        }

        /// <summary>
        /// Dispose DualContextPair instance
        /// </summary>
        public void Dispose()
        {
            kernelSession.clientSession1.Dispose();
            kernelSession.clientSession2.Dispose();
        }

        // Utility function to return the single pending result immediately after detecting status.IsPending
        (Status, TOutput output) GetSinglePendingResult<TKey, TValue, TInput, TOutput, TSessionFunctions, TStoreFunctions, TAllocator, TKeyLocker, TEpochGuard>(DualContext<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> dualContext)
            where TSessionFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
            where TStoreFunctions : IStoreFunctions<TKey, TValue>
            where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TEpochGuard.EndUnsafe(ref kernelSession);
            _ = dualContext.CompletePendingWithOutputs<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker>(
                            out var completedOutputs, wait: true);
            _ = completedOutputs.Next();
            var (status, output) = (completedOutputs.Current.Status, completedOutputs.Current.Output);
            completedOutputs.Dispose();
            TEpochGuard.BeginUnsafe(ref kernelSession);
            return (status, output);
        }

        #region ITsavoriteContext

        public ClientSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1> Session => kernelSession.clientSession1;

        public long GetKeyHash(ref TKey1 key) => kernelSession.clientSession1.Store.GetKeyHash(ref key);

        public long GetKeyHash(ref TKey2 key) => kernelSession.clientSession2.Store.GetKeyHash(ref key);

        #region Read store1
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(TKey1 key, out TOutput1 output, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TInput1 input = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, out output, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(ref TKey1 key, out TOutput1 output, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TInput1 input = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, out output, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(TKey1 key, TInput1 input, out TOutput1 output, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            ReadOptions readOptions = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, out output, ref readOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(ref TKey1 key, ref TInput1 input, out TOutput1 output, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            ReadOptions readOptions = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, out output, ref readOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(TKey1 key, TInput1 input, out TOutput1 output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => Read<TKeyLocker, TEpochGuard>(ref key, ref input, out output, ref readOptions, out recordMetadata, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput1 output) Read<TKeyLocker, TEpochGuard>(TKey1 key, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TInput1 input = default;
            return (Read<TKeyLocker, TEpochGuard>(ref key, ref input, out var output, userContext), output);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput1 output) Read<TKeyLocker, TEpochGuard>(TKey1 key, TInput1 input, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => (Read<TKeyLocker, TEpochGuard>(ref key, ref input, out var output, userContext), output);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput1 output) Read<TKeyLocker, TEpochGuard>(ref TKey1 key, ref TInput1 input, ref ReadOptions readOptions, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => (Read<TKeyLocker, TEpochGuard>(ref key, ref input, out var output, ref readOptions, out _, userContext), output);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(ref TKey1 key, ref TInput1 input, out TOutput1 output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            var status = Kernel.EnterForRead<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                    ref kernelSession, readOptions.KeyHash ?? GetKeyHash(ref key), FirstPartitionId, out var hei);
            if (!status.Found)
            {
                output = default;
                recordMetadata = default;
                return status;
            }

            try
            {
                return kernelSession.dualContext1.Read<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker>(
                        ref hei, ref key, ref input, out output, ref readOptions, out recordMetadata, userContext, ref kernelSession);
            }
            finally
            {
                Kernel.ExitForRead<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                        ref kernelSession, ref hei);
            }
        }
        #endregion Read store1

        #region Read store2
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(TKey2 key, out TOutput2 output, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TInput2 input = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, out output, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(ref TKey2 key, out TOutput2 output, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TInput2 input = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, out output, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(TKey2 key, TInput2 input, out TOutput2 output, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            ReadOptions readOptions = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, out output, ref readOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(ref TKey2 key, ref TInput2 input, out TOutput2 output, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            ReadOptions readOptions = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, out output, ref readOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(TKey2 key, TInput2 input, out TOutput2 output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => Read<TKeyLocker, TEpochGuard>(ref key, ref input, out output, ref readOptions, out recordMetadata, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput2 output) Read<TKeyLocker, TEpochGuard>(TKey2 key, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TInput2 input = default;
            return (Read<TKeyLocker, TEpochGuard>(ref key, ref input, out var output, userContext), output);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput2 output) Read<TKeyLocker, TEpochGuard>(TKey2 key, TInput2 input, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => (Read<TKeyLocker, TEpochGuard>(ref key, ref input, out var output, userContext), output);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput2 output) Read<TKeyLocker, TEpochGuard>(ref TKey2 key, ref TInput2 input, ref ReadOptions readOptions, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => (Read<TKeyLocker, TEpochGuard>(ref key, ref input, out var output, ref readOptions, out _, userContext), output);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(ref TKey2 key, ref TInput2 input, out TOutput2 output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            var status = Kernel.EnterForRead<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                    ref kernelSession, readOptions.KeyHash ?? GetKeyHash(ref key), FirstPartitionId, out var hei);
            if (!status.Found)
            {
                output = default;
                recordMetadata = default;
                return status;
            }

            try
            {
                return kernelSession.dualContext2.Read<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker>(
                        ref hei, ref key, ref input, out output, ref readOptions, out recordMetadata, userContext, ref kernelSession);
            }
            finally
            {
                Kernel.ExitForRead<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                        ref kernelSession, ref hei);
            }
        }
        #endregion Read store2

        #region Read both stores
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status, ushort storeId) Read<TKeyLocker, TEpochGuard>(ref TKey1 key1, ref TInput1 input1, out TOutput1 output1, out TOutput2 output2, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            ReadOptions readOptions = default;
            return Read<TKeyLocker, TEpochGuard>(ref key1, ref input1, out output1, out output2, ref readOptions, out _, userContext);
        }

        public (Status, ushort storeId) Read<TKeyLocker, TEpochGuard>(TKey1 key1, TInput1 input1, out TOutput1 output1, out TOutput2 output2, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            ReadOptions readOptions = default;
            return Read<TKeyLocker, TEpochGuard>(ref key1, ref input1, out output1, out output2, ref readOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status, ushort storeId) Read<TKeyLocker, TEpochGuard>(TKey1 key1, TInput1 input1, out TOutput1 output1, out TOutput2 output2, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => Read<TKeyLocker, TEpochGuard>(ref key1, ref input1, out output1, out output2, ref readOptions, out recordMetadata, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status, ushort) Read<TKeyLocker, TEpochGuard>(ref TKey1 key1, ref TInput1 input1, out TOutput1 output1, out TOutput2 output2, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            var keyHash = readOptions.KeyHash ?? GetKeyHash(ref key1);
            var status = Kernel.EnterForRead<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                    ref kernelSession, keyHash, FirstPartitionId, out var hei);
            output1 = default;
            output2 = default;
            recordMetadata = default;

            try
            {
                TKey2 key2;
                TInput2 input2;
                if (status.Found)
                { 
                    // Tag was found in the first store; see if it has the key (it may have been a collision)
                    status = kernelSession.dualContext1.Read<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker>(
                            ref hei, ref key1, ref input1, out output1, ref readOptions, out recordMetadata, userContext, ref kernelSession);
                    if (status.IsPending)
                        (status, output1) = GetSinglePendingResult<TKey1, TValue1, TInput1, TOutput1, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKeyLocker, TEpochGuard>(kernelSession.dualContext1);
                    if (status.Found)
                        return (status, FirstStoreId);

                    // Did not find the key and the bucket is still locked--move to the second store.
                    inputConverter.ConvertForRead(ref key1, ref input1, out key2, out input2, out output2);
                    Debug.Assert(hei.HashCodeEquals(GetKeyHash(ref key2)), "Main and Object hash codes are not the same");
                    status = Kernel.EnterForReadDual2(SecondPartitionId, ref hei);
                }
                else
                {
                    // First partition tag was not found so the bucket was not locked. Try to find and lock for the second partition.
                    status = Kernel.EnterForRead<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                            ref kernelSession, keyHash, SecondPartitionId, out hei);
                    if (status.NotFound)    // Tag was not found for either partition.
                        return (status, FirstStoreId);
                    inputConverter.ConvertForRead(ref key1, ref input1, out key2, out input2, out output2);
                }

                status = kernelSession.dualContext2.Read<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker>(
                        ref hei, ref key2, ref input2, out output2, ref readOptions, out recordMetadata, userContext, ref kernelSession);
                if (status.IsPending)
                    (status, output2) = GetSinglePendingResult<TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2, TKeyLocker, TEpochGuard>(kernelSession.dualContext2);
                return (status, SecondStoreId);
            }
            finally
            {
                Kernel.ExitForRead<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                        ref kernelSession, ref hei);
            }
        }
        #endregion Read both stores

        #region Upsert store1
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(TKey1 key, TValue1 desiredValue, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TInput1 input = default;
            UpsertOptions upsertOptions = default;
            return Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, out _, ref upsertOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(ref TKey1 key, ref TValue1 desiredValue, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TInput1 input = default;
            UpsertOptions upsertOptions = default;
            return Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, out _, ref upsertOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(TKey1 key, TInput1 input, TValue1 desiredValue, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(ref TKey1 key, ref TInput1 input, ref TValue1 desiredValue, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            UpsertOptions upsertOptions = default;
            return Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, out _, ref upsertOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(TKey1 key, TInput1 input, TValue1 desiredValue, out TOutput1 output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, out output, ref upsertOptions, out recordMetadata, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(ref TKey1 key, ref TInput1 input, ref TValue1 desiredValue, out TOutput1 output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            var status = Kernel.EnterForUpdate<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                    ref kernelSession, upsertOptions.KeyHash ?? GetKeyHash(ref key), FirstPartitionId, kernelSession.clientSession1.Store.Log.BeginAddress, out var hei);
            Debug.Assert(status.Found, "Should always 'find' the tag when specifying BeginAddress to EnterForUpdate");

            try
            {
                return kernelSession.dualContext1.Upsert<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker>(
                        ref hei, ref key, ref input, ref desiredValue, out output, out recordMetadata, userContext, ref kernelSession);
            }
            finally
            {
                Kernel.ExitForUpdate<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                        ref kernelSession, ref hei);
            }
        }
        #endregion Upsert store1

        #region Upsert store2
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(TKey2 key, TValue2 desiredValue, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TInput2 input = default;
            UpsertOptions upsertOptions = default;
            return Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, out _, ref upsertOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(ref TKey2 key, ref TValue2 desiredValue, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TInput2 input = default;
            UpsertOptions upsertOptions = default;
            return Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, out _, ref upsertOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(TKey2 key, TInput2 input, TValue2 desiredValue, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(ref TKey2 key, ref TInput2 input, ref TValue2 desiredValue, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            UpsertOptions upsertOptions = default;
            return Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, out _, ref upsertOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(TKey2 key, TInput2 input, TValue2 desiredValue, out TOutput2 output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, out output, ref upsertOptions, out recordMetadata, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert<TKeyLocker, TEpochGuard>(ref TKey2 key, ref TInput2 input, ref TValue2 desiredValue, out TOutput2 output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            var status = Kernel.EnterForUpdate<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                    ref kernelSession, upsertOptions.KeyHash ?? GetKeyHash(ref key), FirstPartitionId, kernelSession.clientSession2.Store.Log.BeginAddress, out var hei);
            Debug.Assert(status.Found, "Should always 'find' the tag when specifying BeginAddress to EnterForUpdate");

            try
            {
                return kernelSession.dualContext2.Upsert<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker>(
                        ref hei, ref key, ref input, ref desiredValue, out output, out recordMetadata, userContext, ref kernelSession);
            }
            finally
            {
                Kernel.ExitForUpdate<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                        ref kernelSession, ref hei);
            }
        }
        #endregion Upsert store2

        #region Upsert both stores
        // We do not Upsert to both stores because the Key+Value will be specific to a single store.
        #endregion Upsert both stores


        #region RMW store1
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(TKey1 key, TInput1 input, out TOutput1 output, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => RMW<TKeyLocker, TEpochGuard>(ref key, ref input, out output, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(TKey1 key, TInput1 input, out TOutput1 output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => RMW<TKeyLocker, TEpochGuard>(ref key, ref input, out output, ref rmwOptions, out recordMetadata, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(ref TKey1 key, ref TInput1 input, out TOutput1 output, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            RMWOptions rmwOptions = default;
            return RMW<TKeyLocker, TEpochGuard>(ref key, ref input, out output, ref rmwOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(ref TKey1 key, ref TInput1 input, out TOutput1 output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            var status = Kernel.EnterForUpdate<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                    ref kernelSession, rmwOptions.KeyHash ?? GetKeyHash(ref key), FirstPartitionId, kernelSession.clientSession2.Store.Log.BeginAddress, out var hei);
            Debug.Assert(status.Found, "Should always 'find' the tag when specifying BeginAddress to EnterForUpdate");

            try
            {
                return kernelSession.dualContext1.RMW<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker>(
                        ref hei, ref key, ref input, out output, out recordMetadata, userContext, ref kernelSession);
            }
            finally
            {
                Kernel.ExitForUpdate<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                        ref kernelSession, ref hei);
            }
        }
        #endregion RMW store1

        #region RMW store2
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(TKey2 key, TInput2 input, out TOutput2 output, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => RMW<TKeyLocker, TEpochGuard>(ref key, ref input, out output, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(TKey2 key, TInput2 input, out TOutput2 output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => RMW<TKeyLocker, TEpochGuard>(ref key, ref input, out output, ref rmwOptions, out recordMetadata, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(ref TKey2 key, ref TInput2 input, out TOutput2 output, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            RMWOptions rmwOptions = default;
            return RMW<TKeyLocker, TEpochGuard>(ref key, ref input, out output, ref rmwOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(ref TKey2 key, ref TInput2 input, out TOutput2 output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            var status = Kernel.EnterForUpdate<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                    ref kernelSession, rmwOptions.KeyHash ?? GetKeyHash(ref key), FirstPartitionId, kernelSession.clientSession2.Store.Log.BeginAddress, out var hei);
            Debug.Assert(status.Found, "Should always 'find' the tag when specifying BeginAddress to EnterForUpdate");

            try
            {
                return kernelSession.dualContext2.RMW<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker>(
                        ref hei, ref key, ref input, out output, out recordMetadata, userContext, ref kernelSession);
            }
            finally
            {
                Kernel.ExitForUpdate<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                        ref kernelSession, ref hei);
            }
        }
        #endregion RMW store2

        #region RMW both stores
        // We do not RMW to both stores because the Key+Input will be specific to a single store.
        #endregion RMW both stores

        #region Delete store1
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker, TEpochGuard>(TKey1 key, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => Delete<TKeyLocker, TEpochGuard>(ref key, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker, TEpochGuard>(ref TKey1 key, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            DeleteOptions deleteOptions = default;
            return Delete<TKeyLocker, TEpochGuard>(ref key, ref deleteOptions, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker, TEpochGuard>(TKey1 key, ref DeleteOptions deleteOptions, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => Delete<TKeyLocker, TEpochGuard>(ref key, ref deleteOptions, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker, TEpochGuard>(ref TKey1 key, ref DeleteOptions deleteOptions, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            var status = Kernel.EnterForUpdate<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                    ref kernelSession, deleteOptions.KeyHash ?? GetKeyHash(ref key), FirstPartitionId, kernelSession.clientSession2.Store.Log.BeginAddress, out var hei);
            Debug.Assert(status.Found, "Should always 'find' the tag when specifying BeginAddress to EnterForUpdate");

            try
            {
                return kernelSession.dualContext1.Delete<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker>(
                        ref hei, ref key, userContext, ref kernelSession);
            }
            finally
            {
                Kernel.ExitForUpdate<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                        ref kernelSession, ref hei);
            }
        }
        #endregion Delete store1

        #region Delete store2
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker, TEpochGuard>(TKey2 key, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => Delete<TKeyLocker, TEpochGuard>(ref key, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker, TEpochGuard>(ref TKey2 key, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            DeleteOptions deleteOptions = default;
            return Delete<TKeyLocker, TEpochGuard>(ref key, ref deleteOptions, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker, TEpochGuard>(TKey2 key, ref DeleteOptions deleteOptions, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => Delete<TKeyLocker, TEpochGuard>(ref key, ref deleteOptions, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker, TEpochGuard>(ref TKey2 key, ref DeleteOptions deleteOptions, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            var status = Kernel.EnterForUpdate<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                    ref kernelSession, deleteOptions.KeyHash ?? GetKeyHash(ref key), FirstPartitionId, kernelSession.clientSession2.Store.Log.BeginAddress, out var hei);
            Debug.Assert(status.Found, "Should always 'find' the tag when specifying BeginAddress to EnterForUpdate");

            try
            {
                return kernelSession.dualContext2.Delete<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker>(
                        ref hei, ref key, userContext, ref kernelSession);
            }
            finally
            {
                Kernel.ExitForUpdate<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                        ref kernelSession, ref hei);
            }
        }
        #endregion Delete store2

        #region Delete both stores
        // TODO Delete both--doees this make sense, since we don't go to disk
        #endregion Delete both stores

        // TODO ReadAtAddress
        // TODO ResetModified clarification (how will it know which store--it is blind now for Both), no IsModified?
        // TODO propagate <TKernelSession, TKeyLocker> through XxxInternal, CompletePending, etc.
        // TODO RENAME

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetModified(ref TKey1 key)
            => kernelSession.clientSession1.UnsafeResetModified(sessionFunctions, ref key);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsModified<TKeyLocker, TEpochGuard>(ref TKey1 key1, bool checkBothStores)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => IsModified<TKeyLocker, TEpochGuard>(ref key1, GetKeyHash(ref key1), checkBothStores);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsModified<TKeyLocker, TEpochGuard>(ref TKey1 key1, long keyHash, bool checkBothStores)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            //=> kernelSession.clientSession1.UnsafeIsModified(sessionFunctions, ref key);
        {
            var status = Kernel.EnterForRead<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                    ref kernelSession, keyHash, FirstPartitionId, out var hei);

            try
            {
                TKey2 key2;
                if (status.Found)
                {
                    // Tag was found in the first store; see if it has the key (it may have been a collision). This operation does not return pending.
                    status = kernelSession.dualContext1.IsModified<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker>(ref key1);
                    if (status.IsPending)
                        (status, output1) = GetSinglePendingResult<TKey1, TValue1, TInput1, TOutput1, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKeyLocker, TEpochGuard>(kernelSession.dualContext1);
                    if (status.Found)
                        return (status, FirstStoreId);

                    // Did not find the key and the bucket is still locked--move to the second store.
                    inputConverter.ConvertForRead(ref key1, ref input1, out key2, out input2, out output2);
                    Debug.Assert(hei.HashCodeEquals(GetKeyHash(ref key2)), "Main and Object hash codes are not the same");
                    status = Kernel.EnterForReadDual2(SecondPartitionId, ref hei);
                }
                else
                {
                    // First partition tag was not found so the bucket was not locked. Try to find and lock for the second partition.
                    status = Kernel.EnterForRead<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                            ref kernelSession, keyHash, SecondPartitionId, out hei);
                    if (status.NotFound)    // Tag was not found for either partition.
                        return (status, FirstStoreId);
                    inputConverter.ConvertForRead(ref key1, ref input1, out key2, out input2, out output2);
                }

                status = kernelSession.dualContext2.Read<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker>(
                        ref hei, ref key2, ref input2, out output2, ref readOptions, out recordMetadata, userContext, ref kernelSession);
                if (status.IsPending)
                    (status, output2) = GetSinglePendingResult<TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2, TKeyLocker, TEpochGuard>(kernelSession.dualContext2);
                return (status, SecondStoreId);
            }
            finally
            {
                Kernel.ExitForRead<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                        ref kernelSession, ref hei);
            }
        }

        #endregion ITsavoriteContext
    }
}