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
    public partial class DualContext<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1,
                                 TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2, TDualInputConverter> : IDisposable
        where TSessionFunctions1 : ISessionFunctions<TKey1, TValue1, TInput1, TOutput1, TContext>
        where TSessionFunctions2 : ISessionFunctions<TKey2, TValue2, TInput2, TOutput2, TContext>
        where TStoreFunctions1 : IStoreFunctions<TKey1, TValue1>
        where TStoreFunctions2 : IStoreFunctions<TKey2, TValue2>
        where TAllocator1 : IAllocator<TKey1, TValue1, TStoreFunctions1>
        where TAllocator2 : IAllocator<TKey2, TValue2, TStoreFunctions2>
        where TDualInputConverter : IDualInputConverter<TKey1, TInput1, TKey2, TInput2, TOutput2>
    {
        public DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1,
                                 TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2> KernelSession;

        private readonly TDualInputConverter inputConverter;
        private readonly IPendingMetrics pendingMetrics;

        public const ushort StoreId1 = 0;
        public const ushort StoreId2 = 1;

        // Stores may be subdivided into partitions later, so keep these separate.
        public const ushort PartitionId1 = 0;
        public const ushort PartitionId2 = 1;

        public int SessionID => KernelSession.clientSession1.ID;

        /// <summary>The Tsavorite kernel</summary>
        public TsavoriteKernel Kernel => KernelSession.clientSession1.Store.Kernel;
        public TsavoriteKV<TKey1, TValue1, TStoreFunctions1, TAllocator1> Store1 => KernelSession.clientSession1.Store;
        public TsavoriteKV<TKey2, TValue2, TStoreFunctions2, TAllocator2> Store2 => KernelSession.clientSession2.Store;

        /// <summary>Whether this dual session has a second store</summary>
        public bool IsDual => KernelSession.clientSession2 is not null;

        public DualItemContext<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1> ItemContext1 => KernelSession.itemContext1;
        public DualItemContext<TKey2, TValue2, TInput2, TOutput2, TContext, TSessionFunctions2, TStoreFunctions2, TAllocator2> ItemContext2 => KernelSession.itemContext2;

        public DualContext(TsavoriteKV<TKey1, TValue1, TStoreFunctions1, TAllocator1> store1, TSessionFunctions1 sessionFunctions1,
                                 TsavoriteKV<TKey2, TValue2, TStoreFunctions2, TAllocator2> store2, TSessionFunctions2 sessionFunctions2,
                                 TDualInputConverter inputConverter, IPendingMetrics pendingMetrics = null,
                                 ReadCopyOptions readCopyOptions = default)
        {
            this.inputConverter = inputConverter;
            this.pendingMetrics = pendingMetrics;
            KernelSession = new(store1.NewSession<TInput1, TOutput1, TContext, TSessionFunctions1>(sessionFunctions1, readCopyOptions: readCopyOptions),
                                store2?.NewSession<TInput2, TOutput2, TContext, TSessionFunctions2>(sessionFunctions2, readCopyOptions: readCopyOptions));
        }

        /// <summary>
        /// Dispose DualContextPair instance
        /// </summary>
        public void Dispose()
        {
            KernelSession.clientSession1.Dispose();
            KernelSession.clientSession2?.Dispose();
        }

        #region ITsavoriteContext

        public ClientSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1> Session1 => KernelSession.clientSession1;
        public ClientSession<TKey2, TValue2, TInput2, TOutput2, TContext, TSessionFunctions2, TStoreFunctions2, TAllocator2> Session2 => KernelSession.clientSession2;

        #region Kernel utilities
        public long GetKeyHash(ref TKey1 key) => KernelSession.clientSession1.Store.GetKeyHash(ref key);

        public long GetKeyHash(ref TKey2 key) => KernelSession.clientSession2.Store.GetKeyHash(ref key);

        public HashEntryInfo CreateHei(ref TKey1 key) => CreateHei1(GetKeyHash(ref key));
        public HashEntryInfo CreateHei1(long keyHash) => new(keyHash, PartitionId1);
        public HashEntryInfo CreateHei(ref TKey2 key) => CreateHei2(GetKeyHash(ref key));
        public HashEntryInfo CreateHei2(long keyHash) => new(keyHash, PartitionId1);

        public Status EnterKernelForRead<TKeyLocker, TEpochGuard>(long keyHash, ushort partitionId, out HashEntryInfo hei)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            // This can throw on lock timeout so make sure it is called within try/finally to ensure epoch is released
            return Kernel.EnterForRead<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                    ref KernelSession, keyHash, partitionId, out hei);
        }

        public void ExitKernelForRead<TKeyLocker, TEpochGuard>(ref HashEntryInfo hei)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            // This can throw on lock timeout so make sure it is called within try/finally to ensure epoch is released
            Kernel.ExitForRead<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                    ref KernelSession, ref hei);
        }

        public Status EnterKernelForUpdate<TKeyLocker, TEpochGuard>(long keyHash, ushort partitionId, long beginAddress, out HashEntryInfo hei)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            // This can throw on lock timeout so make sure it is called within try/finally to ensure epoch is released
            var status = Kernel.EnterForUpdate<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                    ref KernelSession, keyHash, partitionId, beginAddress, out hei);
            return status;
        }

        public void ExitKernelForUpdate<TKeyLocker, TEpochGuard>(ref HashEntryInfo hei)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            Kernel.ExitForUpdate<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>, TKeyLocker, TEpochGuard>(
                    ref KernelSession, ref hei);
        }
        #endregion Kernel utilities

        #region Read store1
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(TKey1 key, ref TOutput1 output, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TInput1 input = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(ref TKey1 key, ref TOutput1 output, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TInput1 input = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(TKey1 key, TInput1 input, ref TOutput1 output, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            ReadOptions readOptions = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ref readOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(ref TKey1 key, ref TInput1 input, ref TOutput1 output, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            ReadOptions readOptions = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ref readOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(TKey1 key, TInput1 input, ref TOutput1 output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ref readOptions, out recordMetadata, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput1 output) Read<TKeyLocker, TEpochGuard>(TKey1 key, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TInput1 input = default;
            TOutput1 output = default;
            return (Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, userContext), output);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput1 output) Read<TKeyLocker, TEpochGuard>(TKey1 key, TInput1 input, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TOutput1 output = default;
            return (Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, userContext), output);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput1 output) Read<TKeyLocker, TEpochGuard>(ref TKey1 key, ref TInput1 input, ref ReadOptions readOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TOutput1 output = default;
            return (Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ref readOptions, out _, userContext), output);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(ref TKey1 key, ref TInput1 input, ref TOutput1 output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            HashEntryInfo hei = default;
            try
            {
                var status = EnterKernelForRead<TKeyLocker, TEpochGuard>(readOptions.KeyHash ?? GetKeyHash(ref key), PartitionId1, out hei);
                if (!status.Found)
                {
                    output = default;
                    recordMetadata = default;
                    return status;
                }

                return ItemContext1.Read<TKeyLocker>(ref hei, ref key, ref input, ref output, ref readOptions, out recordMetadata, userContext);
            }
            finally
            {
                ExitKernelForRead<TKeyLocker, TEpochGuard>(ref hei);
            }
        }

        #endregion Read store1

        #region Read store2
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(TKey2 key, ref TOutput2 output, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TInput2 input = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(ref TKey2 key, ref TOutput2 output, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TInput2 input = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(TKey2 key, TInput2 input, ref TOutput2 output, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            ReadOptions readOptions = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ref readOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(ref TKey2 key, ref TInput2 input, ref TOutput2 output, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            ReadOptions readOptions = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ref readOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(TKey2 key, TInput2 input, ref TOutput2 output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ref readOptions, out recordMetadata, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput2 output) Read<TKeyLocker, TEpochGuard>(TKey2 key, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TInput2 input = default;
            TOutput2 output = default;
            return (Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, userContext), output);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput2 output) Read<TKeyLocker, TEpochGuard>(TKey2 key, TInput2 input, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TOutput2 output = default;
            return (Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, userContext), output);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput2 output) Read<TKeyLocker, TEpochGuard>(ref TKey2 key, ref TInput2 input, ref ReadOptions readOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TOutput2 output = default;
            return (Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ref readOptions, out _, userContext), output);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(ref TKey2 key, ref TInput2 input, ref TOutput2 output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            HashEntryInfo hei = default;
            try
            {
                var status = EnterKernelForRead<TKeyLocker, TEpochGuard>(readOptions.KeyHash ?? GetKeyHash(ref key), PartitionId2, out hei);
                if (!status.Found)
                {
                    output = default;
                    recordMetadata = default;
                    return status;
                }
                return ItemContext2.Read<TKeyLocker>(ref hei, ref key, ref input, ref output, ref readOptions, out recordMetadata, userContext);
            }
            finally
            {
                ExitKernelForRead<TKeyLocker, TEpochGuard>(ref hei);
            }
        }
        #endregion Read store2

        #region Read both stores
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status, ushort storeId) Read<TKeyLocker, TEpochGuard>(ref TKey1 key1, ref TInput1 input1, ref TOutput1 output1, ref TOutput2 output2, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            ReadOptions readOptions = default;
            return DualRead<TKeyLocker, TEpochGuard>(ref key1, ref input1, ref output1, ref output2, ref readOptions, out _, userContext);
        }

        public (Status, ushort storeId) Read<TKeyLocker, TEpochGuard>(TKey1 key1, TInput1 input1, ref TOutput1 output1, ref TOutput2 output2, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            ReadOptions readOptions = default;
            return DualRead<TKeyLocker, TEpochGuard>(ref key1, ref input1, ref output1, ref output2, ref readOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status, ushort storeId) Read<TKeyLocker, TEpochGuard>(TKey1 key1, TInput1 input1, ref TOutput1 output1, ref TOutput2 output2, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => DualRead<TKeyLocker, TEpochGuard>(ref key1, ref input1, ref output1, ref output2, ref readOptions, out recordMetadata, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status, ushort) DualRead<TKeyLocker, TEpochGuard>(ref TKey1 key1, ref TInput1 input1, ref TOutput1 output1, ref TOutput2 output2, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            var keyHash = readOptions.KeyHash ?? GetKeyHash(ref key1);
            HashEntryInfo hei = default;

            try
            {
                var status = EnterKernelForRead<TKeyLocker, TEpochGuard>(keyHash, PartitionId1, out hei);
                output1 = default;
                output2 = default;
                recordMetadata = default;

                TKey2 key2;
                TInput2 input2;
                if (status.Found)
                { 
                    // Tag was found in the first store; see if it has the key (it may have been a collision)
                    status = ItemContext1.Read<TKeyLocker>(ref hei, ref key1, ref input1, ref output1, ref readOptions, out recordMetadata, userContext);
                    if (status.IsPending)
                        (status, output1) = ItemContext1.GetSinglePendingResult<TKeyLocker>(pendingMetrics);
                    if (status.Found || !IsDual)
                        return (status, StoreId1);

                    // Did not find the key and the bucket is still locked--move to the second store.
                    inputConverter.ConvertForRead(ref key1, ref input1, out key2, out input2, out output2);
                    Debug.Assert(hei.HashCodeEquals(GetKeyHash(ref key2)), "Main and Object hash codes are not the same");
                    status = Kernel.EnterForReadDual2(PartitionId2, ref hei);
                }
                else
                {
                    // First partition tag was not found so the bucket was not locked. Try to find and lock for the second partition.
                    status = EnterKernelForRead<TKeyLocker, TEpochGuard>(keyHash, PartitionId2, out hei);
                    if (status.NotFound)    // Tag was not found for either store's partition bit.
                        return (status, StoreId2);
                    inputConverter.ConvertForRead(ref key1, ref input1, out key2, out input2, out output2);
                }

                status = ItemContext2.Read<TKeyLocker>(ref hei, ref key2, ref input2, ref output2, ref readOptions, out recordMetadata, userContext);
                if (status.IsPending)
                    (status, output2) = ItemContext2.GetSinglePendingResult<TKeyLocker>(pendingMetrics);
                return (status, StoreId2);
            }
            finally
            {
                ExitKernelForRead<TKeyLocker, TEpochGuard>(ref hei);
            }
        }
        #endregion Read both stores

        #region ReadAtAddress
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress<TKeyLocker, TEpochGuard>(long address, ref TInput1 input, ref TOutput1 output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TKey1 key = default;
            return ReadAtAddress<TKeyLocker, TEpochGuard>(address, ref key, isNoKey: true, ref input, ref output, ref readOptions, out recordMetadata, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress<TKeyLocker, TEpochGuard>(long address, ref TKey1 key, bool isNoKey, ref TInput1 input, ref TOutput1 output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            HashEntryInfo hei = default;
            try
            {
                var status = Store1.EnterKernelForReadAtAddress
                        <DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>,
                        TKeyLocker, TEpochGuard>(ref KernelSession, PartitionId1, address, ref key, readOptions.KeyHash ?? GetKeyHash(ref key), isNoKey, out hei);
                if (!status.Found)
                {
                    output = default;
                    recordMetadata = default;
                    return status;
                }

                return ItemContext1.ReadAtAddress<TKeyLocker>(ref hei, ref key, isNoKey, ref input, ref output, ref readOptions, out recordMetadata, userContext);
            }
            finally
            {
                ExitKernelForRead<TKeyLocker, TEpochGuard>(ref hei);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress<TKeyLocker, TEpochGuard>(long address, ref TInput2 input, ref TOutput2 output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TKey2 key = default;
            return ReadAtAddress<TKeyLocker, TEpochGuard>(address, ref key, isNoKey: true, ref input, ref output, ref readOptions, out recordMetadata, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress<TKeyLocker, TEpochGuard>(long address, ref TKey2 key, bool isNoKey, ref TInput2 input, ref TOutput2 output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            HashEntryInfo hei = default;
            try
            {
                var status = Store2.EnterKernelForReadAtAddress
                        <DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>,
                        TKeyLocker, TEpochGuard>(ref KernelSession, PartitionId2, address, ref key, readOptions.KeyHash ?? GetKeyHash(ref key), isNoKey, out hei);
                if (!status.Found)
                {
                    output = default;
                    recordMetadata = default;
                    return status;
                }

                return ItemContext2.ReadAtAddress<TKeyLocker>(ref hei, ref key, isNoKey, ref input, ref output, ref readOptions, out recordMetadata, userContext);
            }
            finally
            {
                ExitKernelForRead<TKeyLocker, TEpochGuard>(ref hei);
            }
        }
        #endregion

        #region Upsert store1
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(TKey1 key, TValue1 desiredValue, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TInput1 input = default;
            TOutput1 output = default;
            UpsertOptions upsertOptions = default;
            return Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, ref output, ref upsertOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(ref TKey1 key, ref TValue1 desiredValue, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TInput1 input = default;
            TOutput1 output = default;
            UpsertOptions upsertOptions = default;
            return Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, ref output, ref upsertOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(TKey1 key, TInput1 input, TValue1 desiredValue, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(ref TKey1 key, ref TInput1 input, ref TValue1 desiredValue, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            UpsertOptions upsertOptions = default;
            TOutput1 output = default;
            return Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, ref output, ref upsertOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(TKey1 key, TInput1 input, TValue1 desiredValue, ref TOutput1 output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, ref output, ref upsertOptions, out recordMetadata, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(ref TKey1 key, ref TInput1 input, ref TValue1 desiredValue, ref TOutput1 output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            HashEntryInfo hei = default;
            try
            {
                _ = EnterKernelForUpdate<TKeyLocker, TEpochGuard>(upsertOptions.KeyHash ?? GetKeyHash(ref key), PartitionId1, KernelSession.clientSession1.Store.Log.BeginAddress, out hei);
                return ItemContext1.Upsert<TKeyLocker>(ref hei, ref key, ref input, ref desiredValue, ref output, out recordMetadata, userContext);
            }
            finally
            {
                ExitKernelForUpdate<TKeyLocker, TEpochGuard>(ref hei);
            }
        }
        #endregion Upsert store1

        #region Upsert store2
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(TKey2 key, TValue2 desiredValue, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TInput2 input = default;
            TOutput2 output = default;
            UpsertOptions upsertOptions = default;
            return Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, ref output, ref upsertOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(ref TKey2 key, ref TValue2 desiredValue, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            TInput2 input = default;
            TOutput2 output = default;
            UpsertOptions upsertOptions = default;
            return Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, ref output, ref upsertOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(TKey2 key, TInput2 input, TValue2 desiredValue, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(ref TKey2 key, ref TInput2 input, ref TValue2 desiredValue, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            UpsertOptions upsertOptions = default;
            TOutput2 output = default;
            return Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, ref output, ref upsertOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(TKey2 key, TInput2 input, TValue2 desiredValue, ref TOutput2 output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, ref output, ref upsertOptions, out recordMetadata, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert<TKeyLocker, TEpochGuard>(ref TKey2 key, ref TInput2 input, ref TValue2 desiredValue, ref TOutput2 output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            HashEntryInfo hei = default;
            try
            {
                _ = EnterKernelForUpdate<TKeyLocker, TEpochGuard>(upsertOptions.KeyHash ?? GetKeyHash(ref key), PartitionId2, KernelSession.clientSession2.Store.Log.BeginAddress, out hei);
                return ItemContext2.Upsert<TKeyLocker>(ref hei, ref key, ref input, ref desiredValue, ref output, out recordMetadata, userContext);
            }
            finally
            {
                ExitKernelForUpdate<TKeyLocker, TEpochGuard>(ref hei);
            }
        }
        #endregion Upsert store2

        #region Upsert both stores
        // We do not Upsert to both stores because the Key+Value will be specific to a single store.
        #endregion Upsert both stores

        #region RMW store1
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(TKey1 key, TInput1 input, ref TOutput1 output, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => RMW<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(TKey1 key, TInput1 input, ref TOutput1 output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => RMW<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ref rmwOptions, out recordMetadata, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(ref TKey1 key, ref TInput1 input, ref TOutput1 output, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            RMWOptions rmwOptions = default;
            return RMW<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ref rmwOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(ref TKey1 key, ref TInput1 input, ref TOutput1 output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            HashEntryInfo hei = default;
            try
            {
                _ = EnterKernelForUpdate<TKeyLocker, TEpochGuard>(rmwOptions.KeyHash ?? GetKeyHash(ref key), PartitionId1, KernelSession.clientSession1.Store.Log.BeginAddress, out hei);
                return ItemContext1.RMW<TKeyLocker>(ref hei, ref key, ref input, ref output, out recordMetadata, userContext);
            }
            finally
            {
                ExitKernelForUpdate<TKeyLocker, TEpochGuard>(ref hei);
            }
        }
        #endregion RMW store1

        #region RMW store2
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(TKey2 key, TInput2 input, ref TOutput2 output, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => RMW<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(TKey2 key, TInput2 input, ref TOutput2 output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => RMW<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ref rmwOptions, out recordMetadata, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(ref TKey2 key, ref TInput2 input, ref TOutput2 output, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            RMWOptions rmwOptions = default;
            return RMW<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ref rmwOptions, out _, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(ref TKey2 key, ref TInput2 input, ref TOutput2 output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            HashEntryInfo hei = default;
            try
            {
                _ = EnterKernelForUpdate<TKeyLocker, TEpochGuard>(rmwOptions.KeyHash ?? GetKeyHash(ref key), PartitionId2, KernelSession.clientSession2.Store.Log.BeginAddress, out hei);
                return ItemContext2.RMW<TKeyLocker>(ref hei, ref key, ref input, ref output, out recordMetadata, userContext);
            }
            finally
            {
                ExitKernelForUpdate<TKeyLocker, TEpochGuard>(ref hei);
            }
        }
        #endregion RMW store2

        #region RMW both stores
        // We do not RMW to both stores because the Key+Input will be specific to a single store.
        #endregion RMW both stores

        #region Delete store1
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker, TEpochGuard>(TKey1 key, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => Delete<TKeyLocker, TEpochGuard>(ref key, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker, TEpochGuard>(ref TKey1 key, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            DeleteOptions deleteOptions = default;
            return Delete<TKeyLocker, TEpochGuard>(ref key, ref deleteOptions, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker, TEpochGuard>(TKey1 key, ref DeleteOptions deleteOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => Delete<TKeyLocker, TEpochGuard>(ref key, ref deleteOptions, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker, TEpochGuard>(ref TKey1 key, ref DeleteOptions deleteOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            HashEntryInfo hei = default;
            try
            {
                _ = EnterKernelForUpdate<TKeyLocker, TEpochGuard>(deleteOptions.KeyHash ?? GetKeyHash(ref key), PartitionId1, TsavoriteKernel.DoNotCreateSlotAddress, out hei);
                return ItemContext1.Delete<TKeyLocker>(ref hei, ref key, userContext);
            }
            finally
            {
                ExitKernelForUpdate<TKeyLocker, TEpochGuard>(ref hei);
            }
        }
        #endregion Delete store1

        #region Delete store2
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker, TEpochGuard>(TKey2 key, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => Delete<TKeyLocker, TEpochGuard>(ref key, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker, TEpochGuard>(ref TKey2 key, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            DeleteOptions deleteOptions = default;
            return Delete<TKeyLocker, TEpochGuard>(ref key, ref deleteOptions, userContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker, TEpochGuard>(TKey2 key, ref DeleteOptions deleteOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => Delete<TKeyLocker, TEpochGuard>(ref key, ref deleteOptions, userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker, TEpochGuard>(ref TKey2 key, ref DeleteOptions deleteOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            HashEntryInfo hei = default;
            try
            {
                _ = EnterKernelForUpdate<TKeyLocker, TEpochGuard>(deleteOptions.KeyHash ?? GetKeyHash(ref key), PartitionId1, TsavoriteKernel.DoNotCreateSlotAddress, out hei);
                return ItemContext2.Delete<TKeyLocker>(ref hei, ref key, userContext);
            }
            finally
            {
                ExitKernelForUpdate<TKeyLocker, TEpochGuard>(ref hei);
            }
        }
        #endregion Delete store2

        #region Delete both stores
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status DualDelete<TKeyLocker, TEpochGuard>(ref TKey1 key1, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        { 
            DeleteOptions deleteOptions = default;
            return DualDelete<TKeyLocker, TEpochGuard>(ref key1, ref deleteOptions);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status DualDelete<TKeyLocker, TEpochGuard>(ref TKey1 key1, ref DeleteOptions deleteOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            HashEntryInfo hei = default;
            try
            {
                // Delete doesn't go to disk, so we will just delete it in both stores blindly. If the key slot is not found, then there is nothing to delete.
                var status1 = EnterKernelForUpdate<TKeyLocker, TEpochGuard>(deleteOptions.KeyHash ?? GetKeyHash(ref key1), PartitionId1, TsavoriteKernel.DoNotCreateSlotAddress, out hei);
                if (status1.Found)
                    status1 = ItemContext1.Delete<TKeyLocker>(ref hei, ref key1, userContext);

                // Create key for to the second store.
                inputConverter.ConvertKey(ref key1, out var key2);
                Debug.Assert(hei.HashCodeEquals(GetKeyHash(ref key2)), "Main and Object hash codes are not the same");
                var status2 = Kernel.EnterForReadDual2(PartitionId2, ref hei);
                if (status2.Found)
                    status2 = ItemContext2.Delete<TKeyLocker>(ref hei, ref key2, userContext);
                return (status1.Found || status2.Found) ? new(StatusCode.Found) : new(StatusCode.NotFound);
            }
            finally
            {
                ExitKernelForUpdate<TKeyLocker, TEpochGuard>(ref hei);
            }
        }
        #endregion Delete both stores

        #region ResetModified
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetModified<TKeyLocker, TEpochGuard>(TKey1 key1, out RecordInfo modifiedInfo, bool reset = true)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => ResetModified<TKeyLocker, TEpochGuard>(ref key1, out modifiedInfo, reset);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetModified<TKeyLocker, TEpochGuard>(ref TKey1 key1, out RecordInfo modifiedInfo, bool reset = true)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            HashEntryInfo hei = default;
            try
            {
                // Reset doesn't go to disk, so we will just Reset it in both stores blindly. If the key slot is not found, then there is nothing to reset.
                var status = EnterKernelForUpdate<TKeyLocker, TEpochGuard>(GetKeyHash(ref key1), PartitionId1, TsavoriteKernel.DoNotCreateSlotAddress, out hei);
                if (status.Found)
                    ItemContext1.ResetModified<TKeyLocker>(ref hei, ref key1, out modifiedInfo, reset);
                else
                    modifiedInfo = default;
            }
            finally
            {
                ExitKernelForUpdate<TKeyLocker, TEpochGuard>(ref hei);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetModified<TKeyLocker, TEpochGuard>(TKey2 key2, out RecordInfo modifiedInfo, bool reset = true)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
            => ResetModified<TKeyLocker, TEpochGuard>(ref key2, out modifiedInfo, reset);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetModified<TKeyLocker, TEpochGuard>(ref TKey2 key2, out RecordInfo modifiedInfo, bool reset = true)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            HashEntryInfo hei = default;
            try
            {
                // Reset doesn't go to disk, so we will just Reset it in both stores blindly. If the key slot is not found, then there is nothing to reset.
                var status = EnterKernelForUpdate<TKeyLocker, TEpochGuard>(GetKeyHash(ref key2), PartitionId1, TsavoriteKernel.DoNotCreateSlotAddress, out hei);
                if (status.Found)
                    ItemContext2.ResetModified<TKeyLocker>(ref hei, ref key2, out modifiedInfo, reset);
                else
                    modifiedInfo = default;
            }
            finally
            {
                ExitKernelForUpdate<TKeyLocker, TEpochGuard>(ref hei);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DualResetModified<TKeyLocker, TEpochGuard>(ref TKey1 key1, out RecordInfo modifiedInfo, bool reset = true)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2>>
        {
            HashEntryInfo hei = default;
            modifiedInfo = default;
            try
            {
                // Reset doesn't go to disk, so we will just Reset it in both stores blindly. If the key slot is not found, then there is nothing to reset.
                var status = EnterKernelForUpdate<TKeyLocker, TEpochGuard>(GetKeyHash(ref key1), PartitionId1, TsavoriteKernel.DoNotCreateSlotAddress, out hei);
                if (status.Found)
                    ItemContext1.ResetModified<TKeyLocker>(ref hei, ref key1, out modifiedInfo, reset);

                // Create key for to the second store.
                inputConverter.ConvertKey(ref key1, out var key2);
                Debug.Assert(hei.HashCodeEquals(GetKeyHash(ref key2)), "Main and Object hash codes are not the same");
                status = Kernel.EnterForReadDual2(PartitionId2, ref hei);
                if (status.Found)
                    ItemContext2.ResetModified<TKeyLocker>(ref hei, ref key2, out modifiedInfo, reset);
            }
            finally
            {
                ExitKernelForUpdate<TKeyLocker, TEpochGuard>(ref hei);
            }
        }
        #endregion ResetModified

        #endregion ITsavoriteContext
    }
}