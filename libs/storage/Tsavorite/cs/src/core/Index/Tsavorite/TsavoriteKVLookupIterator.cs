// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core;

internal sealed class TsavoriteKVLookupIterator<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> : ITsavoriteScanIteratorWithPush<TKey, TValue>
    where TFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
    where TStoreFunctions : IStoreFunctions<TKey, TValue>
    where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
{
    private readonly TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store;
    private readonly ITsavoriteScanIterator<TKey, TValue> mainKvIter;
    private readonly IPushScanIterator<TKey> pushScanIterator;
    private ITsavoriteScanIterator<TKey, TValue> tempKvIter;

    public TsavoriteKVLookupIterator(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, TFunctions functions, long untilAddress, ILoggerFactory loggerFactory = null)
    {
        this.store = store;
        mainKvIter = store.Log.Scan(store.Log.BeginAddress, untilAddress);
        pushScanIterator = mainKvIter as IPushScanIterator<TKey>;
    }

    public long CurrentAddress => mainKvIter.CurrentAddress;

    public long NextAddress => mainKvIter.NextAddress;

    public long BeginAddress => mainKvIter.BeginAddress;

    public long EndAddress => mainKvIter.EndAddress;

    public void Dispose()
    {
        mainKvIter?.Dispose();
        tempKvIter?.Dispose();
    }

    public ref TKey GetKey() => ref mainKvIter.GetKey();

    public ref TValue GetValue() => ref mainKvIter.GetValue();

    public bool GetNext(out RecordInfo recordInfo)
    {
        throw new NotImplementedException();
    }

    public bool PushNext<TScanFunctions>(ref TScanFunctions scanFunctions, long numRecords, out bool stop)
        where TScanFunctions : IScanIteratorFunctions<TKey, TValue>
    {
        throw new NotImplementedException();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    bool IsTailmostMainKvRecord(ref TKey key, RecordInfo mainKvRecordInfo, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
    {
        throw new NotImplementedException();
    }

    public bool GetNext(out RecordInfo recordInfo, out TKey key, out TValue value)
    {
        throw new NotImplementedException();
    }
}