// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet;

using MainStoreAllocator =
    SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

internal class MainStoreWrapper : IDisposable
{
    public readonly TsavoriteKV<SpanByte, SpanByte, MainStoreFunctions, MainStoreAllocator> store;

    readonly KVSettings<SpanByte, SpanByte> kvSettings;

    public MainStoreWrapper(TsavoriteKV<SpanByte, SpanByte, MainStoreFunctions, MainStoreAllocator> store,
        KVSettings<SpanByte, SpanByte> kvSettings)
    {
        this.store = store;
        this.kvSettings = kvSettings;
    }

    public void Dispose()
    {
        store?.Dispose();
        kvSettings?.LogDevice?.Dispose();
        kvSettings?.Dispose();
    }
}