// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

global using BasicGarnetApi = Garnet.server.GarnetApi<
    Tsavorite.core.BasicContext<Garnet.server.StringInput, Tsavorite.core.SpanByteAndMemory, long, Garnet.server.MainSessionFunctions,
        /* MainStoreFunctions */ Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>,
    Tsavorite.core.BasicContext<Garnet.server.ObjectInput, Garnet.server.ObjectOutput, long, Garnet.server.ObjectSessionFunctions,
        /* ObjectStoreFunctions */ Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>,
    Tsavorite.core.BasicContext<Garnet.server.UnifiedInput, Garnet.server.UnifiedOutput, long, Garnet.server.UnifiedSessionFunctions,
        /* UnifiedStoreFunctions */ Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>>;
#if SERVER_PROJECT
global using StoreAllocator = Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>;
global using StoreFunctions = Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>;
global using TransactionalGarnetApi = Garnet.server.GarnetApi<
    Tsavorite.core.TransactionalContext<Garnet.server.StringInput, Tsavorite.core.SpanByteAndMemory, long, Garnet.server.MainSessionFunctions,
        /* MainStoreFunctions */ Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>,
    Tsavorite.core.TransactionalContext<Garnet.server.ObjectInput, Garnet.server.ObjectOutput, long, Garnet.server.ObjectSessionFunctions,
        /* ObjectStoreFunctions */ Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>,
    Tsavorite.core.TransactionalContext<Garnet.server.UnifiedInput, Garnet.server.UnifiedOutput, long, Garnet.server.UnifiedSessionFunctions,
        /* UnifiedStoreFunctions */ Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>>;

global using StoreBasicContext = Tsavorite.core.BasicContext<
    Garnet.server.StringInput,
    Tsavorite.core.SpanByteAndMemory,
    long, Garnet.server.MainSessionFunctions,
    Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>;

global using StoreTransactionalContext = Tsavorite.core.TransactionalContext<
    Garnet.server.StringInput,
    Tsavorite.core.SpanByteAndMemory,
    long, Garnet.server.MainSessionFunctions,
    Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>;

global using ObjectBasicContext = Tsavorite.core.BasicContext<
    Garnet.server.ObjectInput,
    Garnet.server.ObjectOutput,
    long, Garnet.server.ObjectSessionFunctions,
    Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>;

global using ObjectTransactionalContext = Tsavorite.core.TransactionalContext<
    Garnet.server.ObjectInput,
    Garnet.server.ObjectOutput,
    long,
    Garnet.server.ObjectSessionFunctions,
    Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>;

global using UnifiedBasicContext = Tsavorite.core.BasicContext<
    Garnet.server.UnifiedInput,
    Garnet.server.UnifiedOutput,
    long, Garnet.server.UnifiedSessionFunctions,
    Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>;

global using UnifiedTransactionalContext = Tsavorite.core.TransactionalContext<
    Garnet.server.UnifiedInput,
    Garnet.server.UnifiedOutput,
    long, Garnet.server.UnifiedSessionFunctions,
    Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>;

#endif