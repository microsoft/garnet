// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if SERVER_PROJECT || TEST_PROJECT || CLUSTER_PROJECT
global using BasicGarnetApi = Garnet.server.GarnetApi<
    Tsavorite.core.BasicContext<Garnet.common.FixedSpanByteKey, Garnet.server.StringInput, Garnet.server.StringOutput, long, Garnet.server.MainSessionFunctions,
        /* MainStoreFunctions */ Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>,
        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>>>,
    Tsavorite.core.BasicContext<Garnet.common.FixedSpanByteKey, Garnet.server.ObjectInput, Garnet.server.ObjectOutput, long, Garnet.server.ObjectSessionFunctions,
        /* ObjectStoreFunctions */ Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>,
        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>>>,
    Tsavorite.core.BasicContext<Garnet.common.FixedSpanByteKey, Garnet.server.UnifiedInput, Garnet.server.UnifiedOutput, long, Garnet.server.UnifiedSessionFunctions,
        /* UnifiedStoreFunctions */ Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>,
        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>>>>;
#endif

#if SERVER_PROJECT || TEST_PROJECT || HOST_PROJECT
global using StoreAllocator = Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>>;
global using StoreFunctions = Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>;
#endif

#if SERVER_PROJECT || CLUSTER_PROJECT
global using StringBasicContext = Tsavorite.core.BasicContext<
    Garnet.common.FixedSpanByteKey,
    Garnet.server.StringInput,
    Garnet.server.StringOutput,
    long, Garnet.server.MainSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>>>;

global using VectorBasicContext = Tsavorite.core.BasicContext<
    Garnet.common.VectorElementKey,
    Garnet.server.VectorInput,
    Garnet.server.VectorOutput,
    long, Garnet.server.VectorSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>>>;
#endif

#if SERVER_PROJECT
global using TransactionalGarnetApi = Garnet.server.GarnetApi<
    Tsavorite.core.TransactionalContext<Garnet.common.FixedSpanByteKey, Garnet.server.StringInput, Garnet.server.StringOutput, long, Garnet.server.MainSessionFunctions,
        /* MainStoreFunctions */ Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>,
        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>>>,
    Tsavorite.core.TransactionalContext<Garnet.common.FixedSpanByteKey, Garnet.server.ObjectInput, Garnet.server.ObjectOutput, long, Garnet.server.ObjectSessionFunctions,
        /* ObjectStoreFunctions */ Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>,
        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>>>,
    Tsavorite.core.TransactionalContext<Garnet.common.FixedSpanByteKey, Garnet.server.UnifiedInput, Garnet.server.UnifiedOutput, long, Garnet.server.UnifiedSessionFunctions,
        /* UnifiedStoreFunctions */ Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>,
        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>>>>;

global using StringTransactionalContext = Tsavorite.core.TransactionalContext<
    Garnet.common.FixedSpanByteKey,
    Garnet.server.StringInput,
    Garnet.server.StringOutput,
    long, Garnet.server.MainSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>>>;

global using ObjectBasicContext = Tsavorite.core.BasicContext<
    Garnet.common.FixedSpanByteKey,
    Garnet.server.ObjectInput,
    Garnet.server.ObjectOutput,
    long, Garnet.server.ObjectSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>>>;

global using ObjectTransactionalContext = Tsavorite.core.TransactionalContext<
    Garnet.common.FixedSpanByteKey,
    Garnet.server.ObjectInput,
    Garnet.server.ObjectOutput,
    long,
    Garnet.server.ObjectSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>>>;

global using UnifiedBasicContext = Tsavorite.core.BasicContext<
    Garnet.common.FixedSpanByteKey,
    Garnet.server.UnifiedInput,
    Garnet.server.UnifiedOutput,
    long, Garnet.server.UnifiedSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>>>;

global using UnifiedTransactionalContext = Tsavorite.core.TransactionalContext<
    Garnet.common.FixedSpanByteKey,
    Garnet.server.UnifiedInput,
    Garnet.server.UnifiedOutput,
    long, Garnet.server.UnifiedSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>>>;

global using VectorTransactionalContext = Tsavorite.core.TransactionalContext<
    Garnet.common.VectorElementKey,
    Garnet.server.VectorInput,
    Garnet.server.VectorOutput,
    long, Garnet.server.VectorSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTrigger>>>;

#endif