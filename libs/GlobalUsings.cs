// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if SERVER_PROJECT || TEST_PROJECT || CLUSTER_PROJECT
global using BasicGarnetApi = Garnet.server.GarnetApi<
    Tsavorite.core.BasicContext<Garnet.common.FixedSpanByteKey, Garnet.server.StringInput, Garnet.server.StringOutput, long, Garnet.server.MainSessionFunctions,
        /* MainStoreFunctions */ Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>,
    Tsavorite.core.BasicContext<Garnet.common.FixedSpanByteKey, Garnet.server.ObjectInput, Garnet.server.ObjectOutput, long, Garnet.server.ObjectSessionFunctions,
        /* ObjectStoreFunctions */ Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>,
    Tsavorite.core.BasicContext<Garnet.common.FixedSpanByteKey, Garnet.server.UnifiedInput, Garnet.server.UnifiedOutput, long, Garnet.server.UnifiedSessionFunctions,
        /* UnifiedStoreFunctions */ Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>>;
#endif

#if SERVER_PROJECT
global using ConsistentReadGarnetApi = Garnet.server.GarnetApi<
        Tsavorite.core.ConsistentReadContext<Garnet.common.FixedSpanByteKey, Garnet.server.StringInput, Garnet.server.StringOutput, long, Garnet.server.MainSessionFunctions,
            /* MainStoreFunctions */ Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
            Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>,
        Tsavorite.core.ConsistentReadContext<Garnet.common.FixedSpanByteKey, Garnet.server.ObjectInput, Garnet.server.ObjectOutput, long, Garnet.server.ObjectSessionFunctions,
            /* ObjectStoreFunctions */ Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
            Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>,
        Tsavorite.core.ConsistentReadContext<Garnet.common.FixedSpanByteKey, Garnet.server.UnifiedInput, Garnet.server.UnifiedOutput, long, Garnet.server.UnifiedSessionFunctions,
            /* UnifiedStoreFunctions */ Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
            Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>>;
global using TransactionalConsistentReadGarnetApi = Garnet.server.GarnetApi<
        Tsavorite.core.TransactionalConsistentReadContext<Garnet.common.FixedSpanByteKey, Garnet.server.StringInput, Garnet.server.StringOutput, long, Garnet.server.MainSessionFunctions,
            /* MainStoreFunctions */ Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
            Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>,
        Tsavorite.core.TransactionalConsistentReadContext<Garnet.common.FixedSpanByteKey, Garnet.server.ObjectInput, Garnet.server.ObjectOutput, long, Garnet.server.ObjectSessionFunctions,
            /* ObjectStoreFunctions */ Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
            Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>,
        Tsavorite.core.TransactionalConsistentReadContext<Garnet.common.FixedSpanByteKey, Garnet.server.UnifiedInput, Garnet.server.UnifiedOutput, long, Garnet.server.UnifiedSessionFunctions,
            /* UnifiedStoreFunctions */ Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
            Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>>;
#endif

#if SERVER_PROJECT || TEST_PROJECT || HOST_PROJECT
global using StoreAllocator = Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>;
global using StoreFunctions = Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>;
#endif

#if SERVER_PROJECT || CLUSTER_PROJECT
global using StringBasicContext = Tsavorite.core.BasicContext<
    Garnet.common.FixedSpanByteKey,
    Garnet.server.StringInput,
    Garnet.server.StringOutput,
    long, Garnet.server.MainSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>;

global using VectorBasicContext = Tsavorite.core.BasicContext<
    Garnet.common.VectorElementKey,
    Garnet.server.VectorInput,
    Garnet.server.VectorOutput,
    long, Garnet.server.VectorSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>;
#endif

#if SERVER_PROJECT
global using TransactionalGarnetApi = Garnet.server.GarnetApi<
    Tsavorite.core.TransactionalContext<Garnet.common.FixedSpanByteKey, Garnet.server.StringInput, Garnet.server.StringOutput, long, Garnet.server.MainSessionFunctions,
        /* MainStoreFunctions */ Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>,
    Tsavorite.core.TransactionalContext<Garnet.common.FixedSpanByteKey, Garnet.server.ObjectInput, Garnet.server.ObjectOutput, long, Garnet.server.ObjectSessionFunctions,
        /* ObjectStoreFunctions */ Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>,
    Tsavorite.core.TransactionalContext<Garnet.common.FixedSpanByteKey, Garnet.server.UnifiedInput, Garnet.server.UnifiedOutput, long, Garnet.server.UnifiedSessionFunctions,
        /* UnifiedStoreFunctions */ Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>>;

global using StringTransactionalContext = Tsavorite.core.TransactionalContext<
    Garnet.common.FixedSpanByteKey,
    Garnet.server.StringInput,
    Garnet.server.StringOutput,
    long, Garnet.server.MainSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>;

global using StringTransactionalUnsafeContext = Tsavorite.core.TransactionalUnsafeContext<
    Garnet.common.FixedSpanByteKey,
    Garnet.server.StringInput,
    Garnet.server.StringOutput,
    long,
    Garnet.server.MainSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>;

global using ConsistentReadStringBasicContext = Tsavorite.core.ConsistentReadContext<
    Garnet.common.FixedSpanByteKey,
    Garnet.server.StringInput,
    Garnet.server.StringOutput,
    long,
    Garnet.server.MainSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>;

global using ConsistentReadStringTransactionalContext = Tsavorite.core.TransactionalConsistentReadContext<
    Garnet.common.FixedSpanByteKey,
    Garnet.server.StringInput,
    Garnet.server.StringOutput,
    long,
    Garnet.server.MainSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>;

global using ObjectBasicContext = Tsavorite.core.BasicContext<
    Garnet.common.FixedSpanByteKey,
    Garnet.server.ObjectInput,
    Garnet.server.ObjectOutput,
    long, Garnet.server.ObjectSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>;

global using ObjectTransactionalContext = Tsavorite.core.TransactionalContext<
    Garnet.common.FixedSpanByteKey,
    Garnet.server.ObjectInput,
    Garnet.server.ObjectOutput,
    long,
    Garnet.server.ObjectSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>;

global using ConsistentReadObjectBasicContext = Tsavorite.core.ConsistentReadContext<
    Garnet.common.FixedSpanByteKey,
    Garnet.server.ObjectInput,
    Garnet.server.ObjectOutput,
    long, Garnet.server.ObjectSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>;

global using ConsistentReadObjectTransactionalContext = Tsavorite.core.TransactionalConsistentReadContext<
    Garnet.common.FixedSpanByteKey,
    Garnet.server.ObjectInput,
    Garnet.server.ObjectOutput,
    long,
    Garnet.server.ObjectSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>;

global using UnifiedBasicContext = Tsavorite.core.BasicContext<
    Garnet.common.FixedSpanByteKey,
    Garnet.server.UnifiedInput,
    Garnet.server.UnifiedOutput,
    long,
    Garnet.server.UnifiedSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>;

global using UnifiedTransactionalContext = Tsavorite.core.TransactionalContext<
    Garnet.common.FixedSpanByteKey,
    Garnet.server.UnifiedInput,
    Garnet.server.UnifiedOutput,
    long,
    Garnet.server.UnifiedSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>;

global using ConsistentReadUnifiedBasicContext = Tsavorite.core.ConsistentReadContext<
    Garnet.common.FixedSpanByteKey,
    Garnet.server.UnifiedInput,
    Garnet.server.UnifiedOutput,
    long,
    Garnet.server.UnifiedSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>;

global using ConsistentReadUnifiedTransactionalContext = Tsavorite.core.TransactionalConsistentReadContext<
    Garnet.common.FixedSpanByteKey,
    Garnet.server.UnifiedInput,
    Garnet.server.UnifiedOutput,
    long,
    Garnet.server.UnifiedSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>;

global using VectorTransactionalContext = Tsavorite.core.TransactionalContext<
    Garnet.common.VectorElementKey,
    Garnet.server.VectorInput,
    Garnet.server.VectorOutput,
    long, Garnet.server.VectorSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>;

#endif