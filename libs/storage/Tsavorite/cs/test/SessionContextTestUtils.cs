// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test.ReadCacheTests
{
    internal enum RecordRegion { Immutable, OnDisk, Mutable }

    internal static class ReadCacheChainTestUtils
    {
        internal static (long logicalAddress, long physicalAddress) SkipReadCacheChain<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, TestSpanByteKey key)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            var (la, pa) = GetHashChain(store, key, out _, out _, out bool isReadCache);
            while (isReadCache)
                (la, pa) = NextInChain(store, pa, out _, out _, ref isReadCache);
            return (la, pa);
        }

        static (long logicalAddress, long physicalAddress) GetHashChain<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, TestSpanByteKey key, out PinnedSpanByte recordKey, out bool invalid, out bool isReadCache)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            var tagExists = store.FindHashBucketEntryForKey(key, out var entry);
            ClassicAssert.IsTrue(tagExists);

            isReadCache = entry.IsReadCache;
            var log = isReadCache ? store.readcacheBase : store.hlogBase;
            var pa = log.GetPhysicalAddress(entry.Address);
            recordKey = PinnedSpanByte.FromPinnedSpan(LogRecord.GetInlineKey(pa));
            invalid = LogRecord.GetInfo(pa).Invalid;

            return (entry.Address, pa);
        }

        static (long logicalAddress, long physicalAddress) NextInChain<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, long physicalAddress, out PinnedSpanByte recordKey, out bool invalid, ref bool isReadCache)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            var log = isReadCache ? store.readcacheBase : store.hlogBase;
            var info = LogRecord.GetInfo(physicalAddress);
            var la = info.PreviousAddress;

            isReadCache = LogAddress.IsReadCache(la);
            log = isReadCache ? store.readcacheBase : store.hlogBase;
            var pa = log.GetPhysicalAddress(la);
            recordKey = PinnedSpanByte.FromPinnedSpan(LogRecord.GetInlineKey(pa));
            invalid = LogRecord.GetInfo(pa).Invalid;
            return (la, pa);
        }
    }
}

namespace Tsavorite.test.TransactionalUnsafeContext
{
    internal enum LockOperationType { Lock, Unlock }

    internal static class TransactionalUnsafeContextTestUtils
    {
        internal static IEnumerable<int> EnumActionKeyIndices(FixedLengthTransactionalKeyStruct[] keys, LockOperationType lockOpType)
        {
            if (lockOpType == LockOperationType.Lock)
            {
                for (int ii = 0; ii < keys.Length; ++ii)
                {
                    if (ii == 0 || keys[ii].KeyHash != keys[ii - 1].KeyHash)
                        yield return ii;
                }
                yield break;
            }

            for (int ii = keys.Length - 1; ii >= 0; --ii)
            {
                if (ii == 0 || keys[ii].KeyHash != keys[ii - 1].KeyHash)
                    yield return ii;
            }
        }
    }
}