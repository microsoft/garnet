// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - SORT and SORT_RO
    /// </summary>
    sealed partial class StorageSession : IDisposable
    {
        /// <summary>
        /// Upper bound on lock-set refinement rounds when BY/GET patterns read other keys. Each round locks the keys the
        /// previous round needed, so this only matters if the source keeps changing between rounds.
        /// </summary>
        const int SortMaxLockAttempts = 64;

        /// <summary>
        /// Sorts the elements of the list, set or sorted set at <paramref name="key"/> (SORT / SORT_RO without STORE).
        /// </summary>
        /// <param name="key">The key holding the list, set or sorted set.</param>
        /// <param name="options">Parsed SORT options.</param>
        /// <param name="result">The reply elements. When GET patterns are used, a null entry is a nil reply.</param>
        /// <param name="invalidScore">True if a numeric sort found a value that is not a valid double.</param>
        public GarnetStatus Sort(PinnedSpanByte key, SortOptions options, out List<byte[]> result, out bool invalidScore)
            => SortInternal(key, default, store: false, options, out result, out invalidScore);

        /// <summary>
        /// Sorts the elements of the list, set or sorted set at <paramref name="key"/> and stores them as a list at
        /// <paramref name="destination"/> (SORT ... STORE). An empty result deletes the destination.
        /// </summary>
        /// <param name="key">The key holding the list, set or sorted set.</param>
        /// <param name="destination">The key to store the result in.</param>
        /// <param name="options">Parsed SORT options.</param>
        /// <param name="count">The number of elements stored.</param>
        /// <param name="invalidScore">True if a numeric sort found a value that is not a valid double; nothing is stored.</param>
        public GarnetStatus SortStore(PinnedSpanByte key, PinnedSpanByte destination, SortOptions options, out int count, out bool invalidScore)
        {
            var status = SortInternal(key, destination, store: true, options, out var result, out invalidScore);
            count = status == GarnetStatus.OK && !invalidScore ? result.Count : 0;
            return status;
        }

        private unsafe GarnetStatus SortInternal(PinnedSpanByte key, PinnedSpanByte destination, bool store, SortOptions options, out List<byte[]> result, out bool invalidScore)
        {
            result = null;
            invalidScore = false;

            var readsOtherKeys = options.ReadsOtherKeys;
            var createTransaction = txnManager.state != TxnState.Running;

            // Inside a running transaction (MULTI/EXEC, transactional Lua) the lock set was fixed from the command's key
            // specifications before execution, so keys derived from BY/GET patterns cannot be locked. The RESP layer
            // rejects that combination before getting here.
            if (!createTransaction && readsOtherKeys)
                throw new GarnetException("SORT with BY/GET patterns that read other keys is not supported inside a transaction");

            // Keys we need locked, beyond the source and destination. Grows until it covers every key the patterns
            // derive from the current contents of the source.
            HashSet<byte[]> patternKeys = null;
            var storedList = false;

            for (var attempt = 0; ; attempt++)
            {
                if (createTransaction)
                {
                    Debug.Assert(txnManager.state == TxnState.None);
                    txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Main | TransactionStoreTypes.Object | TransactionStoreTypes.Unified);
                    txnManager.SaveKeyEntryToLock(key, LockType.Shared);
                    if (store)
                        txnManager.SaveKeyEntryToLock(destination, LockType.Exclusive);
                    if (patternKeys != null)
                    {
                        foreach (var patternKey in patternKeys)
                        {
                            fixed (byte* ptr = patternKey)
                                txnManager.SaveKeyEntryToLock(PinnedSpanByte.FromPinnedPointer(ptr, patternKey.Length), LockType.Shared);
                        }
                    }
                    _ = txnManager.Run(true);
                }

                var objectContext = txnManager.ObjectTransactionalContext;
                var stringContext = txnManager.StringTransactionalContext;
                var unifiedContext = txnManager.UnifiedTransactionalContext;

                try
                {
                    var status = ReadSortSource(key, options, ref objectContext, out var elements, out var sourceIsSet);
                    if (status != GarnetStatus.OK)
                        return status;

                    if (readsOtherKeys)
                    {
                        // Make sure every key the patterns will read is locked; if not, retry with a larger lock set.
                        var missing = false;
                        patternKeys ??= new HashSet<byte[]>(ByteArrayComparer.Instance);
                        foreach (var element in elements)
                        {
                            if (options.SortsByPattern)
                                missing |= AddPatternKey(options.ByPattern, element, patternKeys);
                            foreach (var getPattern in options.GetPatterns)
                                missing |= AddPatternKey(getPattern, element, patternKeys);
                        }

                        if (missing)
                        {
                            if (attempt + 1 >= SortMaxLockAttempts)
                                throw new GarnetException("SORT could not lock the keys referenced by its BY/GET patterns because the source kept changing");
                            continue;
                        }
                    }

                    // Sets have no defined order, so a stored unsorted result could differ between runs. Like Redis,
                    // sort it lexicographically instead.
                    var forceAlpha = store && sourceIsSet && options.DontSort;

                    result = SortElements(elements, options, forceAlpha, ref stringContext, ref objectContext, out invalidScore);
                    if (invalidScore)
                        return GarnetStatus.OK;

                    if (store)
                    {
                        if (result.Count > 0)
                        {
                            var listObject = new ListObject();
                            foreach (var item in result)
                            {
                                var value = item ?? [];
                                _ = listObject.LnkList.AddLast(value);
                                listObject.UpdateSize(value);
                            }
                            _ = SET(destination, listObject, ref objectContext);
                            storedList = true;
                        }
                        else
                        {
                            _ = EXPIRE(destination, TimeSpan.Zero, out _, out _, ExpireOption.None, ref unifiedContext);
                        }
                    }

                    return GarnetStatus.OK;
                }
                finally
                {
                    if (createTransaction)
                        txnManager.Commit(true);

                    // Wake clients blocked on the destination (e.g. BLPOP), like the other list-writing commands. Inside
                    // MULTI/EXEC this runs before the transaction commits, which matches LMOVE and the other STORE paths.
                    if (storedList)
                        itemBroker?.HandleCollectionUpdate(destination.ReadOnlySpan.ToArray());
                }
            }
        }

        /// <summary>
        /// Adds the key a pattern derives from <paramref name="element"/> to <paramref name="keys"/>.
        /// </summary>
        /// <returns>True if the key was not already present.</returns>
        private static bool AddPatternKey(byte[] pattern, byte[] element, HashSet<byte[]> keys)
        {
            if (!SortOptions.TryGetPatternKey(pattern, element, out var patternKey, out _))
                return false;
            return keys.Add(patternKey);
        }

        /// <summary>
        /// Reads the elements to sort, in the order SORT visits them when it does not sort.
        /// </summary>
        private GarnetStatus ReadSortSource<TObjectContext>(PinnedSpanByte key, SortOptions options, ref TObjectContext objectContext, out List<byte[]> elements, out bool isSet)
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            elements = [];
            isSet = false;

            var status = GET(key, out var output, ref objectContext);
            if (status == GarnetStatus.NOTFOUND)
                return GarnetStatus.OK;
            if (status != GarnetStatus.OK)
                return GarnetStatus.WRONGTYPE;

            switch (output.GarnetObject)
            {
                case ListObject listObject:
                    elements.AddRange(listObject.LnkList);
                    break;
                case SetObject setObject:
                    elements.AddRange(setObject.Set);
                    isSet = true;
                    break;
                case SortedSetObject sortedSetObject:
                    // An unsorted (BY with no '*') sort of a sorted set returns members in score order
                    elements.AddRange(sortedSetObject.GetMembersInScoreOrder());
                    break;
                default:
                    return GarnetStatus.WRONGTYPE;
            }

            // Like Redis, DESC reverses the stored order when sorting is skipped
            if (options.DontSort && options.Descending)
                elements.Reverse();

            return GarnetStatus.OK;
        }

        private struct SortEntry
        {
            public int Index;
            public byte[] Element;
            public double Score;
            public byte[] ByValue;
        }

        private List<byte[]> SortElements<TStringContext, TObjectContext>(List<byte[]> elements, SortOptions options, bool forceAlpha,
            ref TStringContext stringContext, ref TObjectContext objectContext, out bool invalidScore)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            invalidScore = false;

            var dontSort = options.DontSort;
            var alpha = options.Alpha;
            var sortsByPattern = options.SortsByPattern;

            if (forceAlpha)
            {
                dontSort = false;
                alpha = true;
                sortsByPattern = false;
            }

            var entries = new SortEntry[elements.Count];
            for (var i = 0; i < entries.Length; i++)
            {
                entries[i].Index = i;
                entries[i].Element = elements[i];
                if (dontSort)
                    continue;

                byte[] byValue;
                if (sortsByPattern)
                {
                    byValue = LookupSortPattern(options.ByPattern, elements[i], ref stringContext, ref objectContext);
                    // A missing BY key sorts as 0 (numeric) or before every present value (ALPHA).
                    if (byValue == null)
                        continue;
                }
                else
                {
                    byValue = elements[i];
                }

                if (alpha)
                {
                    if (sortsByPattern)
                        entries[i].ByValue = byValue;
                }
                else if (!TryParseSortScore(byValue, out entries[i].Score))
                {
                    invalidScore = true;
                    return null;
                }
            }

            if (!dontSort)
            {
                var descending = options.Descending;
                Comparison<SortEntry> comparison = alpha
                    ? sortsByPattern
                        ? (a, b) => CompareNullable(a.ByValue, b.ByValue, descending)
                        : (a, b) => ApplyDirection(a.Element.AsSpan().SequenceCompareTo(b.Element), descending)
                    : (a, b) =>
                    {
                        var cmp = a.Score.CompareTo(b.Score);
                        // Equal scores fall back to comparing the elements, so the order is fully defined.
                        if (cmp == 0)
                            cmp = a.Element.AsSpan().SequenceCompareTo(b.Element);
                        return ApplyDirection(cmp, descending);
                    };

                // Break remaining ties (only possible under ALPHA BY) on the source position, so the order is stable
                Array.Sort(entries, (a, b) =>
                {
                    var cmp = comparison(a, b);
                    return cmp != 0 ? cmp : a.Index.CompareTo(b.Index);
                });
            }

            // Apply LIMIT the way Redis does
            // (clamping the count first, so a huge offset + count cannot overflow)
            long start = options.LimitOffset < 0 ? 0 : options.LimitOffset;
            long end;
            if (start >= entries.Length)
            {
                start = entries.Length - 1;
                end = entries.Length - 2;
            }
            else
            {
                var count = options.LimitCount < 0 ? entries.Length : Math.Min(options.LimitCount, entries.Length);
                end = Math.Min(start + count - 1, entries.Length - 1);
            }

            var outputCount = (int)Math.Max(0, end - start + 1);
            var getPatterns = options.GetPatterns;
            var result = new List<byte[]>(outputCount * Math.Max(1, getPatterns.Count));
            for (var i = start; i <= end; i++)
            {
                var element = entries[i].Element;
                if (getPatterns.Count == 0)
                {
                    result.Add(element);
                    continue;
                }

                foreach (var getPattern in getPatterns)
                    result.Add(LookupSortPattern(getPattern, element, ref stringContext, ref objectContext));
            }

            return result;
        }

        private static int ApplyDirection(int cmp, bool descending) => descending ? -cmp : cmp;

        private static int CompareNullable(byte[] a, byte[] b, bool descending)
        {
            int cmp;
            if (a == null || b == null)
                cmp = a == b ? 0 : a == null ? -1 : 1;
            else
                cmp = a.AsSpan().SequenceCompareTo(b);
            return ApplyDirection(cmp, descending);
        }

        /// <summary>
        /// Parses a SORT score like Redis: the whole value must be a double (or an infinity literal), so empty values,
        /// surrounding whitespace, NaN and values that overflow to infinity are rejected.
        /// </summary>
        private static bool TryParseSortScore(ReadOnlySpan<byte> value, out double score)
        {
            if (!NumUtils.TryParseWithInfinity(value, out score))
                return false;

            // An infinity is only valid when spelled out; "1e400" overflowing to infinity is a range error
            return !double.IsInfinity(score) || value.IndexOfAny((byte)'i', (byte)'I') >= 0;
        }

        /// <summary>
        /// Resolves a BY/GET pattern for one element: "#" is the element itself, "prefix*suffix" reads a string key and
        /// "prefix*suffix->field" reads a hash field. Missing keys, missing fields and wrong types resolve to null.
        /// </summary>
        private unsafe byte[] LookupSortPattern<TStringContext, TObjectContext>(byte[] pattern, byte[] element,
            ref TStringContext stringContext, ref TObjectContext objectContext)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            if (SortOptions.IsElementPattern(pattern))
                return element;

            if (!SortOptions.TryGetPatternKey(pattern, element, out var patternKey, out var field))
                return null;

            fixed (byte* keyPtr = patternKey)
            {
                var keySlice = PinnedSpanByte.FromPinnedPointer(keyPtr, patternKey.Length);

                if (field == null)
                {
                    if (GET(keySlice, out MemoryResult<byte> value, ref stringContext) != GarnetStatus.OK)
                        return null;

                    try
                    {
                        return value.Length == 0 ? [] : value.Span.ToArray();
                    }
                    finally
                    {
                        value.Dispose();
                    }
                }

                if (GET(keySlice, out ObjectOutput output, ref objectContext) != GarnetStatus.OK)
                    return null;

                return output.GarnetObject is HashObject hashObject && hashObject.TryGetFieldValue(field, out var fieldValue)
                    ? fieldValue
                    : null;
            }
        }
    }
}