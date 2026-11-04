// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - SORT and SORT_RO
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC | DESC] [ALPHA] [STORE destination]
        /// SORT_RO key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC | DESC] [ALPHA]
        /// </summary>
        private bool NetworkSORT<TGarnetApi>(bool readOnly, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 1)
                return AbortWithWrongNumberOfArguments(readOnly ? nameof(RespCommand.SORT_RO) : nameof(RespCommand.SORT));

            var key = parseState.GetArgSliceByRef(0);
            var options = new SortOptions();
            var hasStore = false;
            var destinationIndex = -1;
            PinnedSpanByte destination = default;

            // Options can appear in any order and repeat; later ones win, except GET which accumulates.
            for (var i = 1; i < parseState.Count; i++)
            {
                var option = parseState.GetArgSliceByRef(i).ReadOnlySpan;
                var argsLeft = parseState.Count - i - 1;

                if (option.EqualsUpperCaseSpanIgnoringCase(CmdStrings.ASC))
                {
                    options.Descending = false;
                }
                else if (option.EqualsUpperCaseSpanIgnoringCase(CmdStrings.DESC))
                {
                    options.Descending = true;
                }
                else if (option.EqualsUpperCaseSpanIgnoringCase(CmdStrings.ALPHA))
                {
                    options.Alpha = true;
                }
                else if (option.EqualsUpperCaseSpanIgnoringCase(CmdStrings.LIMIT) && argsLeft >= 2)
                {
                    if (!parseState.TryGetLong(i + 1, out var offset) || !parseState.TryGetLong(i + 2, out var count))
                        return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);

                    options.LimitOffset = offset;
                    options.LimitCount = count;
                    i += 2;
                }
                else if (!readOnly && option.EqualsUpperCaseSpanIgnoringCase(CmdStrings.STORE) && argsLeft >= 1)
                {
                    hasStore = true;
                    destinationIndex = i + 1;
                    destination = parseState.GetArgSliceByRef(i + 1);
                    i++;
                }
                else if (option.EqualsUpperCaseSpanIgnoringCase(CmdStrings.BY) && argsLeft >= 1)
                {
                    var pattern = parseState.GetArgSliceByRef(i + 1).ReadOnlySpan;
                    options.ByPattern = pattern.ToArray();

                    // A pattern with no '*' only disables sorting and reads no keys
                    if (options.SortsByPattern && !CanServeSortPattern(pattern, key))
                        return AbortWithErrorMessage(CmdStrings.RESP_ERR_SORT_BY_CROSSSLOT);
                    i++;
                }
                else if (option.EqualsUpperCaseSpanIgnoringCase(CmdStrings.GET) && argsLeft >= 1)
                {
                    var pattern = parseState.GetArgSliceByRef(i + 1).ReadOnlySpan;
                    if (!SortOptions.IsElementPattern(pattern) && pattern.IndexOf((byte)'*') >= 0 && !CanServeSortPattern(pattern, key))
                        return AbortWithErrorMessage(CmdStrings.RESP_ERR_SORT_GET_CROSSSLOT);

                    options.GetPatterns.Add(pattern.ToArray());
                    i++;
                }
                else
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                }
            }

            // The STORE key specification (used for transaction locks and cluster slot checks) takes the argument after
            // the first STORE token, which could be another option's operand or an earlier STORE. Refuse to write to any
            // other destination, so the key that is written is always the key that was locked and slot-checked.
            if (hasStore && FirstSortStoreTokenIndex() + 1 != destinationIndex)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_SORT_AMBIGUOUS_STORE);

            // Inside MULTI/EXEC the locks come from the command's key specifications, which cannot include keys
            // derived from the contents of the sorted key.
            if (txnManager.state == TxnState.Running && options.ReadsOtherKeys)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_SORT_PATTERN_IN_TXN);

            if (hasStore)
            {
                var status = storageApi.SortStore(key, destination, options, out var count, out var invalidScore);
                if (status == GarnetStatus.WRONGTYPE)
                    WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
                else if (invalidScore)
                    WriteError(CmdStrings.RESP_ERR_SORT_SCORE_NOT_DOUBLE);
                else
                    WriteInt32(count);
            }
            else
            {
                var status = storageApi.Sort(key, options, out var result, out var invalidScore);
                if (status == GarnetStatus.WRONGTYPE)
                {
                    WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
                }
                else if (invalidScore)
                {
                    WriteError(CmdStrings.RESP_ERR_SORT_SCORE_NOT_DOUBLE);
                }
                else
                {
                    WriteArrayLength(result.Count);
                    foreach (var item in result)
                    {
                        if (item == null)
                            WriteNull();
                        else
                            WriteBulkString(item);
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Index of the first argument after the key that reads STORE (case-insensitive), as the key specification finds it.
        /// </summary>
        private int FirstSortStoreTokenIndex()
        {
            for (var i = 1; i < parseState.Count; i++)
            {
                if (parseState.GetArgSliceByRef(i).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.STORE))
                    return i;
            }
            return -1;
        }

        /// <summary>
        /// In cluster mode, BY/GET patterns may only form keys in the slot of the sorted key, which Redis requires to be
        /// guaranteed by a hash tag in the pattern.
        /// </summary>
        private bool CanServeSortPattern(ReadOnlySpan<byte> pattern, PinnedSpanByte key)
            => !storeWrapper.serverOptions.EnableCluster
               || SortOptions.PatternHashSlot(pattern) == HashSlotUtils.HashSlot(key.ReadOnlySpan);
    }
}