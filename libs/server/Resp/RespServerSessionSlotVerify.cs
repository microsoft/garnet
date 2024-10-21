// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// RESP server session
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// This method is used to verify slot ownership for provided array of key argslices.
        /// </summary>
        /// <param name="keys">Array of key ArgSlice</param>
        /// <param name="readOnly">Whether caller is going to perform a readonly or read/write operation</param>
        /// <param name="count">Key count if different than keys array length</param>
        /// <returns>True when ownership is verified, false otherwise</returns>
        bool NetworkKeyArraySlotVerify(Span<ArgSlice> keys, bool readOnly, int count = -1)
            => clusterSession != null && clusterSession.NetworkKeyArraySlotVerify(keys, readOnly, SessionAsking, ref dcurr, ref dend, count);

        bool CanServeSlot(RespCommand cmd)
        {
            Debug.Assert(clusterSession != null);

            // Verify slot for command if it falls into data command category
            if (!cmd.IsDataCommand())
                return true;

            cmd = cmd.NormalizeForACLs();
            if (!RespCommandsInfo.TryFastGetRespCommandInfo(cmd, out var commandInfo))
                // This only happens if we failed to parse the json file
                return false;

            // The provided command is not a data command
            // so we can serve without any slot restrictions
            if (commandInfo == null)
                return true;

            csvi.keyNumOffset = -1;
            var specs = commandInfo.KeySpecifications;
            switch (specs.Length)
            {
                case 1:
                    var searchIndex = (BeginSearchIndex)specs[0].BeginSearch;

                    switch (specs[0].FindKeys)
                    {
                        case FindKeysRange:
                            var findRange = (FindKeysRange)specs[0].FindKeys;
                            csvi.firstKey = searchIndex.Index - 1;
                            csvi.lastKey = findRange.LastKey < 0 ? findRange.LastKey + parseState.Count + 1 : findRange.LastKey - searchIndex.Index + 1;
                            csvi.step = findRange.KeyStep;
                            break;
                        case FindKeysKeyNum:
                            var findKeysKeyNum = (FindKeysKeyNum)specs[0].FindKeys;
                            csvi.firstKey = searchIndex.Index + findKeysKeyNum.FirstKey - 1;
                            csvi.lastKey = csvi.firstKey + parseState.GetInt(searchIndex.Index + findKeysKeyNum.KeyNumIdx - 1);
                            csvi.step = findKeysKeyNum.KeyStep;
                            break;
                        case FindKeysUnknown:
                        default:
                            throw new GarnetException("FindKeys spec not known");
                    }

                    break;
                case 2:
                    searchIndex = (BeginSearchIndex)specs[0].BeginSearch;
                    switch (specs[0].FindKeys)
                    {
                        case FindKeysRange:
                            csvi.firstKey = RespCommand.BITOP == cmd ? searchIndex.Index - 2 : searchIndex.Index - 1;
                            break;
                        case FindKeysKeyNum:
                        case FindKeysUnknown:
                        default:
                            throw new GarnetException("FindKeys spec not known");
                    }

                    var searchIndex1 = (BeginSearchIndex)specs[1].BeginSearch;
                    switch (specs[1].FindKeys)
                    {
                        case FindKeysRange:
                            var findRange = (FindKeysRange)specs[1].FindKeys;
                            csvi.lastKey = findRange.LastKey < 0 ? findRange.LastKey + parseState.Count + 1 : findRange.LastKey + searchIndex1.Index - searchIndex.Index + 1;
                            csvi.step = findRange.KeyStep;
                            break;
                        case FindKeysKeyNum:
                            var findKeysKeyNum = (FindKeysKeyNum)specs[1].FindKeys;
                            csvi.keyNumOffset = searchIndex1.Index + findKeysKeyNum.KeyNumIdx - 1;
                            csvi.lastKey = searchIndex1.Index + parseState.GetInt(csvi.keyNumOffset);
                            csvi.step = findKeysKeyNum.KeyStep;
                            break;
                        case FindKeysUnknown:
                        default:
                            throw new GarnetException("FindKeys spec not known");
                    }

                    break;
                default:
                    throw new GarnetException("KeySpecification not supported count");
            }
            csvi.readOnly = cmd.IsReadOnly();
            csvi.sessionAsking = SessionAsking;
            return !clusterSession.NetworkMultiKeySlotVerify(ref parseState, ref csvi, ref dcurr, ref dend);
        }
    }
}