// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    /// <summary>
    /// RESP server session
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// This method is used to verify slot ownership for provided key.
        /// On error this method writes to response buffer but does not drain recv buffer (caller is responsible for draining).
        /// </summary>
        /// <param name="key">Key bytes</param>
        /// <param name="readOnly">Whether caller is going to perform a readonly or read/write operation.</param>
        /// <returns>True when ownership is verified, false otherwise</returns>
        bool NetworkSingleKeySlotVerify(ReadOnlySpan<byte> key, bool readOnly)
            => clusterSession != null && clusterSession.NetworkSingleKeySlotVerify(key, readOnly, SessionAsking, ref dcurr, ref dend);

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
            // If cluster is disable all commands
            if (clusterSession == null)
                return true;

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

            var specs = commandInfo.KeySpecifications;
            switch (cmd)
            {
                case RespCommand.ZDIFF:
                    var beginSearch = (BeginSearchIndex)specs[0].BeginSearch;
                    var findKeysKeyNum = (FindKeysKeyNum)specs[0].FindKeys;
                    csvi.firstKey = beginSearch.Index;
                    csvi.lastKey = beginSearch.Index + parseState.GetInt(0);
                    csvi.step = findKeysKeyNum.KeyStep;
                    break;
                case RespCommand.BITOP:
                    beginSearch = (BeginSearchIndex)specs[0].BeginSearch;
                    var findKeys = (FindKeysRange)specs[1].FindKeys;
                    csvi.firstKey = beginSearch.Index - 2;
                    csvi.lastKey = findKeys.LastKey < 0 ? findKeys.LastKey + parseState.count + 1 : findKeys.LastKey - beginSearch.Index + 1;
                    csvi.step = findKeys.KeyStep;
                    break;
                case RespCommand.PFMERGE:
                case RespCommand.SDIFFSTORE:
                case RespCommand.SUNIONSTORE:
                case RespCommand.SINTERSTORE:
                    beginSearch = (BeginSearchIndex)specs[0].BeginSearch;
                    findKeys = (FindKeysRange)specs[1].FindKeys;
                    csvi.firstKey = beginSearch.Index - 1;
                    csvi.lastKey = findKeys.LastKey < 0 ? findKeys.LastKey + parseState.count + 1 : findKeys.LastKey - beginSearch.Index + 1;
                    csvi.step = findKeys.KeyStep;
                    break;
                case RespCommand.RENAME:
                case RespCommand.SMOVE:
                case RespCommand.LMOVE:
                    beginSearch = (BeginSearchIndex)specs[0].BeginSearch;
                    var beginSearch1 = (BeginSearchIndex)specs[1].BeginSearch;
                    findKeys = (FindKeysRange)specs[1].FindKeys;
                    csvi.firstKey = beginSearch.Index - 1;
                    csvi.lastKey = beginSearch1.Index - beginSearch.Index + 1;
                    csvi.step = findKeys.KeyStep;
                    break;
                default:
                    beginSearch = (BeginSearchIndex)specs[0].BeginSearch;
                    findKeys = (FindKeysRange)specs[0].FindKeys;
                    csvi.firstKey = beginSearch.Index - 1;
                    csvi.lastKey = findKeys.LastKey < 0 ? findKeys.LastKey + parseState.count + 1 : findKeys.LastKey - beginSearch.Index + 1;
                    csvi.step = findKeys.KeyStep;
                    break;
            }

            csvi.readOnly = cmd.IsReadOnly();
            csvi.sessionAsking = SessionAsking;

            //csvi.firstKey = cmd switch
            //{
            //    RespCommand.ZDIFF => 1, // ZDIFF first key comes after keyCount parameter
            //    _ => 0 // firstKey always starts at position zero since command name has been extracted earlier
            //};

            //csvi.lastKey = cmd switch
            //{
            //    RespCommand.ZDIFF => csvi.firstKey + parseState.GetInt(0), // ZDIFF count of keys is part of parameters
            //    _ => commandInfo.LastKey < 0 ? commandInfo.LastKey + parseState.count + 1 : commandInfo.LastKey - commandInfo.FirstKey + 1
            //};
            //csvi.step = commandInfo.Step;
            return !clusterSession.NetworkMultiKeySlotVerify(ref parseState, ref csvi, ref dcurr, ref dend);
        }
    }
}