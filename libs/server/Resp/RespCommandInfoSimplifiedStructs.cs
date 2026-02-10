// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace Garnet.server
{
    /// <summary>
    /// Represents a simplified version of RESP command's information
    /// </summary>
    public struct SimpleRespCommandInfo
    {
        /// <summary>
        /// Command Arity
        /// </summary>
        public sbyte Arity;

        /// <summary>
        /// True if command is allowed in a transaction context
        /// </summary>
        public bool AllowedInTxn;

        /// <summary>
        /// True if command has sub-commands
        /// </summary>
        public bool IsParent;

        /// <summary>
        /// True if command is a sub-command
        /// </summary>
        public bool IsSubCommand;

        /// <summary>
        /// Simplified command key specifications
        /// </summary>
        public SimpleRespKeySpec[] KeySpecs;

        /// <summary>
        /// Store type that the command operates on (None/Main/Object/All). Default: None for commands without key arguments.
        /// </summary>
        public StoreType StoreType;

        /// <summary>
        /// Default SimpleRespCommandInfo
        /// </summary>
        public static SimpleRespCommandInfo Default = new();
    }

    /// <summary>
    /// Represents a simplified version of a single key specification of a RESP command
    /// </summary>
    public struct SimpleRespKeySpecBeginSearch
    {
        /// <summary>
        /// Keyword that precedes the keys in the command arguments
        /// </summary>
        public byte[] Keyword;

        /// <summary>
        /// Index of first key or the index at which to start searching for keyword (if begin search is of keyword type)
        /// </summary>
        public int Index;

        /// <summary>
        /// If true - begin search is of type index, otherwise begin search is of type keyword
        /// </summary>
        public bool IsIndexType;

        /// <summary>
        /// Set begin search of type index
        /// </summary>
        /// <param name="index">Index of first key</param>
        public SimpleRespKeySpecBeginSearch(int index)
        {
            IsIndexType = true;
            Index = index;
        }

        /// <summary>
        /// Set begin search of type keyword
        /// </summary>
        /// <param name="keyword">Keyword that precedes the keys in the command arguments</param>
        /// <param name="startIdx">Index at which to start searching for keyword</param>
        public SimpleRespKeySpecBeginSearch(string keyword, int startIdx)
        {
            Index = startIdx;
            Keyword = Encoding.UTF8.GetBytes(keyword);
        }
    }

    /// <summary>
    /// Represents a simplified version of a single key specification of a RESP command
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct SimpleRespKeySpecFindKeys
    {
        /// <summary>
        /// Size of struct
        /// </summary>
        public const int Size = 10;

        /// <summary>
        /// The index (relative to begin search) of the argument containing the number of keys
        /// </summary>
        [FieldOffset(0)]
        public int KeyNumIndex;

        /// <summary>
        /// the index (relative to begin search) of the first key
        /// </summary>
        [FieldOffset(4)]
        public int FirstKey;

        /// <summary>
        /// The index (relative to begin search) of the last key argument or limit - stops the key search by a factor.
        /// 0 and 1 mean no limit. 2 means half of the remaining arguments, 3 means a third, and so on.
        /// Limit is used if IsRangeLimitType is set.
        /// </summary>
        [FieldOffset(0)]
        public int LastKeyOrLimit;

        /// <summary>
        /// The number of arguments that should be skipped, after finding a key, to find the next one.
        /// </summary>
        [FieldOffset(4)]
        public int KeyStep;

        /// <summary>
        /// If true - find keys is of type range, otherwise find keys is of type keynum
        /// </summary>
        [FieldOffset(8)]
        public bool IsRangeType;

        /// <summary>
        /// If true - find keys is of type range and limit is used, otherwise find keys is of type range and last key is used.
        /// </summary>
        [FieldOffset(9)]
        public bool IsRangeLimitType;

        /// <summary>
        /// Set find keys of type range
        /// </summary>
        /// <param name="keyStep">The number of arguments that should be skipped, after finding a key, to find the next one</param>
        /// <param name="lastKeyOrLimit">The index of the last key argument or the limit</param>
        /// <param name="isLimit">If preceding argument represents a limit</param>
        public SimpleRespKeySpecFindKeys(int keyStep, int lastKeyOrLimit, bool isLimit)
        {
            IsRangeType = true;
            KeyStep = keyStep;
            LastKeyOrLimit = lastKeyOrLimit;
            IsRangeLimitType = isLimit;
        }

        /// <summary>
        /// Set find keys of type keynum
        /// </summary>
        /// <param name="keyNumIndex">The index of the argument containing the number of keys</param>
        /// <param name="firstKey">The index of the first key</param>
        /// <param name="keyStep">The number of arguments that should be skipped, after finding a key, to find the next one</param>
        public SimpleRespKeySpecFindKeys(int keyNumIndex, int firstKey, int keyStep)
        {
            KeyNumIndex = keyNumIndex;
            FirstKey = firstKey;
            KeyStep = keyStep;
        }
    }

    /// <summary>
    /// Represents a simplified version of a single key specification of a RESP command
    /// </summary>
    public struct SimpleRespKeySpec
    {
        /// <summary>
        /// Begin search specification
        /// </summary>
        public SimpleRespKeySpecBeginSearch BeginSearch;

        /// <summary>
        /// Find keys specification
        /// </summary>
        public SimpleRespKeySpecFindKeys FindKeys;

        /// <summary>
        /// Key specification flags
        /// </summary>
        public KeySpecificationFlags Flags;
    }

    /// <summary>
    /// Extension methods for obtaining simplified RESP command info structs
    /// </summary>
    public static class RespCommandInfoExtensions
    {
        /// <summary>
        /// Populates a SimpleRespCommandInfo struct from a RespCommandsInfo instance
        /// </summary>
        /// <param name="cmdInfo">The source RespCommandsInfo</param>
        /// <param name="simpleCmdInfo">The destination SimpleRespCommandInfo</param>
        public static void PopulateSimpleCommandInfo(this RespCommandsInfo cmdInfo, ref SimpleRespCommandInfo simpleCmdInfo)
        {
            var arity = cmdInfo.Arity;

            // Verify that arity is in the signed byte range (-128 to 127)
            Debug.Assert(arity is <= sbyte.MaxValue and >= sbyte.MinValue);

            simpleCmdInfo.Arity = (sbyte)arity;
            simpleCmdInfo.AllowedInTxn = (cmdInfo.Flags & RespCommandFlags.NoMulti) == 0;
            simpleCmdInfo.IsParent = (cmdInfo.SubCommands?.Length ?? 0) > 0;
            simpleCmdInfo.IsSubCommand = cmdInfo.Parent != null;
            simpleCmdInfo.StoreType = cmdInfo.StoreType;

            if (cmdInfo.KeySpecifications != null)
            {
                var tmpSimpleKeySpecs = new List<SimpleRespKeySpec>();

                foreach (var keySpec in cmdInfo.KeySpecifications)
                {
                    if (keySpec.TryGetSimpleKeySpec(out var simpleKeySpec))
                        tmpSimpleKeySpecs.Add(simpleKeySpec);
                }

                simpleCmdInfo.KeySpecs = tmpSimpleKeySpecs.ToArray();
            }
        }

        /// <summary>
        /// Tries to convert a RespCommandKeySpecification to a SimpleRespKeySpec
        /// </summary>
        /// <param name="keySpec">The source RespCommandKeySpecification</param>
        /// <param name="simpleKeySpec">The resulting SimpleRespKeySpec</param>
        /// <returns>True if successful</returns>
        public static bool TryGetSimpleKeySpec(this RespCommandKeySpecification keySpec, out SimpleRespKeySpec simpleKeySpec)
        {
            simpleKeySpec = new SimpleRespKeySpec();

            if (keySpec.BeginSearch is BeginSearchUnknown || keySpec.FindKeys is FindKeysUnknown)
                return false;

            simpleKeySpec.BeginSearch = keySpec.BeginSearch switch
            {
                BeginSearchIndex bsi => new SimpleRespKeySpecBeginSearch(bsi.Index),
                BeginSearchKeyword bsk => new SimpleRespKeySpecBeginSearch(bsk.Keyword, bsk.StartFrom),
                _ => throw new NotSupportedException()
            };

            simpleKeySpec.FindKeys = keySpec.FindKeys switch
            {
                FindKeysRange fkr => fkr.LastKey == -1
                    ? new SimpleRespKeySpecFindKeys(fkr.KeyStep, fkr.Limit, true)
                    : new SimpleRespKeySpecFindKeys(fkr.KeyStep, fkr.LastKey, false),
                FindKeysKeyNum fkk => new SimpleRespKeySpecFindKeys(fkk.KeyNumIdx, fkk.FirstKey, fkk.KeyStep),
                _ => throw new NotSupportedException()
            };

            simpleKeySpec.Flags = keySpec.Flags;

            return true;
        }

        /// <summary>
        /// Determines if a simplified key spec represents a multi-key command
        /// </summary>
        /// <param name="simpleCommandInfo">The simplified key spec</param>
        /// <returns>True if successful</returns>
        public static bool IsMultiKeyCommand(this SimpleRespCommandInfo simpleCommandInfo)
        {
            if (simpleCommandInfo.KeySpecs == null)
                return false;

            if (simpleCommandInfo.KeySpecs.Length > 1) 
                return true;

            var findKeys = simpleCommandInfo.KeySpecs[0].FindKeys;
            return !findKeys.IsRangeType || findKeys.IsRangeLimitType ||
                   findKeys.LastKeyOrLimit < 0 ||
                   findKeys.FirstKey < findKeys.LastKeyOrLimit;
        }

        /// <summary>
        /// Determines if a simplified key spec represents an overwrite command
        /// </summary>
        /// <param name="simpleCommandInfo">The simplified key spec</param>
        /// <returns>True if successful</returns>
        public static bool IsOverwriteCommand(this SimpleRespCommandInfo simpleCommandInfo)
        {
            return simpleCommandInfo.KeySpecs.Length > 0 && simpleCommandInfo.KeySpecs.Any(ks =>
                (ks.Flags & KeySpecificationFlags.OW) == KeySpecificationFlags.OW);
        }
    }
}