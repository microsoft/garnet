// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;

namespace Garnet.server.Resp
{
    public class RespCommandDocsParser
    {
        /// <summary>
        /// Tries to parse RespCommandDocs from RESP format
        /// </summary>
        /// <param name="ptr">Pointer to current RESP chunk to read</param>
        /// <param name="end">Pointer to end of RESP chunk to read</param>
        /// <param name="cmdDocs">Parsed RespCommandDocs object</param>
        /// <returns>True if parsing successful</returns>
        public static unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out RespCommandDocs cmdDocs)
        {
            cmdDocs = default;
            string summary = null;
            var group = RespCommandGroup.None;
            string complexity = null;
            var docFlags = RespCommandDocFlags.None;
            RespCommand? replacedBy = null;
            RespCommandDocs[] subCommands = null;
            RespCommandArgumentBase[] arguments = null;

            if (!RespReadUtils.ReadStringWithLengthHeader(out var cmdName, ref ptr, end) ||
                !Enum.TryParse(cmdName, true, out RespCommand cmd)) return false;

            if (!RespReadUtils.ReadUnsignedArrayLength(out var elemCount, ref ptr, end)) return false;

            for (var elemIdx = 0; elemIdx < elemCount; elemIdx += 2)
            {
                if (!RespReadUtils.ReadStringWithLengthHeader(out var argKey, ref ptr, end)) return false;

                if (string.Equals(argKey, "summary", StringComparison.Ordinal))
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out summary, ref ptr, end)) return false;
                }
                else if (string.Equals(argKey, "group", StringComparison.Ordinal))
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out var strGroup, ref ptr, end) ||
                        !EnumUtils.TryParseEnumFromDescription(strGroup, out group))
                        return false;
                }
                else if (string.Equals(argKey, "complexity", StringComparison.Ordinal))
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out complexity, ref ptr, end)) return false;
                }
                else if (string.Equals(argKey, "doc_flags", StringComparison.Ordinal))
                {
                    if (!RespReadUtils.ReadUnsignedArrayLength(out var flagsCount, ref ptr, end)) return false;
                    for (var flagIdx = 0; flagIdx < flagsCount; flagIdx++)
                    {
                        if (!RespReadUtils.ReadSimpleString(out var strFlag, ref ptr, end)
                            || !EnumUtils.TryParseEnumFromDescription<RespCommandDocFlags>(strFlag, out var flag))
                            return false;
                        docFlags |= flag;
                    }
                }
                else if (string.Equals(argKey, "replaced_by", StringComparison.Ordinal))
                {
                    if (!RespReadUtils.ReadSimpleString(out var strReplacedBy, ref ptr, end))
                        return false;
                    if (Enum.TryParse(strReplacedBy, true, out RespCommand tmpReplacedBy))
                        replacedBy = tmpReplacedBy;
                }
                else if (string.Equals(argKey, "arguments", StringComparison.Ordinal))
                {
                    if (!RespReadUtils.ReadUnsignedArrayLength(out var argCount, ref ptr, end)) return false;
                    arguments = new RespCommandArgumentBase[argCount];
                    for (var i = 0; i < argCount; i++)
                    {
                        if (!RespCommandArgumentParser.TryReadFromResp(ref ptr, end, out var arg))
                            return false;
                        arguments[i] = arg;
                    }
                }
                else if (string.Equals(argKey, "subcommands", StringComparison.Ordinal))
                {
                    if (!RespReadUtils.ReadUnsignedArrayLength(out var subCmdCount, ref ptr, end)) return false;
                    subCommands = new RespCommandDocs[subCmdCount];
                    for (var i = 0; i < subCmdCount; i++)
                    {
                        if (!TryReadFromResp(ref ptr, end, out var subCommand))
                            return false;
                        subCommands[i] = subCommand;
                    }
                }
                else if (string.Equals(argKey, "since", StringComparison.Ordinal) 
                    || string.Equals(argKey, "deprecated_since", StringComparison.Ordinal))
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out _, ref ptr, end)) return false;
                }
                else if (string.Equals(argKey, "history", StringComparison.Ordinal))
                {
                    if (!RespReadUtils.ReadUnsignedArrayLength(out var entryCount, ref ptr, end)) return false;
                    for (var i = 0; i < entryCount; i++)
                    {
                        if (!RespReadUtils.ReadUnsignedArrayLength(out _, ref ptr, end)) return false;
                        if (!RespReadUtils.ReadStringWithLengthHeader(out _, ref ptr, end)) return false;
                        if (!RespReadUtils.ReadStringWithLengthHeader(out _, ref ptr, end)) return false;
                    }
                }
            }

            cmdDocs = new RespCommandDocs(cmd, summary, group, complexity, docFlags, replacedBy, arguments, subCommands);

            return true;
        }
    }

    internal class RespCommandArgumentParser
    {
        /// <summary>
        /// Tries to parse RespCommandArgumentBase from RESP format
        /// </summary>
        /// <param name="ptr">Pointer to current RESP chunk to read</param>
        /// <param name="end">Pointer to end of RESP chunk to read</param>
        /// <param name="cmdArg">Parsed RespCommandArgumentBase object</param>
        /// <returns>True if parsing successful</returns>
        internal static unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out RespCommandArgumentBase cmdArg)
        {
            cmdArg = default;
            string name = null;
            string displayText = null;
            var argType = RespCommandArgumentType.None;
            var keySpecIdx = -1;
            string token = null;
            string summary = null;
            var flags = RespCommandArgumentFlags.None;
            string strVal = null;
            RespCommandArgumentBase[] nestedArgsVal = default;

            if (!RespReadUtils.ReadUnsignedArrayLength(out var elemCount, ref ptr, end)) return false;

            for (var elemIdx = 0; elemIdx < elemCount; elemIdx += 2)
            {
                if (!RespReadUtils.ReadStringWithLengthHeader(out var argKey, ref ptr, end)) return false;

                if (string.Equals(argKey, "name", StringComparison.Ordinal))
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out name, ref ptr, end)) return false;
                }
                else if (string.Equals(argKey, "display_text", StringComparison.Ordinal))
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out displayText, ref ptr, end)) return false;
                }
                else if (string.Equals(argKey, "type", StringComparison.Ordinal))
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out var strArgType, ref ptr, end) ||
                        !EnumUtils.TryParseEnumFromDescription(strArgType, out argType))
                        return false;
                }
                else if (string.Equals(argKey, "key_spec_index", StringComparison.Ordinal))
                {
                    if (!RespReadUtils.ReadIntegerAsString(out var strKeySpecIdx, ref ptr, end)
                        || !int.TryParse(strKeySpecIdx, out keySpecIdx))
                        return false;
                }
                else if (string.Equals(argKey, "token", StringComparison.Ordinal))
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out token, ref ptr, end)) return false;
                }
                else if (string.Equals(argKey, "summary", StringComparison.Ordinal))
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out summary, ref ptr, end)) return false;
                }
                else if (string.Equals(argKey, "flags", StringComparison.Ordinal))
                {
                    if (!RespReadUtils.ReadUnsignedArrayLength(out var flagsCount, ref ptr, end)) return false;
                    for (var flagIdx = 0; flagIdx < flagsCount; flagIdx++)
                    {
                        if (!RespReadUtils.ReadSimpleString(out var strFlag, ref ptr, end)
                            || !EnumUtils.TryParseEnumFromDescription<RespCommandArgumentFlags>(strFlag, out var flag))
                            return false;
                        flags |= flag;
                    }
                }
                else if (string.Equals(argKey, "value", StringComparison.Ordinal))
                {
                    if (argType == RespCommandArgumentType.None)
                        return false;
                    if (!RespReadUtils.ReadStringWithLengthHeader(out strVal, ref ptr, end)) return false;
                }
                else if (string.Equals(argKey, "arguments", StringComparison.Ordinal))
                {
                    if (argType != RespCommandArgumentType.OneOf && argType != RespCommandArgumentType.Block)
                        return false;

                    if (!RespReadUtils.ReadUnsignedArrayLength(out var nestedArgsCount, ref ptr, end)) return false;
                    nestedArgsVal = new RespCommandArgumentBase[nestedArgsCount];
                    for (var i = 0; i < nestedArgsCount; i++)
                    {
                        if (!TryReadFromResp(ref ptr, end, out var nestedArg))
                            return false;
                        nestedArgsVal[i] = nestedArg;
                    }
                }
                else if (string.Equals(argKey, "since", StringComparison.Ordinal)
                    || string.Equals(argKey, "deprecated_since", StringComparison.Ordinal))
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out _, ref ptr, end)) return false;
                }
            }

            if (name == string.Empty || argType == RespCommandArgumentType.None || 
                (argType == RespCommandArgumentType.Key && keySpecIdx == -1)) return false;

            cmdArg = argType switch
            {
                RespCommandArgumentType.Key => new RespCommandKeyArgument(name, displayText, token, summary, flags, strVal, keySpecIdx),
                RespCommandArgumentType.OneOf or RespCommandArgumentType.Block => new RespCommandContainerArgument(name,
                    displayText, argType, token, summary, flags, nestedArgsVal),
                _ => new RespCommandArgument(name, displayText, argType, token, summary, flags, strVal)
            };

            return true;
        }
    }
}
