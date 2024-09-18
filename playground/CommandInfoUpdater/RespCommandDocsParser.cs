// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices.ComTypes;
using Garnet.common;
using Garnet.server;
using Garnet.server.Resp;
using StackExchange.Redis;

namespace CommandInfoUpdater
{
    public class RespCommandDocsParser
    {
        /// <summary>
        /// Tries to parse RespCommandDocs from RESP format
        /// </summary>
        /// <param name="result">RedisResult returned by SE.Redis client</param>
        /// <param name="resultStartIdx">Index from which to start reading RedisResult</param>
        /// <param name="supportedCommands">Mapping between command name and Garnet RespCommand and ArrayCommand values</param>
        /// <param name="cmdDocs">Parsed RespCommandDocs object</param>
        /// <param name="cmdName"></param>
        /// <param name="parentCommand">Name of parent command, null if none</param>
        /// <returns>True if parsing successful</returns>
        public static bool TryReadFromResp(RedisResult result, int resultStartIdx, IReadOnlyDictionary<string, RespCommand> supportedCommands, out RespCommandDocs cmdDocs, out string cmdName, string parentCommand = null)
        {
            cmdDocs = default;
            cmdName = default;
            string summary = null;
            var group = RespCommandGroup.None;
            string complexity = null;
            var docFlags = RespCommandDocFlags.None;
            string replacedBy = null;
            RespCommandDocs[] subCommands = null;
            RespCommandArgumentBase[] arguments = null;

            if (result.Length - resultStartIdx < 2) return false;

            if (result[resultStartIdx].Resp3Type != ResultType.BulkString) return false;
            cmdName = result[resultStartIdx].ToString().ToUpper();

            if (result[resultStartIdx + 1].Resp3Type != ResultType.Array) return false;
            var elemCount = result[resultStartIdx + 1].Length;

            var elemArr = result[resultStartIdx + 1];
            for (var i = 0; i < elemCount; i += 2)
            {
                var elemKey = elemArr[i];
                if (elemKey.Resp3Type != ResultType.BulkString) return false;
                var key = elemKey.ToString();

                var elemVal = elemArr[i + 1];
                if (string.Equals(key, "summary"))
                {
                    if (elemVal.Resp3Type != ResultType.BulkString) return false;
                    summary = elemVal.ToString();
                }
                else if (string.Equals(key, "group"))
                {
                    if (elemVal.Resp3Type != ResultType.BulkString ||
                        !EnumUtils.TryParseEnumFromDescription(elemVal.ToString(), out group)) return false;
                }
                else if (string.Equals(key, "complexity"))
                {
                    if (elemVal.Resp3Type != ResultType.BulkString) return false;
                    complexity = elemVal.ToString();
                }
                else if (string.Equals(key, "doc_flags"))
                {
                    if (elemVal.Resp3Type != ResultType.Array) return false;
                    var flagsCount = elemVal.Length;
                    for (var j = 0; j < flagsCount; j++)
                    {
                        if (elemVal[j].Resp3Type != ResultType.SimpleString ||
                            !EnumUtils.TryParseEnumFromDescription<RespCommandDocFlags>(elemVal[j].ToString(),
                                out var flag))
                            continue;
                        docFlags |= flag;
                    }
                }
                else if (string.Equals(key, "replaced_by"))
                {
                    if (elemVal.Resp3Type != ResultType.BulkString) return false;
                    replacedBy = elemVal.ToString();
                }
                else if (string.Equals(key, "arguments"))
                {
                    if (elemVal.Resp3Type != ResultType.Array) return false;
                    var argCount = elemVal.Length;
                    arguments = new RespCommandArgumentBase[argCount];
                    for (var j = 0; j < argCount; j++)
                    {
                        if (!RespCommandArgumentParser.TryReadFromResp(elemVal[j], out var arg))
                            return false;
                        arguments[j] = arg;
                    }
                }
                else if (string.Equals(key, "subcommands"))
                {
                    if (elemVal.Resp3Type != ResultType.Array) return false;
                    var scCount = elemVal.Length / 2;
                    subCommands = new RespCommandDocs[scCount];
                    for (var j = 0; j < scCount; j++)
                    {
                        if (!TryReadFromResp(elemVal, j * 2, supportedCommands, out var subCommand, out _, cmdName))
                            return false;
                        subCommands[j] = subCommand;
                    }
                }
            }

            cmdDocs = new RespCommandDocs(supportedCommands.GetValueOrDefault(cmdName, RespCommand.NONE), cmdName, summary, group, complexity,
                docFlags, replacedBy, arguments, subCommands);

            return true;
        }
    }

    internal class RespCommandArgumentParser
    {
        /// <summary>
        /// Tries to parse RespCommandArgumentBase from RESP format
        /// </summary>
        /// <param name="result"></param>
        /// <param name="cmdArg">Parsed RespCommandArgumentBase object</param>
        /// <returns>True if parsing successful</returns>
        internal static bool TryReadFromResp(RedisResult result, out RespCommandArgumentBase cmdArg)
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

            if (result.Resp3Type != ResultType.Array) return false;
            var elemCount = result.Length;
            
            for (var i = 0; i < elemCount; i += 2)
            {
                var elemKey = result[i];
                if (elemKey.Resp3Type != ResultType.BulkString) return false;
                var key = elemKey.ToString();

                var elemVal = result[i + 1];

                if (string.Equals(key, "name"))
                {
                    if (elemVal.Resp3Type != ResultType.BulkString) return false;
                    name = elemVal.ToString().ToUpper();
                }
                else if (string.Equals(key, "display_text"))
                {
                    if (elemVal.Resp3Type != ResultType.BulkString) return false;
                    displayText = elemVal.ToString();
                }
                else if (string.Equals(key, "type"))
                {
                    if (elemVal.Resp3Type != ResultType.BulkString ||
                        !EnumUtils.TryParseEnumFromDescription(elemVal.ToString(), out argType))
                        return false;
                }
                else if (string.Equals(key, "key_spec_index"))
                {
                    if (elemVal.Resp3Type != ResultType.Integer || !int.TryParse(elemVal.ToString(), out keySpecIdx))
                        return false;
                }
                else if (string.Equals(key, "token"))
                {
                    if (elemVal.Resp3Type != ResultType.BulkString) return false;
                    token = elemVal.ToString();
                }
                else if (string.Equals(key, "summary"))
                {
                    if (elemVal.Resp3Type != ResultType.BulkString) return false;
                    summary = elemVal.ToString();
                }
                else if (string.Equals(key, "flags"))
                {
                    if (elemVal.Resp3Type != ResultType.Array) return false;
                    var flagsCount = elemVal.Length;
                    for (var j = 0; j < flagsCount; j++)
                    {
                        if (elemVal[j].Resp3Type != ResultType.SimpleString ||
                            !EnumUtils.TryParseEnumFromDescription<RespCommandArgumentFlags>(elemVal[j].ToString(),
                                out var flag))
                            continue;
                        flags |= flag;
                    }
                }
                else if (string.Equals(key, "value"))
                {
                    if (argType == RespCommandArgumentType.None)
                        return false;
                    if (elemVal.Resp3Type != ResultType.BulkString) return false;
                    strVal = elemVal.ToString();
                }
                else if (string.Equals(key, "arguments"))
                {
                    if (argType != RespCommandArgumentType.OneOf && argType != RespCommandArgumentType.Block)
                        return false;

                    if (elemVal.Resp3Type != ResultType.Array) return false;
                    var argCount = elemVal.Length;
                    nestedArgsVal = new RespCommandArgumentBase[argCount];
                    for (var j = 0; j < argCount; j++)
                    {
                        if (!TryReadFromResp(elemVal[j], out var arg))
                            return false;
                        nestedArgsVal[j] = arg;
                    }
                }
            }

            if (name == string.Empty || argType == RespCommandArgumentType.None || 
                (argType == RespCommandArgumentType.Key && keySpecIdx == -1)) return false;

            cmdArg = argType switch
            {
                RespCommandArgumentType.Key => new RespCommandKeyArgument(name, displayText, token, summary, flags, strVal, keySpecIdx),
                RespCommandArgumentType.OneOf or RespCommandArgumentType.Block => new RespCommandContainerArgument(name,
                    displayText, argType, token, summary, flags, nestedArgsVal),
                _ => new RespCommandBasicArgument(name, displayText, argType, token, summary, flags, strVal)
            };

            return true;
        }
    }
}
