// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace Garnet.server
{
    /// <summary>
    /// Custom command manager
    /// </summary>
    public class CustomCommandManager
    {
        static readonly int MinMapSize = 8;
        internal static readonly ushort StartOffset = 1;
        internal static readonly int MaxRegistrations = ushort.MaxValue - (ushort)RespCommandExtensions.LastValidCommand - 1;
        internal static readonly byte TypeIdStartOffset = (byte)(GarnetObjectTypeExtensions.LastObjectType + 1);
        internal static readonly int MaxTypeRegistrations = (byte)(GarnetObjectTypeExtensions.FirstSpecialObjectType) - TypeIdStartOffset;

        internal readonly CustomCommandMap rawStringCommandMap;
        internal readonly CustomObjectCommandMap objectCommandMap;
        internal readonly CustomTransactionMap transactionProcMap;
        internal readonly CustomProcedureMap customProcedureMap;

        internal int CustomCommandsInfoCount => CustomCommandsInfo.Count;
        internal readonly Dictionary<string, RespCommandsInfo> CustomCommandsInfo = new(StringComparer.OrdinalIgnoreCase);
        internal readonly Dictionary<string, RespCommandDocs> CustomCommandsDocs = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Create new custom command manager
        /// </summary>
        public CustomCommandManager()
        {
            rawStringCommandMap = new CustomCommandMap(MinMapSize, MaxRegistrations);
            objectCommandMap = new CustomObjectCommandMap(MinMapSize, MaxTypeRegistrations);
            transactionProcMap = new CustomTransactionMap(MinMapSize, MaxRegistrations);
            customProcedureMap = new CustomProcedureMap(MinMapSize, MaxRegistrations);
        }

        internal int Register(string name, CommandType type, CustomRawStringFunctions customFunctions, RespCommandsInfo commandInfo, RespCommandDocs commandDocs, long expirationTicks)
        {
            if (!rawStringCommandMap.TryGetNextId(out var id))
                throw new Exception("Out of registration space");

            rawStringCommandMap[id] = new CustomRawStringCommand(name, (ushort)id, type, customFunctions, expirationTicks);
            if (commandInfo != null) CustomCommandsInfo.Add(name, commandInfo);
            if (commandDocs != null) CustomCommandsDocs.Add(name, commandDocs);
            return id;
        }

        internal int Register(string name, Func<CustomTransactionProcedure> proc, RespCommandsInfo commandInfo = null, RespCommandDocs commandDocs = null)
        {
            if (!transactionProcMap.TryGetNextId(out var id))
                throw new Exception("Out of registration space");

            transactionProcMap[id] = new CustomTransaction(name, (byte)id, proc);
            if (commandInfo != null) CustomCommandsInfo.Add(name, commandInfo);
            if (commandDocs != null) CustomCommandsDocs.Add(name, commandDocs);
            return id;
        }

        internal int RegisterType(CustomObjectFactory factory)
        {
            var dupRegistrationIdx = objectCommandMap.FirstIndexSafe(c => c.factory == factory);
            if (dupRegistrationIdx != -1)
                throw new Exception($"Type already registered with ID {dupRegistrationIdx}");

            if (!objectCommandMap.TryGetNextId(out var type))
                throw new Exception("Out of registration space");

            objectCommandMap[type] = new CustomObjectCommandWrapper((byte)type, factory);

            return type;
        }

        internal (int objectTypeId, int subCommand) Register(string name, CommandType commandType, CustomObjectFactory factory, RespCommandsInfo commandInfo, RespCommandDocs commandDocs, CustomObjectFunctions customObjectFunctions = null)
        {
            var objectTypeId = objectCommandMap.FirstIndexSafe(c => c.factory == factory);

            if (objectTypeId == -1)
            {
                if (!objectCommandMap.TryGetNextId(out objectTypeId))
                    throw new Exception("Out of registration space");

                objectCommandMap[objectTypeId] = new CustomObjectCommandWrapper((byte)objectTypeId, factory);
            }

            var wrapper = objectCommandMap[objectTypeId];
            if (!wrapper.commandMap.TryGetNextId(out var subCommand))
                throw new Exception("Out of registration space");

            wrapper.commandMap[subCommand] = new CustomObjectCommand(name, (byte)objectTypeId, (byte)subCommand, commandType, wrapper.factory, customObjectFunctions);

            if (commandInfo != null) CustomCommandsInfo.Add(name, commandInfo);
            if (commandDocs != null) CustomCommandsDocs.Add(name, commandDocs);

            return (objectTypeId, subCommand);
        }

        /// <summary>
        /// Register custom command
        /// </summary>
        /// <param name="name"></param>
        /// <param name="customProcedure"></param>
        /// <param name="commandInfo"></param>
        /// <param name="commandDocs"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        internal int Register(string name, Func<CustomProcedure> customProcedure, RespCommandsInfo commandInfo = null, RespCommandDocs commandDocs = null)
        {
            if (!customProcedureMap.TryGetNextId(out var id))
                throw new Exception("Out of registration space");

            customProcedureMap[id] = new CustomProcedureWrapper(name, (byte)id, customProcedure, this);
            if (commandInfo != null) CustomCommandsInfo.Add(name, commandInfo);
            if (commandDocs != null) CustomCommandsDocs.Add(name, commandDocs);
            return id;
        }

        internal bool Match(ReadOnlySpan<byte> command, out CustomRawStringCommand cmd)
            => rawStringCommandMap.MatchCommandSafe(command, out cmd);

        internal bool Match(ReadOnlySpan<byte> command, out CustomTransaction cmd)
            => transactionProcMap.MatchCommandSafe(command, out cmd);

        internal bool Match(ReadOnlySpan<byte> command, out CustomObjectCommand cmd)
            => objectCommandMap.MatchSubCommandSafe(command, out cmd);

        internal bool Match(ReadOnlySpan<byte> command, out CustomProcedureWrapper cmd)
            => customProcedureMap.MatchCommandSafe(command, out cmd);

        internal bool TryGetCustomCommandInfo(string cmdName, out RespCommandsInfo respCommandsInfo)
        {
            return this.CustomCommandsInfo.TryGetValue(cmdName, out respCommandsInfo);
        }

        internal bool TryGetCustomCommandDocs(string cmdName, out RespCommandDocs respCommandsDocs)
        {
            return this.CustomCommandsDocs.TryGetValue(cmdName, out respCommandsDocs);
        }
    }
}