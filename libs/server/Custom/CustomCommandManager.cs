// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Garnet.server
{
    /// <summary>
    /// Custom command manager
    /// </summary>
    public class CustomCommandManager
    {
        internal static readonly int MinMapSize = 8;
        internal static readonly byte CommandIdStartOffset = 1;
        internal static readonly int MaxCommandRegistrations = ushort.MaxValue - (ushort)RespCommandExtensions.LastValidCommand - 1;
        internal static readonly byte TypeIdStartOffset = byte.MaxValue - (byte)GarnetObjectTypeExtensions.FirstSpecialObjectType;
        internal static readonly int MaxTypeRegistrations = byte.MaxValue - ((byte)GarnetObjectTypeExtensions.LastObjectType + 1) - TypeIdStartOffset;

        private readonly CustomCommandMap rawStringCommandMap;
        private readonly CustomObjectCommandMap objectCommandMap;
        private readonly CustomTransactionMap transactionProcMap;
        private readonly CustomProcedureMap customProcedureMap;

        internal int CustomCommandsInfoCount => CustomCommandsInfo.Count;
        internal readonly Dictionary<string, RespCommandsInfo> CustomCommandsInfo = new(StringComparer.OrdinalIgnoreCase);
        internal readonly Dictionary<string, RespCommandDocs> CustomCommandsDocs = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Create new custom command manager
        /// </summary>
        public CustomCommandManager()
        {
            rawStringCommandMap = new CustomCommandMap(MinMapSize, MaxCommandRegistrations, CommandIdStartOffset);
            objectCommandMap = new CustomObjectCommandMap(MinMapSize, MaxTypeRegistrations);
            transactionProcMap = new CustomTransactionMap(MinMapSize, byte.MaxValue, 0);
            customProcedureMap = new CustomProcedureMap(MinMapSize, byte.MaxValue, 0);
        }

        internal int Register(string name, CommandType type, CustomRawStringFunctions customFunctions, RespCommandsInfo commandInfo, RespCommandDocs commandDocs, long expirationTicks)
        {
            if (!rawStringCommandMap.TryGetNextIndex(out var index))
                throw new Exception("Out of registration space");
            var cmdId = rawStringCommandMap.GetIdFromIndex(index);
            Debug.Assert(cmdId <= ushort.MaxValue);

            rawStringCommandMap[index] = new CustomRawStringCommand(name, (ushort)cmdId, type, customFunctions, expirationTicks);
            if (commandInfo != null) CustomCommandsInfo.Add(name, commandInfo);
            if (commandDocs != null) CustomCommandsDocs.Add(name, commandDocs);
            return cmdId;
        }

        internal int Register(string name, Func<CustomTransactionProcedure> proc, RespCommandsInfo commandInfo = null, RespCommandDocs commandDocs = null)
        {
            if (!transactionProcMap.TryGetNextIndex(out var index))
                throw new Exception("Out of registration space");
            var cmdId = transactionProcMap.GetIdFromIndex(index);
            Debug.Assert(cmdId <= byte.MaxValue);

            transactionProcMap[index] = new CustomTransaction(name, (byte)cmdId, proc);
            if (commandInfo != null) CustomCommandsInfo.Add(name, commandInfo);
            if (commandDocs != null) CustomCommandsDocs.Add(name, commandDocs);
            return cmdId;
        }

        internal int RegisterType(CustomObjectFactory factory)
        {
            var dupRegistrationIdx = objectCommandMap.FirstIndexSafe(c => c.factory == factory);
            if (dupRegistrationIdx != -1)
                throw new Exception($"Type already registered with ID {dupRegistrationIdx}");

            if (!objectCommandMap.TryGetNextIndex(out var index))
                throw new Exception("Out of registration space");
            var cmdId = objectCommandMap.GetIdFromIndex(index);
            Debug.Assert(cmdId <= byte.MaxValue);

            objectCommandMap[index] = new CustomObjectCommandWrapper((byte)cmdId, factory);

            return cmdId;
        }

        internal (int objectTypeId, int subCommand) Register(string name, CommandType commandType, CustomObjectFactory factory, RespCommandsInfo commandInfo, RespCommandDocs commandDocs, CustomObjectFunctions customObjectFunctions = null)
        {
            var typeIndex = objectCommandMap.FirstIndexSafe(c => c.factory == factory);

            int typeId;
            if (typeIndex == -1)
            {
                if (!objectCommandMap.TryGetNextIndex(out typeIndex))
                    throw new Exception("Out of registration space");

                typeId = objectCommandMap.GetIdFromIndex(typeIndex);
                Debug.Assert(typeId <= byte.MaxValue);

                objectCommandMap[typeIndex] = new CustomObjectCommandWrapper((byte)typeId, factory);
            }

            var wrapper = objectCommandMap[typeIndex];
            if (!wrapper.commandMap.TryGetNextIndex(out var scIndex))
                throw new Exception("Out of registration space");

            var scId = wrapper.commandMap.GetIdFromIndex(scIndex);
            Debug.Assert(scId <= byte.MaxValue);
            typeId = objectCommandMap.GetIdFromIndex(typeIndex);
            wrapper.commandMap[scIndex] = new CustomObjectCommand(name, (byte)typeId, (byte)scId, commandType, wrapper.factory, customObjectFunctions);

            if (commandInfo != null) CustomCommandsInfo.Add(name, commandInfo);
            if (commandDocs != null) CustomCommandsDocs.Add(name, commandDocs);

            return (typeId, scId);
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
            if (!customProcedureMap.TryGetNextIndex(out var index))
                throw new Exception("Out of registration space");

            var cmdId = customProcedureMap.GetIdFromIndex(index);
            Debug.Assert(cmdId <= byte.MaxValue);

            customProcedureMap[index] = new CustomProcedureWrapper(name, (byte)cmdId, customProcedure, this);
            if (commandInfo != null) CustomCommandsInfo.Add(name, commandInfo);
            if (commandDocs != null) CustomCommandsDocs.Add(name, commandDocs);
            return cmdId;
        }

        internal CustomProcedureWrapper GetCustomProcedure(int id)
            => customProcedureMap[customProcedureMap.GetIndexFromId(id)];

        internal CustomTransaction GetCustomTransactionProcedure(int id)
            => transactionProcMap[transactionProcMap.GetIndexFromId(id)];

        internal CustomRawStringCommand GetCustomCommand(int id)
            => rawStringCommandMap[rawStringCommandMap.GetIndexFromId(id)];

        internal CustomObjectCommandWrapper GetCustomObjectCommand(int id)
            => objectCommandMap[objectCommandMap.GetIndexFromId(id)];

        internal CustomObjectCommand GetCustomObjectSubCommand(int id, int subId)
        {
            var typeIdx = objectCommandMap.GetIdFromIndex(id);
            var subCommandMap = objectCommandMap[typeIdx].commandMap;
            return subCommandMap[subCommandMap.GetIndexFromId(subId)];
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