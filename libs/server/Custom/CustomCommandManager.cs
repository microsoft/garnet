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
        internal static readonly byte TypeIdStartOffset = byte.MaxValue - (byte)GarnetObjectTypeExtensions.FirstSpecialObjectType;

        private ExtensibleMap<CustomRawStringCommand> rawStringCommandMap;
        private ExtensibleMap<CustomObjectCommandWrapper> objectCommandMap;
        private ExtensibleMap<CustomTransaction> transactionProcMap;
        private ExtensibleMap<CustomProcedureWrapper> customProcedureMap;

        internal int CustomCommandsInfoCount => customCommandsInfo.Count;
        internal readonly Dictionary<string, RespCommandsInfo> customCommandsInfo = new(StringComparer.OrdinalIgnoreCase);
        internal readonly Dictionary<string, RespCommandDocs> customCommandsDocs = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Create new custom command manager
        /// </summary>
        public CustomCommandManager()
        {
            rawStringCommandMap = new ExtensibleMap<CustomRawStringCommand>(MinMapSize, 
                (ushort)RespCommand.INVALID - 1,
                (ushort)RespCommandExtensions.LastValidCommand + 1);
            objectCommandMap = new ExtensibleMap<CustomObjectCommandWrapper>(MinMapSize,
                (byte)GarnetObjectTypeExtensions.FirstSpecialObjectType - 1,
                (byte)GarnetObjectTypeExtensions.LastObjectType + 1);
            transactionProcMap = new ExtensibleMap<CustomTransaction>(MinMapSize, 0, byte.MaxValue);
            customProcedureMap = new ExtensibleMap<CustomProcedureWrapper>(MinMapSize, 0, byte.MaxValue);
        }

        internal int Register(string name, CommandType type, CustomRawStringFunctions customFunctions, RespCommandsInfo commandInfo, RespCommandDocs commandDocs, long expirationTicks)
        {
            if (!rawStringCommandMap.TryGetNextIndex(out var index))
                throw new Exception("Out of registration space");
            var cmdId = rawStringCommandMap.GetIdFromIndex(index);
            Debug.Assert(cmdId <= ushort.MaxValue);

            rawStringCommandMap[index] = new CustomRawStringCommand(name, (ushort)cmdId, type, customFunctions, expirationTicks);
            if (commandInfo != null) customCommandsInfo.Add(name, commandInfo);
            if (commandDocs != null) customCommandsDocs.Add(name, commandDocs);
            return cmdId;
        }

        internal int Register(string name, Func<CustomTransactionProcedure> proc, RespCommandsInfo commandInfo = null, RespCommandDocs commandDocs = null)
        {
            if (!transactionProcMap.TryGetNextIndex(out var index))
                throw new Exception("Out of registration space");
            var cmdId = transactionProcMap.GetIdFromIndex(index);
            Debug.Assert(cmdId <= byte.MaxValue);

            transactionProcMap[index] = new CustomTransaction(name, (byte)cmdId, proc);
            if (commandInfo != null) customCommandsInfo.Add(name, commandInfo);
            if (commandDocs != null) customCommandsDocs.Add(name, commandDocs);
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

            if (commandInfo != null) customCommandsInfo.Add(name, commandInfo);
            if (commandDocs != null) customCommandsDocs.Add(name, commandDocs);

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
            if (commandInfo != null) customCommandsInfo.Add(name, commandInfo);
            if (commandDocs != null) customCommandsDocs.Add(name, commandDocs);
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
            return this.customCommandsInfo.TryGetValue(cmdName, out respCommandsInfo);
        }

        internal bool TryGetCustomCommandDocs(string cmdName, out RespCommandDocs respCommandsDocs)
        {
            return this.customCommandsDocs.TryGetValue(cmdName, out respCommandsDocs);
        }
    }
}