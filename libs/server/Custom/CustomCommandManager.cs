﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Tsavorite.core;

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
            if (!rawStringCommandMap.TryGetNextId(out var cmdId))
                throw new Exception("Out of registration space");
            Debug.Assert(cmdId <= ushort.MaxValue);
            var newCmd = new CustomRawStringCommand(name, (ushort)cmdId, type, customFunctions, expirationTicks);
            rawStringCommandMap.TrySetValue(cmdId, ref newCmd);
            if (commandInfo != null) customCommandsInfo.Add(name, commandInfo);
            if (commandDocs != null) customCommandsDocs.Add(name, commandDocs);
            return cmdId;
        }

        internal int Register(string name, Func<CustomTransactionProcedure> proc, RespCommandsInfo commandInfo = null, RespCommandDocs commandDocs = null)
        {
            if (!transactionProcMap.TryGetNextId(out var cmdId))
                throw new Exception("Out of registration space");
            Debug.Assert(cmdId <= byte.MaxValue);

            var newCmd = new CustomTransaction(name, (byte)cmdId, proc);
            transactionProcMap.TrySetValue(cmdId, ref newCmd);
            if (commandInfo != null) customCommandsInfo.Add(name, commandInfo);
            if (commandDocs != null) customCommandsDocs.Add(name, commandDocs);
            return cmdId;
        }

        internal int RegisterType(CustomObjectFactory factory)
        {
            var dupRegistrationId = objectCommandMap.FirstIdSafe(c => c.factory == factory);
            if (dupRegistrationId != -1)
                throw new Exception($"Type already registered with ID {dupRegistrationId}");

            if (!objectCommandMap.TryGetNextId(out var cmdId))
                throw new Exception("Out of registration space");
            Debug.Assert(cmdId <= byte.MaxValue);

            var newCmd = new CustomObjectCommandWrapper((byte)cmdId, factory);
            objectCommandMap.TrySetValue(cmdId, ref newCmd);

            return cmdId;
        }

        internal (int objectTypeId, int subCommand) Register(string name, CommandType commandType, CustomObjectFactory factory, RespCommandsInfo commandInfo, RespCommandDocs commandDocs, CustomObjectFunctions customObjectFunctions = null)
        {
            var typeId = objectCommandMap.FirstIdSafe(c => c.factory == factory);

            if (typeId == -1)
            {
                if (!objectCommandMap.TryGetNextId(out typeId))
                    throw new Exception("Out of registration space");

                Debug.Assert(typeId <= byte.MaxValue);

                var newCmd = new CustomObjectCommandWrapper((byte)typeId, factory);
                objectCommandMap.TrySetValue(typeId, ref newCmd);
            }

            objectCommandMap.TryGetValue(typeId, out var wrapper);
            if (!wrapper.commandMap.TryGetNextId(out var scId))
                throw new Exception("Out of registration space");

            Debug.Assert(scId <= byte.MaxValue);
            var newSubCmd = new CustomObjectCommand(name, (byte)typeId, (byte)scId, commandType, wrapper.factory,
                customObjectFunctions);
            wrapper.commandMap.TrySetValue(scId, ref newSubCmd);

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
            if (!customProcedureMap.TryGetNextId(out var cmdId))
                throw new Exception("Out of registration space");

            Debug.Assert(cmdId <= byte.MaxValue);

            var newCmd = new CustomProcedureWrapper(name, (byte)cmdId, customProcedure, this);
            customProcedureMap.TrySetValue(cmdId, ref newCmd);
            if (commandInfo != null) customCommandsInfo.Add(name, commandInfo);
            if (commandDocs != null) customCommandsDocs.Add(name, commandDocs);
            return cmdId;
        }

        internal bool TryGetCustomProcedure(int id, out CustomProcedureWrapper value)
            => customProcedureMap.TryGetValue(id, out value);

        internal bool TryGetCustomTransactionProcedure(int id, out CustomTransaction value)
            => transactionProcMap.TryGetValue(id, out value);

        internal bool TryGetCustomCommand(int id, out CustomRawStringCommand value)
            => rawStringCommandMap.TryGetValue(id, out value);

        internal bool TryGetCustomObjectCommand(int id, out CustomObjectCommandWrapper value)
            => objectCommandMap.TryGetValue(id, out value);

        internal bool TryGetCustomObjectSubCommand(int id, int subId, out CustomObjectCommand value)
        {
            value = default;
            return objectCommandMap.TryGetValue(id, out var wrapper) &&
                   wrapper.commandMap.TryGetValue(subId, out value);
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