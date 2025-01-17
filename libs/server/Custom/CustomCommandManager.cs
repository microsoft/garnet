// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Custom command manager
    /// </summary>
    public class CustomCommandManager
    {
        private static readonly int MaxCustomRawStringCommands = 256;

        private static readonly int CustomRawStringCommandMinId = (ushort)RespCommand.INVALID - MaxCustomRawStringCommands;
        private static readonly int CustomRawStringCommandMaxId = (ushort)RespCommand.INVALID - 1;
        private static readonly int CustomObjectTypeMinId = (byte)GarnetObjectTypeExtensions.LastObjectType + 1;
        private static readonly int CustomObjectTypeMaxId = (byte)GarnetObjectTypeExtensions.FirstSpecialObjectType - 1;

        private ConcurrentExpandableMap<CustomRawStringCommand> rawStringCommandMap;
        private ConcurrentExpandableMap<CustomObjectCommandWrapper> objectCommandMap;
        private ConcurrentExpandableMap<CustomTransaction> transactionProcMap;
        private ConcurrentExpandableMap<CustomProcedureWrapper> customProcedureMap;

        private readonly ConcurrentDictionary<string, ModuleLoadContext> modules;

        internal static readonly int MinMapSize = 8;
        internal static readonly byte CustomTypeIdStartOffset = (byte)CustomObjectTypeMinId;

        internal int CustomCommandsInfoCount => customCommandsInfo.Count;
        internal readonly ConcurrentDictionary<string, RespCommandsInfo> customCommandsInfo = new(StringComparer.OrdinalIgnoreCase);
        internal readonly ConcurrentDictionary<string, RespCommandDocs> customCommandsDocs = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Create new custom command manager
        /// </summary>
        public CustomCommandManager()
        {
            Debug.Assert(CustomRawStringCommandMinId > (ushort)RespCommandExtensions.LastValidCommand);

            rawStringCommandMap = new ConcurrentExpandableMap<CustomRawStringCommand>(MinMapSize,
                CustomRawStringCommandMinId, CustomRawStringCommandMaxId);

            objectCommandMap = new ConcurrentExpandableMap<CustomObjectCommandWrapper>(MinMapSize,
                CustomObjectTypeMinId, CustomObjectTypeMaxId);

            transactionProcMap = new ConcurrentExpandableMap<CustomTransaction>(MinMapSize, 0, byte.MaxValue);
            customProcedureMap = new ConcurrentExpandableMap<CustomProcedureWrapper>(MinMapSize, 0, byte.MaxValue);

            modules = new();
        }

        internal int Register(string name, CommandType type, CustomRawStringFunctions customFunctions,
            RespCommandsInfo commandInfo, RespCommandDocs commandDocs, long expirationTicks)
        {
            if (!rawStringCommandMap.TryGetNextId(out var cmdId))
                throw new Exception("Out of registration space");
            Debug.Assert(cmdId <= ushort.MaxValue);
            var extId = cmdId - CustomRawStringCommandMinId;
            var newCmd = new CustomRawStringCommand(name, (ushort)extId, type, customFunctions, expirationTicks);
            var setSuccessful = rawStringCommandMap.TrySetValue(cmdId, ref newCmd);
            Debug.Assert(setSuccessful);
            if (commandInfo != null) customCommandsInfo.AddOrUpdate(name, commandInfo, (_, _) => commandInfo);
            if (commandDocs != null) customCommandsDocs.AddOrUpdate(name, commandDocs, (_, _) => commandDocs);
            return extId;
        }

        internal int Register(string name, Func<CustomTransactionProcedure> proc, RespCommandsInfo commandInfo = null,
            RespCommandDocs commandDocs = null)
        {
            if (!transactionProcMap.TryGetNextId(out var cmdId))
                throw new Exception("Out of registration space");
            Debug.Assert(cmdId <= byte.MaxValue);

            var newCmd = new CustomTransaction(name, (byte)cmdId, proc);
            var setSuccessful = transactionProcMap.TrySetValue(cmdId, ref newCmd);
            Debug.Assert(setSuccessful);
            if (commandInfo != null) customCommandsInfo.AddOrUpdate(name, commandInfo, (_, _) => commandInfo);
            if (commandDocs != null) customCommandsDocs.AddOrUpdate(name, commandDocs, (_, _) => commandDocs);
            return cmdId;
        }

        internal int RegisterType(CustomObjectFactory factory)
        {
            if (objectCommandMap.TryGetFirstId(c => c.factory == factory, out var dupRegistrationId))
                throw new Exception($"Type already registered with ID {dupRegistrationId}");

            var typeId = RegisterNewType(factory);
            return typeId - CustomObjectTypeMinId;
        }

        internal (int objectTypeId, int subCommand) Register(string name, CommandType commandType, CustomObjectFactory factory, RespCommandsInfo commandInfo, RespCommandDocs commandDocs, CustomObjectFunctions customObjectFunctions = null)
        {
            if (!objectCommandMap.TryGetFirstId(c => c.factory == factory, out var typeId))
            {
                typeId = RegisterNewType(factory);
            }

            var extId = typeId - CustomObjectTypeMinId;

            objectCommandMap.TryGetValue(typeId, out var wrapper);
            if (!wrapper.commandMap.TryGetNextId(out var scId))
                throw new Exception("Out of registration space");

            Debug.Assert(scId <= byte.MaxValue);
            var newSubCmd = new CustomObjectCommand(name, (byte)extId, (byte)scId, commandType, wrapper.factory,
                customObjectFunctions);
            var scSetSuccessful = wrapper.commandMap.TrySetValue(scId, ref newSubCmd);
            Debug.Assert(scSetSuccessful);

            if (commandInfo != null) customCommandsInfo.AddOrUpdate(name, commandInfo, (_, _) => commandInfo);
            if (commandDocs != null) customCommandsDocs.AddOrUpdate(name, commandDocs, (_, _) => commandDocs);

            return (extId, scId);
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
            var setSuccessful = customProcedureMap.TrySetValue(cmdId, ref newCmd);
            Debug.Assert(setSuccessful);

            if (commandInfo != null) customCommandsInfo.AddOrUpdate(name, commandInfo, (_, _) => commandInfo);
            if (commandDocs != null) customCommandsDocs.AddOrUpdate(name, commandDocs, (_, _) => commandDocs);
            return cmdId;
        }

        /// <summary>
        /// Register module
        /// </summary>
        /// <param name="module">Module to register</param>
        /// <param name="moduleArgs">Module arguments</param>
        /// <param name="logger">Logger</param>
        /// <param name="errorMessage">Error message</param>
        /// <returns>True if module registered successfully</returns>
        public bool RegisterModule(ModuleBase module, string[] moduleArgs, ILogger logger,
            out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = default;

            var moduleLoadContext = new ModuleLoadContext(this, logger);
            try
            {
                module.OnLoad(moduleLoadContext, moduleArgs);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error loading module");
                errorMessage = CmdStrings.RESP_ERR_MODULE_ONLOAD;
                return false;
            }

            if (!moduleLoadContext.Initialized)
            {
                errorMessage = CmdStrings.RESP_ERR_MODULE_ONLOAD;
                logger?.LogError("Module {0} failed to initialize", moduleLoadContext.Name);
                return false;
            }

            logger?.LogInformation("Module {0} version {1} loaded", moduleLoadContext.Name, moduleLoadContext.Version);
            return true;
        }

        internal bool TryAddModule(ModuleLoadContext moduleLoadContext)
            => modules.TryAdd(moduleLoadContext.Name, moduleLoadContext);

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

        internal RespCommand GetCustomRespCommand(int id)
            => (RespCommand)(CustomRawStringCommandMinId + id);

        internal GarnetObjectType GetCustomGarnetObjectType(int id)
            => (GarnetObjectType)(CustomObjectTypeMinId + id);

        private int RegisterNewType(CustomObjectFactory factory)
        {
            if (!objectCommandMap.TryGetNextId(out var typeId))
                throw new Exception("Out of registration space");
            Debug.Assert(typeId <= byte.MaxValue);

            var extId = typeId - CustomObjectTypeMinId;
            var newCmd = new CustomObjectCommandWrapper((byte)extId, factory);
            var setSuccessful = objectCommandMap.TrySetValue(typeId, ref newCmd);
            Debug.Assert(setSuccessful);

            return typeId;
        }
    }
}