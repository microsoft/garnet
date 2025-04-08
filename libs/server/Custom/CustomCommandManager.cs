// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Custom command manager
    /// </summary>
    public class CustomCommandManager
    {
        // Maximum number of custom raw string commands
        private static readonly int MaxCustomRawStringCommands = 256;

        // Initial size of expandable maps
        private static readonly int MinMapSize = 1;

        // ID ranges for custom raw string commands and custom object types
        private static readonly int CustomRawStringCommandMinId = (ushort)RespCommand.INVALID - MaxCustomRawStringCommands;
        private static readonly int CustomRawStringCommandMaxId = (ushort)RespCommand.INVALID - 1;
        private static readonly int CustomObjectTypeMinId = (byte)GarnetObjectTypeExtensions.LastObjectType + 1;
        private static readonly int CustomObjectTypeMaxId = (byte)GarnetObjectTypeExtensions.FirstSpecialObjectType - 1;

        // Maps holding different types of custom commands
        private ExpandableMap<CustomRawStringCommand> rawStringCommandMap;
        private ExpandableMap<CustomObjectCommandWrapper> objectCommandMap;
        private ExpandableMap<CustomTransaction> transactionProcMap;
        private ExpandableMap<CustomProcedureWrapper> customProcedureMap;

        // Map holding all registered modules by module name
        private readonly ConcurrentDictionary<string, ModuleLoadContext> modules;

        // Maps holding custom command info and docs by command name
        private readonly ConcurrentDictionary<string, RespCommandsInfo> customCommandsInfo = new(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<string, RespCommandDocs> customCommandsDocs = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Offset from which custom type IDs start in the GarnetObjectType enum
        /// </summary>
        internal static readonly byte CustomTypeIdStartOffset = (byte)CustomObjectTypeMinId;

        /// <summary>
        /// Create new custom command manager
        /// </summary>
        public CustomCommandManager()
        {
            Debug.Assert(CustomRawStringCommandMinId > (ushort)RespCommandExtensions.LastValidCommand);

            rawStringCommandMap = new ExpandableMap<CustomRawStringCommand>(MinMapSize,
                CustomRawStringCommandMinId, CustomRawStringCommandMaxId);

            objectCommandMap = new ExpandableMap<CustomObjectCommandWrapper>(MinMapSize,
                CustomObjectTypeMinId, CustomObjectTypeMaxId);

            transactionProcMap = new ExpandableMap<CustomTransaction>(MinMapSize, 0, byte.MaxValue);
            customProcedureMap = new ExpandableMap<CustomProcedureWrapper>(MinMapSize, 0, byte.MaxValue);

            modules = new();
        }

        /// <summary>
        /// Register custom raw-string command
        /// </summary>
        /// <param name="name">Name of command</param>
        /// <param name="type">Command type</param>
        /// <param name="customFunctions">Custom raw-string functions</param>
        /// <param name="commandInfo">Command info of command</param>
        /// <param name="commandDocs">Command docs of command</param>
        /// <param name="expirationTicks">Command expiration in ticks</param>
        /// <returns>Command ID</returns>
        /// <exception cref="Exception"></exception>
        internal int Register(string name, CommandType type, CustomRawStringFunctions customFunctions,
            RespCommandsInfo commandInfo, RespCommandDocs commandDocs, long expirationTicks)
        {
            if (!rawStringCommandMap.TryGetNextId(out var cmdId))
                throw new Exception("Out of registration space");
            Debug.Assert(cmdId <= ushort.MaxValue);

            var extId = cmdId - CustomRawStringCommandMinId;
            var arity = commandInfo?.Arity ?? 0;
            var newCmd = new CustomRawStringCommand(name, (ushort)extId, type, arity, customFunctions, expirationTicks);
            var setSuccessful = rawStringCommandMap.TrySetValue(cmdId, newCmd);
            Debug.Assert(setSuccessful);
            if (commandInfo != null)
                customCommandsInfo.AddOrUpdate(name, commandInfo, (_, _) => commandInfo);
            if (commandDocs != null)
                customCommandsDocs.AddOrUpdate(name, commandDocs, (_, _) => commandDocs);
            return extId;
        }

        /// <summary>
        /// Register custom transaction
        /// </summary>
        /// <param name="name">Name of transaction</param>
        /// <param name="proc">Delegate for creating transaction</param>
        /// <param name="commandInfo">Command info of transaction</param>
        /// <param name="commandDocs">Command docs of transaction</param>
        /// <returns>Command ID</returns>
        /// <exception cref="Exception"></exception>
        internal int Register(string name, Func<CustomTransactionProcedure> proc, RespCommandsInfo commandInfo = null,
            RespCommandDocs commandDocs = null)
        {
            if (!transactionProcMap.TryGetNextId(out var cmdId))
                throw new Exception("Out of registration space");
            Debug.Assert(cmdId <= byte.MaxValue);

            var arity = commandInfo?.Arity ?? 0;
            var newCmd = new CustomTransaction(name, (byte)cmdId, arity, proc);
            var setSuccessful = transactionProcMap.TrySetValue(cmdId, newCmd);
            Debug.Assert(setSuccessful);
            if (commandInfo != null)
                customCommandsInfo.AddOrUpdate(name, commandInfo, (_, _) => commandInfo);
            if (commandDocs != null)
                customCommandsDocs.AddOrUpdate(name, commandDocs, (_, _) => commandDocs);
            return cmdId;
        }

        /// <summary>
        /// Register custom object type
        /// </summary>
        /// <param name="factory">Factory for creating custom object</param>
        /// <returns>Type ID</returns>
        /// <exception cref="Exception"></exception>
        internal int RegisterType(CustomObjectFactory factory)
        {
            if (objectCommandMap.TryGetFirstId(c => c.factory == factory, out var dupRegistrationId))
                throw new Exception($"Type already registered with ID {dupRegistrationId}");

            var typeId = RegisterNewType(factory);
            return typeId - CustomObjectTypeMinId;
        }

        /// <summary>
        /// Register custom object command
        /// </summary>
        /// <param name="name">Name of command</param>
        /// <param name="commandType">Command type</param>
        /// <param name="factory">Factory for creating custom object</param>
        /// <param name="commandInfo">Command info for custom command</param>
        /// <param name="commandDocs">Command docs for custom command</param>
        /// <param name="customObjectFunctions">Custom object functions</param>
        /// <returns>Object type ID and sub-command ID</returns>
        /// <exception cref="Exception"></exception>
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

            var arity = commandInfo?.Arity ?? 0;
            var newSubCmd = new CustomObjectCommand(name, (byte)extId, (byte)scId, commandType, arity, wrapper.factory,
                customObjectFunctions);
            var scSetSuccessful = wrapper.commandMap.TrySetValue(scId, newSubCmd);
            Debug.Assert(scSetSuccessful);

            if (commandInfo != null)
                customCommandsInfo.AddOrUpdate(name, commandInfo, (_, _) => commandInfo);
            if (commandDocs != null)
                customCommandsDocs.AddOrUpdate(name, commandDocs, (_, _) => commandDocs);

            return (extId, scId);
        }

        /// <summary>
        /// Register custom procedure
        /// </summary>
        /// <param name="name">Name of custom procedure</param>
        /// <param name="customProcedure">Delegate for creating custom procedure</param>
        /// <param name="commandInfo">Command info of custom procedure</param>
        /// <param name="commandDocs">Command docs of custom procedure</param>
        /// <returns>Command ID</returns>
        /// <exception cref="Exception"></exception>
        internal int Register(string name, Func<CustomProcedure> customProcedure, RespCommandsInfo commandInfo = null, RespCommandDocs commandDocs = null)
        {
            if (!customProcedureMap.TryGetNextId(out var cmdId))
                throw new Exception("Out of registration space");

            Debug.Assert(cmdId <= byte.MaxValue);

            var arity = commandInfo?.Arity ?? 0;
            var newCmd = new CustomProcedureWrapper(name, (byte)cmdId, arity, customProcedure, this);
            var setSuccessful = customProcedureMap.TrySetValue(cmdId, newCmd);
            Debug.Assert(setSuccessful);

            if (commandInfo != null)
                customCommandsInfo.AddOrUpdate(name, commandInfo, (_, _) => commandInfo);
            if (commandDocs != null)
                customCommandsDocs.AddOrUpdate(name, commandDocs, (_, _) => commandDocs);
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

        /// <summary>
        /// Try to add a module to the modules map (should only be called by ModuleRegistrar)
        /// </summary>
        /// <param name="moduleLoadContext">Module load context</param>
        /// <returns>True if module was added successfully (i.e. its name doesn't already exist)</returns>
        internal bool TryAddModule(ModuleLoadContext moduleLoadContext)
            => modules.TryAdd(moduleLoadContext.Name, moduleLoadContext);

        /// <summary>
        /// Try to retrieve a custom procedure wrapper by procedure ID (should only be called by CustomCommandManagerSession)
        /// </summary>
        /// <param name="id">Procedure ID</param>
        /// <param name="value">Retrieved custom procedure</param>
        /// <returns>True if custom procedure found</returns>
        internal bool TryGetCustomProcedure(int id, out CustomProcedureWrapper value)
            => customProcedureMap.TryGetValue(id, out value);

        /// <summary>
        /// Try to retrieve a custom transaction by transaction ID (should only be called by CustomCommandManagerSession)
        /// </summary>
        /// <param name="id">Transaction ID</param>
        /// <param name="value">Retrieved custom transaction</param>
        /// <returns>True if custom transaction found</returns>
        internal bool TryGetCustomTransactionProcedure(int id, out CustomTransaction value)
            => transactionProcMap.TryGetValue(id, out value);

        /// <summary>
        /// Try to retrieve a custom raw string command by command ID
        /// </summary>
        /// <param name="id">Command ID</param>
        /// <param name="value">Retrieved command</param>
        /// <returns>True if command found</returns>
        internal bool TryGetCustomCommand(int id, out CustomRawStringCommand value)
            => rawStringCommandMap.TryGetValue(id, out value);

        /// <summary>
        /// Try to retrieve a custom object command by command ID
        /// </summary>
        /// <param name="id">Command ID</param>
        /// <param name="value">Retrieved command</param>
        /// <returns>True if command found</returns>
        internal bool TryGetCustomObjectCommand(int id, out CustomObjectCommandWrapper value)
            => objectCommandMap.TryGetValue(id, out value);

        /// <summary>
        /// Try to retrieve a custom object sub-command command by command ID and sub-command ID
        /// </summary>
        /// <param name="id">Command ID</param>
        /// <param name="subId">Sub-command ID</param>
        /// <param name="value">Retrieved command</param>
        /// <returns>True if command found</returns>
        internal bool TryGetCustomObjectSubCommand(int id, int subId, out CustomObjectCommand value)
        {
            value = null;
            return objectCommandMap.TryGetValue(id, out var wrapper) &&
                   wrapper.commandMap.TryGetValue(subId, out value);
        }

        /// <summary>
        /// Get a custom raw-string command by name
        /// </summary>
        /// <param name="command">The command name to match</param>
        /// <param name="cmd">The matching command</param>
        /// <returns>True if command name matched an existing command</returns>
        internal bool Match(ReadOnlySpan<byte> command, out CustomRawStringCommand cmd)
            => rawStringCommandMap.MatchCommand(command, out cmd);

        /// <summary>
        /// Get a custom transaction by name
        /// </summary>
        /// <param name="command">The transaction name to match</param>
        /// <param name="cmd">The matching transaction</param>
        /// <returns>True if transaction name matched an existing transaction</returns>
        internal bool Match(ReadOnlySpan<byte> command, out CustomTransaction cmd)
            => transactionProcMap.MatchCommand(command, out cmd);

        /// <summary>
        /// Get a custom object command by name
        /// </summary>
        /// <param name="command">The command name to match</param>
        /// <param name="cmd">The matching command</param>
        /// <returns>True if command name matched an existing command</returns>
        internal bool Match(ReadOnlySpan<byte> command, out CustomObjectCommand cmd)
            => objectCommandMap.MatchSubCommand(command, out cmd);

        /// <summary>
        /// Get a custom procedure by name
        /// </summary>
        /// <param name="command">The procedure name to match</param>
        /// <param name="cmd">The matching procedure</param>
        /// <returns>True if procedure name matched an existing procedure</returns>
        internal bool Match(ReadOnlySpan<byte> command, out CustomProcedureWrapper cmd)
            => customProcedureMap.MatchCommand(command, out cmd);

        /// <summary>
        /// Get custom command info by name
        /// </summary>
        /// <param name="cmdName">The command name</param>
        /// <param name="respCommandsInfo">The matching command info</param>
        /// <returns>True if command info was found</returns>
        internal bool TryGetCustomCommandInfo(string cmdName, out RespCommandsInfo respCommandsInfo)
            => this.customCommandsInfo.TryGetValue(cmdName, out respCommandsInfo);

        /// <summary>
        /// Get custom command docs by name
        /// </summary>
        /// <param name="cmdName">The command name</param>
        /// <param name="respCommandsDocs">The matching command docs</param>
        /// <returns>True if command docs was found</returns>
        internal bool TryGetCustomCommandDocs(string cmdName, out RespCommandDocs respCommandsDocs)
            => this.customCommandsDocs.TryGetValue(cmdName, out respCommandsDocs);

        /// <summary>
        /// Get all custom command infos
        /// </summary>
        /// <returns>Map between custom command name and custom command info</returns>
        internal IReadOnlyDictionary<string, RespCommandsInfo> GetAllCustomCommandsInfos()
            => this.customCommandsInfo.AsReadOnly();

        /// <summary>
        /// Get all custom command docs
        /// </summary>
        /// <returns>Map between custom command name and custom command docs</returns>
        internal IReadOnlyDictionary<string, RespCommandDocs> GetAllCustomCommandsDocs()
            => this.customCommandsDocs.AsReadOnly();

        /// <summary>
        /// Get count of all custom command infos
        /// </summary>
        /// <returns>Count</returns>
        internal int GetCustomCommandInfoCount() => customCommandsInfo.Count;

        /// <summary>
        /// Get RespCommand enum by command ID
        /// </summary>
        /// <param name="id">Command ID</param>
        /// <returns>Matching RespCommand</returns>
        internal RespCommand GetCustomRespCommand(int id)
            => (RespCommand)(CustomRawStringCommandMinId + id);

        /// <summary>
        /// Get GarnetObjectType enum by object type ID
        /// </summary>
        /// <param name="id">Object type ID</param>
        /// <returns>Matching GarnetObjectType</returns>
        internal GarnetObjectType GetCustomGarnetObjectType(int id)
            => (GarnetObjectType)(CustomObjectTypeMinId + id);

        private int RegisterNewType(CustomObjectFactory factory)
        {
            if (!objectCommandMap.TryGetNextId(out var typeId))
                throw new Exception("Out of registration space");
            Debug.Assert(typeId <= byte.MaxValue);

            var extId = typeId - CustomObjectTypeMinId;
            var newCmd = new CustomObjectCommandWrapper((byte)extId, factory);
            var setSuccessful = objectCommandMap.TrySetValue(typeId, newCmd);
            Debug.Assert(setSuccessful);

            return typeId;
        }
    }
}