// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Reflection;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace CommandInfoUpdater
{
    public class DataUtils
    {
        /// <summary>
        /// Try to parse JSON file containing commands data
        /// </summary>
        /// <param name="resourcePath">Path to JSON file</param>
        /// <param name="logger">Logger</param>
        /// <param name="commandsData">Dictionary mapping command name to data</param>
        /// <returns>True if deserialization was successful</returns>
        internal static bool TryGetRespCommandsData<TData>(string resourcePath, ILogger logger,
            out IReadOnlyDictionary<string, TData> commandsData)
            where TData : IRespCommandData
        {
            commandsData = default;

            var streamProvider = StreamProviderFactory.GetStreamProvider(FileLocationType.EmbeddedResource, null,
                Assembly.GetExecutingAssembly());
            var commandsInfoProvider = RespCommandsDataProviderFactory.GetRespCommandsDataProvider<TData>();

            var importSucceeded = commandsInfoProvider.TryImportRespCommandsData(resourcePath,
                streamProvider, out var tmpCommandsData, logger);

            if (!importSucceeded) return false;

            commandsData = tmpCommandsData;
            return true;
        }

        /// <summary>
        /// Try to serialize updated commands info to JSON file
        /// </summary>
        /// <param name="outputPath">Output path for JSON file</param>
        /// <param name="commandsData">Commands info to serialize</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if file written successfully</returns>
        internal static bool TryWriteRespCommandsData<TData>(string outputPath,
            IReadOnlyDictionary<string, TData> commandsData, ILogger logger) where TData : IRespCommandData
        {
            var streamProvider = StreamProviderFactory.GetStreamProvider(FileLocationType.Local);
            var commandsInfoProvider = RespCommandsDataProviderFactory.GetRespCommandsDataProvider<TData>();

            var exportSucceeded = commandsInfoProvider.TryExportRespCommandsData(outputPath,
                streamProvider, commandsData, logger);

            if (!exportSucceeded) return false;

            return true;
        }

        /// <summary>
        /// Compare existing commands to supported commands map to find added / removed commands / sub-commands
        /// </summary>
        /// <param name="existingCommandsInfo">Existing command names mapped to current command info</param>
        /// <param name="ignoreCommands">Commands to ignore</param>
        /// <returns>Commands to add and commands to remove mapped to a boolean determining if parent command should be added / removed</returns>
        internal static (IDictionary<SupportedCommand, bool>, IDictionary<SupportedCommand, bool>)
            GetCommandsToAddAndRemove<TData>(IReadOnlyDictionary<string, TData> existingCommandsInfo,
                IEnumerable<string> ignoreCommands) where TData : IRespCommandData, IRespCommandData<TData>
        {
            var commandsToAdd = new Dictionary<SupportedCommand, bool>();
            var commandsToRemove = new Dictionary<SupportedCommand, bool>();
            var commandsToIgnore = ignoreCommands != null ? new HashSet<string>(ignoreCommands) : null;

            // Supported commands
            var supportedCommands = SupportedCommand.SupportedCommandsMap;

            // Find commands / sub-commands to add
            foreach (var supportedCommand in supportedCommands.Values)
            {
                // Ignore command if in commands to ignore
                if (commandsToIgnore != null && commandsToIgnore.Contains(supportedCommand.Command)) continue;

                // If existing commands do not contain parent command, add it and indicate parent command should be added
                if (!existingCommandsInfo.ContainsKey(supportedCommand.Command))
                {
                    commandsToAdd.Add(supportedCommand, true);
                    continue;
                }

                // If existing commands contain parent command and no sub-commands are indicated in supported commands, no sub-commands to add
                if (supportedCommand.SubCommands == null) continue;

                string[] subCommandsToAdd;
                // If existing commands contain parent command and have no sub-commands, set sub-commands to add as supported command's sub-commands
                if (existingCommandsInfo[supportedCommand.Command].SubCommands == null)
                {
                    subCommandsToAdd = [.. supportedCommand.SubCommands];
                }
                // Set sub-commands to add as the difference between existing sub-commands and supported command's sub-commands
                else
                {
                    var existingSubCommands = new HashSet<string>(existingCommandsInfo[supportedCommand.Command]
                        .SubCommands
                        .Select(sc => sc.Name));
                    subCommandsToAdd = supportedCommand.SubCommands
                        .Where(subCommand => !existingSubCommands.Contains(subCommand)).Select(sc => sc).ToArray();
                }

                // If there are sub-commands to add, add a new supported command with the sub-commands to add
                // Indicate that parent command should not be added
                if (subCommandsToAdd.Length > 0)
                {
                    commandsToAdd.Add(
                        new SupportedCommand(supportedCommand.Command, supportedCommand.RespCommand, subCommandsToAdd), false);
                }
            }

            // Find commands / sub-commands to remove
            foreach (var existingCommand in existingCommandsInfo)
            {
                var existingSubCommands = existingCommand.Value.SubCommands;

                // If supported commands do not contain existing parent command, add it to the list and indicate parent command should be removed
                if (!supportedCommands.ContainsKey(existingCommand.Key))
                {
                    commandsToRemove.Add(new SupportedCommand(existingCommand.Key), true);
                    continue;
                }

                // If supported commands contain existing parent command and no sub-commands are indicated in existing commands, no sub-commands to remove
                if (existingSubCommands == null) continue;

                // Set sub-commands to remove as the difference between supported sub-commands and existing command's sub-commands
                var subCommandsToRemove = (supportedCommands[existingCommand.Key].SubCommands == null
                        ? existingSubCommands
                        : existingSubCommands.Where(sc =>
                            !supportedCommands[existingCommand.Key].SubCommands!.Contains(sc.Name)))
                    .Select(sc => sc.Name)
                    .ToArray();

                // If there are sub-commands to remove, add a new supported command with the sub-commands to remove
                // Indicate that parent command should not be removed
                if (subCommandsToRemove.Length > 0)
                {
                    commandsToRemove.Add(
                        new SupportedCommand(existingCommand.Key, existingCommand.Value.Command, subCommandsToRemove), false);
                }
            }

            return (commandsToAdd, commandsToRemove);
        }
    }
}
