// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.ObjectModel;
using System.Net;
using System.Reflection;
using System.Runtime.CompilerServices;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace CommandInfoUpdater
{
    /// <summary>
    /// Main logic for CommandInfoUpdater tool
    /// </summary>
    public class CommandInfoUpdater
    {
        private static readonly string GarnetCommandInfoJsonPath = "GarnetCommandsInfo.json";

        /// <summary>
        /// Tries to generate an updated JSON file containing Garnet's supported commands' info
        /// </summary>
        /// <param name="outputPath">Output path for the updated JSON file</param>
        /// <param name="respServerPort">RESP server port to query commands info</param>
        /// <param name="respServerHost">RESP server host to query commands info</param>
        /// <param name="ignoreCommands">Commands to ignore</param>
        /// <param name="force">Force update all commands</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if file generated successfully</returns>
        public static bool TryUpdateCommandInfo(string outputPath, int respServerPort, IPAddress respServerHost,
            IEnumerable<string> ignoreCommands, bool force, ILogger logger)
        {
            logger.LogInformation("Attempting to update RESP commands info...");

            IReadOnlyDictionary<string, RespCommandsInfo> existingCommandsInfo =
                new Dictionary<string, RespCommandsInfo>();
            if (!force && !RespCommandsInfo.TryGetRespCommandsInfo(out existingCommandsInfo, false, logger))
            {
                logger.LogError($"Unable to get existing RESP commands info.");
                return false;
            }

            var (commandsToAdd, commandsToRemove) =
                GetCommandsToAddAndRemove(existingCommandsInfo, ignoreCommands);

            if (!GetUserConfirmation(commandsToAdd, commandsToRemove, logger))
            {
                logger.LogInformation($"User cancelled update operation.");
                return false;
            }

            if (!TryGetRespCommandsInfo(GarnetCommandInfoJsonPath, logger, out var garnetCommandsInfo) ||
                garnetCommandsInfo == null)
            {
                logger.LogError($"Unable to read Garnet RESP commands info from {GarnetCommandInfoJsonPath}.");
                return false;
            }

            IDictionary<string, RespCommandsInfo> queriedCommandsInfo = new Dictionary<string, RespCommandsInfo>();
            var commandsToQuery = commandsToAdd.Keys.Select(k => k.Command).ToArray();
            if (commandsToQuery.Length > 0 && !TryGetCommandsInfo(commandsToQuery, respServerPort, respServerHost,
                    logger, out queriedCommandsInfo))
            {
                logger.LogError("Unable to get RESP command info from local RESP server.");
                return false;
            }

            var additionalCommandsInfo = new Dictionary<string, RespCommandsInfo>();
            foreach (var cmd in garnetCommandsInfo.Keys.Union(queriedCommandsInfo.Keys))
            {
                if (!additionalCommandsInfo.ContainsKey(cmd))
                {
                    var baseCommandInfo = queriedCommandsInfo.ContainsKey(cmd)
                        ? queriedCommandsInfo[cmd]
                        : garnetCommandsInfo[cmd];
                    additionalCommandsInfo.Add(cmd, new RespCommandsInfo()
                    {
                        Command = baseCommandInfo.Command,
                        Name = baseCommandInfo.Name,
                        Arity = baseCommandInfo.Arity,
                        Flags = baseCommandInfo.Flags,
                        FirstKey = baseCommandInfo.FirstKey,
                        LastKey = baseCommandInfo.LastKey,
                        Step = baseCommandInfo.Step,
                        AclCategories = baseCommandInfo.AclCategories,
                        Tips = baseCommandInfo.Tips,
                        KeySpecifications = baseCommandInfo.KeySpecifications,
                        SubCommands = queriedCommandsInfo.ContainsKey(cmd) && garnetCommandsInfo.ContainsKey(cmd) ?
                            queriedCommandsInfo[cmd].SubCommands.Union(garnetCommandsInfo[cmd].SubCommands).ToArray() :
                            baseCommandInfo.SubCommands
                    });
                }
            }

            var updatedCommandsInfo = GetUpdatedCommandsInfo(existingCommandsInfo, commandsToAdd, commandsToRemove,
                additionalCommandsInfo);

            if (!TryWriteRespCommandsInfo(outputPath, updatedCommandsInfo, logger))
            {
                logger.LogError($"Unable to write RESP commands info to path {outputPath}.");
                return false;
            }

            logger.LogInformation(
                $"RESP commands info updated successfully! Output file written to: {Path.GetFullPath(outputPath)}");
            return true;
        }

        /// <summary>
        /// Try to parse JSON file containing commands info
        /// </summary>
        /// <param name="resourcePath">Path to JSON file</param>
        /// <param name="logger">Logger</param>
        /// <param name="commandsInfo">Dictionary mapping command name to RespCommandsInfo</param>
        /// <returns>True if deserialization was successful</returns>
        private static bool TryGetRespCommandsInfo(string resourcePath, ILogger logger, out IReadOnlyDictionary<string, RespCommandsInfo> commandsInfo)
        {
            commandsInfo = default;

            var streamProvider = StreamProviderFactory.GetStreamProvider(FileLocationType.EmbeddedResource, null, Assembly.GetExecutingAssembly());
            var commandsInfoProvider = RespCommandsInfoProviderFactory.GetRespCommandsInfoProvider();

            var importSucceeded = commandsInfoProvider.TryImportRespCommandsInfo(resourcePath,
                streamProvider, out var tmpCommandsInfo, logger);

            if (!importSucceeded) return false;

            commandsInfo = tmpCommandsInfo;
            return true;
        }

        /// <summary>
        /// Compare existing commands to supported commands map to find added / removed commands / sub-commands
        /// </summary>
        /// <param name="existingCommandsInfo">Existing command names mapped to current command info</param>
        /// <param name="ignoreCommands">Commands to ignore</param>
        /// <returns>Commands to add and commands to remove mapped to a boolean determining if parent command should be added / removed</returns>
        private static (IDictionary<SupportedCommand, bool>, IDictionary<SupportedCommand, bool>)
            GetCommandsToAddAndRemove(IReadOnlyDictionary<string, RespCommandsInfo> existingCommandsInfo,
                IEnumerable<string> ignoreCommands)
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
                    subCommandsToAdd = supportedCommand.SubCommands.ToArray();
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

        /// <summary>
        /// Indicates to the user which commands and sub-commands are added / removed and get their confirmation to proceed
        /// </summary>
        /// <param name="commandsToAdd">Commands to add</param>
        /// <param name="commandsToRemove">Commands to remove</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if user wishes to continue, false otherwise</returns>
        private static bool GetUserConfirmation(IDictionary<SupportedCommand, bool> commandsToAdd, IDictionary<SupportedCommand, bool> commandsToRemove,
            ILogger logger)
        {
            var logCommandsToAdd = commandsToAdd.Where(kvp => kvp.Value).Select(c => c.Key.Command).ToList();
            var logSubCommandsToAdd = commandsToAdd.Where(c => c.Key.SubCommands != null)
                .SelectMany(c => c.Key.SubCommands!).ToList();
            var logCommandsToRemove = commandsToRemove.Where(kvp => kvp.Value).Select(c => c.Key.Command).ToList();
            var logSubCommandsToRemove = commandsToRemove.Where(c => c.Key.SubCommands != null)
                .SelectMany(c => c.Key.SubCommands!).ToList();

            logger.LogInformation(
                $"Found {logCommandsToAdd.Count} commands to add and {logSubCommandsToAdd.Count} sub-commands to add.");
            if (logCommandsToAdd.Count > 0)
                logger.LogInformation($"Commands to add: {string.Join(", ", logCommandsToAdd)}");
            if (logSubCommandsToAdd.Count > 0)
                logger.LogInformation($"Sub-Commands to add: {string.Join(", ", logSubCommandsToAdd)}");
            logger.LogInformation(
                $"Found {logCommandsToRemove.Count} commands to remove and {logSubCommandsToRemove.Count} sub-commands to commandsToRemove.");
            if (logCommandsToRemove.Count > 0)
                logger.LogInformation($"Commands to remove: {string.Join(", ", logCommandsToRemove)}");
            if (logSubCommandsToRemove.Count > 0)
                logger.LogInformation($"Sub-Commands to remove: {string.Join(", ", logSubCommandsToRemove)}");

            if (logCommandsToAdd.Count == 0 && logSubCommandsToAdd.Count == 0 && logCommandsToRemove.Count == 0 &&
                logSubCommandsToRemove.Count == 0)
            {
                logger.LogInformation("No commands to update.");
                return false;
            }

            logger.LogCritical("Would you like to continue? (Y/N)");
            var inputChar = Console.ReadKey();
            while (true)
            {
                switch (inputChar.KeyChar)
                {
                    case 'Y':
                    case 'y':
                        return true;
                    case 'N':
                    case 'n':
                        return false;
                    default:
                        logger.LogCritical("Illegal input. Would you like to continue? (Y/N)");
                        inputChar = Console.ReadKey();
                        break;
                }
            }
        }

        /// <summary>
        /// Query RESP server to get missing commands' info
        /// </summary>
        /// <param name="commandsToQuery">Command to query</param>
        /// <param name="respServerPort">RESP server port to query</param>
        /// <param name="respServerHost">RESP server host to query</param>
        /// <param name="logger">Logger</param>
        /// <param name="commandsInfo">Queried commands info</param>
        /// <returns>True if succeeded</returns>
        private static unsafe bool TryGetCommandsInfo(string[] commandsToQuery, int respServerPort,
            IPAddress respServerHost, ILogger logger, out IDictionary<string, RespCommandsInfo> commandsInfo)
        {
            commandsInfo = default;

            // If there are no commands to query, return
            if (commandsToQuery.Length == 0) return true;

            // Query the RESP server
            byte[] response;
            try
            {
                var lightClient = new LightClientRequest(respServerHost.ToString(), respServerPort, 0);
                response = lightClient.SendCommand($"COMMAND INFO {string.Join(' ', commandsToQuery)}");
            }
            catch (Exception e)
            {
                logger.LogError(e, "Encountered an error while querying local RESP server");
                return false;
            }

            var tmpCommandsInfo = new Dictionary<string, RespCommandsInfo>();

            // Get a map of supported commands to Garnet's RespCommand & ArrayCommand for the parser
            var supportedCommands = new ReadOnlyDictionary<string, RespCommand>(
                SupportedCommand.SupportedCommandsMap.ToDictionary(kvp => kvp.Key,
                    kvp => kvp.Value.RespCommand, StringComparer.OrdinalIgnoreCase));

            // Parse the response
            fixed (byte* respPtr = response)
            {
                var ptr = (byte*)Unsafe.AsPointer(ref respPtr[0]);
                var end = ptr + response.Length;

                // Read the array length (# of commands info returned)
                if (!RespReadUtils.ReadUnsignedArrayLength(out var cmdCount, ref ptr, end))
                {
                    logger.LogError($"Unable to read RESP command info count from server");
                    return false;
                }

                // Parse each command's command info
                for (var cmdIdx = 0; cmdIdx < cmdCount; cmdIdx++)
                {
                    if (!RespCommandInfoParser.TryReadFromResp(ref ptr, end, supportedCommands, out var command) ||
                        command == null)
                    {
                        logger.LogError(
                            $"Unable to read RESP command info from server for command {commandsToQuery[cmdIdx]}");
                        return false;
                    }

                    tmpCommandsInfo.Add(command.Name, command);
                }
            }

            commandsInfo = tmpCommandsInfo;
            return true;
        }

        /// <summary>
        /// Update the mapping of commands info
        /// </summary>
        /// <param name="existingCommandsInfo">Existing command info mapping</param>
        /// <param name="commandsToAdd">Commands to add</param>
        /// <param name="commandsToRemove">Commands to remove</param>
        /// <param name="queriedCommandsInfo">Queried commands info</param>
        /// <returns></returns>
        private static IReadOnlyDictionary<string, RespCommandsInfo> GetUpdatedCommandsInfo(
            IReadOnlyDictionary<string, RespCommandsInfo> existingCommandsInfo,
            IDictionary<SupportedCommand, bool> commandsToAdd,
            IDictionary<SupportedCommand, bool> commandsToRemove,
            IDictionary<string, RespCommandsInfo> queriedCommandsInfo)
        {
            // Define updated commands as commands to add unified with commands to remove
            var updatedCommands =
                new HashSet<string>(commandsToAdd.Keys.Union(commandsToRemove.Keys).Select(c => c.Command));

            // Preserve command info for all commands that have not been updated
            var updatedCommandsInfo = existingCommandsInfo
                .Where(existingCommand => !updatedCommands.Contains(existingCommand.Key))
                .ToDictionary(existingCommand => existingCommand.Key, existingCommand => existingCommand.Value);

            // Update commands info with commands to remove - i.e. update and add commands with removed sub-commands
            // Take only commands whose parent command should not be removed
            foreach (var command in commandsToRemove.Where(kvp => !kvp.Value).Select(kvp => kvp.Key))
            {
                // Determine updated sub-commands by subtracting from existing sub-commands
                var existingSubCommands = existingCommandsInfo[command.Command].SubCommands == null
                    ? null
                    : existingCommandsInfo[command.Command].SubCommands.Select(sc => sc.Name).ToArray();
                var remainingSubCommands = existingSubCommands == null ? null :
                    command.SubCommands == null ? existingSubCommands :
                    existingSubCommands.Except(command.SubCommands).ToArray();

                // Create updated command info based on existing command
                var existingCommand = existingCommandsInfo[command.Command];
                var updatedCommand = new RespCommandsInfo
                {
                    Command = existingCommand.Command,
                    Name = existingCommand.Name,
                    Arity = existingCommand.Arity,
                    Flags = existingCommand.Flags,
                    FirstKey = existingCommand.FirstKey,
                    LastKey = existingCommand.LastKey,
                    Step = existingCommand.Step,
                    AclCategories = existingCommand.AclCategories,
                    Tips = existingCommand.Tips,
                    KeySpecifications = existingCommand.KeySpecifications,
                    SubCommands = remainingSubCommands == null || remainingSubCommands.Length == 0
                        ? null
                        : existingCommand.SubCommands.Where(sc => remainingSubCommands.Contains(sc.Name)).ToArray()
                };

                updatedCommandsInfo.Add(updatedCommand.Name, updatedCommand);
            }

            // Update commands info with commands to add
            foreach (var command in commandsToAdd.Keys)
            {
                RespCommandsInfo baseCommand;
                List<RespCommandsInfo> updatedSubCommands;
                // If parent command already exists
                if (existingCommandsInfo.ContainsKey(command.Command))
                {
                    updatedSubCommands = existingCommandsInfo[command.Command].SubCommands == null
                        ? new List<RespCommandsInfo>()
                        : existingCommandsInfo[command.Command].SubCommands.ToList();

                    // Add sub-commands with updated queried command info
                    foreach (var subCommandToAdd in command.SubCommands!)
                    {
                        updatedSubCommands.Add(queriedCommandsInfo[command.Command].SubCommands
                            .First(sc => sc.Name == subCommandToAdd));
                    }

                    // Set base command as existing sub-command
                    baseCommand = existingCommandsInfo[command.Command];
                }
                // If parent command does not exist
                else
                {
                    // Set base command as queried command
                    baseCommand = queriedCommandsInfo[command.Command];

                    // Update sub-commands to contain supported sub-commands only
                    updatedSubCommands = command.SubCommands == null
                        ? null
                        : baseCommand.SubCommands.Where(sc => command.SubCommands.Contains(sc.Name)).ToList();
                }

                // Create updated command info based on base command & updated sub-commands
                var updatedCommand = new RespCommandsInfo
                {
                    Command = baseCommand.Command,
                    Name = baseCommand.Name,
                    Arity = baseCommand.Arity,
                    Flags = baseCommand.Flags,
                    FirstKey = baseCommand.FirstKey,
                    LastKey = baseCommand.LastKey,
                    Step = baseCommand.Step,
                    AclCategories = baseCommand.AclCategories,
                    Tips = baseCommand.Tips,
                    KeySpecifications = baseCommand.KeySpecifications,
                    SubCommands = updatedSubCommands?.ToArray()
                };

                updatedCommandsInfo.Add(updatedCommand.Name, updatedCommand);
            }

            return updatedCommandsInfo;
        }

        /// <summary>
        /// Try to serialize updated commands info to JSON file
        /// </summary>
        /// <param name="outputPath">Output path for JSON file</param>
        /// <param name="commandsInfo">Commands info to serialize</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if file written successfully</returns>
        private static bool TryWriteRespCommandsInfo(string outputPath,
            IReadOnlyDictionary<string, RespCommandsInfo> commandsInfo, ILogger logger)
        {
            var streamProvider = StreamProviderFactory.GetStreamProvider(FileLocationType.Local);
            var commandsInfoProvider = RespCommandsInfoProviderFactory.GetRespCommandsInfoProvider();

            var exportSucceeded = commandsInfoProvider.TryExportRespCommandsInfo(outputPath,
                streamProvider, commandsInfo, logger);

            if (!exportSucceeded) return false;

            return true;
        }
    }
}