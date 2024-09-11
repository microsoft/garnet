// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.ObjectModel;
using System.Net;
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
                logger.LogError("Unable to get existing RESP commands info.");
                return false;
            }

            var (commandsToAdd, commandsToRemove) =
                DataUtils.GetCommandsToAddAndRemove(existingCommandsInfo, ignoreCommands);

            if (!GetUserConfirmation(commandsToAdd, commandsToRemove, logger))
            {
                logger.LogInformation("User cancelled update operation.");
                return false;
            }

            if (!DataUtils.TryGetRespCommandsData<RespCommandsInfo>(GarnetCommandInfoJsonPath, logger, out var garnetCommandsInfo) ||
                garnetCommandsInfo == null)
            {
                logger.LogError("Unable to read Garnet RESP commands info from {GarnetCommandInfoJsonPath}.", GarnetCommandInfoJsonPath);
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

            if (!DataUtils.TryWriteRespCommandsData(outputPath, updatedCommandsInfo, logger))
            {
                logger.LogError("Unable to write RESP commands info to path {outputPath}.", outputPath);
                return false;
            }

            logger.LogInformation("RESP commands info updated successfully! Output file written to: {fullOutputPath}", Path.GetFullPath(outputPath));

            CommandDocsUpdater.TryUpdateCommandDocs(outputPath, respServerPort, respServerHost,
                commandsToAdd, commandsToRemove, force, logger);

            return true;
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

            logger.LogInformation("Found {logCommandsToAddCount} commands to add and {logSubCommandsToAddCount} sub-commands to add.", logCommandsToAdd.Count, logSubCommandsToAdd.Count);
            if (logCommandsToAdd.Count > 0)
                logger.LogInformation("Commands to add: {commands}", string.Join(", ", logCommandsToAdd));
            if (logSubCommandsToAdd.Count > 0)
                logger.LogInformation("Sub-Commands to add: {commands}", string.Join(", ", logSubCommandsToAdd));
            logger.LogInformation("Found {logCommandsToRemoveCount} commands to remove and {logSubCommandsToRemoveCount} sub-commands to commandsToRemove.", logCommandsToRemove.Count, logSubCommandsToRemove.Count);
            if (logCommandsToRemove.Count > 0)
                logger.LogInformation("Commands to remove: {commands}", string.Join(", ", logCommandsToRemove));
            if (logSubCommandsToRemove.Count > 0)
                logger.LogInformation("Sub-Commands to remove: {commands}", string.Join(", ", logSubCommandsToRemove));

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
                    logger.LogError("Unable to read RESP command info count from server");
                    return false;
                }

                // Parse each command's command info
                for (var cmdIdx = 0; cmdIdx < cmdCount; cmdIdx++)
                {
                    if (!RespCommandInfoParser.TryReadFromResp(ref ptr, end, supportedCommands, out var command) ||
                        command == null)
                    {
                        logger.LogError("Unable to read RESP command info from server for command {command}", commandsToQuery[cmdIdx]);
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
                        : [.. existingCommandsInfo[command.Command].SubCommands];

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
    }
}