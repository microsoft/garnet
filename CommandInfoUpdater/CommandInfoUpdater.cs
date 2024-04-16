// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.ObjectModel;
using System.Net;
using System.Reflection;
using System.Runtime.CompilerServices;
using Garnet;
using Garnet.common;
using Garnet.server;
using Garnet.test;
using Microsoft.Extensions.Logging;

namespace CommandInfoUpdater
{
    public class CommandInfoUpdater
    {
        private static readonly string RespCommandInfoJsonPath = "RespCommandsInfo.json";
        private static readonly string GarnetCommandInfoJsonPath = "GarnetCommandsInfo.json";

        public static bool TryUpdateCommandInfo(string outputPath, ushort localRedisPort, IPAddress localRedisHost, IEnumerable<string> ignoreCommands, ILogger logger)
        {
            logger.LogInformation("Attempting to update RESP commands info...");

            if (!TryGetRespCommandsInfo(RespCommandInfoJsonPath, logger, out var existingCommandsInfo))
            {
                logger.LogError($"Unable to read existing RESP commands info from {RespCommandInfoJsonPath}.");
                return false;
            }

            var (commandsToAdd, commandsToRemove) =
                GetCommandsToAddAndRemove(existingCommandsInfo, ignoreCommands);

            if (!GetUserConfirmation(existingCommandsInfo, commandsToAdd, commandsToRemove, logger))
            {
                logger.LogInformation($"User cancelled update operation.");
                return false;
            }

            if (!TryGetRespCommandsInfo(GarnetCommandInfoJsonPath, logger, out var garnetCommandsInfo))
            {
                logger.LogError($"Unable to read Garnet RESP commands info from {GarnetCommandInfoJsonPath}.");
                return false;
            }

            IDictionary<string, RespCommandsInfo> additionalCommandsInfo;
            IDictionary<string, RespCommandsInfo> queriedCommandsInfo = default;
            var commandsToQuery = commandsToAdd.Select(c => c.Key.Command).Except(garnetCommandsInfo.Keys).ToArray();
            if (commandsToQuery.Length > 0 && !TryGetCommandsInfo(commandsToQuery, localRedisPort, localRedisHost, logger, out queriedCommandsInfo))
            {
                logger.LogError("Unable to get RESP command info from local Redis server.");
                return false;
            }

            additionalCommandsInfo =
                (queriedCommandsInfo == null
                    ? garnetCommandsInfo
                    : queriedCommandsInfo.UnionBy(garnetCommandsInfo, kvp => kvp.Key))
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

            var updatedCommandsInfo = GetUpdatedCommandsInfo(existingCommandsInfo, commandsToAdd, commandsToRemove, additionalCommandsInfo);

            if (!TryWriteRespCommandsInfo(outputPath, updatedCommandsInfo, logger))
            {
                logger.LogError($"Unable to write RESP commands info to path {outputPath}.");
                return false;
            }

            logger.LogInformation($"RESP commands info updated successfully! Output file written to: {Path.GetFullPath(outputPath)}");
            return true;
        }

        private static IReadOnlyDictionary<string, RespCommandsInfo> GetUpdatedCommandsInfo(IReadOnlyDictionary<string, RespCommandsInfo> existingCommandsInfo, IDictionary<SupportedCommand, bool> commandsToAdd,
            IDictionary<SupportedCommand, bool> commandsToRemove, IDictionary<string, RespCommandsInfo> queriedCommandsInfo)
        {
            var updatedCommands = new HashSet<string>(commandsToAdd.Keys.Union(commandsToRemove.Keys).Select(c => c.Command));

            var updatedCommandsInfo = existingCommandsInfo
                .Where(existingCommand => !updatedCommands.Contains(existingCommand.Key))
                .ToDictionary(existingCommand => existingCommand.Key, existingCommand => existingCommand.Value);

                foreach (var command in commandsToRemove.Where(kvp => !kvp.Value).Select(kvp => kvp.Key))
            {
                var existingSubCommands = existingCommandsInfo[command.Command].SubCommands == null ? null
                    : existingCommandsInfo[command.Command].SubCommands.Select(sc => sc.Name).ToArray();
                var remainingSubCommands = existingSubCommands == null ? null :
                    command.SubCommands == null ? existingSubCommands :
                    existingSubCommands.Except(command.SubCommands).ToArray();
                var existingCommand = existingCommandsInfo[command.Command];
                var updatedCommand = new RespCommandsInfo
                {
                    Command = existingCommand.Command,
                    ArrayCommand = existingCommand.ArrayCommand,
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
                        ? null : existingCommand.SubCommands.Where(sc => remainingSubCommands.Contains(sc.Name)).ToArray()
                };

                updatedCommandsInfo.Add(updatedCommand.Name, updatedCommand);
            }

            foreach (var command in commandsToAdd.Keys)
            {
                RespCommandsInfo baseCommand;
                List<RespCommandsInfo>? updatedSubCommands;
                if (existingCommandsInfo.ContainsKey(command.Command))
                {
                    updatedSubCommands = existingCommandsInfo[command.Command].SubCommands == null
                        ? new List<RespCommandsInfo>()
                        : existingCommandsInfo[command.Command].SubCommands.ToList();

                    foreach (var subCommandToAdd in command.SubCommands!)
                    {
                        updatedSubCommands.Add(queriedCommandsInfo[subCommandToAdd]);
                    }

                    baseCommand = existingCommandsInfo[command.Command];
                }
                else
                {
                    baseCommand = queriedCommandsInfo[command.Command];
                    updatedSubCommands = command.SubCommands == null ? null
                        : baseCommand.SubCommands.Where(sc => command.SubCommands.Contains(sc.Name)).ToList();
                }

                var updatedCommand = new RespCommandsInfo
                {
                    Command = baseCommand.Command,
                    ArrayCommand = baseCommand.ArrayCommand,
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

        private static bool GetUserConfirmation(IReadOnlyDictionary<string, RespCommandsInfo> existingCommandsInfo, IDictionary<SupportedCommand, bool> commandsToAdd, IDictionary<SupportedCommand, bool> commandsToRemove, ILogger logger)
        {
            var logCommandsToAdd = commandsToAdd.Where(kvp => kvp.Value).Select(c => c.Key.Command).ToList();
            var logSubCommandsToAdd = commandsToAdd.Where(c => c.Key.SubCommands != null).SelectMany(c => c.Key.SubCommands!).ToList();
            var logCommandsToRemove = commandsToRemove.Where(kvp => kvp.Value).Select(c => c.Key.Command).ToList();
            var logSubCommandsToRemove = commandsToRemove.Where(c => c.Key.SubCommands != null).SelectMany(c => c.Key.SubCommands!).ToList();

            logger.LogInformation($"Found {logCommandsToAdd.Count} commands to add and {logSubCommandsToAdd.Count} sub-commands to add.");
            if (logCommandsToAdd.Count > 0)
                logger.LogInformation($"Commands to add: {string.Join(", ", logCommandsToAdd)}");
            if (logSubCommandsToAdd.Count > 0)
                logger.LogInformation($"Sub-Commands to add: {string.Join(", ", logSubCommandsToAdd)}");
            logger.LogInformation($"Found {logCommandsToRemove.Count} commands to remove and {logSubCommandsToRemove.Count} sub-commands to commandsToRemove.");
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

        private static unsafe bool TryGetCommandsInfo(string[] commandsToQuery, ushort localRedisPort, IPAddress localRedisHost, ILogger logger, out IDictionary<string, RespCommandsInfo> commandsInfo)
        {
            commandsInfo = default;
            if (commandsToQuery.Length == 0) return true;

            byte[] response;
            try
            {
                var lightClient = new LightClientRequest(localRedisHost.ToString(), localRedisPort, 0);
                response = lightClient.SendCommand($"COMMAND INFO {string.Join(' ', commandsToQuery)}");
            }
            catch (Exception e)
            {
                logger.LogError(e, "Encountered an error while querying local Redis server");
                return false;
            }

            var tmpCommandsInfo = new Dictionary<string, RespCommandsInfo>();
            var supportedCommands = new ReadOnlyDictionary<string, (RespCommand, byte?)>(SupportedCommand.SupportedCommandsMap.ToDictionary(kvp => kvp.Key,
                kvp => (kvp.Value.RespCommand, kvp.Value.ArrayCommand), StringComparer.OrdinalIgnoreCase));

            fixed (byte* respPtr = response)
            {
                var ptr = (byte*)Unsafe.AsPointer(ref respPtr[0]);
                var end = ptr + response.Length;
                RespReadUtils.ReadArrayLength(out var cmdCount, ref ptr, end);

                for (var cmdIdx = 0; cmdIdx < cmdCount; cmdIdx++)
                {
                    if (!RespCommandInfoParser.TryReadFromResp(ref ptr, end, supportedCommands, out RespCommandsInfo command) ||
                        command == null)
                    {
                        logger.LogError($"Unable to read RESP command info from Redis for command {commandsToQuery[cmdIdx]}");
                        return false;
                    }
                    tmpCommandsInfo.Add(command.Name, command);
                }
            }

            commandsInfo = tmpCommandsInfo;
            return true;
        }

        private static (IDictionary<SupportedCommand, bool>, IDictionary<SupportedCommand, bool>) GetCommandsToAddAndRemove(IReadOnlyDictionary<string, RespCommandsInfo> existingCommandsInfo, IEnumerable<string> ignoreCommands)
        {
            var commandsToAdd = new Dictionary<SupportedCommand, bool>();
            var commandsToRemove = new Dictionary<SupportedCommand, bool>();
            var commandsToIgnore = new HashSet<string>(ignoreCommands);

            var supportedCommands = SupportedCommand.SupportedCommandsMap;

            foreach (var supportedCommand in supportedCommands.Values)
            {
                if (commandsToIgnore.Contains(supportedCommand.Command)) continue;

                if (!existingCommandsInfo.ContainsKey(supportedCommand.Command))
                {
                    commandsToAdd.Add(supportedCommand, true);
                    continue;
                }

                if (supportedCommand.SubCommands == null) continue;

                string[] subCommandsToAdd;
                if (existingCommandsInfo[supportedCommand.Command].SubCommands == null)
                {
                    subCommandsToAdd = supportedCommand.SubCommands.ToArray();
                }
                else
                {
                    var existingSubCommands = new HashSet<string>(existingCommandsInfo[supportedCommand.Command].SubCommands
                            .Select(sc => sc.Name));
                    subCommandsToAdd = supportedCommand.SubCommands
                        .Where(subCommand => !existingSubCommands.Contains(subCommand)).Select(sc => sc).ToArray();
                }

                if (subCommandsToAdd.Length > 0)
                {
                    commandsToAdd.Add(new SupportedCommand(supportedCommand.Command, supportedCommand.RespCommand, supportedCommand.ArrayCommand, subCommandsToAdd), false);
                }
            }

            foreach (var existingCommand in existingCommandsInfo)
            {
                var existingSubCommands = existingCommand.Value.SubCommands;

                if (!supportedCommands.ContainsKey(existingCommand.Key))
                {
                    commandsToRemove.Add(new SupportedCommand(existingCommand.Key), true);
                    continue;
                }

                if (existingSubCommands == null) continue;

                var subCommandsToRemove = (supportedCommands[existingCommand.Key].SubCommands == null
                    ? existingSubCommands : existingSubCommands.Where(sc =>
                        !supportedCommands[existingCommand.Key].SubCommands!.Contains(sc.Name))).Select(sc => sc.Name).ToArray();

                if (subCommandsToRemove.Length > 0)
                {
                    commandsToRemove.Add(new SupportedCommand(existingCommand.Key, existingCommand.Value.Command, existingCommand.Value.ArrayCommand, subCommandsToRemove), false);
                }
            }

            return (commandsToAdd, commandsToRemove);
        }

        private static bool TryGetRespCommandsInfo(string resourcePath, ILogger logger, out IReadOnlyDictionary<string, RespCommandsInfo> commandsInfo)
        {
            commandsInfo = default;

            var streamProvider = StreamProviderFactory.GetStreamProvider(FileLocationType.EmbeddedResource, null, Assembly.GetExecutingAssembly());
            var commandsInfoProvider = RespCommandsInfoProviderFactory.GetRespCommandsInfoProvider();

            var importSucceeded = commandsInfoProvider.TryImportRespCommandsInfo(resourcePath,
                streamProvider, logger, out var tmpCommandsInfo);

            if (!importSucceeded) return false;

            commandsInfo = tmpCommandsInfo;
            return true;
        }

        private static bool TryWriteRespCommandsInfo(string outputPath, IReadOnlyDictionary<string, RespCommandsInfo> commandsInfo, ILogger logger)
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
