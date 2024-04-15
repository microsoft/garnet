// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Garnet;
using Garnet.common;
using Garnet.server;
using Garnet.test;
using Microsoft.Extensions.Logging;

namespace CommandInfoUpdater
{
    public class CommandInfoUpdater
    {
        private static IPAddress LocalRedisHost = IPAddress.Loopback;
        private static ushort LocalRedisPort = 6379;
        private static string RespCommandInfoJsonPath = "RespCommandsInfo.json";
        private static string SupportedCommandsJsonPath = "SupportedCommands.json";

        public static bool TryUpdateCommandInfo(ILogger logger)
        {
            if (!TryGetRespCommandsInfo(RespCommandInfoJsonPath, logger, out var existingCommandsInfo))
                return false;

            if (!TryGetSupportedCommands(SupportedCommandsJsonPath, logger, out var supportedCommands))
                return false;

            var (commandsToAdd, commandsToRemove) =
                GetCommandsToAddAndRemove(existingCommandsInfo, supportedCommands);

            if (!GetUserConfirmation(existingCommandsInfo, commandsToAdd, commandsToRemove, logger))
                return false;

            if (!TryGetCommandsInfo(commandsToAdd.Select(c => c.Command).ToArray(), logger, out var queriedCommandsInfo))
                return false;

            var updatedCommandsInfo = GetUpdatedCommandsInfo(existingCommandsInfo, commandsToAdd, commandsToRemove, queriedCommandsInfo);

            return true;
        }

        private static IDictionary<string, RespCommandsInfo> GetUpdatedCommandsInfo(IReadOnlyDictionary<string, RespCommandsInfo> existingCommandsInfo, List<SupportedCommand> commandsToAdd,
            List<SupportedCommand> commandsToRemove, IDictionary<string, RespCommandsInfo> queriedCommandsInfo)
        {
            var updatedCommands = new HashSet<string>(commandsToAdd.Union(commandsToRemove).Select(c => c.Command));

            var updatedCommandsInfo = existingCommandsInfo
                .Where(existingCommand => !updatedCommands.Contains(existingCommand.Key))
                .ToDictionary(existingCommand => existingCommand.Key, existingCommand => existingCommand.Value);

            foreach (var command in commandsToRemove)
            {
                var existingSubCommands = existingCommandsInfo[command.Command].SubCommands == null ? null : existingCommandsInfo[command.Command].SubCommands.Select(sc => sc.Name).ToArray();
                var remainingSubCommands = existingSubCommands == null ? null : command.SubCommands == null ? existingSubCommands : existingSubCommands.Except(command.SubCommands).ToArray();
                if (remainingSubCommands != null && remainingSubCommands.Length == 0)
                {
                    var existingCommand = existingCommandsInfo[command.Command];
                    var updatedCommand = new RespCommandsInfo
                    {
                        Name = existingCommand.Name,
                        Arity = existingCommand.Arity,
                        Flags = existingCommand.Flags,
                        FirstKey = existingCommand.FirstKey,
                        LastKey = existingCommand.LastKey,
                        Step = existingCommand.Step,
                        AclCategories = existingCommand.AclCategories,
                        Tips = existingCommand.Tips,
                        KeySpecifications = existingCommand.KeySpecifications,
                        SubCommands = existingCommand.SubCommands
                            .Where(sc => remainingSubCommands.Contains(sc.Name)).ToArray()
                    };

                    updatedCommandsInfo.Add(updatedCommand.Name, updatedCommand);
                }
            }

            foreach (var command in commandsToAdd)
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

        private static bool GetUserConfirmation(IReadOnlyDictionary<string, RespCommandsInfo> existingCommandsInfo, List<SupportedCommand> commandsToAdd, List<SupportedCommand> commandsToRemove, ILogger logger)
        {
            var logCommandsToAdd = commandsToAdd.Where(c => !existingCommandsInfo.ContainsKey(c.Command)).ToList();
            var logSubCommandsToAdd = commandsToAdd.Where(c => c.SubCommands != null).SelectMany(c => c.SubCommands!).ToList();
            var logCommandsToRemove = commandsToRemove.Where(c =>
                existingCommandsInfo[c.Command].SubCommands == null || (c.SubCommands != null && existingCommandsInfo[c.Command].SubCommands
                    .Select(sc => sc.Name).OrderBy(sc => sc)
                    .SequenceEqual(c.SubCommands.OrderBy(sc => sc)))).ToList();
            var logSubCommandsToRemove = commandsToRemove.Where(c => c.SubCommands != null).SelectMany(c => c.SubCommands!).ToList();

            logger.LogInformation($"Found {logCommandsToAdd.Count} commands to add and {logSubCommandsToAdd.Count} sub-commands to add:");
            if (logCommandsToAdd.Count > 0)
                logger.LogInformation($"Commands to add: {string.Join(", ", logCommandsToAdd)}");
            if (logSubCommandsToAdd.Count > 0)
                logger.LogInformation($"Sub-Commands to add: {string.Join(", ", logSubCommandsToAdd)}");
            logger.LogInformation($"Found {logCommandsToRemove.Count} commands to remove and {logSubCommandsToRemove.Count} sub-commands to commandsToRemove:");
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

        private static unsafe bool TryGetCommandsInfo(string[] commandsToQuery, ILogger logger, out IDictionary<string, RespCommandsInfo> commandsInfo)
        {
            commandsInfo = default;
            if (commandsToQuery.Length == 0) return true;

            byte[] response;
            try
            {
                var lightClient = new LightClientRequest(LocalRedisHost.ToString(), LocalRedisPort, 0);
                response = lightClient.SendCommand($"COMMAND INFO {string.Join(' ', commandsToQuery)}");
            }
            catch (Exception e)
            {
                logger.LogError(e, "Encountered an error while querying local Redis server");
                return false;
            }

            var tmpCommandsInfo = new Dictionary<string, RespCommandsInfo>();

            fixed (byte* respPtr = response)
            {
                var ptr = (byte*)Unsafe.AsPointer(ref respPtr[0]);
                var end = ptr + response.Length;
                RespReadUtils.ReadArrayLength(out var cmdCount, ref ptr, end);

                for (var cmdIdx = 0; cmdIdx < cmdCount; cmdIdx++)
                {
                    if (!RespCommandInfoParser.TryReadFromResp(ref ptr, end, out RespCommandsInfo command) ||
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

        private static (List<SupportedCommand>, List<SupportedCommand>) GetCommandsToAddAndRemove(IReadOnlyDictionary<string, RespCommandsInfo> existingCommandsInfo, Dictionary<string, SupportedCommand> supportedCommands)
        {
            var commandsToAdd = new List<SupportedCommand>();
            var commandsToRemove = new List<SupportedCommand>();

            foreach (var supportedCommand in supportedCommands.Values)
            {
                if (!existingCommandsInfo.ContainsKey(supportedCommand.Command))
                {
                    commandsToAdd.Add(supportedCommand);
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
                        .Where(subCommand => !existingSubCommands.Contains(subCommand)).ToArray();
                }

                if (subCommandsToAdd.Length > 0)
                {
                    commandsToAdd.Add(new SupportedCommand(supportedCommand.Command, subCommandsToAdd));
                }
            }

            foreach (var existingCommand in existingCommandsInfo)
            {
                if (supportedCommands[existingCommand.Key].SubCommands == null) continue;

                var existingSubCommands = existingCommand.Value.SubCommands == null
                    ? new HashSet<string>()
                    : new HashSet<string>(existingCommand.Value.SubCommands.Select(sc => sc.Name));

                if (!supportedCommands.ContainsKey(existingCommand.Key))
                {
                    commandsToRemove.Add(new SupportedCommand(existingCommand.Key, existingSubCommands.ToArray()));
                }

                var subCommandsToRemove = existingSubCommands.Where(sc =>
                    !supportedCommands[existingCommand.Key].SubCommands!.Contains(sc)).ToArray();

                if (subCommandsToRemove.Length > 0)
                {
                    commandsToRemove.Add(new SupportedCommand(existingCommand.Key, subCommandsToRemove));
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

        private static bool TryGetSupportedCommands(string resourcePath, ILogger logger, out Dictionary<string, SupportedCommand> supportedCommands)
        {
            supportedCommands = default;

            var streamProvider = StreamProviderFactory.GetStreamProvider(FileLocationType.Local);
            using var stream = streamProvider.Read(resourcePath);
            using var streamReader = new StreamReader(stream);
            var tmpSupportedCommands = new Dictionary<string, SupportedCommand>();

            try
            {
                var desSupportedCommands = JsonSerializer.Deserialize<SupportedCommand[]>(streamReader.ReadToEnd())!;
                foreach (var supportedCommand in desSupportedCommands)
                {
                    tmpSupportedCommands.Add(supportedCommand.Command, supportedCommand);
                }
            }
            catch (JsonException je)
            {
                logger.LogError(je, $"An error occurred while parsing supported commands from file (Path: {resourcePath}).");
                return false;
            }

            supportedCommands = tmpSupportedCommands;
            return true;
        }
    }

    public class SupportedCommand
    {
        public string Command { get; set; }

        public HashSet<string>? SubCommands { get; set; }

        public SupportedCommand()
        {

        }
          
        public SupportedCommand(string command, string[] subcommands)
        {
            this.Command = command;
            this.SubCommands = new HashSet<string>(subcommands);
        }
    }
}
