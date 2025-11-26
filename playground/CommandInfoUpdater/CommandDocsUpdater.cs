// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.ObjectModel;
using System.Net;
using Garnet.server;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace CommandInfoUpdater
{
    public class CommandDocsUpdater
    {
        const int QUERY_CMD_BATCH_SIZE = 25;
        private static readonly string CommandDocsFileName = "RespCommandsDocs.json";
        private static readonly string GarnetCommandDocsJsonPath = "GarnetCommandsDocs.json";

        /// <summary>
        /// Tries to generate an updated JSON file containing Garnet's supported commands' docs
        /// </summary>
        /// <param name="outputDir">Output directory for the updated JSON file</param>
        /// <param name="respServerPort">RESP server port to query commands docs</param>
        /// <param name="respServerHost">RESP server host to query commands docs</param>
        /// <param name="ignoreCommands">Commands to ignore</param>
        /// <param name="updatedCommandsInfo">Updated command info data</param>
        /// <param name="force">Force update all commands</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if file generated successfully</returns>
        public static bool TryUpdateCommandDocs(string outputDir, int respServerPort, IPAddress respServerHost,
            IEnumerable<string> ignoreCommands, IReadOnlyDictionary<string, RespCommandsInfo> updatedCommandsInfo, bool force, ILogger logger)
        {
            logger.LogInformation("Attempting to update RESP commands docs...");

            IReadOnlyDictionary<string, RespCommandDocs> existingCommandsDocs =
                new Dictionary<string, RespCommandDocs>();
            if (!force && !RespCommandDocs.TryGetRespCommandsDocs(out existingCommandsDocs, false, logger))
            {
                logger.LogError("Unable to get existing RESP commands docs.");
                return false;
            }

            var internalSubCommands = new Dictionary<string, HashSet<string>>();
            foreach (var ci in updatedCommandsInfo)
            {
                if (ci.Value.SubCommands == null) continue;
                var internalSubCmds =
                    ci.Value.SubCommands.Where(sc => sc.IsInternal).Select(sc => sc.Name).ToHashSet();

                if (internalSubCmds.Count > 0)
                    internalSubCommands.Add(ci.Key, internalSubCmds);
            }

            var (commandsToAdd, commandsToRemove) =
                CommonUtils.GetCommandsToAddAndRemove(existingCommandsDocs, ignoreCommands, internalSubCommands);

            if (!CommonUtils.GetUserConfirmation(commandsToAdd, commandsToRemove, logger))
            {
                logger.LogInformation("User cancelled update operation.");
                return false;
            }

            if (!CommonUtils.TryGetRespCommandsData<RespCommandDocs>(GarnetCommandDocsJsonPath, logger,
                    out var garnetCommandsDocs) ||
                garnetCommandsDocs == null)
            {
                logger.LogError("Unable to read Garnet RESP commands docs from {GarnetCommandInfoJsonPath}.",
                    GarnetCommandDocsJsonPath);
                return false;
            }

            IDictionary<string, RespCommandDocs> queriedCommandsDocs = new Dictionary<string, RespCommandDocs>();
            var commandsToQuery = commandsToAdd.Keys.Select(k => k.Command)
                .Where(c => updatedCommandsInfo.ContainsKey(c) || (updatedCommandsInfo[c].SubCommands?.Length > 0 && !updatedCommandsInfo[c].IsInternal)).ToArray();
            if (commandsToQuery.Length > 0)
            {
                for (var i = 0; i < commandsToQuery.Length; i += QUERY_CMD_BATCH_SIZE)
                {
                    var batchToQuery = commandsToQuery.Skip(i).Take(QUERY_CMD_BATCH_SIZE).ToArray();
                    if (!TryGetCommandsDocs(batchToQuery, respServerPort, respServerHost,
                            logger, ref queriedCommandsDocs))
                    {
                        logger.LogError("Unable to get RESP command info from local RESP server.");
                        return false;
                    }
                }
            }

            var additionalCommandsDocs = new Dictionary<string, RespCommandDocs>();
            foreach (var cmd in garnetCommandsDocs.Keys.Union(queriedCommandsDocs.Keys))
            {
                if (!additionalCommandsDocs.ContainsKey(cmd))
                {
                    var inQueried = queriedCommandsDocs.TryGetValue(cmd, out var queriedCommandDocs);
                    var inGarnet = garnetCommandsDocs.TryGetValue(cmd, out var garnetCommandDocs);
                    var baseCommandDocs = inGarnet ? garnetCommandDocs : queriedCommandDocs;

                    RespCommandDocs[] subCommandsDocs;
                    if (inQueried && inGarnet)
                    {
                        var subCommandsInfoMap = new Dictionary<string, RespCommandDocs>();

                        if (garnetCommandDocs.SubCommands != null)
                        {
                            foreach (var sc in garnetCommandDocs.SubCommands)
                                subCommandsInfoMap.Add(sc.Name, sc);
                        }

                        if (queriedCommandDocs.SubCommands != null)
                        {
                            foreach (var sc in queriedCommandDocs.SubCommands)
                            {
                                subCommandsInfoMap.TryAdd(sc.Name, sc);
                            }
                        }

                        subCommandsDocs = [.. subCommandsInfoMap.Values];
                    }
                    else
                    {
                        subCommandsDocs = baseCommandDocs.SubCommands;
                    }

                    additionalCommandsDocs.Add(cmd, new RespCommandDocs(
                        baseCommandDocs.Command, baseCommandDocs.Name, baseCommandDocs.Summary, baseCommandDocs.Group,
                        baseCommandDocs.Complexity,
                        baseCommandDocs.DocFlags, baseCommandDocs.ReplacedBy, baseCommandDocs.Arguments,
                        subCommandsDocs));
                }
            }

            var updatedCommandsDocs = GetUpdatedCommandsDocs(existingCommandsDocs, commandsToAdd, commandsToRemove,
                additionalCommandsDocs, updatedCommandsInfo);

            var outputPath = Path.Combine(outputDir ?? string.Empty, CommandDocsFileName);
            if (!CommonUtils.TryWriteRespCommandsData(outputPath, updatedCommandsDocs, logger))
            {
                logger.LogError("Unable to write RESP commands docs to path {outputPath}.", outputPath);
                return false;
            }

            logger.LogInformation("RESP commands docs updated successfully! Output file written to: {fullOutputPath}",
                Path.GetFullPath(outputPath));
            return true;
        }

        /// <summary>
        /// Query RESP server to get missing commands' docs
        /// </summary>
        /// <param name="commandsToQuery">Command to query</param>
        /// <param name="respServerPort">RESP server port to query</param>
        /// <param name="respServerHost">RESP server host to query</param>
        /// <param name="logger">Logger</param>
        /// <param name="commandDocs">Queried commands docs</param>
        /// <returns>True if succeeded</returns>
        private static unsafe bool TryGetCommandsDocs(string[] commandsToQuery, int respServerPort,
            IPAddress respServerHost, ILogger logger, ref IDictionary<string, RespCommandDocs> commandDocs)
        {
            // If there are no commands to query, return
            if (commandsToQuery.Length == 0) return true;

            // Get a map of supported commands to Garnet's RespCommand & ArrayCommand for the parser
            var supportedCommands = new ReadOnlyDictionary<string, RespCommand>(
                SupportedCommand.SupportedCommandsFlattenedMap.ToDictionary(kvp => kvp.Key,
                    kvp => kvp.Value.RespCommand, StringComparer.OrdinalIgnoreCase));

            var configOptions = new ConfigurationOptions()
            {
                EndPoints = new EndPointCollection(new List<EndPoint>
                {
                    new IPEndPoint(respServerHost, respServerPort)
                })
            };

            using var redis = ConnectionMultiplexer.Connect(configOptions);
            var db = redis.GetDatabase(0);

            var cmdArgs = new List<object> { "DOCS" }.Union(commandsToQuery).ToArray();
            var result = db.Execute("COMMAND", cmdArgs);
            var elemCount = result.Length;
            for (var i = 0; i < elemCount; i += 2)
            {
                if (!RespCommandDocsParser.TryReadFromResp(result, i, supportedCommands, out var cmdDocs, out var cmdName) || cmdDocs == null)
                {
                    logger.LogError("Unable to read RESP command docs from server for command {command}",
                        cmdName);
                    return false;
                }

                commandDocs.Add(cmdName, cmdDocs);
            }

            return true;
        }

        /// <summary>
        /// Update the mapping of commands docs
        /// </summary>
        /// <param name="existingCommandsDocs">Existing command docs mapping</param>
        /// <param name="commandsToAdd">Commands to add</param>
        /// <param name="commandsToRemove">Commands to remove</param>
        /// <param name="queriedCommandsDocs">Queried commands docs</param>
        /// <param name="updatedCommandsInfo">Updated commands info</param>
        /// <returns></returns>
        private static IReadOnlyDictionary<string, RespCommandDocs> GetUpdatedCommandsDocs(
            IReadOnlyDictionary<string, RespCommandDocs> existingCommandsDocs,
            IDictionary<SupportedCommand, bool> commandsToAdd,
            IDictionary<SupportedCommand, bool> commandsToRemove,
            IDictionary<string, RespCommandDocs> queriedCommandsDocs,
            IReadOnlyDictionary<string, RespCommandsInfo> updatedCommandsInfo)
        {
            // Define updated commands as commands to add unified with commands to remove
            var updatedCommands =
                new HashSet<string>(commandsToAdd.Keys.Union(commandsToRemove.Keys).Select(c => c.Command));

            // Preserve command docs for all commands that have not been updated
            var updatedCommandsDocs = existingCommandsDocs
                .Where(existingCommand => !updatedCommands.Contains(existingCommand.Key))
                .ToDictionary(existingCommand => existingCommand.Key, existingCommand => existingCommand.Value);

            // Update commands docs with commands to remove
            foreach (var command in commandsToRemove.Where(kvp => !kvp.Value).Select(kvp => kvp.Key))
            {
                // Determine updated sub-commands by subtracting from existing sub-commands
                var existingSubCommands = existingCommandsDocs[command.Command].SubCommands == null
                    ? null
                    : existingCommandsDocs[command.Command].SubCommands.Select(sc => sc.Name).ToArray();
                var remainingSubCommands = existingSubCommands == null ? null :
                    command.SubCommands == null ? existingSubCommands :
                    [.. existingSubCommands.Except(command.SubCommands.Keys)];

                // Create updated command docs based on existing command
                var existingCommandDoc = existingCommandsDocs[command.Command];
                var updatedCommandDoc = new RespCommandDocs(
                    existingCommandDoc.Command,
                    existingCommandDoc.Name,
                    existingCommandDoc.Summary,
                    existingCommandDoc.Group,
                    existingCommandDoc.Complexity,
                    existingCommandDoc.DocFlags,
                    existingCommandDoc.ReplacedBy,
                    existingCommandDoc.Arguments,
                    remainingSubCommands == null || remainingSubCommands.Length == 0
                        ? null
                        : [.. existingCommandDoc.SubCommands.Where(sc => remainingSubCommands.Contains(sc.Name))]);

                updatedCommandsDocs.Add(updatedCommandDoc.Name, updatedCommandDoc);
            }

            // Update commands docs with commands to add
            foreach (var command in commandsToAdd.Keys)
            {
                RespCommandDocs baseCommandDocs;
                List<RespCommandDocs> updatedSubCommandsDocs;
                // If parent command already exists
                if (existingCommandsDocs.ContainsKey(command.Command))
                {
                    updatedSubCommandsDocs = existingCommandsDocs[command.Command].SubCommands == null
                        ? new List<RespCommandDocs>()
                        : [.. existingCommandsDocs[command.Command].SubCommands];

                    // Add sub-commands with updated queried command docs
                    foreach (var subCommandToAdd in command.SubCommands!)
                    {
                        updatedSubCommandsDocs.Add(queriedCommandsDocs[command.Command].SubCommands
                            .First(sc => sc.Name == subCommandToAdd.Key));
                    }

                    // Set base command as existing sub-command
                    baseCommandDocs = existingCommandsDocs[command.Command];
                }
                // If parent command does not exist
                else
                {
                    if (!queriedCommandsDocs.ContainsKey(command.Command) &&
                        updatedCommandsInfo.ContainsKey(command.Command) &&
                        updatedCommandsInfo[command.Command].IsInternal) continue;

                    // Set base command as queried command
                    baseCommandDocs = queriedCommandsDocs[command.Command];

                    // Update sub-commands to contain supported sub-commands only
                    updatedSubCommandsDocs = command.SubCommands == null
                        ? null
                        : [.. baseCommandDocs.SubCommands.Where(sc => command.SubCommands.Keys.Contains(sc.Name))];
                }

                // Create updated command docs based on base command & updated sub-commands
                var updatedCommandDocs = new RespCommandDocs(
                    baseCommandDocs.Command,
                    baseCommandDocs.Name,
                    baseCommandDocs.Summary,
                    baseCommandDocs.Group,
                    baseCommandDocs.Complexity,
                    baseCommandDocs.DocFlags,
                    baseCommandDocs.ReplacedBy,
                    baseCommandDocs.Arguments,
                    updatedSubCommandsDocs?.OrderBy(sc => sc.Name).ToArray());

                updatedCommandsDocs.Add(updatedCommandDocs.Name, updatedCommandDocs);
            }

            return updatedCommandsDocs;
        }
    }
}