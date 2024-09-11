// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using System.Runtime.CompilerServices;
using Garnet.common;
using Garnet.server;
using Garnet.server.Resp;
using Microsoft.Extensions.Logging;

namespace CommandInfoUpdater
{
    public class CommandDocsUpdater
    {
        private static readonly string GarnetCommandDocsJsonPath = "GarnetCommandsDocs.json";

        /// <summary>
        /// Tries to generate an updated JSON file containing Garnet's supported commands' docs
        /// </summary>
        /// <param name="outputPath">Output path for the updated JSON file</param>
        /// <param name="respServerPort">RESP server port to query commands docs</param>
        /// <param name="respServerHost">RESP server host to query commands docs</param>
        /// <param name="commandsToRemove"></param>
        /// <param name="force">Force update all commands</param>
        /// <param name="logger">Logger</param>
        /// <param name="commandsToAdd"></param>
        /// <returns>True if file generated successfully</returns>
        public static bool TryUpdateCommandDocs(string outputPath, int respServerPort, IPAddress respServerHost, IDictionary<SupportedCommand, bool> commandsToAdd, IDictionary<SupportedCommand, bool> commandsToRemove, bool force, ILogger logger)
        {
            logger.LogInformation("Attempting to update RESP commands docs...");

            IReadOnlyDictionary<string, RespCommandDocs> existingCommandsDocs =
                new Dictionary<string, RespCommandDocs>();
            if (!force && !RespCommandsInfo.TryGetRespCommandsDocs(out existingCommandsDocs, false, logger))
            {
                logger.LogError("Unable to get existing RESP commands docs.");
                return false;
            }

            if (!DataUtils.TryGetRespCommandsData<RespCommandDocs>(GarnetCommandDocsJsonPath, logger, out var garnetCommandsDocs) ||
                garnetCommandsDocs == null)
            {
                logger.LogError("Unable to read Garnet RESP commands docs from {GarnetCommandInfoJsonPath}.", GarnetCommandDocsJsonPath);
                return false;
            }

            IDictionary<string, RespCommandDocs> queriedCommandsDocs = new Dictionary<string, RespCommandDocs>();
            var commandsToQuery = commandsToAdd.Keys.Select(k => k.Command).ToArray();
            if (commandsToQuery.Length > 0 && !TryGetCommandsDocs(commandsToQuery, respServerPort, respServerHost,
                    logger, out queriedCommandsDocs))
            {
                logger.LogError("Unable to get RESP command docs from local RESP server.");
                return false;
            }

            var additionalCommandsDocs = new Dictionary<string, RespCommandDocs>();
            foreach (var cmd in garnetCommandsDocs.Keys.Union(queriedCommandsDocs.Keys))
            {
                if (!additionalCommandsDocs.ContainsKey(cmd))
                {
                    var baseCommandDocs = queriedCommandsDocs.TryGetValue(cmd, out var doc) ? doc : garnetCommandsDocs[cmd];
                    additionalCommandsDocs.Add(cmd, new RespCommandDocs(
                        baseCommandDocs.Command, baseCommandDocs.Summary, baseCommandDocs.Group, baseCommandDocs.Complexity,
                        baseCommandDocs.DocFlags, baseCommandDocs.ReplacedBy, baseCommandDocs.Arguments,
                        queriedCommandsDocs.ContainsKey(cmd) && garnetCommandsDocs.ContainsKey(cmd) ?
                            queriedCommandsDocs[cmd].SubCommands.Union(garnetCommandsDocs[cmd].SubCommands).ToArray() :
                            baseCommandDocs.SubCommands));
                }
            }

            var updatedCommandsDocs = GetUpdatedCommandsDocs(existingCommandsDocs, commandsToAdd, commandsToRemove,
                additionalCommandsDocs);

            if (!DataUtils.TryWriteRespCommandsData(outputPath, updatedCommandsDocs, logger))
            {
                logger.LogError("Unable to write RESP commands docs to path {outputPath}.", outputPath);
                return false;
            }

            logger.LogInformation("RESP commands docs updated successfully! Output file written to: {fullOutputPath}", Path.GetFullPath(outputPath));
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
            IPAddress respServerHost, ILogger logger, out IDictionary<string, RespCommandDocs> commandDocs)
        {
            commandDocs = default;

            // If there are no commands to query, return
            if (commandsToQuery.Length == 0) return true;

            // Query the RESP server
            byte[] response;
            try
            {
                var lightClient = new LightClientRequest(respServerHost.ToString(), respServerPort, 0);
                response = lightClient.SendCommand($"COMMAND DOCS {string.Join(' ', commandsToQuery)}");
            }
            catch (Exception e)
            {
                logger.LogError(e, "Encountered an error while querying local RESP server");
                return false;
            }

            var tmpCommandsDocs = new Dictionary<string, RespCommandDocs>();

            // Parse the response
            fixed (byte* respPtr = response)
            {
                var ptr = (byte*)Unsafe.AsPointer(ref respPtr[0]);
                var end = ptr + response.Length;

                // Read the array length (# of commands docs returned)
                if (!RespReadUtils.ReadUnsignedArrayLength(out var cmdCount, ref ptr, end))
                {
                    logger.LogError("Unable to read RESP command docs count from server");
                    return false;
                }

                // Parse each command's command docs
                for (var cmdIdx = 0; cmdIdx < cmdCount; cmdIdx++)
                {
                    var currPtr = ptr;
                    if (!RespReadUtils.ReadStringWithLengthHeader(out var cmdName, ref currPtr, end) ||
                        !Enum.TryParse(cmdName, true, out RespCommand _))
                    {
                        logger.LogError("Unable to parse RESP command type for command {command}", cmdName);
                        return false;
                    }

                    if (!RespCommandDocsParser.TryReadFromResp(ref ptr, end, out var cmdDocs) ||
                        cmdDocs == null)
                    {
                        logger.LogError("Unable to read RESP command docs from server for command {command}", cmdName);
                        return false;
                    }

                    tmpCommandsDocs.Add(cmdDocs.RespCommandName, cmdDocs);
                }
            }

            commandDocs = tmpCommandsDocs;
            return true;
        }

        /// <summary>
        /// Update the mapping of commands docs
        /// </summary>
        /// <param name="existingCommandsDocs">Existing command docs mapping</param>
        /// <param name="commandsToAdd">Commands to add</param>
        /// <param name="commandsToRemove">Commands to remove</param>
        /// <param name="queriedCommandsDocs">Queried commands docs</param>
        /// <returns></returns>
        private static IReadOnlyDictionary<string, RespCommandDocs> GetUpdatedCommandsDocs(
            IReadOnlyDictionary<string, RespCommandDocs> existingCommandsDocs,
            IDictionary<SupportedCommand, bool> commandsToAdd,
            IDictionary<SupportedCommand, bool> commandsToRemove,
            IDictionary<string, RespCommandDocs> queriedCommandsDocs)
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
                    : existingCommandsDocs[command.Command].SubCommands.Select(sc => sc.RespCommandName).ToArray();
                var remainingSubCommands = existingSubCommands == null ? null :
                    command.SubCommands == null ? existingSubCommands :
                    existingSubCommands.Except(command.SubCommands).ToArray();

                // Create updated command docs based on existing command
                var existingCommandDoc = existingCommandsDocs[command.Command];
                var updatedCommandDoc = new RespCommandDocs(
                    existingCommandDoc.Command,
                    existingCommandDoc.Summary,
                    existingCommandDoc.Group,
                    existingCommandDoc.Complexity,
                    existingCommandDoc.DocFlags,
                    existingCommandDoc.ReplacedBy,
                    existingCommandDoc.Arguments,
                    remainingSubCommands == null || remainingSubCommands.Length == 0
                        ? null
                        : existingCommandDoc.SubCommands.Where(sc => remainingSubCommands.Contains(sc.RespCommandName)).ToArray());

                updatedCommandsDocs.Add(updatedCommandDoc.RespCommandName, updatedCommandDoc);
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
                            .First(sc => sc.RespCommandName == subCommandToAdd));
                    }

                    // Set base command as existing sub-command
                    baseCommandDocs = existingCommandsDocs[command.Command];
                }
                // If parent command does not exist
                else
                {
                    // Set base command as queried command
                    baseCommandDocs = queriedCommandsDocs[command.Command];

                    // Update sub-commands to contain supported sub-commands only
                    updatedSubCommandsDocs = command.SubCommands == null
                        ? null
                        : baseCommandDocs.SubCommands.Where(sc => command.SubCommands.Contains(sc.RespCommandName)).ToList();
                }

                // Create updated command docs based on base command & updated sub-commands
                var updatedCommandDocs = new RespCommandDocs(
                    baseCommandDocs.Command,
                    baseCommandDocs.Summary,
                    baseCommandDocs.Group,
                    baseCommandDocs.Complexity,
                    baseCommandDocs.DocFlags,
                    baseCommandDocs.ReplacedBy,
                    baseCommandDocs.Arguments,
                    updatedSubCommandsDocs?.ToArray());

                updatedCommandsDocs.Add(updatedCommandDocs.RespCommandName, updatedCommandDocs);
            }

            return updatedCommandsDocs;
        }
    }
}
