// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.Json.Serialization;
using Garnet.common;
using Garnet.server.Resp;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Represents a RESP command's docs
    /// </summary>
    public class RespCommandDocs : IRespSerializable, IRespCommandData<RespCommandDocs>
    {
        /// <inheritdoc />
        public RespCommand Command { get; init; }

        /// <inheritdoc />
        public string Name { get; init; }

        /// <summary>
        /// Short command description
        /// </summary>
        public string Summary { get; init; }

        /// <summary>
        /// The functional group to which the command belong
        /// </summary>
        public RespCommandGroup Group { get; init; }

        /// <summary>
        /// A short explanation about the command's time complexity
        /// </summary>
        public string Complexity { get; init; }

        /// <summary>
        /// Documentation flags
        /// </summary>
        public RespCommandDocFlags DocFlags
        {
            get => docFlags;
            init
            {
                docFlags = value;
                respFormatDocFlags = EnumUtils.GetEnumDescriptions(docFlags);
            }
        }

        /// <summary>
        /// The alternative for a deprecated command
        /// </summary>
        public string ReplacedBy { get; init; }

        /// <summary>
        /// The command's arguments
        /// </summary>
        public RespCommandDocs[] SubCommands { get; init; }

        /// <inheritdoc />
        [JsonIgnore]
        public RespCommandDocs Parent { get; set; }

        /// <summary>
        /// The command's arguments
        /// </summary>
        public RespCommandArgumentBase[] Arguments { get; init; }

        private const string RespCommandsDocsEmbeddedFileName = @"RespCommandsDocs.json";

        private readonly RespCommandDocFlags docFlags;
        private readonly string[] respFormatDocFlags;

        private static bool IsInitialized = false;
        private static readonly object IsInitializedLock = new();
        private static IReadOnlyDictionary<string, RespCommandDocs> AllRespCommandsDocs = null;
        private static IReadOnlyDictionary<string, RespCommandDocs> AllRespSubCommandsDocs = null;
        private static IReadOnlyDictionary<string, RespCommandDocs> ExternalRespCommandsDocs = null;
        private static IReadOnlyDictionary<string, RespCommandDocs> ExternalRespSubCommandsDocs = null;

        public RespCommandDocs(RespCommand command, string name, string summary, RespCommandGroup group, string complexity,
            RespCommandDocFlags docFlags, string replacedBy, RespCommandArgumentBase[] args, RespCommandDocs[] subCommands) : this()
        {
            Command = command;
            Name = name;
            Summary = summary;
            Group = group;
            Complexity = complexity;
            DocFlags = docFlags;
            ReplacedBy = replacedBy;
            Arguments = args;
            SubCommands = subCommands;
        }

        /// <summary>
        /// Empty constructor for JSON deserialization
        /// </summary>
        public RespCommandDocs()
        {

        }

        private static bool TryInitialize(ILogger logger)
        {
            lock (IsInitializedLock)
            {
                if (IsInitialized) return true;

                IsInitialized = TryInitializeRespCommandsDocs(logger);
                return IsInitialized;
            }
        }

        private static bool TryInitializeRespCommandsDocs(ILogger logger = null)
        {
            var importSucceeded = RespCommandDataCommon.TryImportRespCommandsData<RespCommandDocs>(
                RespCommandsDocsEmbeddedFileName,
                out var tmpAllRespCommandsDocs, logger);

            if (!importSucceeded) return false;

            if (!RespCommandsInfo.TryGetRespCommandNames(out var allExternalCommands, true, logger))
                return false;

            var tmpAllSubCommandsDocs = new Dictionary<string, RespCommandDocs>(StringComparer.OrdinalIgnoreCase);
            var tmpExternalSubCommandsDocs = new Dictionary<string, RespCommandDocs>(StringComparer.OrdinalIgnoreCase);
            foreach (var kvp in tmpAllRespCommandsDocs)
            {
                if (kvp.Value.SubCommands == null) continue;

                foreach (var sc in kvp.Value.SubCommands)
                {
                    tmpAllSubCommandsDocs.Add(sc.Name, sc);

                    // If parent command or sub-subcommand info mark the command as internal,
                    // don't add it to the external sub-command map
                    if (!RespCommandsInfo.TryGetRespCommandInfo(sc.Command, out var subCmdInfo) ||
                        subCmdInfo.IsInternal || subCmdInfo.Parent.IsInternal)
                        continue;

                    tmpExternalSubCommandsDocs.Add(sc.Name, sc);
                }
            }

            AllRespCommandsDocs = tmpAllRespCommandsDocs.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
            AllRespSubCommandsDocs = tmpAllSubCommandsDocs.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
            ExternalRespCommandsDocs = tmpAllRespCommandsDocs
                .Where(ci => allExternalCommands.Contains(ci.Key))
                .ToFrozenDictionary(kvp => kvp.Key, kvp => kvp.Value, StringComparer.OrdinalIgnoreCase);
            ExternalRespSubCommandsDocs = tmpExternalSubCommandsDocs.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);

            return true;
        }

        /// <summary>
        /// Gets all the command docs objects of commands supported by Garnet
        /// </summary>
        /// <param name="respCommandsDocs">Mapping between command name to command docs</param>
        /// <param name="externalOnly">Return only commands that are visible externally</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if initialization was successful and data was retrieved successfully</returns>
        public static bool TryGetRespCommandsDocs(out IReadOnlyDictionary<string, RespCommandDocs> respCommandsDocs, bool externalOnly = false, ILogger logger = null)
        {
            respCommandsDocs = default;
            if (!IsInitialized && !TryInitialize(logger)) return false;

            respCommandsDocs = externalOnly ? ExternalRespCommandsDocs : AllRespCommandsDocs;
            return true;
        }

        /// <summary>
        /// Gets command docs by command name
        /// </summary>
        /// <param name="cmdName">The command name</param>
        /// <param name="respCommandsDocs">The command docs</param>
        /// <param name="externalOnly">Return command docs only if command is visible externally</param>
        /// <param name="includeSubCommands">Include sub-commands in command name search</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if initialization was successful and command docs was found</returns>
        internal static bool TryGetRespCommandDocs(string cmdName, out RespCommandDocs respCommandsDocs, bool externalOnly = false, bool includeSubCommands = false, ILogger logger = null)
        {
            respCommandsDocs = default;

            return (TryGetRespCommandsDocs(out var cmdsDocs, externalOnly, logger)
                    && cmdsDocs.TryGetValue(cmdName, out respCommandsDocs))
                   || (includeSubCommands && TryGetRespSubCommandsDocs(out var subCmdsDocs, externalOnly, logger)
                       && subCmdsDocs.TryGetValue(cmdName, out respCommandsDocs));
        }

        /// <summary>
        /// Gets all the command docs of sub-commands supported by Garnet
        /// </summary>
        /// <param name="respSubCommandsDocs">Mapping between sub-command name to command docs</param>
        /// <param name="externalOnly">Return only sub-commands that are visible externally</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if initialization was successful and data was retrieved successfully</returns>
        public static bool TryGetRespSubCommandsDocs(out IReadOnlyDictionary<string, RespCommandDocs> respSubCommandsDocs, bool externalOnly = false, ILogger logger = null)
        {
            respSubCommandsDocs = default;
            if (!IsInitialized && !TryInitialize(logger)) return false;

            respSubCommandsDocs = externalOnly ? ExternalRespSubCommandsDocs : AllRespSubCommandsDocs;
            return true;
        }

        /// <inheritdoc />
        public void ToRespFormat(ref RespMemoryWriter writer)
        {
            var argCount = 1; // group

            if (Summary != null)
                argCount++;

            if (Complexity != null)
                argCount++;

            if (DocFlags != RespCommandDocFlags.None)
                argCount++;

            if (ReplacedBy != null)
                argCount++;

            if (Arguments != null)
                argCount++;

            if (SubCommands != null)
                argCount++;

            writer.WriteAsciiBulkString(Name);
            writer.WriteMapLength(argCount);

            if (Summary != null)
            {
                writer.WriteBulkString("summary"u8);
                writer.WriteAsciiBulkString(Summary);
            }

            writer.WriteBulkString("group"u8);
            var respType = EnumUtils.GetEnumDescriptions(Group)[0];
            writer.WriteAsciiBulkString(respType);

            if (Complexity != null)
            {
                writer.WriteBulkString("complexity"u8);
                writer.WriteAsciiBulkString(Complexity);
            }

            if (DocFlags != RespCommandDocFlags.None)
            {
                writer.WriteBulkString("doc_flags"u8);
                writer.WriteSetLength(respFormatDocFlags.Length);
                foreach (var respDocFlag in respFormatDocFlags)
                {
                    writer.WriteSimpleString(respDocFlag);
                }
            }

            if (ReplacedBy != null)
            {
                writer.WriteBulkString("replaced_by"u8);
                writer.WriteAsciiBulkString(ReplacedBy);
            }

            if (Arguments != null)
            {
                writer.WriteBulkString("arguments"u8);
                writer.WriteArrayLength(Arguments.Length);
                foreach (var argument in Arguments)
                {
                    argument.ToRespFormat(ref writer);
                }
            }

            if (SubCommands != null)
            {
                writer.WriteBulkString("subcommands"u8);
                writer.WriteMapLength(SubCommands.Length);
                foreach (var subCommand in SubCommands)
                {
                    subCommand.ToRespFormat(ref writer);
                }
            }
        }
    }

    /// <summary>
    /// Enum representing the functional group to which the command belongs
    /// </summary>
    public enum RespCommandGroup : byte
    {
        None,
        [Description("bitmap")]
        Bitmap,
        [Description("cluster")]
        Cluster,
        [Description("connection")]
        Connection,
        [Description("generic")]
        Generic,
        [Description("geo")]
        Geo,
        [Description("hash")]
        Hash,
        [Description("hyperloglog")]
        HyperLogLog,
        [Description("list")]
        List,
        [Description("module")]
        Module,
        [Description("pubsub")]
        PubSub,
        [Description("scripting")]
        Scripting,
        [Description("sentinel")]
        Sentinel,
        [Description("server")]
        Server,
        [Description("set")]
        Set,
        [Description("sorted-set")]
        SortedSet,
        [Description("stream")]
        Stream,
        [Description("string")]
        String,
        [Description("transactions")]
        Transactions,
        [Description("vector")]
        Vector,
    }

    /// <summary>
    /// Documentation flags
    /// </summary>
    [Flags]
    public enum RespCommandDocFlags : byte
    {
        None = 0,
        /// <summary>
        /// The command is deprecated
        /// </summary>
        [Description("deprecated")]
        Deprecated = 1,

        /// <summary>
        /// A system command that isn't meant to be called by users
        /// </summary>
        [Description("syscmd")]
        SysCmd = 1 << 1,
    }
}