// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json.Serialization;
using Garnet.common;
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

        /// <summary>
        /// Returns the serialized representation of the current object in RESP format
        /// This property returns a cached value, if exists (this value should never change after object initialization)
        /// </summary>
        [JsonIgnore]
        public string RespFormat => respFormat ??= ToRespFormat();

        private const string RespCommandsDocsEmbeddedFileName = @"RespCommandsDocs.json";

        private string respFormat;
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
            var streamProvider = StreamProviderFactory.GetStreamProvider(FileLocationType.EmbeddedResource, null,
                Assembly.GetExecutingAssembly());
            var commandsDocsProvider = RespCommandsDataProviderFactory.GetRespCommandsDataProvider<RespCommandDocs>();

            var importSucceeded = commandsDocsProvider.TryImportRespCommandsData(RespCommandsDocsEmbeddedFileName,
                streamProvider, out var tmpAllRespCommandsDocs, logger);

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

            AllRespCommandsDocs =
                new Dictionary<string, RespCommandDocs>(tmpAllRespCommandsDocs, StringComparer.OrdinalIgnoreCase);
            AllRespSubCommandsDocs = new ReadOnlyDictionary<string, RespCommandDocs>(tmpAllSubCommandsDocs);
            ExternalRespCommandsDocs = new ReadOnlyDictionary<string, RespCommandDocs>(tmpAllRespCommandsDocs
                .Where(ci => allExternalCommands.Contains(ci.Key))
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value, StringComparer.OrdinalIgnoreCase));
            ExternalRespSubCommandsDocs = new ReadOnlyDictionary<string, RespCommandDocs>(tmpExternalSubCommandsDocs);

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
        public string ToRespFormat()
        {
            var sb = new StringBuilder();
            var argCount = 0;

            string key;

            if (this.Summary != null)
            {
                key = "summary";
                sb.Append($"${key.Length}\r\n{key}\r\n");
                sb.Append($"${this.Summary.Length}\r\n{this.Summary}\r\n");
                argCount += 2;
            }

            key = "group";
            sb.Append($"${key.Length}\r\n{key}\r\n");
            var respType = EnumUtils.GetEnumDescriptions(this.Group)[0];
            sb.Append($"${respType.Length}\r\n{respType}\r\n");
            argCount += 2;

            if (this.Complexity != null)
            {
                key = "complexity";
                sb.Append($"${key.Length}\r\n{key}\r\n");
                sb.Append($"${this.Complexity.Length}\r\n{this.Complexity}\r\n");
                argCount += 2;
            }

            if (this.DocFlags != RespCommandDocFlags.None)
            {
                key = "doc_flags";
                sb.Append($"${key.Length}\r\n{key}\r\n");
                sb.Append($"*{respFormatDocFlags.Length}\r\n");
                foreach (var respDocFlag in respFormatDocFlags)
                {
                    sb.Append($"+{respDocFlag.Length}\r\n");
                }

                argCount += 2;
            }

            if (this.ReplacedBy != null)
            {
                key = "replaced_by";
                sb.Append($"${key.Length}\r\n{key}\r\n");
                sb.Append($"${this.ReplacedBy.Length}\r\n{this.ReplacedBy}\r\n");
                argCount += 2;
            }

            if (Arguments != null)
            {
                key = "arguments";
                sb.Append($"${key.Length}\r\n{key}\r\n");
                sb.Append($"*{Arguments.Length}\r\n");
                foreach (var argument in Arguments)
                {
                    sb.Append(argument.RespFormat);
                }

                argCount += 2;
            }

            if (SubCommands != null)
            {
                key = "subcommands";
                sb.Append($"${key.Length}\r\n{key}\r\n");
                sb.Append($"*{SubCommands.Length * 2}\r\n");
                foreach (var subCommand in SubCommands)
                {
                    sb.Append(subCommand.RespFormat);
                }

                argCount += 2;
            }

            sb.Insert(0, $"${Name.Length}\r\n{Name}\r\n*{argCount}\r\n");
            return sb.ToString();
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