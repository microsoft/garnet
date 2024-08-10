// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Collections.ObjectModel;
using System.Linq;
using System.Numerics;
using System.Reflection;
using System.Text;
using System.Text.Json.Serialization;
using Garnet.common;
using Garnet.server.ACL;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Represents a RESP command's information
    /// </summary>
    public class RespCommandsInfo : IRespSerializable
    {
        /// <summary>
        /// Garnet's RespCommand enum command representation
        /// </summary>
        public RespCommand Command { get; init; }

        /// <summary>
        /// The command's name
        /// </summary>
        public string Name { get; init; }

        /// <summary>
        /// Determines if the command is Garnet internal-only (i.e. not exposed to clients) 
        /// </summary>
        public bool IsInternal { get; init; }

        /// <summary>
        /// The command's arity, i.e. the number of arguments a command expects
        /// * A positive integer means a fixed number of arguments
        /// * A negative integer means a minimal number of arguments
        /// </summary>
        public int Arity { get; init; }

        /// <summary>
        /// RESP command flags
        /// </summary>
        public RespCommandFlags Flags
        {
            get => this.flags;
            init
            {
                this.flags = value;
                this.respFormatFlags = EnumUtils.GetEnumDescriptions(this.flags);
            }
        }

        /// <summary>
        /// The position of the command's first key name argument
        /// </summary>
        public int FirstKey { get; init; }

        /// <summary>
        /// The position of the command's last key name argument
        /// </summary>
        public int LastKey { get; init; }

        /// <summary>
        /// The step, or increment, between the first key and the position of the next key
        /// </summary>
        public int Step { get; init; }

        /// <summary>
        /// ACL categories to which the command belongs
        /// </summary>
        public RespAclCategories AclCategories
        {
            get => this.aclCategories;
            init
            {
                this.aclCategories = value;
                this.respFormatAclCategories = EnumUtils.GetEnumDescriptions(this.aclCategories);
            }
        }

        /// <summary>
        /// Helpful information about the command
        /// </summary>
        public string[] Tips { get; init; }

        /// <summary>
        /// Methods for locating keys in the command's arguments
        /// </summary>
        public RespCommandKeySpecification[] KeySpecifications { get; init; }

        /// <summary>
        /// All the command's sub-commands, if any
        /// </summary>
        public RespCommandsInfo[] SubCommands { get; init; }

        /// <summary>
        /// Returns the serialized representation of the current object in RESP format
        /// This property returns a cached value, if exists (this value should never change after object initialization)
        /// </summary>
        [JsonIgnore]
        public string RespFormat => respFormat ??= ToRespFormat();

        [JsonIgnore]
        public RespCommandsInfo Parent { get; set; }

        [JsonIgnore]
        public RespCommand? SubCommand { get; set; }

        private const string RespCommandsEmbeddedFileName = @"RespCommandsInfo.json";

        private string respFormat;

        private static bool IsInitialized = false;
        private static readonly object IsInitializedLock = new();
        private static IReadOnlyDictionary<string, RespCommandsInfo> AllRespCommandsInfo = null;
        private static IReadOnlyDictionary<string, RespCommandsInfo> ExternalRespCommandsInfo = null;
        private static IReadOnlyDictionary<RespCommand, RespCommandsInfo> BasicRespCommandsInfo = null;
        private static IReadOnlySet<string> AllRespCommandNames = null;
        private static IReadOnlySet<string> ExternalRespCommandNames = null;
        private static IReadOnlyDictionary<RespAclCategories, IReadOnlyList<RespCommandsInfo>> AclCommandInfo = null;

        private readonly RespCommandFlags flags;
        private readonly RespAclCategories aclCategories;

        private readonly string[] respFormatFlags;
        private readonly string[] respFormatAclCategories;

        private static bool TryInitialize(ILogger logger)
        {
            lock (IsInitializedLock)
            {
                if (IsInitialized) return true;

                IsInitialized = TryInitializeRespCommandsInfo(logger);
                return IsInitialized;
            }
        }

        private static bool TryInitializeRespCommandsInfo(ILogger logger = null)
        {
            var streamProvider = StreamProviderFactory.GetStreamProvider(FileLocationType.EmbeddedResource, null,
                Assembly.GetExecutingAssembly());
            var commandsInfoProvider = RespCommandsInfoProviderFactory.GetRespCommandsInfoProvider();

            var importSucceeded = commandsInfoProvider.TryImportRespCommandsInfo(RespCommandsEmbeddedFileName,
                streamProvider, out var scratchAllRespCommandsInfo, logger);

            if (!importSucceeded) return false;

            // force sub commands into a well known order so we can quickly validate them against ACL lists
            // setup parent refs so we can navigate from child -> parent

            // todo: remove all of this once sub command ids is dead

            var tmpAllRespCommandsInfo =
                scratchAllRespCommandsInfo.ToDictionary(
                    static kv => kv.Key,
                    static kv =>
                    {
                        if (kv.Value.SubCommands != null)
                        {
                            SetupSubCommands(kv.Value);
                        }

                        return kv.Value;

                        static void SetupSubCommands(RespCommandsInfo cmd)
                        {
                            foreach (var subCommand in cmd.SubCommands)
                            {
                                subCommand.Parent = cmd;

                                if (!Enum.TryParse(subCommand.Name.Replace("|", "_").Replace("-", ""), out RespCommand parsed))
                                {
                                    throw new ACLException($"Couldn't map '{subCommand.Name}' to a member of {nameof(RespCommand)} this will break ACLs");
                                }

                                subCommand.SubCommand = parsed;

                                if (subCommand.SubCommands != null)
                                {
                                    SetupSubCommands(subCommand);
                                }
                            }
                        }
                    }
                );

            var tmpBasicRespCommandsInfo = new Dictionary<RespCommand, RespCommandsInfo>();
            foreach (var respCommandInfo in tmpAllRespCommandsInfo.Values)
            {
                if (respCommandInfo.Command == RespCommand.NONE) continue;

                // For historical reasons, this command is accepted but isn't "real"
                // So let's prefer the SECONDARYOF or REPLICAOF alternatives
                if (respCommandInfo.Name == "SLAVEOF") continue;

                tmpBasicRespCommandsInfo.Add(respCommandInfo.Command, respCommandInfo);

                if (respCommandInfo.SubCommands != null)
                {
                    foreach (var subRespCommandInfo in respCommandInfo.SubCommands)
                    {
                        tmpBasicRespCommandsInfo.Add(subRespCommandInfo.SubCommand.Value, subRespCommandInfo);
                    }
                }
            }

            AllRespCommandsInfo =
                new Dictionary<string, RespCommandsInfo>(tmpAllRespCommandsInfo, StringComparer.OrdinalIgnoreCase);
            ExternalRespCommandsInfo = new ReadOnlyDictionary<string, RespCommandsInfo>(tmpAllRespCommandsInfo
                .Where(ci => !ci.Value.IsInternal)
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value, StringComparer.OrdinalIgnoreCase));
            AllRespCommandNames = ImmutableHashSet.Create(StringComparer.OrdinalIgnoreCase, AllRespCommandsInfo.Keys.ToArray());
            ExternalRespCommandNames = ImmutableHashSet.Create(StringComparer.OrdinalIgnoreCase, ExternalRespCommandsInfo.Keys.ToArray());
            BasicRespCommandsInfo = new ReadOnlyDictionary<RespCommand, RespCommandsInfo>(tmpBasicRespCommandsInfo);

            AclCommandInfo =
                new ReadOnlyDictionary<RespAclCategories, IReadOnlyList<RespCommandsInfo>>(
                    AllRespCommandsInfo
                        .SelectMany(static kv => (kv.Value.SubCommands ?? []).Append(kv.Value))
                        .SelectMany(static c => IndividualAcls(c.AclCategories).Select(a => (Acl: a, CommandInfo: c)))
                        .GroupBy(static t => t.Acl)
                        .ToDictionary(
                            static grp => grp.Key,
                            static grp => (IReadOnlyList<RespCommandsInfo>)ImmutableArray.CreateRange(grp.Select(static t => t.CommandInfo))
                        )
                );

            return true;

            // Yield each bit set in aclCategories as it's own value
            static IEnumerable<RespAclCategories> IndividualAcls(RespAclCategories aclCategories)
            {
                var remaining = aclCategories;
                while (remaining != 0)
                {
                    var shift = BitOperations.TrailingZeroCount((int)remaining);
                    var single = (RespAclCategories)(1 << shift);

                    remaining &= ~single;

                    yield return single;
                }
            }
        }

        /// <summary>
        /// Gets commands which are covered by the given ACL category.
        /// </summary>
        internal static bool TryGetCommandsforAclCategory(RespAclCategories acl, out IReadOnlyList<RespCommandsInfo> respCommands, ILogger logger = null)
        {
            if (!IsInitialized && !TryInitialize(logger))
            {
                respCommands = null;
                return false;
            }

            return AclCommandInfo.TryGetValue(acl, out respCommands);
        }

        /// <summary>
        /// Gets the number of commands supported by Garnet
        /// </summary>
        /// <param name="count">The count value</param>
        /// <param name="externalOnly">Return number of commands that are visible externally</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if initialization was successful and data was retrieved successfully</returns>
        internal static bool TryGetRespCommandsInfoCount(out int count, bool externalOnly = false, ILogger logger = null)
        {
            count = -1;
            if (!IsInitialized && !TryInitialize(logger)) return false;

            count = externalOnly ? ExternalRespCommandsInfo!.Count : AllRespCommandsInfo!.Count;
            return true;
        }

        /// <summary>
        /// Gets all the command info objects of commands supported by Garnet
        /// </summary>
        /// <param name="respCommandsInfo">Mapping between command name to command info</param>
        /// <param name="externalOnly">Return only commands that are visible externally</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if initialization was successful and data was retrieved successfully</returns>
        public static bool TryGetRespCommandsInfo(out IReadOnlyDictionary<string, RespCommandsInfo> respCommandsInfo, bool externalOnly = false, ILogger logger = null)
        {
            respCommandsInfo = default;
            if (!IsInitialized && !TryInitialize(logger)) return false;

            respCommandsInfo = externalOnly ? ExternalRespCommandsInfo : AllRespCommandsInfo;
            return true;
        }

        /// <summary>
        /// Gets all the command names of commands supported by Garnet
        /// </summary>
        /// <param name="respCommandNames">The command names</param>
        /// <param name="externalOnly">Return only names of commands that are visible externally</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if initialization was successful and data was retrieved successfully</returns>
        public static bool TryGetRespCommandNames(out IReadOnlySet<string> respCommandNames, bool externalOnly = false, ILogger logger = null)
        {
            respCommandNames = default;
            if (!IsInitialized && !TryInitialize(logger)) return false;

            respCommandNames = externalOnly ? ExternalRespCommandNames : AllRespCommandNames;
            return true;
        }

        /// <summary>
        /// Gets command info by command name
        /// </summary>
        /// <param name="cmdName">The command name</param>
        /// <param name="respCommandsInfo">The command info</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if initialization was successful and command info was found</returns>
        internal static bool TryGetRespCommandInfo(string cmdName, out RespCommandsInfo respCommandsInfo, ILogger logger = null)
        {
            respCommandsInfo = default;
            if ((!IsInitialized && !TryInitialize(logger)) ||
                !AllRespCommandsInfo.ContainsKey(cmdName)) return false;

            respCommandsInfo = AllRespCommandsInfo[cmdName];
            return true;
        }

        /// <summary>
        /// Gets command info by RespCommand enum and sub-command byte, if applicable
        /// </summary>
        /// <param name="cmd">The RespCommand enum</param>
        /// <param name="logger">Logger</param>
        /// <param name="respCommandsInfo">The commands info</param>
        /// <param name="txnOnly">Return only commands that are allowed in a transaction context (False by default)</param>
        /// <returns>True if initialization was successful and command info was found</returns>
        public static bool TryGetRespCommandInfo(RespCommand cmd,
            out RespCommandsInfo respCommandsInfo, bool txnOnly = false, ILogger logger = null)
        {
            respCommandsInfo = default;
            if (!IsInitialized && !TryInitialize(logger)) return false;

            RespCommandsInfo tmpRespCommandInfo = default;
            if (BasicRespCommandsInfo.ContainsKey(cmd))
                tmpRespCommandInfo = BasicRespCommandsInfo[cmd];

            if (tmpRespCommandInfo == default ||
                (txnOnly && tmpRespCommandInfo.Flags.HasFlag(RespCommandFlags.NoMulti))) return false;

            respCommandsInfo = tmpRespCommandInfo;
            return true;
        }

        /// <summary>
        /// Serializes the current object to RESP format
        /// </summary>
        /// <returns>Serialized value</returns>
        public string ToRespFormat()
        {
            if (string.IsNullOrWhiteSpace(this.Name))
                return "$-1\r\n";

            var sb = new StringBuilder();
            sb.Append("*10\r\n");
            // 1) Name
            sb.Append($"${this.Name.Length}\r\n{this.Name}\r\n");
            // 2) Arity
            sb.Append($":{this.Arity}\r\n");
            // 3) Flags
            sb.Append($"*{this.respFormatFlags?.Length ?? 0}\r\n");
            if (this.respFormatFlags != null && this.respFormatFlags.Length > 0)
            {
                foreach (var flag in this.respFormatFlags)
                    sb.Append($"+{flag}\r\n");
            }

            // 4) First key
            sb.Append($":{this.FirstKey}\r\n");
            // 5) Last key
            sb.Append($":{this.LastKey}\r\n");
            // 6) Step
            sb.Append($":{this.Step}\r\n");
            // 7) ACL categories
            sb.Append($"*{this.respFormatAclCategories?.Length ?? 0}\r\n");
            if (this.respFormatAclCategories != null && this.respFormatAclCategories.Length > 0)
            {
                foreach (var aclCat in this.respFormatAclCategories)
                    sb.Append($"+@{aclCat}\r\n");
            }

            // 8) Tips
            var tipCount = this.Tips?.Length ?? 0;
            sb.Append($"*{tipCount}\r\n");
            if (this.Tips != null && tipCount > 0)
            {
                foreach (var tip in this.Tips)
                    sb.Append($"${tip.Length}\r\n{tip}\r\n");
            }

            // 9) Key specifications
            var ksCount = this.KeySpecifications?.Length ?? 0;
            sb.Append($"*{ksCount}\r\n");
            if (this.KeySpecifications != null && ksCount > 0)
            {
                foreach (var ks in this.KeySpecifications)
                    sb.Append(ks.RespFormat);
            }

            // 10) SubCommands
            var subCommandCount = this.SubCommands?.Length ?? 0;
            sb.Append($"*{subCommandCount}\r\n");
            if (this.SubCommands != null && subCommandCount > 0)
            {
                foreach (var subCommand in SubCommands)
                    sb.Append(subCommand.RespFormat);
            }

            return sb.ToString();
        }
    }
}