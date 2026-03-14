// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Collections.ObjectModel;
using System.Linq;
using System.Numerics;
using System.Text.Json.Serialization;
using Garnet.common;
using Garnet.server.Resp;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Represents a RESP command's information
    /// </summary>
    public class RespCommandsInfo : IRespSerializable, IRespCommandData<RespCommandsInfo>
    {
        /// <inheritdoc />
        public RespCommand Command { get; init; }

        /// <inheritdoc />
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
        /// Store type that the command operates on (None/Main/Object/All). Default: None for commands without key arguments.
        /// </summary>
        public StoreType StoreType { get; set; }

        /// <inheritdoc />
        public RespCommandsInfo[] SubCommands { get; init; }

        /// <inheritdoc />
        [JsonIgnore]
        public RespCommandsInfo Parent { get; set; }

        private const string RespCommandsInfoEmbeddedFileName = @"RespCommandsInfo.json";
        private const string UnknownCommandName = "UNKNOWN";

        private static bool IsInitialized = false;
        private static readonly object IsInitializedLock = new();
        private static IReadOnlyDictionary<string, RespCommandsInfo> AllRespCommandsInfo = null;
        private static IReadOnlyDictionary<string, RespCommandsInfo> AllRespSubCommandsInfo = null;
        private static IReadOnlyDictionary<string, RespCommandsInfo> ExternalRespCommandsInfo = null;
        private static IReadOnlyDictionary<string, RespCommandsInfo> ExternalRespSubCommandsInfo = null;
        private static IReadOnlyDictionary<RespCommand, RespCommandsInfo> FlattenedRespCommandsInfo = null;
        private static IReadOnlySet<string> AllRespCommandNames = null;
        private static IReadOnlySet<string> ExternalRespCommandNames = null;
        private static IReadOnlyDictionary<RespAclCategories, IReadOnlyList<RespCommandsInfo>> AclCommandInfo = null;
        private static SimpleRespCommandInfo[] SimpleRespCommandsInfo = null;

        private static RespCommandsInfo[] FastBasicRespCommandsInfo = null;

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
            var importSucceeded = RespCommandDataCommon.TryImportRespCommandsData<RespCommandsInfo>(RespCommandsInfoEmbeddedFileName,
                out var tmpAllRespCommandsInfo, logger);

            if (!importSucceeded) return false;

            var tmpFlattenedRespCommandsInfo = new Dictionary<RespCommand, RespCommandsInfo>();
            foreach (var respCommandInfo in tmpAllRespCommandsInfo.Values)
            {
                if (respCommandInfo.Command == RespCommand.NONE) continue;

                // For historical reasons, this command is accepted but isn't "real"
                // So let's prefer the SECONDARYOF or REPLICAOF alternatives
                if (respCommandInfo.Name == "SLAVEOF") continue;

                tmpFlattenedRespCommandsInfo.Add(respCommandInfo.Command, respCommandInfo);

                if (respCommandInfo.SubCommands != null)
                {
                    foreach (var subRespCommandInfo in respCommandInfo.SubCommands)
                    {
                        tmpFlattenedRespCommandsInfo.Add(subRespCommandInfo.Command, subRespCommandInfo);
                    }
                }
            }

            var tmpSimpleRespCommandInfo = new SimpleRespCommandInfo[(int)RespCommandExtensions.LastValidCommand + 1];
            for (var cmdId = (int)RespCommandExtensions.FirstReadCommand; cmdId < tmpSimpleRespCommandInfo.Length; cmdId++)
            {
                if (!tmpFlattenedRespCommandsInfo.TryGetValue((RespCommand)cmdId, out var cmdInfo))
                {
                    tmpSimpleRespCommandInfo[cmdId] = SimpleRespCommandInfo.Default;
                    continue;
                }

                cmdInfo.PopulateSimpleCommandInfo(ref tmpSimpleRespCommandInfo[cmdId]);
            }

            var tmpAllSubCommandsInfo = new Dictionary<string, RespCommandsInfo>(StringComparer.OrdinalIgnoreCase);
            var tmpExternalSubCommandsInfo = new Dictionary<string, RespCommandsInfo>(StringComparer.OrdinalIgnoreCase);
            foreach (var kvp in tmpAllRespCommandsInfo)
            {
                if (kvp.Value.SubCommands == null) continue;

                foreach (var sc in kvp.Value.SubCommands)
                {
                    tmpAllSubCommandsInfo.Add(sc.Name, sc);
                    if (!kvp.Value.IsInternal && !sc.IsInternal)
                        tmpExternalSubCommandsInfo.Add(sc.Name, sc);
                }
            }

            AllRespCommandsInfo = tmpAllRespCommandsInfo;
            AllRespSubCommandsInfo = new ReadOnlyDictionary<string, RespCommandsInfo>(tmpAllSubCommandsInfo);
            ExternalRespCommandsInfo = new ReadOnlyDictionary<string, RespCommandsInfo>(tmpAllRespCommandsInfo
                .Where(ci => !ci.Value.IsInternal)
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value, StringComparer.OrdinalIgnoreCase));
            ExternalRespSubCommandsInfo = new ReadOnlyDictionary<string, RespCommandsInfo>(tmpExternalSubCommandsInfo);
            AllRespCommandNames = ImmutableHashSet.Create(StringComparer.OrdinalIgnoreCase, [.. AllRespCommandsInfo.Keys]);
            ExternalRespCommandNames = ImmutableHashSet.Create(StringComparer.OrdinalIgnoreCase, [.. ExternalRespCommandsInfo.Keys]);
            FlattenedRespCommandsInfo = new ReadOnlyDictionary<RespCommand, RespCommandsInfo>(tmpFlattenedRespCommandsInfo);
            SimpleRespCommandsInfo = tmpSimpleRespCommandInfo;

            AclCommandInfo =
                new ReadOnlyDictionary<RespAclCategories, IReadOnlyList<RespCommandsInfo>>(
                    AllRespCommandsInfo
                        .SelectMany(static kv => (kv.Value.SubCommands ?? []).Append(kv.Value))
                        .SelectMany(static c => IndividualAcls(c.AclCategories).Select(a => (Acl: a, CommandInfo: c)))
                        .GroupBy(static t => t.Acl)
                        .ToDictionary(
                            static grp => grp.Key,
                            static grp => (IReadOnlyList<RespCommandsInfo>)[.. grp.Select(static t => t.CommandInfo)]
                        )
                );

            FastBasicRespCommandsInfo = new RespCommandsInfo[(int)RespCommandExtensions.LastDataCommand - (int)RespCommandExtensions.FirstReadCommand + 1];
            for (var i = (int)RespCommandExtensions.FirstReadCommand; i <= (int)RespCommandExtensions.LastDataCommand; i++)
            {
                FlattenedRespCommandsInfo.TryGetValue((RespCommand)i, out var commandInfo);
                FastBasicRespCommandsInfo[i - (int)RespCommandExtensions.FirstReadCommand] = commandInfo;
            }

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
        /// <param name="externalOnly">Return command info only if command is visible externally</param>
        /// <param name="includeSubCommands">Include sub-commands in command name search</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if initialization was successful and command info was found</returns>
        internal static bool TryGetRespCommandInfo(string cmdName, out RespCommandsInfo respCommandsInfo,
            bool externalOnly = false, bool includeSubCommands = false, ILogger logger = null)
        {
            respCommandsInfo = default;

            return ((TryGetRespCommandsInfo(out var cmdsInfo, externalOnly, logger)
                     && cmdsInfo.TryGetValue(cmdName, out respCommandsInfo)) ||
                    ((includeSubCommands && TryGetRespSubCommandsInfo(out var subCmdsInfo, externalOnly, logger))
                     && subCmdsInfo.TryGetValue(cmdName, out respCommandsInfo)));
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
            if (FlattenedRespCommandsInfo.ContainsKey(cmd))
                tmpRespCommandInfo = FlattenedRespCommandsInfo[cmd];

            if (tmpRespCommandInfo == default ||
                (txnOnly && (tmpRespCommandInfo.Flags & RespCommandFlags.NoMulti) == RespCommandFlags.NoMulti)) return false;

            respCommandsInfo = tmpRespCommandInfo;
            return true;
        }

        /// <summary>
        /// Get command info by RespCommand enum from array of RespCommandsInfo
        /// </summary>
        /// <param name="cmd"></param>
        /// <param name="respCommandsInfo"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        public static bool TryFastGetRespCommandInfo(RespCommand cmd, out RespCommandsInfo respCommandsInfo, ILogger logger = null)
        {
            respCommandsInfo = null;
            if (!IsInitialized && !TryInitialize(logger)) return false;

            var offset = (int)cmd - (int)RespCommandExtensions.FirstReadCommand;
            if (offset < 0 || offset >= FastBasicRespCommandsInfo.Length)
                return false;

            respCommandsInfo = FastBasicRespCommandsInfo[offset];
            return true;
        }

        /// <summary>
        /// Gets all the command info objects of sub-commands supported by Garnet
        /// </summary>
        /// <param name="respSubCommandsInfo">Mapping between sub-command name to command info</param>
        /// <param name="externalOnly">Return only sub-commands that are visible externally</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if initialization was successful and data was retrieved successfully</returns>
        public static bool TryGetRespSubCommandsInfo(out IReadOnlyDictionary<string, RespCommandsInfo> respSubCommandsInfo, bool externalOnly = false, ILogger logger = null)
        {
            respSubCommandsInfo = default;
            if (!IsInitialized && !TryInitialize(logger)) return false;

            respSubCommandsInfo = externalOnly ? ExternalRespSubCommandsInfo : AllRespSubCommandsInfo;
            return true;
        }

        /// <summary>
        /// Gets all the command info objects of sub-commands supported by Garnet
        /// </summary>
        /// <param name="simpleRespCommandsInfo">Mapping between sub-command name to command info</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if initialization was successful and data was retrieved successfully</returns>
        public static bool TryGetSimpleRespCommandsInfo(out SimpleRespCommandInfo[] simpleRespCommandsInfo, ILogger logger = null)
        {
            simpleRespCommandsInfo = default;
            if (!IsInitialized && !TryInitialize(logger)) return false;

            simpleRespCommandsInfo = SimpleRespCommandsInfo;
            return true;
        }

        /// <summary>
        /// Gets command's simplified info
        /// </summary>
        /// <param name="cmd">Resp command</param>
        /// <param name="cmdInfo">Arity</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if valid command</returns>
        public static bool TryGetSimpleRespCommandInfo(RespCommand cmd, out SimpleRespCommandInfo cmdInfo, ILogger logger = null)
        {
            cmdInfo = SimpleRespCommandInfo.Default;

            if (!IsInitialized && !TryInitialize(logger))
                return false;

            var cmdId = (ushort)cmd;
            if (cmdId >= SimpleRespCommandsInfo.Length)
                return false;

            cmdInfo = SimpleRespCommandsInfo[cmdId];
            return true;
        }

        /// <summary>
        /// Gets command's name
        /// </summary>
        /// <param name="cmd">Resp command</param>
        /// <param name="logger">Logger</param>
        /// <returns>Command name</returns>
        public static string GetRespCommandName(RespCommand cmd, ILogger logger = null)
            => TryGetRespCommandInfo(cmd, out var commandInfo, logger: logger) ? commandInfo.Name : UnknownCommandName;

        /// <summary>
        /// Serializes the current object to RESP format
        /// </summary>
        /// <returns>Serialized value</returns>
        public void ToRespFormat(ref RespMemoryWriter writer)
        {
            if (string.IsNullOrWhiteSpace(Name))
            {
                writer.WriteNull();
            }
            else
            {
                writer.WriteArrayLength(10);
                // 1) Name
                writer.WriteAsciiBulkString(Name);
                // 2) Arity
                writer.WriteInt32(Arity);
                // 3) Flags
                writer.WriteSetLength(respFormatFlags?.Length ?? 0);
                if (respFormatFlags != null && respFormatFlags.Length > 0)
                {
                    foreach (var flag in respFormatFlags)
                        writer.WriteSimpleString(flag);
                }

                // 4) First key
                writer.WriteInt32(FirstKey);
                // 5) Last key
                writer.WriteInt32(LastKey);
                // 6) Step
                writer.WriteInt32(Step);
                // 7) ACL categories
                writer.WriteSetLength(respFormatAclCategories?.Length ?? 0);
                if (respFormatAclCategories != null && respFormatAclCategories.Length > 0)
                {
                    foreach (var aclCat in respFormatAclCategories)
                        writer.WriteSimpleString('@' + aclCat);
                }

                // 8) Tips
                var tipCount = Tips?.Length ?? 0;
                writer.WriteSetLength(tipCount);
                if (Tips != null && tipCount > 0)
                {
                    foreach (var tip in Tips)
                        writer.WriteAsciiBulkString(tip);
                }

                // 9) Key specifications
                var ksCount = KeySpecifications?.Length ?? 0;
                writer.WriteSetLength(ksCount);
                if (KeySpecifications != null && ksCount > 0)
                {
                    foreach (var ks in KeySpecifications)
                        ks.ToRespFormat(ref writer);
                }

                // 10) SubCommands
                var subCommandCount = SubCommands?.Length ?? 0;
                writer.WriteArrayLength(subCommandCount);
                if (SubCommands != null && subCommandCount > 0)
                {
                    foreach (var subCommand in SubCommands)
                        subCommand.ToRespFormat(ref writer);
                }
            }
        }
    }
}