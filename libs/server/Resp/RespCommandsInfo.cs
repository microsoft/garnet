// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json.Serialization;
using Garnet.common;
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
        /// Garnet's sub-command enum value representation
        /// </summary>
        public byte? ArrayCommand { get; init; }

        /// <summary>
        /// The command's name
        /// </summary>
        public string Name { get; init; }

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

        private const string RespCommandsEmbeddedFileName = @"RespCommandsInfo.json";

        private string respFormat;

        private static bool IsInitialized = false;
        private static IReadOnlyDictionary<string, RespCommandsInfo> AllRespCommandsInfo = null;
        private static IReadOnlyDictionary<RespCommand, RespCommandsInfo> BasicRespCommandsInfo = null;
        private static IReadOnlyDictionary<RespCommand, IReadOnlyDictionary<byte, RespCommandsInfo>> ArrayRespCommandsInfo = null;
        private static IReadOnlySet<string> AllRespCommandNames = null;

        private readonly RespCommandFlags flags;
        private readonly RespAclCategories aclCategories;

        private readonly string[] respFormatFlags;
        private readonly string[] respFormatAclCategories;

        private static bool TryInitializeRespCommandsInfo(ILogger logger)
        {
            var streamProvider = StreamProviderFactory.GetStreamProvider(FileLocationType.EmbeddedResource, null,
                Assembly.GetExecutingAssembly());
            var commandsInfoProvider = RespCommandsInfoProviderFactory.GetRespCommandsInfoProvider();

            var importSucceeded = commandsInfoProvider.TryImportRespCommandsInfo(RespCommandsEmbeddedFileName,
                streamProvider, logger, out var tmpAllRespCommandsInfo);

            if (!importSucceeded) return false;

            var tmpBasicRespCommandsInfo = new Dictionary<RespCommand, RespCommandsInfo>();
            var tmpArrayRespCommandsInfo = new Dictionary<RespCommand, Dictionary<byte, RespCommandsInfo>>();
            foreach (var respCommandInfo in tmpAllRespCommandsInfo.Values)
            {
                if (respCommandInfo.Command == RespCommand.NONE) continue;

                if (respCommandInfo.ArrayCommand.HasValue)
                {
                    if (!tmpArrayRespCommandsInfo.ContainsKey(respCommandInfo.Command))
                        tmpArrayRespCommandsInfo.Add(respCommandInfo.Command, new Dictionary<byte, RespCommandsInfo>());
                    tmpArrayRespCommandsInfo[respCommandInfo.Command]
                        .Add(respCommandInfo.ArrayCommand.Value, respCommandInfo);
                }
                else
                {
                    tmpBasicRespCommandsInfo.Add(respCommandInfo.Command, respCommandInfo);
                }
            }

            AllRespCommandsInfo = tmpAllRespCommandsInfo;
            AllRespCommandNames = ImmutableHashSet.Create(StringComparer.OrdinalIgnoreCase, AllRespCommandsInfo.Keys.ToArray());
            BasicRespCommandsInfo = new ReadOnlyDictionary<RespCommand, RespCommandsInfo>(tmpBasicRespCommandsInfo);
            ArrayRespCommandsInfo = new ReadOnlyDictionary<RespCommand, IReadOnlyDictionary<byte, RespCommandsInfo>>(
                tmpArrayRespCommandsInfo
                    .ToDictionary(kvp => kvp.Key,
                        kvp =>
                            (IReadOnlyDictionary<byte, RespCommandsInfo>)new ReadOnlyDictionary<byte, RespCommandsInfo>(
                                kvp.Value)));

            IsInitialized = true;
            return true;
        }

        /// <summary>
        /// Gets the number of commands supported by Garnet
        /// </summary>
        /// <param name="logger">Logger</param>
        /// <param name="count">The count value</param>
        /// <returns>True if initialization was successful and data was retrieved successfully</returns>
        internal static bool TryGetRespCommandsInfoCount(ILogger logger, out int count)
        {
            count = -1;
            if (!IsInitialized && !TryInitializeRespCommandsInfo(logger)) return false;

            count = AllRespCommandsInfo!.Count;
            return true;
        }

        /// <summary>
        /// Gets all the command info objects of commands supported by Garnet
        /// </summary>
        /// <param name="logger">Logger</param>
        /// <param name="respCommandsInfo">The commands info</param>
        /// <returns>True if initialization was successful and data was retrieved successfully</returns>
        internal static bool TryGetRespCommandsInfo(ILogger logger, out IEnumerable<RespCommandsInfo> respCommandsInfo)
        {
            respCommandsInfo = default;
            if (!IsInitialized && !TryInitializeRespCommandsInfo(logger)) return false;

            respCommandsInfo = AllRespCommandsInfo!.Values;
            return true;
        }

        /// <summary>
        /// Gets all the command names of commands supported by Garnet
        /// </summary>
        /// <param name="logger">Logger</param>
        /// <param name="respCommandNames">The command names</param>
        /// <returns>True if initialization was successful and data was retrieved successfully</returns>
        internal static bool TryGetRespCommandNames(ILogger logger, out IReadOnlySet<string> respCommandNames)
        {
            respCommandNames = default;
            if (!IsInitialized && !TryInitializeRespCommandsInfo(logger)) return false;

            respCommandNames = AllRespCommandNames;
            return true;
        }

        /// <summary>
        /// Gets command info by command name
        /// </summary>
        /// <param name="cmdName">The command name</param>
        /// <param name="logger">Logger</param>
        /// <param name="respCommandsInfo">The command info</param>
        /// <returns>True if initialization was successful and command info was found</returns>
        internal static bool TryGetRespCommandInfo(string cmdName, ILogger logger, out RespCommandsInfo respCommandsInfo)
        {
            respCommandsInfo = default;
            if ((!IsInitialized && !TryInitializeRespCommandsInfo(logger)) ||
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
        /// <param name="subCmd">The sub-command byte, if applicable</param>
        /// <param name="txnOnly">Return only commands that are allowed in a transaction context (False by default)</param>
        /// <returns>True if initialization was successful and command info was found</returns>
        internal static bool TryGetRespCommandInfo(RespCommand cmd, ILogger logger,
            out RespCommandsInfo respCommandsInfo, byte subCmd = 0, bool txnOnly = false)
        {
            respCommandsInfo = default;
            if ((!IsInitialized && !TryInitializeRespCommandsInfo(logger))) return false;

            RespCommandsInfo tmpRespCommandInfo = default;
            if (ArrayRespCommandsInfo.ContainsKey(cmd) && ArrayRespCommandsInfo[cmd].ContainsKey(subCmd))
                tmpRespCommandInfo = ArrayRespCommandsInfo[cmd][subCmd];
            else if (BasicRespCommandsInfo.ContainsKey(cmd))
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
            var sb = new StringBuilder();

            sb.Append("*10\r\n");
            // 1) Name
            sb.Append($"${this.Name.Length}\r\n{this.Name}\r\n");
            // 2) Arity
            sb.Append($":{this.Arity}\r\n");
            // 3) Flags
            sb.Append($"*{this.respFormatFlags.Length}\r\n");
            foreach (var flag in this.respFormatFlags)
                sb.Append($"+{flag}\r\n");
            // 4) First key
            sb.Append($":{this.FirstKey}\r\n");
            // 5) Last key
            sb.Append($":{this.LastKey}\r\n");
            // 6) Step
            sb.Append($":{this.Step}\r\n");
            // 7) ACL categories
            sb.Append($"*{this.respFormatAclCategories.Length}\r\n");
            foreach (var aclCat in this.respFormatAclCategories)
                sb.Append($"+@{aclCat}\r\n");
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