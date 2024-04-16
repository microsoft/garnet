// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json.Serialization;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    public class RespCommandsInfo : IRespSerializable
    {
        public RespCommand Command { get; init; }

        public byte? ArrayCommand { get; init; }

        public string Name { get; init; }

        public int Arity { get; init; }

        public RespCommandFlags Flags
        {
            get => this.flags;
            init
            {
                this.flags = value;
                this.respFormatFlags = EnumUtils.GetEnumDescriptions(this.flags);
            }
        }

        public int FirstKey { get; init; }

        public int LastKey { get; init; }

        public int Step { get; init; }

        public RespAclCategories AclCategories
        {
            get => this.aclCategories;
            init
            {
                this.aclCategories = value;
                this.respFormatAclCategories = EnumUtils.GetEnumDescriptions(this.aclCategories);
            }
        }

        public string[]? Tips { get; init; }

        public RespCommandKeySpecification[] KeySpecifications { get; init; }

        public RespCommandsInfo[] SubCommands { get; init; }

        [JsonIgnore]
        public string RespFormat => respFormat ??= ToRespFormat();

        private string respFormat;

        private static bool isInitialized = false;
        private static IReadOnlyDictionary<string, RespCommandsInfo> allRespCommandsInfo = null;
        private static IReadOnlyDictionary<RespCommand, RespCommandsInfo> basicRespCommandsInfo = null;

        private static IReadOnlyDictionary<RespCommand, IReadOnlyDictionary<byte, RespCommandsInfo>>
            arrayRespCommandsInfo = null;

        private const string RespCommandsEmbeddedFileName = @"RespCommandsInfo.json";

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

            allRespCommandsInfo = tmpAllRespCommandsInfo;
            basicRespCommandsInfo = new ReadOnlyDictionary<RespCommand, RespCommandsInfo>(tmpBasicRespCommandsInfo);
            arrayRespCommandsInfo = new ReadOnlyDictionary<RespCommand, IReadOnlyDictionary<byte, RespCommandsInfo>>(
                tmpArrayRespCommandsInfo
                    .ToDictionary(kvp => kvp.Key,
                        kvp =>
                            (IReadOnlyDictionary<byte, RespCommandsInfo>)new ReadOnlyDictionary<byte, RespCommandsInfo>(
                                kvp.Value)));

            return true;
        }

        internal static bool TryGetRespCommandsInfoCount(ILogger logger, out int count)
        {
            count = -1;
            if (!isInitialized && !TryInitializeRespCommandsInfo(logger)) return false;

            count = allRespCommandsInfo!.Count;
            return true;
        }

        internal static bool TryGetRespCommandsInfo(ILogger logger, out IEnumerable<RespCommandsInfo> respCommandsInfo)
        {
            respCommandsInfo = default;
            if (!isInitialized && !TryInitializeRespCommandsInfo(logger)) return false;

            respCommandsInfo = allRespCommandsInfo!.Values;
            return true;
        }

        internal static bool TryGetRespCommandInfo(string cmdName, ILogger logger,
            out RespCommandsInfo respCommandsInfo)
        {
            respCommandsInfo = default;
            if ((!isInitialized && !TryInitializeRespCommandsInfo(logger)) ||
                !allRespCommandsInfo.ContainsKey(cmdName)) return false;

            respCommandsInfo = allRespCommandsInfo[cmdName];
            return true;
        }

        internal static bool TryGetRespCommandInfo(RespCommand cmd, ILogger logger,
            out RespCommandsInfo respCommandsInfo, byte subCmd = 0, bool txnOnly = false)
        {
            respCommandsInfo = default;
            if ((!isInitialized && !TryInitializeRespCommandsInfo(logger))) return false;

            RespCommandsInfo tmpRespCommandInfo = default;
            if (arrayRespCommandsInfo.ContainsKey(cmd) && arrayRespCommandsInfo[cmd].ContainsKey(subCmd))
                tmpRespCommandInfo = arrayRespCommandsInfo[cmd][subCmd];
            else if (basicRespCommandsInfo.ContainsKey(cmd))
                tmpRespCommandInfo = basicRespCommandsInfo[cmd];

            if (tmpRespCommandInfo == default ||
                (txnOnly && tmpRespCommandInfo.Flags.HasFlag(RespCommandFlags.NoMulti))) return false;

            respCommandsInfo = tmpRespCommandInfo;
            return true;
        }

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
