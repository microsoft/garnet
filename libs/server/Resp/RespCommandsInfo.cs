﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using Garnet.common;
using Microsoft.Extensions.Logging.Abstractions;

namespace Garnet.server
{
    public class RespCommandsInfo
    {
        public RespCommand Command { get; init; }

        public byte? ArrayCommand { get; set; }

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

        public RespCommandKeySpecifications? KeySpecifications { get; init; }

        public RespCommandsInfo[]? SubCommands { get; init; }

        [JsonIgnore]
        internal string RespFormat => this.respFormat ??= GetRespFormat();

        internal static int RespCommandsInfoCount => allRespCommandsInfo.Count;

        internal static IEnumerable<RespCommandsInfo> AllRespCommandsInfo => allRespCommandsInfo.Values;

        private static IReadOnlyDictionary<string, RespCommandsInfo> allRespCommandsInfo;
        private static IReadOnlyDictionary<RespCommand, RespCommandsInfo> basicRespCommandsInfo;
        private static IReadOnlyDictionary<RespCommand, IReadOnlyDictionary<byte, RespCommandsInfo>> arrayRespCommandsInfo;

        private const string RespCommandsEmbeddedFileName = @"RespCommandsInfo.json";

        private readonly RespCommandFlags flags;
        private readonly RespAclCategories aclCategories;

        private string respFormat;
        private readonly string[] respFormatFlags;
        private readonly string[] respFormatAclCategories;

        static RespCommandsInfo()
        {
            InitRespCommandsInfo();
        }

        private static void InitRespCommandsInfo()
        {
            var streamProvider = StreamProviderFactory.GetStreamProvider(FileLocationType.EmbeddedResource);
            var commandsInfoProvider = RespCommandsInfoProviderFactory.GetRespCommandsInfoProvider();

            var importSucceeded = commandsInfoProvider.TryImportRespCommandsInfo(RespCommandsEmbeddedFileName,
                streamProvider, NullLogger.Instance, out allRespCommandsInfo);

            if (!importSucceeded) return;

            var tmpBasicRespCommandsInfo = new Dictionary<RespCommand, RespCommandsInfo>();
            var tmpArrayRespCommandsInfo = new Dictionary<RespCommand, Dictionary<byte, RespCommandsInfo>>();
            foreach (var respCommandInfo in allRespCommandsInfo.Values)
            {
                if (respCommandInfo.Command == RespCommand.NONE) continue;

                if (respCommandInfo.ArrayCommand.HasValue)
                {
                    if (!tmpArrayRespCommandsInfo.ContainsKey(respCommandInfo.Command))
                        tmpArrayRespCommandsInfo.Add(respCommandInfo.Command, new Dictionary<byte, RespCommandsInfo>());
                    tmpArrayRespCommandsInfo[respCommandInfo.Command].Add(respCommandInfo.ArrayCommand.Value, respCommandInfo);
                }
                else
                {
                    tmpBasicRespCommandsInfo.Add(respCommandInfo.Command, respCommandInfo);
                }
            }

            basicRespCommandsInfo = new ReadOnlyDictionary<RespCommand, RespCommandsInfo>(tmpBasicRespCommandsInfo);
            arrayRespCommandsInfo = new ReadOnlyDictionary<RespCommand, IReadOnlyDictionary<byte, RespCommandsInfo>>(tmpArrayRespCommandsInfo
                .ToDictionary(kvp => kvp.Key, 
                    kvp => (IReadOnlyDictionary<byte, RespCommandsInfo>)new ReadOnlyDictionary<byte, RespCommandsInfo>(kvp.Value)));
        }

        public static RespCommandsInfo GetRespCommandInfo(string cmdName)
        {
            return allRespCommandsInfo.ContainsKey(cmdName) ? allRespCommandsInfo[cmdName] : null;
        }

        public static RespCommandsInfo GetRespCommandInfo(RespCommand cmd, byte subCmd = 0, bool txnOnly = false)
        { 
            RespCommandsInfo result = null;
            if (arrayRespCommandsInfo.ContainsKey(cmd) && arrayRespCommandsInfo[cmd].ContainsKey(subCmd))
                result = arrayRespCommandsInfo[cmd][subCmd];
            else if (basicRespCommandsInfo.ContainsKey(cmd))
                result = basicRespCommandsInfo[cmd];

            return !txnOnly || (result != null && !result.Flags.HasFlag(RespCommandFlags.NoMulti)) ? result : null;
        }

        private string GetRespFormat()
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
            if (this.KeySpecifications == null)
            {
                sb.Append("*0\r\n");
            }
            else
            {
                sb.Append(this.KeySpecifications.RespFormat);
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
