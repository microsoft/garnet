// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Garnet.common;

namespace Garnet.server
{
    public class RespCommandsInfoNew
    {
        public static RespCommandsInfoNew[] AllRespCommands;

        public static RespCommandsInfoNew[] AllTxnRespCommands;

        public RespCommand Command { get; init; }

        public string Name { get; init; }

        public int Arity { get; init; }

        public RespCommandFlags Flags
        {
            get => this._flags;
            init
            {
                this._flags = value;
                this._respFormatFlags = EnumUtils.GetEnumDescriptions(this._flags);
            }
        }

        public int FirstKey { get; init; }

        public int LastKey { get; init; }

        public int Step { get; init; }

        public RespAclCategories AclCategories
        {
            get => this._aclCategories;
            init
            {
                this._aclCategories = value;
                this._respFormatAclCategories = EnumUtils.GetEnumDescriptions(this._aclCategories);
            }
        }

        public string[]? Tips { get; init; }

        public RespCommandKeySpecifications? KeySpecifications { get; init; }

        public RespCommandsInfoNew[]? SubCommands { get; init; }

        [JsonIgnore]
        public string RespFormat => this._respFormat ??= GetRespFormat();

        private const string RespCommandsEmbeddedFileName = @"RespCommands.json";

        private readonly RespCommandFlags _flags;
        private readonly RespAclCategories _aclCategories;

        private string? _respFormat;
        private readonly string[] _respFormatFlags;
        private readonly string[] _respFormatAclCategories;

        static RespCommandsInfoNew()
        {
            InitRespCommands();
        }

        private static void InitRespCommands()
        {
            var assembly = Assembly.GetExecutingAssembly();
            var resourceName = assembly.GetManifestResourceNames().FirstOrDefault(rn => rn.EndsWith(RespCommandsEmbeddedFileName))!;

            var serializerOptions = new JsonSerializerOptions
            {
                WriteIndented = true,
                Converters = { new JsonStringEnumConverter(), new KeySpecConverter() }
            };

            using var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream(resourceName)!;
            AllRespCommands = JsonSerializer.Deserialize<RespCommandsInfoNew[]>(stream, serializerOptions)!;
            AllTxnRespCommands = AllRespCommands.Where(rc => !rc.Flags.HasFlag(RespCommandFlags.NoMulti)).ToArray();
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
            sb.Append($"*{this._respFormatFlags.Length}\r\n");
            foreach (var flag in this._respFormatFlags)
                sb.Append($"+{flag}\r\n");
            // 4) First key
            sb.Append($":{this.FirstKey}\r\n");
            // 5) Last key
            sb.Append($":{this.LastKey}\r\n");
            // 6) Step
            sb.Append($":{this.Step}\r\n");
            // 7) ACL categories
            sb.Append($"*{this._respFormatAclCategories.Length}\r\n");
            foreach (var aclCat in this._respFormatAclCategories)
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

    [Flags]
    public enum RespCommandFlags
    {
        None = 0,
        [Description("admin")]
        Admin = 1,
        [Description("asking")]
        Asking = 1 << 1,
        [Description("blocking")]
        Blocking = 1 << 2,
        [Description("denyoom")]
        DenyOom = 1 << 3,
        [Description("fast")]
        Fast = 1 << 4,
        [Description("loading")]
        Loading = 1 << 5,
        [Description("moveablekeys")]
        MoveableKeys = 1 << 6,
        [Description("no_auth")]
        NoAuth = 1 << 7,
        [Description("no_async_loading")]
        NoAsyncLoading = 1 << 8,
        [Description("no_mandatory_keys")]
        NoMandatoryKeys = 1 << 9,
        [Description("no_multi")]
        NoMulti = 1 << 10,
        [Description("noscript")]
        NoScript = 1 << 11,
        [Description("pubsub")]
        PubSub = 1 << 12,
        [Description("random")]
        Random = 1 << 13,
        [Description("readonly")]
        ReadOnly = 1 << 14,
        [Description("sort_for_script")]
        SortForScript = 1 << 15,
        [Description("skip_monitor")]
        SkipMonitor = 1 << 16,
        [Description("skip_slowlog")]
        SkipSlowLog = 1 << 17,
        [Description("stale")]
        Stale = 1 << 18,
        [Description("write")]
        Write = 1 << 19,
    }

    [Flags]
    public enum RespAclCategories
    {
        None = 0,
        [Description("admin")]
        Admin = 1,
        [Description("bitmap")]
        Bitmap = 1 << 1,
        [Description("blocking")]
        Blocking = 1 << 2,
        [Description("connection")]
        Connection = 1 << 3,
        [Description("dangerous")]
        Dangerous = 1 << 4,
        [Description("geo")]
        Geo = 1 << 5,
        [Description("hash")]
        Hash = 1 << 6,
        [Description("hyperloglog")]
        HyperLogLog = 1 << 7,
        [Description("fast")]
        Fast = 1 << 8,
        [Description("keyspace")]
        KeySpace = 1 << 9,
        [Description("list")]
        List = 1 << 10,
        [Description("pubsub")]
        PubSub = 1 << 11,
        [Description("read")]
        Read = 1 << 12,
        [Description("scripting")]
        Scripting = 1 << 13,
        [Description("set")]
        Set = 1 << 14,
        [Description("sortedset")]
        SortedSet = 1 << 15,
        [Description("slow")]
        Slow = 1 << 16,
        [Description("stream")]
        Stream = 1 << 17,
        [Description("string")]
        String = 1 << 18,
        [Description("transaction")]
        Transaction = 1 << 19,
        [Description("write")]
        Write = 1 << 20,
    }

    [Flags]
    public enum RespCommandOptionsNew : ushort
    {
        None = 0,
        EX = 1,
        NX = 1 << 1,
        XX = 1 << 2,
        GET = 1 << 3,
        PX = 1 << 4,
        EXAT = 1 << 5,
        PXAT = 1 << 6,
        PERSIST = 1 << 7,
        GT = 1 << 8,
        LT = 1 << 9,
    }
}
