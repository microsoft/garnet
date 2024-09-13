// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.ComponentModel;
using System.Text;
using System.Text.Json.Serialization;
using Garnet.common;

namespace Garnet.server.Resp
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

        private string respFormat;
        private readonly RespCommandDocFlags docFlags;
        private readonly string[] respFormatDocFlags;

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
