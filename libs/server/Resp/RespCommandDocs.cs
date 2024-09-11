// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.ComponentModel;

namespace Garnet.server.Resp
{
    /// <summary>
    /// Represents a RESP command's docs
    /// </summary>
    public class RespCommandDocs : IRespCommandData<RespCommandDocs>
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
        public RespCommandDocFlags DocFlags { get; init; }

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

        public RespCommandDocs()
        {
            
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
