// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.ComponentModel;

namespace Garnet.server.Resp
{
    /// <summary>
    /// A base class that represents a RESP command's argument
    /// </summary>
    public abstract class RespCommandArgumentBase
    {
        /// <summary>
        /// The argument's name
        /// </summary>
        public string Name { get; init; }

        /// <summary>
        /// The argument's display string
        /// </summary>
        public string DisplayText { get; init; }

        /// <summary>
        /// The argument's type
        /// </summary>
        public RespCommandArgumentType Type { get; init; }

        /// <summary>
        /// A constant literal that precedes the argument (user input) itself
        /// </summary>
        public string Token { get; init; }

        /// <summary>
        /// A short description of the argument
        /// </summary>
        public string Summary { get; init; }

        /// <summary>
        /// Argument flags
        /// </summary>
        public RespCommandArgumentFlags ArgumentFlags { get; init; }

        protected RespCommandArgumentBase(string name, string displayText, RespCommandArgumentType type, string token, string summary, RespCommandArgumentFlags flags)
        {
            Name = name;
            DisplayText = displayText;
            Type = type;
            Token = token;
            Summary = summary;
            ArgumentFlags = flags;
        }
    }

    /// <summary>
    /// Represents a RESP command's argument
    /// </summary>
    /// <typeparam name="TValue">Type of value</typeparam>
    public class RespCommandArgument<TValue> : RespCommandArgumentBase
    {
        /// <summary>
        /// The argument's value.
        /// For arguments types other than OneOf and Block, this is a string that describes the value in the command's syntax.
        /// For the OneOf and Block types, this is an array of nested arguments, each being a map as described in this section.
        /// </summary>
        public TValue Value { get; init; }

        public RespCommandArgument(string name, string displayText, RespCommandArgumentType type, string token,
            string summary, RespCommandArgumentFlags flags, TValue value) : base(name, displayText, type, token,
            summary, flags) => Value = value;
    }

    /// <summary>
    /// Represents a RESP command's argument of type key
    /// </summary>
    public class RespCommandKeyArgument : RespCommandArgument<string>
    {
        /// <summary>
        /// This value is available for every argument of the key type.
        /// It is a 0-based index of the specification in the command's key specifications that corresponds to the argument.
        /// </summary>
        public int KeySpecIndex { get; init; }

        public RespCommandKeyArgument(string name, string displayText, string token,
            string summary, RespCommandArgumentFlags flags, string value, int keySpecIndex) : base(name, displayText,
            RespCommandArgumentType.Key, token, summary, flags, value) =>
            KeySpecIndex = keySpecIndex;
    }

    /// <summary>
    /// Represents a RESP command's argument of all types except OneOf and Block
    /// </summary>
    public class RespCommandArgument : RespCommandArgument<string>
    {
        public RespCommandArgument(string name, string displayText, RespCommandArgumentType type, string token,
            string summary, RespCommandArgumentFlags flags, string value) : base(name, displayText, type, token,
            summary, flags, value)
        {
        }
    }

    /// <summary>
    /// Represents a RESP command's argument of type OneOf or Block
    /// </summary>
    public class RespCommandContainerArgument : RespCommandArgument<RespCommandArgumentBase[]>
    {
        public RespCommandContainerArgument(string name, string displayText, RespCommandArgumentType type, string token,
            string summary, RespCommandArgumentFlags flags, RespCommandArgumentBase[] value) : base(name, displayText,
            type, token, summary, flags, value)
        {
        }
    }

    /// <summary>
    /// An enum representing a RESP command argument's type
    /// </summary>
    public enum RespCommandArgumentType : byte
    {
        None,

        /// <summary>
        /// A string argument
        /// </summary>
        [Description("string")]
        String,

        /// <summary>
        /// An integer argument
        /// </summary>
        [Description("integer")]
        Integer,

        /// <summary>
        /// A double-precision argument
        /// </summary>
        [Description("double")]
        Double,

        /// <summary>
        /// A string that represents the name of a key
        /// </summary>
        [Description("key")]
        Key,

        /// <summary>
        /// A string that represents a glob-like pattern
        /// </summary>
        [Description("pattern")]
        Pattern,

        /// <summary>
        /// An integer that represents a Unix timestamp
        /// </summary>
        [Description("unix-time")]
        UnixTime,

        /// <summary>
        /// A token, meaning a reserved keyword, which may or may not be provided
        /// </summary>
        [Description("pure-token")]
        PureToken,

        /// <summary>
        /// A container for nested arguments. This type enables choice among several nested arguments.
        /// </summary>
        [Description("oneof")]
        OneOf,

        /// <summary>
        /// A container for nested arguments. This type enables grouping arguments and applying a property.
        /// </summary>
        [Description("block")]
        Block,
    }

    /// <summary>
    /// Argument flags
    /// </summary>
    [Flags]
    public enum RespCommandArgumentFlags : byte
    {
        None = 0,
        /// <summary>
        /// Denotes that the argument is optional
        /// </summary>
        [Description("optional")]
        Optional = 1,

        /// <summary>
        /// Denotes that the argument is optional
        /// </summary>
        [Description("multiple")]
        Multiple = 1 << 1,

        /// <summary>
        /// Denotes that the argument is optional
        /// </summary>
        [Description("multiple-token")]
        MultipleToken = 1 << 2,
    }
}
