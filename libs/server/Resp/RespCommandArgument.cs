// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text.Json.Serialization;
using System.Text.Json;

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

        protected RespCommandArgumentBase(string name, string displayText, RespCommandArgumentType type, string token, string summary, RespCommandArgumentFlags flags) : this()
        {
            Name = name;
            DisplayText = displayText;
            Type = type;
            Token = token;
            Summary = summary;
            ArgumentFlags = flags;
        }

        protected RespCommandArgumentBase()
        {
            
        }
    }

    /// <summary>
    /// Represents a RESP command's argument of type key
    /// </summary>
    public class RespCommandKeyArgument : RespCommandArgument
    {
        /// <summary>
        /// This value is available for every argument of the key type.
        /// It is a 0-based index of the specification in the command's key specifications that corresponds to the argument.
        /// </summary>
        public int KeySpecIndex { get; init; }

        public RespCommandKeyArgument(string name, string displayText, string token,
            string summary, RespCommandArgumentFlags flags, string value, int keySpecIndex) : base(name, displayText,
            RespCommandArgumentType.Key, token, summary, flags, value) => KeySpecIndex = keySpecIndex;

        public RespCommandKeyArgument()
        {
            
        }
    }

    /// <summary>
    /// Represents a RESP command's argument of all types except OneOf and Block
    /// </summary>
    public class RespCommandArgument : RespCommandArgumentBase
    {
        /// <summary>
        /// The argument's value - a string that describes the value in the command's syntax
        /// </summary>
        public string Value { get; init; }

        public RespCommandArgument(string name, string displayText, RespCommandArgumentType type, string token,
            string summary, RespCommandArgumentFlags flags, string value) : base(name, displayText, type, token,
            summary, flags) => this.Value = value;

        public RespCommandArgument()
        {
            
        }
    }

    /// <summary>
    /// Represents a RESP command's argument of type OneOf or Block
    /// </summary>
    public class RespCommandContainerArgument : RespCommandArgumentBase
    {
        /// <summary>
        /// An array of nested arguments
        /// </summary>
        public RespCommandArgumentBase[] Arguments { get; init; }

        public RespCommandContainerArgument(string name, string displayText, RespCommandArgumentType type, string token,
            string summary, RespCommandArgumentFlags flags, RespCommandArgumentBase[] arguments) : base(name,
            displayText, type, token, summary, flags)
        {
            this.Arguments = arguments;
        }

        public RespCommandContainerArgument()
        {
            
        }
    }

    /// <summary>
    /// JSON converter for objects implementing RespCommandArgumentBase
    /// </summary>
    public class RespCommandArgumentConverter : JsonConverter<RespCommandArgumentBase>
    {
        /// <inheritdoc />
        public override bool CanConvert(Type typeToConvert) => typeof(RespCommandArgumentBase).IsAssignableFrom(typeToConvert);

        /// <inheritdoc />
        public override RespCommandArgumentBase Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (!typeof(RespCommandArgumentBase).IsAssignableFrom(typeToConvert)) return null;

            if (reader.TokenType != JsonTokenType.StartObject)
            {
                throw new JsonException();
            }

            reader.Read();
            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                throw new JsonException();
            }

            var propertyName = reader.GetString();
            if (propertyName != "TypeDiscriminator")
            {
                throw new JsonException();
            }

            reader.Read();
            if (reader.TokenType != JsonTokenType.String)
            {
                throw new JsonException();
            }

            var typeDiscriminator = reader.GetString();

            string name = null;
            string displayText = null;
            var type = RespCommandArgumentType.None;
            string token = null;
            string summary = null;
            var flags = RespCommandArgumentFlags.None;
            var keySpecIdx = -1;
            string strVal = null;
            RespCommandArgumentBase[] nestedArgs = null;

            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject)
                {
                    return typeDiscriminator switch
                    {
                        nameof(RespCommandKeyArgument) => new RespCommandKeyArgument(name, displayText, token, summary, flags, strVal, keySpecIdx),
                        nameof(RespCommandContainerArgument) => new RespCommandContainerArgument(name, displayText, type, token, summary, flags, nestedArgs),
                        nameof(RespCommandArgument) => new RespCommandArgument(name, displayText, type, token, summary, flags, strVal),
                        _ => throw new JsonException()
                    };
                }

                if (reader.TokenType == JsonTokenType.PropertyName)
                {
                    propertyName = reader.GetString();
                    reader.Read();

                    switch (propertyName)
                    {
                        case nameof(RespCommandArgumentBase.Name):
                            name = reader.GetString();
                            break;
                        case nameof(RespCommandArgumentBase.DisplayText):
                            displayText = reader.GetString();
                            break;
                        case nameof(RespCommandArgumentBase.Token):
                            token = reader.GetString();
                            break;
                        case nameof(RespCommandArgumentBase.Summary):
                            summary = reader.GetString();
                            break;
                        case nameof(RespCommandArgumentBase.ArgumentFlags):
                            flags = Enum.Parse<RespCommandArgumentFlags>(reader.GetString(), true);
                            break;
                        default:
                            switch (typeDiscriminator)
                            {
                                case (nameof(RespCommandKeyArgument)):
                                    switch (propertyName)
                                    {
                                        case nameof(RespCommandKeyArgument.KeySpecIndex):
                                            keySpecIdx = reader.GetInt32();
                                            break;
                                        case nameof(RespCommandKeyArgument.Value):
                                            strVal = reader.GetString();
                                            break;
                                    }
                                    break;
                                case (nameof(RespCommandArgument)):
                                    switch (propertyName)
                                    {
                                        case nameof(RespCommandArgument.Value):
                                            strVal = reader.GetString();
                                            break;
                                    }
                                    break;
                                case (nameof(RespCommandContainerArgument)):
                                    switch (propertyName)
                                    {
                                        case nameof(RespCommandContainerArgument.Arguments):
                                            if (reader.TokenType == JsonTokenType.StartArray)
                                            {
                                                var args = new List<RespCommandArgumentBase>();

                                                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                                                {
                                                    var item = JsonSerializer.Deserialize<RespCommandArgumentBase>(ref reader, options);
                                                    args.Add(item);
                                                }

                                                nestedArgs = args.ToArray();
                                            }
                                            break;
                                    }
                                    break;
                            }
                            break;
                    }
                }
            }

            throw new JsonException();
        }

        /// <inheritdoc />
        public override void Write(Utf8JsonWriter writer, RespCommandArgumentBase cmdArg, JsonSerializerOptions options)
        {
            writer.WriteStartObject();

            writer.WriteString(nameof(RespCommandArgumentBase.Name), cmdArg.Name);
            if (cmdArg.DisplayText != null)
                writer.WriteString(nameof(RespCommandArgumentBase.DisplayText), cmdArg.DisplayText);
            writer.WriteString(nameof(RespCommandArgumentBase.Type), cmdArg.Type.ToString());
            if (cmdArg.Token != null)
                writer.WriteString(nameof(RespCommandArgumentBase.Token), cmdArg.Token);
            if (cmdArg.Summary != null)
                writer.WriteString(nameof(RespCommandArgumentBase.Summary), cmdArg.Summary);
            if (cmdArg.ArgumentFlags != RespCommandArgumentFlags.None)
                writer.WriteString(nameof(RespCommandArgumentBase.ArgumentFlags), cmdArg.ArgumentFlags.ToString());

            switch (cmdArg)
            {
                case RespCommandKeyArgument keyArg:
                    writer.WriteString("TypeDiscriminator", nameof(RespCommandKeyArgument));
                    writer.WriteNumber(nameof(RespCommandKeyArgument.KeySpecIndex), keyArg.KeySpecIndex);
                    if (keyArg.Value != null) 
                        writer.WriteString(nameof(RespCommandKeyArgument.Value), keyArg.Value);
                    break;
                case RespCommandContainerArgument containerArg:
                    if (containerArg.Arguments != null)
                    {
                        writer.WriteString("TypeDiscriminator", nameof(RespCommandContainerArgument));
                        writer.WritePropertyName(nameof(RespCommandContainerArgument.Arguments));
                        writer.WriteStartArray();
                        foreach (var arg in containerArg.Arguments)
                        {
                            JsonSerializer.Serialize(writer, arg, options);
                        }
                        writer.WriteEndArray();
                    }
                    break;
                case RespCommandArgument respCmdArg:
                    if (respCmdArg.Value != null)
                    {
                        writer.WriteString("TypeDiscriminator", nameof(RespCommandArgument));
                        writer.WriteString(nameof(RespCommandKeyArgument.Value), respCmdArg.Value);
                    }
                    break;
                default:
                    throw new JsonException();
            }

            writer.WriteEndObject();
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
