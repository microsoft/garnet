// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text.Json.Serialization;
using System.Text.Json;
using Garnet.common;
using System.Text;

namespace Garnet.server.Resp
{
    /// <summary>
    /// A base class that represents a RESP command's argument
    /// </summary>
    public abstract class RespCommandArgumentBase : IRespSerializable
    {
        /// <summary>
        /// The argument's name
        /// </summary>
        public string Name { get; init; }

        /// <summary>
        /// The argument's type
        /// </summary>
        public RespCommandArgumentType Type { get; init; }

        /// <summary>
        /// The argument's display string
        /// </summary>
        public string DisplayText { get; init; }

        /// <summary>
        /// A constant literal that precedes the argument (user input) itself
        /// </summary>
        public string Token { get; init; }

        /// <summary>
        /// A short description of the argument
        /// </summary>
        public string Summary { get; init; }

        protected int ArgCount { get; set;}

        /// <summary>
        /// Argument flags
        /// </summary>
        public RespCommandArgumentFlags ArgumentFlags
        {
            get => argFlags;
            init
            {
                argFlags = value;
                respFormatArgFlags = EnumUtils.GetEnumDescriptions(argFlags);
            }
        }

        /// <summary>
        /// Returns the serialized representation of the current object in RESP format
        /// This property returns a cached value, if exists (this value should never change after object initialization)
        /// </summary>
        [JsonIgnore]
        public string RespFormat => respFormat ??= ToRespFormat();

        private string respFormat;
        private readonly RespCommandArgumentFlags argFlags;
        private readonly string[] respFormatArgFlags;

        protected RespCommandArgumentBase(string name, string displayText, RespCommandArgumentType type, string token, string summary, RespCommandArgumentFlags flags) : this()
        {
            Name = name;
            DisplayText = displayText;
            Type = type;
            Token = token;
            Summary = summary;
            ArgumentFlags = flags;
        }

        /// <summary>
        /// Empty constructor for JSON deserialization
        /// </summary>
        protected RespCommandArgumentBase()
        {
            ArgCount = 0;
        }

        /// <inheritdoc />
        public virtual string ToRespFormat()
        {
            var sb = new StringBuilder();

            var key = "name";
            sb.Append($"${key.Length}\r\n{key}\r\n");
            sb.Append($"${this.Name.Length}\r\n{this.Name}\r\n");
            ArgCount += 2;

            key = "type";
            sb.Append($"${key.Length}\r\n{key}\r\n");
            var respType = EnumUtils.GetEnumDescriptions(this.Type)[0];
            sb.Append($"${respType.Length}\r\n{respType}\r\n");
            ArgCount += 2;

            if (this.DisplayText != null)
            {
                key = "display_text";
                sb.Append($"${key.Length}\r\n{key}\r\n");
                sb.Append($"${this.DisplayText.Length}\r\n{this.DisplayText}\r\n");
                ArgCount += 2;
            }

            if (this.Token != null)
            {
                key = "token";
                sb.Append($"${key.Length}\r\n{key}\r\n");
                sb.Append($"${this.Token.Length}\r\n{this.Token}\r\n");
                ArgCount += 2;
            }

            if (this.Summary != null)
            {
                key = "summary";
                sb.Append($"${key.Length}\r\n{key}\r\n");
                sb.Append($"${this.Summary.Length}\r\n{this.Summary}\r\n");
                ArgCount += 2;
            }

            if (this.ArgumentFlags != RespCommandArgumentFlags.None)
            {
                key = "flags";
                sb.Append($"${key.Length}\r\n{key}\r\n");
                sb.Append($"*{respFormatArgFlags.Length}\r\n");
                foreach (var respArgFlag in respFormatArgFlags)
                {
                    sb.Append($"+{respArgFlag}\r\n");
                }

                ArgCount += 2;
            }

            return sb.ToString();
        }
    }

    /// <summary>
    /// Represents a RESP command's argument of type key
    /// </summary>
    public sealed class RespCommandKeyArgument : RespCommandArgumentBase
    {
        /// <summary>
        /// The argument's value - a string that describes the value in the command's syntax
        /// </summary>
        public string Value { get; init; }

        /// <summary>
        /// This value is available for every argument of the key type.
        /// It is a 0-based index of the specification in the command's key specifications that corresponds to the argument.
        /// </summary>
        public int KeySpecIndex { get; init; }

        public RespCommandKeyArgument(string name, string displayText, string token,
            string summary, RespCommandArgumentFlags flags, string value, int keySpecIndex) : base(name, displayText,
            RespCommandArgumentType.Key, token, summary, flags)
        {
            Value = value;
            KeySpecIndex = keySpecIndex;
        }

        /// <inheritdoc />
        public RespCommandKeyArgument()
        {
            
        }

        /// <inheritdoc />
        public override string ToRespFormat()
        {
            var baseRespFormat = base.ToRespFormat();
            var sb = new StringBuilder();
            sb.Append(baseRespFormat);
            var key = "key_spec_index";
            sb.Append($"${key.Length}\r\n{key}\r\n");
            sb.Append($":{KeySpecIndex}\r\n");
            ArgCount += 2;
            sb.Insert(0, $"*{ArgCount}\r\n");
            return sb.ToString();
        }
    }

    /// <summary>
    /// Represents a RESP command's argument of all types except OneOf and Block
    /// </summary>
    public abstract class RespCommandArgument : RespCommandArgumentBase
    {
        /// <summary>
        /// The argument's value - a string that describes the value in the command's syntax
        /// </summary>
        public string Value { get; init; }

        protected RespCommandArgument(string name, string displayText, RespCommandArgumentType type, string token,
            string summary, RespCommandArgumentFlags flags, string value) : base(name, displayText, type, token,
            summary, flags) => this.Value = value;

        protected RespCommandArgument()
        {
            
        }

        /// <inheritdoc />
        public override string ToRespFormat()
        {
            var baseRespFormat = base.ToRespFormat();
            if (Value == null) return baseRespFormat;

            var sb = new StringBuilder();
            sb.Append(baseRespFormat);
            var key = "value";
            sb.Append($"${key.Length}\r\n{key}\r\n");
            sb.Append($"${Value.Length}\r\n{Value}\r\n");
            ArgCount += 2;
            return sb.ToString();
        }
    }

    /// <summary>
    /// Represents a RESP command's argument of all types except OneOf and Block
    /// </summary>
    public sealed class RespCommandBasicArgument : RespCommandArgument
    {
        public RespCommandBasicArgument(string name, string displayText, RespCommandArgumentType type, string token,
            string summary, RespCommandArgumentFlags flags, string value) : base(name, displayText, type, token,
            summary, flags, value)
        {

        }

        /// <inheritdoc />
        public RespCommandBasicArgument()
        {

        }

        /// <inheritdoc />
        public override string ToRespFormat()
        {
            var baseRespFormat = base.ToRespFormat();
            var sb = new StringBuilder();
            sb.Append($"*{ArgCount}\r\n");
            sb.Append(baseRespFormat);
            return sb.ToString();
        }
    }

    /// <summary>
    /// Represents a RESP command's argument of type OneOf or Block
    /// </summary>
    public sealed class RespCommandContainerArgument : RespCommandArgumentBase
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

        /// <inheritdoc />
        public RespCommandContainerArgument()
        {
            
        }

        /// <inheritdoc />
        public override string ToRespFormat()
        {
            var baseRespFormat = base.ToRespFormat();
            var sb = new StringBuilder();
            sb.Append(baseRespFormat);
            if (Arguments != null)
            {
                var key = "arguments";
                sb.Append($"${key.Length}\r\n{key}\r\n");
                sb.Append($"*{Arguments.Length}\r\n");
                foreach (var argument in Arguments)
                {
                    sb.Append(argument.RespFormat);
                }
                
                ArgCount += 2;
            }
            
            sb.Insert(0, $"*{ArgCount}\r\n");
            return sb.ToString();
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
                        nameof(RespCommandBasicArgument) => new RespCommandBasicArgument(name, displayText, type, token, summary, flags, strVal),
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
                        case nameof(RespCommandArgumentBase.Type):
                            type = Enum.Parse<RespCommandArgumentType>(reader.GetString(), true);
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
                                case (nameof(RespCommandBasicArgument)):
                                    switch (propertyName)
                                    {
                                        case nameof(RespCommandBasicArgument.Value):
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

            writer.WriteString("TypeDiscriminator", cmdArg.GetType().Name);

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
                    writer.WriteNumber(nameof(RespCommandKeyArgument.KeySpecIndex), keyArg.KeySpecIndex);
                    if (keyArg.Value != null) 
                        writer.WriteString(nameof(RespCommandKeyArgument.Value), keyArg.Value);
                    break;
                case RespCommandContainerArgument containerArg:
                    if (containerArg.Arguments != null)
                    {
                        writer.WritePropertyName(nameof(RespCommandContainerArgument.Arguments));
                        writer.WriteStartArray();
                        foreach (var arg in containerArg.Arguments)
                        {
                            JsonSerializer.Serialize(writer, arg, options);
                        }
                        writer.WriteEndArray();
                    }
                    break;
                case RespCommandBasicArgument respCmdArg:
                    if (respCmdArg.Value != null)
                    {
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
