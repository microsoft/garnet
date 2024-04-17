// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.ComponentModel;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Represents a RESP command's key specification
    /// A key specification describes a rule for extracting the names of one or more keys from the arguments of a given command
    /// </summary>
    public class RespCommandKeySpecification : IRespSerializable
    {
        /// <summary>
        /// BeginSearch value of a specification informs the client of the extraction's beginning
        /// </summary>
        public KeySpecBase BeginSearch { get; init; }

        /// <summary>
        /// FindKeys value of a key specification tells the client how to continue the search for key names
        /// </summary>
        public KeySpecBase FindKeys { get; init; }

        /// <summary>
        /// Notes about non-obvious key specs considerations
        /// </summary>
        public string? Notes { get; init; }

        /// <summary>
        /// Flags that provide more details about the key
        /// </summary>
        public KeySpecificationFlags Flags
        {
            get => this.flags;
            init
            {
                this.flags = value;
                this.respFormatFlags = EnumUtils.GetEnumDescriptions(this.flags);
            }
        }

        /// <summary>
        /// Returns the serialized representation of the current object in RESP format
        /// This property returns a cached value, if exists (this value should never change after object initialization)
        /// </summary>
        [JsonIgnore]
        public string RespFormat => respFormat ??= ToRespFormat();

        private string respFormat;
        private readonly KeySpecificationFlags flags;
        private readonly string[] respFormatFlags;

        /// <summary>
        /// Serializes the current object to RESP format
        /// </summary>
        /// <returns>Serialized value</returns>
        public string ToRespFormat()
        {
            var sb = new StringBuilder();
            var elemCount = 0;

            if (this.Notes != null)
            {
                elemCount += 2;
                sb.Append("$5\r\nnotes\r\n");
                sb.Append($"${this.Notes.Length}\r\n{this.Notes}\r\n");
            }

            if (this.Flags != KeySpecificationFlags.None)
            {
                elemCount += 2;
                sb.Append("$5\r\nflags\r\n");
                sb.Append($"*{this.respFormatFlags.Length}\r\n");
                foreach (var flag in this.respFormatFlags)
                    sb.Append($"+{flag}\r\n");
            }

            if (this.BeginSearch != null)
            {
                elemCount += 2;
                sb.Append(this.BeginSearch.RespFormat);
            }

            if (this.FindKeys != null)
            {
                elemCount += 2;
                sb.Append(this.FindKeys.RespFormat);
            }

            return $"*{elemCount}\r\n{sb}";
        }
    }

    /// <summary>
    /// RESP key specification flags
    /// </summary>
    [Flags]
    public enum KeySpecificationFlags : ushort
    {
        None = 0,

        // Access type flags 
        RW = 1,
        RO = 1 << 1,
        OW = 1 << 2,
        RM = 1 << 3,

        // Logical operation flags
        [Description("access")]
        Access = 1 << 4,
        [Description("update")]
        Update = 1 << 5,
        [Description("insert")]
        Insert = 1 << 6,
        [Description("delete")]
        Delete = 1 << 7,

        // Miscellaneous flags
        [Description("not_key")]
        NotKey = 1 << 8,
        [Description("incomplete")]
        Incomplete = 1 << 9,
        [Description("variable_flags")]
        VariableFlags = 1 << 10,
    }

    /// <summary>
    /// Base class representing BeginSearch / FindKeys key specification types
    /// </summary>
    public abstract class KeySpecBase : IRespSerializable
    {
        /// <summary>
        /// Name of the key specification (begin_search / find_keys)
        /// </summary>
        public abstract string KeySpecName { get; }

        /// <summary>
        /// Type of the key specification in RESP format
        /// </summary>
        public abstract string RespFormatType { get; }

        /// <summary>
        /// Spec of the key specification in RESP format
        /// </summary>
        public abstract string RespFormatSpec { get; }

        /// <summary>
        /// Returns the serialized representation of the current object in RESP format
        /// This property returns a cached value, if exists (this value should never change after object initialization)
        /// </summary>
        public string RespFormat => respFormat ??= ToRespFormat();

        private string respFormat;

        /// <summary>
        /// Serializes the current object to RESP format
        /// </summary>
        /// <returns>Serialized value</returns>
        public string ToRespFormat()
        {
            var sb = new StringBuilder();
            sb.Append($"${this.KeySpecName.Length}\r\n{this.KeySpecName}\r\n");
            sb.Append("*4\r\n");
            sb.Append("$4\r\ntype\r\n");
            sb.Append($"{this.RespFormatType}\r\n");
            sb.Append("$4\r\nspec\r\n");
            sb.Append($"{this.RespFormatSpec}\r\n");
            return sb.ToString();
        }
    }

    /// <summary>
    /// Base class representing BeginSearch key specification types
    /// </summary>
    public abstract class BeginSearchKeySpecBase : KeySpecBase
    {
        /// <summary>
        /// Name of the key specification
        /// </summary>
        public sealed override string KeySpecName => "begin_search";
    }

    /// <summary>
    /// Represents BeginSearch key specification of type "index"
    /// Indicates that input keys appear at a constant index
    /// </summary>
    public class BeginSearchIndex : BeginSearchKeySpecBase
    {
        /// <summary>
        /// The 0-based index from which the client should start extracting key names
        /// </summary>
        public int Index { get; init; }

        [JsonIgnore]
        public sealed override string RespFormatType => "$5\r\nindex";

        [JsonIgnore]
        public sealed override string RespFormatSpec
        {
            get { return this.respFormatSpec ??= $"*2\r\n$5\r\nindex\r\n:{this.Index}"; }
        }

        private string respFormatSpec;

        public BeginSearchIndex()
        {
        }

        public BeginSearchIndex(int index) : this()
        {
            this.Index = index;
        }
    }

    /// <summary>
    /// Represents BeginSearch key specification of type "keyword"
    /// Indicates that a literal token precedes key name arguments
    /// </summary>
    public class BeginSearchKeyword : BeginSearchKeySpecBase
    {
        /// <summary>
        /// The keyword that marks the beginning of key name arguments
        /// </summary>
        public string Keyword { get; init; }

        /// <summary>
        /// An index to the arguments array from which the client should begin searching
        /// </summary>
        public int StartFrom { get; init; }

        [JsonIgnore]
        public sealed override string RespFormatType => "$7\r\nkeyword";

        [JsonIgnore]
        public sealed override string RespFormatSpec
        {
            get { return this.respFormatSpec ??= $"*4\r\n$7\r\nkeyword\r\n${this.Keyword?.Length ?? 0}\r\n{this.Keyword}\r\n$9\r\nstartfrom\r\n:{this.StartFrom}"; }
        }

        private string respFormatSpec;

        public BeginSearchKeyword() { }

        public BeginSearchKeyword(string? keyword, int startFrom) : this()
        {
            this.Keyword = keyword;
            this.StartFrom = startFrom;
        }
    }

    /// <summary>
    /// Represents BeginSearch key specification of unknown type
    /// </summary>
    public class BeginSearchUnknown : BeginSearchKeySpecBase
    {
        [JsonIgnore]
        public sealed override string RespFormatType => "$7\r\nunknown";

        [JsonIgnore]
        public sealed override string RespFormatSpec
        {
            get { return this._respFormatSpec ??= $"*0"; }
        }

        private string? _respFormatSpec;
    }

    /// <summary>
    /// Base class representing FindKeys key specification types
    /// </summary>
    public abstract class FindKeysKeySpecBase : KeySpecBase
    {
        /// <summary>
        /// Name of the key specification
        /// </summary>
        public sealed override string KeySpecName => "find_keys";
    }

    /// <summary>
    /// Represents FindKeys key specification of type "range"
    /// Indicates that keys stop at a specific index or relative to the last argument
    /// </summary>
    public class FindKeysRange : FindKeysKeySpecBase
    {
        /// <summary>
        /// The index, relative to BeginSearch, of the last key argument
        /// </summary>
        public int LastKey { get; init; }

        /// <summary>
        /// The number of arguments that should be skipped, after finding a key, to find the next one
        /// </summary>
        public int KeyStep { get; init; }

        /// <summary>
        /// If LastKey is has the value of -1, Limit is used to stop the search by a factor
        /// </summary>
        public int Limit { get; init; }

        [JsonIgnore]
        public sealed override string RespFormatType => "$5\r\nrange";

        [JsonIgnore]
        public sealed override string RespFormatSpec
        {
            get { return this.respFormatSpec ??= $"*6\r\n$7\r\nlastkey\r\n:{this.LastKey}\r\n$7\r\nkeystep\r\n:{this.KeyStep}\r\n$5\r\nlimit\r\n:{this.Limit}"; }
        }

        private string? respFormatSpec;

        public FindKeysRange() { }

        public FindKeysRange(int lastKey, int keyStep, int limit) : this()
        {
            this.LastKey = lastKey;
            this.KeyStep = keyStep;
            this.Limit = limit;
        }
    }

    /// <summary>
    /// Represents FindKeys key specification of type "keynum"
    /// Indicates that an additional argument specifies the number of input keys
    /// </summary>
    public class FindKeysKeyNum : FindKeysKeySpecBase
    {
        /// <summary>
        /// The index, relative to BeginSearch, of the argument containing the number of keys
        /// </summary>
        public int KeyNumIdx { get; init; }

        /// <summary>
        /// The index, relative to BeginSearch, of the first key
        /// </summary>
        public int FirstKey { get; init; }

        /// <summary>
        /// The number of arguments that should be skipped, after finding a key, to find the next one
        /// </summary>
        public int KeyStep { get; init; }

        [JsonIgnore]
        public sealed override string RespFormatType => "$6\r\nkeynum";

        [JsonIgnore]
        public sealed override string RespFormatSpec
        {
            get { return this.respFormatSpec ??= $"*6\r\n$9\r\nkeynumidx\r\n:{this.KeyNumIdx}\r\n$8\r\nfirstkey\r\n:{this.FirstKey}\r\n$7\r\nkeystep\r\n:{this.KeyStep}"; }
        }

        private string? respFormatSpec;

        public FindKeysKeyNum() { }

        public FindKeysKeyNum(int keyNumIdx, int firstKey, int keyStep) : this()
        {
            this.KeyNumIdx = keyNumIdx;
            this.FirstKey = firstKey;
            this.KeyStep = keyStep;
        }
    }

    /// <summary>
    /// Represents FindKeys key specification of unknown type
    /// </summary>
    public class FindKeysUnknown : FindKeysKeySpecBase
    {
        [JsonIgnore]
        public sealed override string RespFormatType => "$7\r\nunknown";

        [JsonIgnore]
        public sealed override string RespFormatSpec
        {
            get { return this._respFormatSpec ??= $"*0"; }
        }

        private string? _respFormatSpec;
    }

    /// <summary>
    /// JSON converter for objects implementing KeySpecBase
    /// </summary>
    public class KeySpecConverter : JsonConverter<KeySpecBase>
    {
        public override bool CanConvert(Type typeToConvert) => typeof(KeySpecBase).IsAssignableFrom(typeToConvert);

        public override KeySpecBase Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (!typeof(KeySpecBase).IsAssignableFrom(typeToConvert)) return null;

            if (reader.TokenType != JsonTokenType.StartObject)
            {
                throw new JsonException();
            }

            reader.Read();
            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                throw new JsonException();
            }

            string? propertyName = reader.GetString();
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

            int index = 0;
            string keyword = null;
            var startFrom = 0;
            var lastKey = 0;
            var keyStep = 0;
            var limit = 0;
            var keyNumIdx = 0;
            var firstKey = 0;

            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject)
                {
                    return typeDiscriminator switch
                    {
                        nameof(BeginSearchIndex) => new BeginSearchIndex(index),
                        nameof(BeginSearchKeyword) => new BeginSearchKeyword(keyword, startFrom),
                        nameof(BeginSearchUnknown) => new BeginSearchUnknown(),
                        nameof(FindKeysRange) => new FindKeysRange(lastKey, keyStep, limit),
                        nameof(FindKeysKeyNum) => new FindKeysKeyNum(keyNumIdx, firstKey, keyStep),
                        nameof(FindKeysUnknown) => new FindKeysUnknown(),
                        _ => throw new JsonException()
                    };
                }

                if (reader.TokenType == JsonTokenType.PropertyName)
                {
                    propertyName = reader.GetString();
                    reader.Read();

                    switch (typeDiscriminator)
                    {
                        case (nameof(BeginSearchIndex)):
                            switch (propertyName)
                            {
                                case nameof(BeginSearchIndex.Index):
                                    index = reader.GetInt32();
                                    break;
                            }

                            break;
                        case (nameof(BeginSearchKeyword)):
                            switch (propertyName)
                            {
                                case nameof(BeginSearchKeyword.Keyword):
                                    keyword = reader.GetString();
                                    break;
                                case nameof(BeginSearchKeyword.StartFrom):
                                    startFrom = reader.GetInt32();
                                    break;
                            }

                            break;
                        case (nameof(FindKeysRange)):
                            switch (propertyName)
                            {
                                case nameof(FindKeysRange.LastKey):
                                    lastKey = reader.GetInt32();
                                    break;
                                case nameof(FindKeysRange.KeyStep):
                                    keyStep = reader.GetInt32();
                                    break;
                                case nameof(FindKeysRange.Limit):
                                    limit = reader.GetInt32();
                                    break;
                            }
                            break;
                        case (nameof(FindKeysKeyNum)):
                            switch (propertyName)
                            {
                                case nameof(FindKeysKeyNum.KeyNumIdx):
                                    keyNumIdx = reader.GetInt32();
                                    break;
                                case nameof(FindKeysKeyNum.FirstKey):
                                    firstKey = reader.GetInt32();
                                    break;
                                case nameof(FindKeysKeyNum.KeyStep):
                                    keyStep = reader.GetInt32();
                                    break;
                            }
                            break;
                    }
                }
            }

            throw new JsonException();
        }

        public override void Write(Utf8JsonWriter writer, KeySpecBase keySpec, JsonSerializerOptions options)
        {
            writer.WriteStartObject();

            switch (keySpec)
            {
                case BeginSearchIndex beginSearchIndex:
                    writer.WriteString("TypeDiscriminator", nameof(BeginSearchIndex));
                    writer.WriteNumber(nameof(BeginSearchIndex.Index), beginSearchIndex.Index);
                    break;
                case BeginSearchKeyword beginSearchKeyword:
                    writer.WriteString("TypeDiscriminator", nameof(BeginSearchKeyword));
                    writer.WriteString(nameof(beginSearchKeyword.Keyword), beginSearchKeyword.Keyword);
                    writer.WriteNumber(nameof(beginSearchKeyword.StartFrom), beginSearchKeyword.StartFrom);
                    break;
                case BeginSearchUnknown beginSearchUnknown:
                    writer.WriteString("TypeDiscriminator", nameof(BeginSearchUnknown));
                    break;
                case FindKeysRange findKeysRange:
                    writer.WriteString("TypeDiscriminator", nameof(FindKeysRange));
                    writer.WriteNumber(nameof(FindKeysRange.LastKey), findKeysRange.LastKey);
                    writer.WriteNumber(nameof(FindKeysRange.KeyStep), findKeysRange.KeyStep);
                    writer.WriteNumber(nameof(FindKeysRange.Limit), findKeysRange.Limit);
                    break;
                case FindKeysKeyNum findKeysKeyNum:
                    writer.WriteString("TypeDiscriminator", nameof(FindKeysKeyNum));
                    writer.WriteNumber(nameof(FindKeysKeyNum.KeyNumIdx), findKeysKeyNum.KeyNumIdx);
                    writer.WriteNumber(nameof(FindKeysKeyNum.FirstKey), findKeysKeyNum.FirstKey);
                    writer.WriteNumber(nameof(FindKeysKeyNum.KeyStep), findKeysKeyNum.KeyStep);
                    break;
                case FindKeysUnknown findKeysUnknown:
                    writer.WriteString("TypeDiscriminator", nameof(FindKeysUnknown));
                    break;
                default:
                    throw new JsonException();
            }

            writer.WriteEndObject();
        }
    }
}