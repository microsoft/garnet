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
    public class RespCommandKeySpecification : IRespSerializable
    {
        public KeySpecBase BeginSearch { get; init; }

        public KeySpecBase FindKeys { get; init; }

        public string? Notes { get; init; }

        public KeySpecificationFlags Flags
        {
            get => this._flags;
            init
            {
                this._flags = value;
                this._respFormatFlags = EnumUtils.GetEnumDescriptions(this._flags);
            }
        }

        [JsonIgnore]
        public string RespFormat => _respFormat ??= ToRespFormat();

        private string _respFormat;
        private KeySpecificationFlags _flags;
        private readonly string[] _respFormatFlags;

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
                sb.Append($"*{this._respFormatFlags.Length}\r\n");
                foreach (var flag in this._respFormatFlags)
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

    public abstract class KeySpecBase : IRespSerializable
    {
        public abstract string KeySpecName { get; }

        public abstract string RespFormatType { get; }

        public abstract string RespFormatSpec { get; }

        public string RespFormat => _respFormat ??= ToRespFormat();

        private string _respFormat;

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

    public abstract class BeginSearchKeySpecBase : KeySpecBase
    {
        public sealed override string KeySpecName => "begin_search";
    }

    public class BeginSearchIndex : BeginSearchKeySpecBase
    {
        public int Index { get; init; }

        [JsonIgnore]
        public sealed override string RespFormatType => "$5\r\nindex";

        [JsonIgnore]
        public sealed override string RespFormatSpec
        {
            get { return this._respFormatSpec ??= $"*2\r\n$5\r\nindex\r\n:{this.Index}"; }
        }

        private string _respFormatSpec;

        public BeginSearchIndex()
        {
        }

        public BeginSearchIndex(int index) : this()
        {
            this.Index = index;
        }
    }

    public class BeginSearchKeyword : BeginSearchKeySpecBase
    {
        public string Keyword { get; init; }

        public int StartFrom { get; init; }

        [JsonIgnore]
        public sealed override string RespFormatType => "$7\r\nkeyword";

        [JsonIgnore]
        public sealed override string RespFormatSpec
        {
            get { return this._respFormatSpec ??= $"*4\r\n$7\r\nkeyword\r\n${this.Keyword?.Length ?? 0}\r\n{this.Keyword}\r\n$9\r\nstartfrom\r\n:{this.StartFrom}"; }
        }

        private string _respFormatSpec;

        public BeginSearchKeyword() { }

        public BeginSearchKeyword(string? keyword, int startFrom) : this()
        {
            this.Keyword = keyword;
            this.StartFrom = startFrom;
        }
    }

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

    public abstract class FindKeysKeySpecBase : KeySpecBase
    {
        public sealed override string KeySpecName => "find_keys";
    }

    public class FindKeysRange : FindKeysKeySpecBase
    {
        public int LastKey { get; init; }

        public int KeyStep { get; init; }

        public int Limit { get; init; }

        [JsonIgnore]
        public sealed override string RespFormatType => "$5\r\nrange";

        [JsonIgnore]
        public sealed override string RespFormatSpec
        {
            get { return this._respFormatSpec ??= $"*6\r\n$7\r\nlastkey\r\n:{this.LastKey}\r\n$7\r\nkeystep\r\n:{this.KeyStep}\r\n$5\r\nlimit\r\n:{this.Limit}"; }
        }

        private string? _respFormatSpec;

        public FindKeysRange() { }

        public FindKeysRange(int lastKey, int keyStep, int limit) : this()
        {
            this.LastKey = lastKey;
            this.KeyStep = keyStep;
            this.Limit = limit;
        }
    }

    public class FindKeysKeyNum : FindKeysKeySpecBase
    {
        public int KeyNumIdx { get; init; }

        public int FirstKey { get; init; }

        public int KeyStep { get; init; }

        [JsonIgnore]
        public sealed override string RespFormatType => "$6\r\nkeynum";

        [JsonIgnore]
        public sealed override string RespFormatSpec
        {
            get { return this._respFormatSpec ??= $"*6\r\n$9\r\nkeynumidx\r\n:{this.KeyNumIdx}\r\n$8\r\nfirstkey\r\n:{this.FirstKey}\r\n$7\r\nkeystep\r\n:{this.KeyStep}"; }
        }

        private string? _respFormatSpec;

        public FindKeysKeyNum() { }

        public FindKeysKeyNum(int keyNumIdx, int firstKey, int keyStep) : this()
        {
            this.KeyNumIdx = keyNumIdx;
            this.FirstKey = firstKey;
            this.KeyStep = keyStep;
        }
    }

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