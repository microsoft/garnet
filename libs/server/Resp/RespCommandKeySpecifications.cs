// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.ComponentModel;
using System.Formats.Asn1;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Garnet.common;

namespace Garnet.server
{
    public class RespCommandKeySpecifications
    {
        public IBeginSearchKeySpec? BeginSearch { get; init; }

        public IFindKeysKeySpec? FindKeys { get; init; }

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
        public string RespFormat => this._respFormat ??= this.GetRespFormat();

        private readonly KeySpecificationFlags _flags;
        private string? _respFormat;
        private readonly string[] _respFormatFlags;

        private string GetRespFormat()
        {
            var sb = new StringBuilder();
            var elemCount = 0;

            if (this.Notes != null)
            {
                elemCount += 2;
                sb.Append("+notes\r\n");
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
                sb.Append("$12\r\nbegin_search\r\n");
                sb.Append("*4\r\n");
                sb.Append("$4\r\ntype\r\n");
                sb.Append($"{this.BeginSearch.RespFormatType}\r\n");
                sb.Append("$4\r\nspec\r\n");
                sb.Append($"{this.BeginSearch.RespFormatSpec}\r\n");
            }

            if (this.FindKeys != null)
            {
                elemCount += 2;
                sb.Append("+find_keys\r\n");
                sb.Append("*4\r\n");
                sb.Append("$4\r\ntype\r\n");
                sb.Append($"{this.FindKeys.RespFormatType}\r\n");
                sb.Append("$4\r\nspec\r\n");
                sb.Append($"{this.FindKeys.RespFormatSpec}\r\n");
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

    public interface IKeySpec
    {
        string RespFormatType { get; }

        string RespFormatSpec { get; }
    }

    public interface IBeginSearchKeySpec : IKeySpec
    {

    }

    public class BeginSearchIndex : IBeginSearchKeySpec
    {
        public int Index { get; init; }

        [JsonIgnore]
        public string RespFormatType => "$5\r\nindex";

        [JsonIgnore]
        public string RespFormatSpec
        {
            get { return this._respFormatSpec ??= $"*2\r\n$5\r\nindex\r\n:{this.Index}"; }
        }

        private string? _respFormatSpec;

        public BeginSearchIndex() { }

        public BeginSearchIndex(int index) : this()
        {
            this.Index = index;
        }
    }

    public class BeginSearchKeyword : IBeginSearchKeySpec
    {
        public string? Keyword { get; init; }

        public int StartFrom { get; init; }

        [JsonIgnore]
        public string RespFormatType => "$7\r\nkeyword";

        [JsonIgnore]
        public string RespFormatSpec
        {
            get { return this._respFormatSpec ??= $"*4\r\n$7\r\nkeyword\r\n${this.Keyword?.Length ?? 0}\r\n{this.Keyword}\r\n$9\r\nstartfrom\r\n:{this.StartFrom}"; }
        }

        private string? _respFormatSpec;

        public BeginSearchKeyword() { }

        public BeginSearchKeyword(string? keyword, int startFrom) : this()
        {
            this.Keyword = keyword;
            this.StartFrom = startFrom;
        }
    }

    public class BeginSearchUnknown : IBeginSearchKeySpec
    {
        [JsonIgnore]
        public string RespFormatType => "$7\r\nunknown";

        [JsonIgnore]
        public string RespFormatSpec
        {
            get { return this._respFormatSpec ??= $"*0\r\n"; }
        }

        private string? _respFormatSpec;
    }

    public interface IFindKeysKeySpec : IKeySpec
    {

    }

    public class FindKeysRange : IFindKeysKeySpec
    {
        public int LastKey { get; init; }

        public int KeyStep { get; init; }

        public int Limit { get; init; }

        [JsonIgnore]
        public string RespFormatType => "$5\r\nrange";

        [JsonIgnore]
        public string RespFormatSpec
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

    public class FindKeysKeyNum : IFindKeysKeySpec
    {
        public int KeyNumIdx { get; init; }

        public int FirstKey { get; init; }

        public int KeyStep { get; init; }

        [JsonIgnore]
        public string RespFormatType => "$6\r\nkeynum";

        [JsonIgnore]
        public string RespFormatSpec
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

    public class FindKeysUnknown : IFindKeysKeySpec
    {
        [JsonIgnore]
        public string RespFormatType => "$7\r\nunknown";

        [JsonIgnore]
        public string RespFormatSpec
        {
            get { return this._respFormatSpec ??= $"*0\r\n"; }
        }

        private string? _respFormatSpec;
    }

    public class KeySpecConverter : JsonConverter<IKeySpec>
    {
        public override bool CanConvert(Type typeToConvert) => typeof(IKeySpec).IsAssignableFrom(typeToConvert);

        public override IKeySpec Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (!typeof(IKeySpec).IsAssignableFrom(typeToConvert)) return null;

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

        public override void Write(Utf8JsonWriter writer, IKeySpec keySpec, JsonSerializerOptions options)
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
