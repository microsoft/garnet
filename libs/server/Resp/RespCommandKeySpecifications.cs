// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.ComponentModel;
using System.Text;
using System.Text.Json.Serialization;
using Garnet.common;

namespace Garnet.server.Resp
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
                sb.Append("+flags\r\n");
                sb.Append($"*{this._respFormatFlags.Length}\r\n");
                foreach (var flag in this._respFormatFlags)
                    sb.Append($"+{flag}\r\n");
            }

            if (this.BeginSearch != null)
            {
                elemCount += 2;
                sb.Append("+begin_search\r\n");
                sb.Append("*4\r\n");
                sb.Append($"+type\r\n");
                sb.Append($"+{this.BeginSearch.RespFormatType}\r\n");
                sb.Append($"+spec\r\n");
                sb.Append($"+{this.BeginSearch.RespFormatSpec}\r\n");
            }

            if (this.FindKeys != null)
            {
                elemCount += 2;
                sb.Append("+find_keys\r\n");
                sb.Append("*4\r\n");
                sb.Append($"+type\r\n");
                sb.Append($"+{this.FindKeys.RespFormatType}\r\n");
                sb.Append($"+spec\r\n");
                sb.Append($"+{this.FindKeys.RespFormatSpec}\r\n");
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

    [JsonDerivedType(typeof(BeginSearchIndex), "index")]
    [JsonDerivedType(typeof(BeginSearchKeyword), "keyword")]
    [JsonDerivedType(typeof(BeginSearchUnknown), "unknown")]
    public interface IBeginSearchKeySpec : IKeySpec
    {

    }

    public class BeginSearchIndex : IBeginSearchKeySpec
    {
        public int Index { get; init; }

        [JsonIgnore]
        public string RespFormatType => "index";

        [JsonIgnore]
        public string RespFormatSpec
        {
            get { return this._respFormatSpec ??= $"*2\r\n+index\r\n:{this.Index}"; }
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
        public string RespFormatType => "keyword";

        [JsonIgnore]
        public string RespFormatSpec
        {
            get { return this._respFormatSpec ??= $"*4\r\n+keyword\r\n:{this.Keyword}\r\n+startfrom\r\n:{this.StartFrom}"; }
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
        public string RespFormatType => "unknown";

        [JsonIgnore]
        public string RespFormatSpec
        {
            get { return this._respFormatSpec ??= $"*0\r\n"; }
        }

        private string? _respFormatSpec;
    }

    [JsonDerivedType(typeof(FindKeysRange), "range")]
    [JsonDerivedType(typeof(FindKeysKeyNum), "keynum")]
    [JsonDerivedType(typeof(FindKeysUnknown), "unknown")]
    public interface IFindKeysKeySpec : IKeySpec
    {

    }

    public class FindKeysRange : IFindKeysKeySpec
    {
        public int LastKey { get; init; }

        public int KeyStep { get; init; }

        public int Limit { get; init; }

        [JsonIgnore]
        public string RespFormatType => "range";

        [JsonIgnore]
        public string RespFormatSpec
        {
            get { return this._respFormatSpec ??= $"*6\r\n+lastkey\r\n:{this.LastKey}\r\n+keystep\r\n:{this.KeyStep}\r\n+limit\r\n:{this.Limit}"; }
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
        public string RespFormatType => "keynum";

        [JsonIgnore]
        public string RespFormatSpec
        {
            get { return this._respFormatSpec ??= $"*6\r\n+keynumidx\r\n:{this.KeyNumIdx}\r\n+firstkey\r\n:{this.FirstKey}\r\n+keystep\r\n:{this.KeyStep}"; }
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
        public string RespFormatType => "unknown";

        [JsonIgnore]
        public string RespFormatSpec
        {
            get { return this._respFormatSpec ??= $"*0\r\n"; }
        }

        private string? _respFormatSpec;
    }
}
