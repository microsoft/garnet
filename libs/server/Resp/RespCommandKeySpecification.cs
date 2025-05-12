// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Garnet.common;
using Tsavorite.core;

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
        public KeySpecMethodBase BeginSearch { get; init; }

        /// <summary>
        /// FindKeys value of a key specification tells the client how to continue the search for key names
        /// </summary>
        public KeySpecMethodBase FindKeys { get; init; }

        /// <summary>
        /// Notes about non-obvious key specs considerations
        /// </summary>
        public string Notes { get; init; }

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

        [JsonIgnore]
        public string[] RespFormatFlags => respFormatFlags;

        private readonly KeySpecificationFlags flags;
        private readonly string[] respFormatFlags;

        /// <summary>
        /// Serializes the current object to RESP format
        /// </summary>
        /// <returns>Serialized value</returns>
        public void ToRespFormat(ref RespMemoryWriter writer)
        {
            var elemCount = 0;

            if (Notes != null)
                elemCount++;

            if (Flags != KeySpecificationFlags.None)
                elemCount++;

            if (BeginSearch != null)
                elemCount++;

            if (FindKeys != null)
                elemCount++;

            writer.WriteMapLength(elemCount);

            if (Notes != null)
            {
                writer.WriteBulkString("notes"u8);
                writer.WriteAsciiBulkString(Notes);
            }

            if (Flags != KeySpecificationFlags.None)
            {
                writer.WriteBulkString("flags"u8);
                writer.WriteSetLength(respFormatFlags.Length);

                foreach (var flag in respFormatFlags)
                    writer.WriteSimpleString(flag);
            }

            if (BeginSearch != null)
            {
                BeginSearch.ToRespFormat(ref writer);
            }

            if (FindKeys != null)
            {
                FindKeys.ToRespFormat(ref writer);
            }
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
        [Description("RW")]
        RW = 1,
        [Description("RO")]
        RO = 1 << 1,
        [Description("OW")]
        OW = 1 << 2,
        [Description("RM")]
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
    /// Base class representing key specification methods
    /// </summary>
    public abstract class KeySpecMethodBase : IRespSerializable
    {
        /// <summary>
        /// Name of the key specification method
        /// </summary>
        public abstract string MethodName { get; }

        /// <inheritdoc />
        public abstract void ToRespFormat(ref RespMemoryWriter writer);
    }

    /// <summary>
    /// Base class representing BeginSearch key specification method types
    /// </summary>
    public abstract class BeginSearchKeySpecMethodBase : KeySpecMethodBase
    {
        /// <summary>
        /// Name of the key specification
        /// </summary>
        public sealed override string MethodName => "begin_search";

        /// <summary>
        /// Attempts to find the start index for key extraction based on the specified keyword.
        /// </summary>
        /// <param name="parseState">The current session parse state.</param>
        /// <param name="index">The index where the keyword is found, plus one.</param>
        /// <returns>True if the keyword is found; otherwise, false.</returns>
        public abstract bool TryGetStartIndex(ref SessionParseState parseState, out int index);
    }

    /// <summary>
    /// Represents BeginSearch key specification method of type "index"
    /// Indicates that input keys appear at a constant index
    /// </summary>
    public class BeginSearchIndex : BeginSearchKeySpecMethodBase
    {
        /// <summary>
        /// The 0-based index from which the client should start extracting key names
        /// </summary>
        public int Index { get; init; }

        /// <inheritdoc />
        public override void ToRespFormat(ref RespMemoryWriter writer)
        {
            writer.WriteAsciiBulkString(MethodName);
            writer.WriteMapLength(2);
            writer.WriteBulkString("type"u8);
            writer.WriteBulkString("index"u8);
            writer.WriteBulkString("spec"u8);
            writer.WriteMapLength(1);
            writer.WriteBulkString("index"u8);
            writer.WriteInt32(Index);
        }

        /// <inheritdoc />
        public BeginSearchIndex()
        {
        }

        /// <inheritdoc />
        public BeginSearchIndex(int index) : this()
        {
            this.Index = index;
        }

        /// <inheritdoc />
        public override bool TryGetStartIndex(ref SessionParseState parseState, out int index)
        {
            if (Index < 0)
            {
                index = parseState.Count + Index;
                return true;
            }

            index = Index;
            return true;
        }
    }

    /// <summary>
    /// Represents BeginSearch key specification method of type "keyword"
    /// Indicates that a literal token precedes key name arguments
    /// </summary>
    public class BeginSearchKeyword : BeginSearchKeySpecMethodBase
    {
        /// <summary>
        /// The keyword that marks the beginning of key name arguments
        /// </summary>
        public string Keyword { get; init; }

        /// <summary>
        /// An index to the arguments array from which the client should begin searching
        /// </summary>
        public int StartFrom { get; init; }

        /// <inheritdoc />
        public override void ToRespFormat(ref RespMemoryWriter writer)
        {
            writer.WriteAsciiBulkString(MethodName);
            writer.WriteMapLength(2);
            writer.WriteBulkString("type"u8);
            writer.WriteBulkString("keyword"u8);
            writer.WriteBulkString("spec"u8);
            writer.WriteMapLength(2);
            writer.WriteBulkString("keyword"u8);
            writer.WriteAsciiBulkString(Keyword);
            writer.WriteBulkString("startfrom"u8);
            writer.WriteInt32(StartFrom);
        }

        /// <inheritdoc />
        public BeginSearchKeyword() { }

        /// <inheritdoc />
        public BeginSearchKeyword(string keyword, int startFrom) : this()
        {
            this.Keyword = keyword;
            this.StartFrom = startFrom;
        }

        /// <inheritdoc />
        public override bool TryGetStartIndex(ref SessionParseState parseState, out int index)
        {
            var keyword = Encoding.UTF8.GetBytes(Keyword);

            // Handle negative StartFrom by converting to positive index from end
            int searchStartIndex = StartFrom < 0 ? parseState.Count + StartFrom : StartFrom;

            // Determine the search direction
            int increment = StartFrom < 0 ? -1 : 1;
            int start = searchStartIndex;
            int end = StartFrom < 0 ? -1 : parseState.Count;

            // Search for the keyword
            for (int i = start; i != end; i += increment)
            {
                if (parseState.GetArgSliceByRef(i).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(keyword))
                {
                    index = i + 1;
                    return true;
                }
            }

            index = default;
            return false;
        }
    }

    /// <summary>
    /// Represents BeginSearch key specification method of unknown type
    /// </summary>
    public class BeginSearchUnknown : BeginSearchKeySpecMethodBase
    {
        /// <inheritdoc />
        public override void ToRespFormat(ref RespMemoryWriter writer)
        {
            writer.WriteAsciiBulkString(MethodName);
            writer.WriteMapLength(2);
            writer.WriteBulkString("type"u8);
            writer.WriteBulkString("unknown"u8);
            writer.WriteBulkString("spec"u8);
            writer.WriteEmptyArray();
        }

        /// <inheritdoc />
        public override bool TryGetStartIndex(ref SessionParseState parseState, out int index)
        {
            index = default;
            return false;
        }
    }

    /// <summary>
    /// Base class representing FindKeys key specification method types
    /// </summary>
    public abstract class FindKeysKeySpecMethodBase : KeySpecMethodBase
    {
        /// <summary>
        /// Name of the key specification
        /// </summary>
        public sealed override string MethodName => "find_keys";

        /// <summary>
        /// Extracts keys from the specified parse state starting from the given index.
        /// </summary>
        /// <param name="state">The current session parse state.</param>
        /// <param name="startIndex">The index from which to start extracting keys.</param>
        /// <param name="keys">The list to which extracted keys will be added.</param>
        public abstract void ExtractKeys(ref SessionParseState state, int startIndex, List<PinnedSpanByte> keys);
    }

    /// <summary>
    /// Represents FindKeys key specification method of type "range"
    /// Indicates that keys stop at a specific index or relative to the last argument
    /// </summary>
    public class FindKeysRange : FindKeysKeySpecMethodBase
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

        /// <inheritdoc />
        public override void ToRespFormat(ref RespMemoryWriter writer)
        {
            writer.WriteAsciiBulkString(MethodName);
            writer.WriteMapLength(2);
            writer.WriteBulkString("type"u8);
            writer.WriteBulkString("range"u8);
            writer.WriteBulkString("spec"u8);
            writer.WriteMapLength(3);
            writer.WriteBulkString("lastkey"u8);
            writer.WriteInt32(LastKey);
            writer.WriteBulkString("keystep"u8);
            writer.WriteInt32(KeyStep);
            writer.WriteBulkString("limit"u8);
            writer.WriteInt32(Limit);
        }

        /// <inheritdoc />
        public FindKeysRange() { }

        /// <inheritdoc />
        public FindKeysRange(int lastKey, int keyStep, int limit) : this()
        {
            this.LastKey = lastKey;
            this.KeyStep = keyStep;
            this.Limit = limit;
        }

        /// <inheritdoc />
        public override void ExtractKeys(ref SessionParseState state, int startIndex, List<PinnedSpanByte> keys)
        {
            int lastKey;
            if (LastKey < 0)
            {
                // For negative LastKey, calculate limit based on the factor
                int availableArgs = state.Count - startIndex;
                int limitFactor = Limit <= 1 ? availableArgs : availableArgs / Limit;

                // Calculate available slots based on keyStep
                int slotsAvailable = (limitFactor + KeyStep - 1) / KeyStep;
                lastKey = startIndex + (slotsAvailable * KeyStep) - KeyStep;
                lastKey = Math.Min(lastKey, state.Count - 1);
            }
            else
            {
                lastKey = Math.Min(startIndex + LastKey, state.Count - 1);
            }

            for (int i = startIndex; i <= lastKey; i += KeyStep)
            {
                var argSlice = state.GetArgSliceByRef(i);
                if (argSlice.length > 0)
                {
                    keys.Add(argSlice);
                }
            }
        }
    }

    /// <summary>
    /// Represents FindKeys key specification method of type "keynum"
    /// Indicates that an additional argument specifies the number of input keys
    /// </summary>
    public class FindKeysKeyNum : FindKeysKeySpecMethodBase
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

        /// <inheritdoc />
        public override void ToRespFormat(ref RespMemoryWriter writer)
        {
            writer.WriteAsciiBulkString(MethodName);
            writer.WriteMapLength(2);
            writer.WriteBulkString("type"u8);
            writer.WriteBulkString("keynum"u8);
            writer.WriteBulkString("spec"u8);
            writer.WriteMapLength(3);
            writer.WriteBulkString("keynumidx"u8);
            writer.WriteInt32(KeyNumIdx);
            writer.WriteBulkString("firstkey"u8);
            writer.WriteInt32(FirstKey);
            writer.WriteBulkString("keystep"u8);
            writer.WriteInt32(KeyStep);
        }

        /// <inheritdoc />
        public FindKeysKeyNum() { }

        /// <inheritdoc />
        public FindKeysKeyNum(int keyNumIdx, int firstKey, int keyStep) : this()
        {
            this.KeyNumIdx = keyNumIdx;
            this.FirstKey = firstKey;
            this.KeyStep = keyStep;
        }

        /// <inheritdoc />
        public override void ExtractKeys(ref SessionParseState state, int startIndex, List<PinnedSpanByte> keys)
        {
            int numKeys = 0;
            int firstKey = startIndex + FirstKey;

            // Handle negative FirstKey
            if (FirstKey < 0)
                firstKey = state.Count + FirstKey;

            // Get number of keys from the KeyNumIdx
            if (KeyNumIdx >= 0)
            {
                var keyNumPos = startIndex + KeyNumIdx;
                if (keyNumPos < state.Count && state.TryGetInt(keyNumPos, out var count))
                {
                    numKeys = count;
                }
            }
            else
            {
                // Negative KeyNumIdx means count from the end
                var keyNumPos = state.Count + KeyNumIdx;
                if (keyNumPos >= 0 && state.TryGetInt(keyNumPos, out var count))
                {
                    numKeys = count;
                }
            }

            // Extract keys based on numKeys, firstKey, and keyStep
            if (numKeys > 0 && firstKey >= 0)
            {
                for (int i = 0; i < numKeys && firstKey + (i * KeyStep) < state.Count; i++)
                {
                    var keyIndex = firstKey + i * KeyStep;
                    keys.Add(state.GetArgSliceByRef(keyIndex));
                }
            }
        }
    }

    /// <summary>
    /// Represents FindKeys key specification method of unknown type
    /// </summary>
    public class FindKeysUnknown : FindKeysKeySpecMethodBase
    {
        /// <inheritdoc />
        public override void ToRespFormat(ref RespMemoryWriter writer)
        {
            writer.WriteAsciiBulkString(MethodName);
            writer.WriteMapLength(2);
            writer.WriteBulkString("type"u8);
            writer.WriteBulkString("unknown"u8);
            writer.WriteBulkString("spec"u8);
            writer.WriteEmptyArray();
        }

        /// <inheritdoc />
        public override void ExtractKeys(ref SessionParseState state, int startIndex, List<PinnedSpanByte> keys)
        {
            // Do nothing
        }
    }

    /// <summary>
    /// JSON converter for objects implementing KeySpecMethodBase
    /// </summary>
    public class KeySpecConverter : JsonConverter<KeySpecMethodBase>
    {
        /// <inheritdoc />
        public override bool CanConvert(Type typeToConvert) => typeof(KeySpecMethodBase).IsAssignableFrom(typeToConvert);

        /// <inheritdoc />
        public override KeySpecMethodBase Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (!typeof(KeySpecMethodBase).IsAssignableFrom(typeToConvert)) return null;

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

            var index = 0;
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

        /// <inheritdoc />
        public override void Write(Utf8JsonWriter writer, KeySpecMethodBase keySpecMethod, JsonSerializerOptions options)
        {
            writer.WriteStartObject();

            switch (keySpecMethod)
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