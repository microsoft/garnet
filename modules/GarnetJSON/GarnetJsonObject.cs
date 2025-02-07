// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;
using Garnet.server;
using GarnetJSON.JSONPath;

namespace GarnetJSON
{
    /// <summary>
    /// Represents a factory for creating instances of <see cref="GarnetJsonObject"/>.
    /// </summary>
    public class GarnetJsonObjectFactory : CustomObjectFactory
    {
        /// <summary>
        /// Creates a new instance of <see cref="GarnetJsonObject"/> with the specified type.
        /// </summary>
        /// <param name="type">The type of the object.</param>
        /// <returns>A new instance of <see cref="GarnetJsonObject"/>.</returns>
        public override CustomObjectBase Create(byte type)
            => new GarnetJsonObject(type);

        /// <summary>
        /// Deserializes a <see cref="GarnetJsonObject"/> from the specified binary reader.
        /// </summary>
        /// <param name="type">The type of the object.</param>
        /// <param name="reader">The binary reader to deserialize from.</param>
        /// <returns>A deserialized instance of <see cref="GarnetJsonObject"/>.</returns>
        public override CustomObjectBase Deserialize(byte type, BinaryReader reader)
            => new GarnetJsonObject(type, reader);
    }

    /// <summary>
    /// Represents a JSON object that supports SET and GET operations using JSON path.
    /// </summary>
    public class GarnetJsonObject : CustomObjectBase
    {
        private static readonly JsonSerializerOptions DefaultJsonSerializerOptions = new JsonSerializerOptions { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };
        private static readonly JsonSerializerOptions IndentedJsonSerializerOptions = new JsonSerializerOptions { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping, WriteIndented = true };

        private JsonNode? rootNode;

        /// <summary>
        /// Initializes a new instance of the <see cref="GarnetJsonObject"/> class with the specified type.
        /// </summary>
        /// <param name="type">The type of the object.</param>
        public GarnetJsonObject(byte type)
            : base(type, 0, MemoryUtils.DictionaryOverhead)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="GarnetJsonObject"/> class by deserializing from the specified binary reader.
        /// </summary>
        /// <param name="type">The type of the object.</param>
        /// <param name="reader">The binary reader to deserialize from.</param>
        public GarnetJsonObject(byte type, BinaryReader reader)
            : base(type, reader)
        {
            Debug.Assert(reader != null);

            var jsonString = reader.ReadString();
            rootNode = JsonNode.Parse(jsonString);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="GarnetJsonObject"/> class by cloning another <see cref="GarnetJsonObject"/> instance.
        /// </summary>
        /// <param name="obj">The <see cref="GarnetJsonObject"/> instance to clone.</param>
        public GarnetJsonObject(GarnetJsonObject obj)
            : base(obj)
        {
            rootNode = obj.rootNode;
        }

        /// <summary>
        /// Creates a new instance of <see cref="GarnetJsonObject"/> that is a clone of the current instance.
        /// </summary>
        /// <returns>A new instance of <see cref="GarnetJsonObject"/> that is a clone of the current instance.</returns>
        public override CustomObjectBase CloneObject() => new GarnetJsonObject(this);

        /// <summary>
        /// Serializes the <see cref="GarnetJsonObject"/> to the specified binary writer.
        /// </summary>
        /// <param name="writer">The binary writer to serialize to.</param>
        public override void SerializeObject(BinaryWriter writer)
        {
            if (rootNode == null) return;

            writer.Write(rootNode.ToJsonString());
        }

        /// <summary>
        /// Disposes the <see cref="GarnetJsonObject"/> instance.
        /// </summary>
        public override void Dispose() { }

        /// <inheritdoc/>
        public override unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10,
            byte* pattern = default, int patternLength = 0, bool isNoValue = false) =>
            throw new NotImplementedException();

        /// <summary>
        /// Tries to get the JSON values for the specified paths and writes them to the output stream.
        /// </summary>
        /// <param name="paths">The JSON paths to get the values for.</param>
        /// <param name="output">The output stream to write the values to.</param>
        /// <param name="errorMessage">The error message if the operation fails.</param>
        /// <param name="indent">The string to use for indentation.</param>
        /// <param name="newLine">The string to use for new lines.</param>
        /// <param name="space">The string to use for spaces.</param>
        /// <returns>True if the operation is successful; otherwise, false.</returns>
        public bool TryGet(ReadOnlySpan<ArgSlice> paths, Stream output, out ReadOnlySpan<byte> errorMessage, string? indent = null, string? newLine = null, string? space = null)
        {
            if (paths.Length == 1)
            {
                return TryGet(paths[0].ReadOnlySpan, output, out errorMessage, indent, newLine, space);
            }

            output.WriteByte((byte)'{');
            var isFirst = true;
            foreach (var item in paths)
            {
                if (!isFirst)
                {
                    output.WriteByte((byte)',');
                }
                isFirst = false;

                output.WriteByte((byte)'"');
                output.Write(item.ReadOnlySpan);
                output.WriteByte((byte)'"');
                output.WriteByte((byte)':');

                if (!TryGet(item.ReadOnlySpan, output, out errorMessage, indent, newLine, space))
                {
                    return false;
                }
            }
            output.WriteByte((byte)'}');

            errorMessage = default;
            return true;
        }

        /// <summary>
        /// Tries to get the JSON value for the specified path and writes it to the output stream.
        /// </summary>
        /// <param name="path">The JSON path to get the value for.</param>
        /// <param name="output">The output stream to write the value to.</param>
        /// <param name="errorMessage">The error message if the operation fails.</param>
        /// <param name="indent">The string to use for indentation.</param>
        /// <param name="newLine">The string to use for new lines.</param>
        /// <param name="space">The string to use for spaces.</param>
        /// <returns>True if the operation is successful; otherwise, false.</returns>
        public bool TryGet(ReadOnlySpan<byte> path, Stream output, out ReadOnlySpan<byte> errorMessage, string? indent = null, string? newLine = null, string? space = null)
        {
            try
            {
                if (rootNode is null)
                {
                    errorMessage = default;
                    return true;
                }

                if (path.Length == 0)
                {
                    // System.Text.Json doesn't support customizing indentation, new line, and space github/runtime#111899, so for now if any of these are set, we will use the default indented serializer options
                    JsonSerializer.Serialize(output, rootNode, indent is null && newLine is null && space is null ? DefaultJsonSerializerOptions : IndentedJsonSerializerOptions);
                    errorMessage = default;
                    return true;
                }

                var pathStr = Encoding.UTF8.GetString(path);
                var result = rootNode.SelectNodes(pathStr);

                output.WriteByte((byte)'[');
                var isFirst = true;
                foreach (var item in result)
                {
                    if (!isFirst)
                    {
                        output.WriteByte((byte)',');
                    }
                    isFirst = false;

                    // System.Text.Json doesn't support customizing indentation, new line, and space github/runtime#111899, so for now if any of these are set, we will use the default indented serializer options
                    JsonSerializer.Serialize(output, item, indent is null && newLine is null && space is null ? DefaultJsonSerializerOptions : IndentedJsonSerializerOptions);
                }
                output.WriteByte((byte)']');
                errorMessage = default;
                return true;
            }
            catch (JsonException ex)
            {
                errorMessage = Encoding.UTF8.GetBytes(ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Sets the value at the specified JSON path.
        /// </summary>
        /// <param name="path">The JSON path.</param>
        /// <param name="value">The value to set.</param>
        /// <param name="existOptions">The options for existence checks.</param>
        /// <param name="errorMessage">The error message if the operation fails.</param>
        /// <returns>The result of the set operation.</returns>
        /// <exception cref="JsonException">Thrown when there is an error in JSON processing.</exception>
        public SetResult Set(ReadOnlySpan<byte> path, ReadOnlySpan<byte> value, ExistOptions existOptions, out ReadOnlySpan<byte> errorMessage)
        {
            try
            {
                var pathStr = Encoding.UTF8.GetString(path);

                if (pathStr.Length == 1 && pathStr[0] == '$')
                {
                    rootNode = JsonNode.Parse(value);
                    errorMessage = default;
                    return SetResult.Success;
                }

                if (rootNode is null)
                {
                    errorMessage = JsonCmdStrings.RESP_NEW_OBJECT_AT_ROOT;
                    return SetResult.Error;
                }

                // Need ToArray to avoid modifying collection while iterating
                JsonPath jsonPath = new JsonPath(pathStr);
                var result = jsonPath.Evaluate(rootNode, rootNode, null).ToArray();

                if (!result.Any())
                {
                    if (existOptions == ExistOptions.XX)
                    {
                        errorMessage = default;
                        return SetResult.ConditionNotMet;
                    }

                    if (!jsonPath.IsStaticPath())
                    {
                        errorMessage = JsonCmdStrings.RESP_WRONG_STATIC_PATH;
                        return SetResult.Error;
                    }

                    // Find parent node using parent path
                    var parentNode = rootNode.SelectNodes(GetParentPath(pathStr, out var pathParentOffset)).FirstOrDefault();
                    if (result is null)
                    {
                        errorMessage = default;
                        return SetResult.ConditionNotMet;
                    }

                    var childNode = JsonNode.Parse(value);
                    var itemPropName = GetPropertyName(pathStr, pathParentOffset);

                    if (parentNode is JsonObject matchObject)
                    {
                        matchObject.Add(itemPropName.ToString(), childNode);
                    }
                    else if (parentNode is JsonArray matchArray && int.TryParse(itemPropName, out var index))
                    {
                        matchArray.Insert(index, childNode);
                    }
                    else
                    {
                        errorMessage = default;
                        return SetResult.ConditionNotMet;
                    }

                    errorMessage = default;
                    return SetResult.Success;
                }

                if (existOptions == ExistOptions.NX)
                {
                    errorMessage = default;
                    return SetResult.ConditionNotMet;
                }

                foreach (var match in result.ToList())
                {
                    var valNode = JsonNode.Parse(value);

                    if (rootNode == match)
                    {
                        rootNode = valNode;
                        break;
                    }

                    // Known issue: When the value to be replaced is null, replace won't work as there is no NullJsonValue, instead .net returns null
                    match?.ReplaceWith(valNode);
                }

                errorMessage = default;
                return SetResult.Success;
            }
            catch (JsonException ex)
            {
                errorMessage = Encoding.UTF8.GetBytes(ex.Message);
                return SetResult.Error;
            }
        }

        private static string GetParentPath(string path, out int pathOffset)
        {
            var pathSpan = path.AsSpan();
            // Removed the last character from the path to remove the trailing ']' or '.', it shouldn't affect the result even if it doesn't have
            pathOffset = pathSpan[..^1].LastIndexOfAny('.', ']');

            if (pathOffset == -1)
            {
                return "$";
            }

            if (pathSpan[pathOffset] == ']')
            {
                pathOffset++;
            }

            return path.Substring(0, pathOffset);
        }

        private static ReadOnlySpan<char> GetPropertyName(string path, int pathOffset)
        {
            var pathSpan = path.AsSpan();
            if (pathSpan[pathOffset] is '.')
            {
                pathOffset++;
            }

            var propertSpan = pathSpan[pathOffset..];
            if (propertSpan[0] is '[')
            {
                propertSpan = propertSpan[1..^1];
            }

            if (propertSpan[0] is '"' or '\'')
            {
                propertSpan = propertSpan[1..^1];
            }

            return propertSpan;
        }
    }
}