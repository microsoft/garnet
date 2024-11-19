// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Garnet.server;
using Json.Path;
using Microsoft.Extensions.Logging;

namespace GarnetJSON
{
    /// <summary>
    /// Represents a factory for creating instances of <see cref="JsonObject"/>.
    /// </summary>
    public class JsonObjectFactory : CustomObjectFactory
    {
        /// <summary>
        /// Creates a new instance of <see cref="JsonObject"/> with the specified type.
        /// </summary>
        /// <param name="type">The type of the object.</param>
        /// <returns>A new instance of <see cref="JsonObject"/>.</returns>
        public override CustomObjectBase Create(byte type)
            => new JsonObject(type);

        /// <summary>
        /// Deserializes a <see cref="JsonObject"/> from the specified binary reader.
        /// </summary>
        /// <param name="type">The type of the object.</param>
        /// <param name="reader">The binary reader to deserialize from.</param>
        /// <returns>A deserialized instance of <see cref="JsonObject"/>.</returns>
        public override CustomObjectBase Deserialize(byte type, BinaryReader reader)
            => new JsonObject(type, reader);
    }

    /// <summary>
    /// Represents a JSON object that supports SET and GET operations using JSON path.
    /// </summary>
    public class JsonObject : CustomObjectBase
    {
        private const string JsonPathPattern = @"(\.[^.\[]+)|(\['[^']+'\])|(\[\d+\])";

        private JsonNode? jNode;

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonObject"/> class with the specified type.
        /// </summary>
        /// <param name="type">The type of the object.</param>
        public JsonObject(byte type)
            : base(type, 0, MemoryUtils.DictionaryOverhead)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonObject"/> class by deserializing from the specified binary reader.
        /// </summary>
        /// <param name="type">The type of the object.</param>
        /// <param name="reader">The binary reader to deserialize from.</param>
        public JsonObject(byte type, BinaryReader reader)
            : base(type, reader)
        {
            Debug.Assert(reader != null);

            var jsonString = reader.ReadString();
            jNode = JsonNode.Parse(jsonString);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonObject"/> class by cloning another <see cref="JsonObject"/> instance.
        /// </summary>
        /// <param name="obj">The <see cref="JsonObject"/> instance to clone.</param>
        public JsonObject(JsonObject obj)
            : base(obj)
        {
            jNode = obj.jNode;
        }

        /// <summary>
        /// Creates a new instance of <see cref="JsonObject"/> that is a clone of the current instance.
        /// </summary>
        /// <returns>A new instance of <see cref="JsonObject"/> that is a clone of the current instance.</returns>
        public override CustomObjectBase CloneObject() => new JsonObject(this);

        /// <summary>
        /// Serializes the <see cref="JsonObject"/> to the specified binary writer.
        /// </summary>
        /// <param name="writer">The binary writer to serialize to.</param>
        public override void SerializeObject(BinaryWriter writer)
        {
            if (jNode == null) return;

            writer.Write(jNode.ToJsonString());
        }

        /// <summary>
        /// Disposes the <see cref="JsonObject"/> instance.
        /// </summary>
        public override void Dispose() { }

        /// <summary>
        /// Tries to get the value at the specified JSON path.
        /// </summary>
        /// <param name="path">The JSON path.</param>
        /// <param name="jsonString">The JSON string value at the specified path, or <c>null</c> if the value is not found.</param>
        /// <param name="logger">The logger to log any errors.</param>
        /// <returns><c>true</c> if the value was successfully retrieved; otherwise, <c>false</c>.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="path"/> is <c>null</c>.</exception>
        public bool TryGet(string path, out string jsonString, ILogger? logger = null)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));

            try
            {
                // Find all items matching JSON path
                var jPath = JsonPath.Parse(path);
                var result = jPath.Evaluate(jNode);

                // Return matches in JSON array format
                jsonString = $"[{string.Join(",", result.Matches.Select(m => m.Value!.ToJsonString()))}]";

                return true;
            }
            catch (JsonException ex)
            {
                logger?.LogError(ex, "Failed to get JSON value");
                jsonString = string.Empty;
                return false;
            }
        }

        /// <summary>
        /// Tries to set the value at the specified JSON path.
        /// </summary>
        /// <param name="path">The JSON path.</param>
        /// <param name="value">The value to set.</param>
        /// <param name="logger">The logger to log any errors.</param>
        /// <returns><c>true</c> if the value was successfully set; otherwise, <c>false</c>.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="path"/> or <paramref name="value"/> is <c>null</c>.</exception>
        public bool TrySet(string path, string value, ILogger? logger = null)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            try
            {
                Set(path, value);
                return true;
            }
            catch (JsonException ex)
            {
                logger?.LogError(ex, "Failed to set JSON value");
                return false;
            }
        }

        private void Set(string path, string value)
        {
            // Find all items matching JSON path
            var jPath = JsonPath.Parse(path);
            var result = jPath.Evaluate(jNode);

            // No matched items
            if (result.Matches.Count == 0)
            {
                // Find parent node path
                var parentPath = JsonPath.Parse(GetParentPathExt(path));
                result = parentPath.Evaluate(jNode);
                if (result.Matches.Count == 0)
                    throw new JsonException("Unable to find parent node(s) for JSON path.");

                // Get parent node from path & parse child node from input value
                var parentNode = result.Matches[0].Value;
                var childNode = JsonNode.Parse(value);

                // Check if parent node is a JsonObject
                if (parentNode is System.Text.Json.Nodes.JsonObject matchObject)
                {
                    // Get key name from JSON path
                    var propName = GetPropertyName(path);

                    // Add key & child node to the parent node
                    matchObject.Add(propName, childNode);
                }
                // Check if parent node is a JsonArray
                else if (parentNode is JsonArray matchArray)
                {
                    // Get child index in parent array
                    var index = GetArrayIndex(path);

                    // Add child node to parent array
                    matchArray.Insert(index, childNode);
                }
            }
            // Matches found
            else
            {
                foreach (var match in result.Matches)
                {
                    // Parse node from input value
                    var valNode = JsonNode.Parse(value);

                    // If matched node is root
                    if (match.Value == null && match.Location?.ToString() == "$")
                    {
                        // Set root node to parsed value node
                        jNode = valNode;
                        continue;
                    }

                    // Replace matched value with input value
                    if (match.Value is JsonValue matchValue)
                    {
                        matchValue.ReplaceWith(valNode);
                    }
                }
            }
        }

        static string GetParentPathExt(string jsonPath)
        {
            var matches = Regex.Matches(jsonPath, JsonPathPattern);

            if (matches.Count == 0) return "$";

            return jsonPath.Substring(0, matches[^1].Index);
        }

        private string GetPropertyName(string path)
        {
            var lastDotIndex = path.LastIndexOf('.');
            return lastDotIndex >= 0 ? path.Substring(lastDotIndex + 1) : path;
        }

        private int GetArrayIndex(string path)
        {
            var startIndex = path.LastIndexOf('[');
            var endIndex = path.LastIndexOf(']');
            if (startIndex >= 0 && endIndex >= 0 && endIndex > startIndex)
            {
                var indexString = path.Substring(startIndex + 1, endIndex - startIndex - 1);
                if (int.TryParse(indexString, out var index))
                {
                    return index;
                }
            }

            throw new ArgumentException("Invalid array index in path");
        }

        /// <inheritdoc/>
        public override unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = null, int patternLength = 0, bool isNoValue = false) => throw new NotImplementedException();
    }
}